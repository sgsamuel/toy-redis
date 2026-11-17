from pathlib import Path
from unittest.mock import AsyncMock, patch
import tempfile
import time
import unittest

from app.commands.common import ClientSession
from app.commands.executor import CommandExecutor
from app.data.store import RedisStore
from app.main import main
from app.network.auth import ACLState
from app.network.config import ServerConfig
from app.network.pubsub import PubSubState
from app.persistence.aof import AOFManager
from app.protocol import encode_command
from app.replication.primary import ReplicationState


class AOFReplayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = ServerConfig(dir=self.temp.name, appendonly='yes')
        self.manager = AOFManager(self.config)

    def executor(self, *, replica=False):
        return CommandExecutor(RedisStore(), ReplicationState(replica_of=('localhost', 6379) if replica else None),
                               self.config, ACLState(), PubSubState(), self.manager)

    def test_constructor_initializes_and_preserves(self):
        self.assertEqual(self.manager.file.read_bytes(), b'')
        self.assertEqual(self.manager.manifest.read_text(), 'file appendonly.aof.1.incr.aof seq 1 type i\n')
        self.manager.write(['SET', 'key', 'value'])
        before = self.manager.file.read_bytes()
        replacement = AOFManager(self.config)
        self.assertEqual(replacement.file.read_bytes(), before)
        self.assertFalse(hasattr(replacement, 'initialize'))

    async def test_disabled_does_not_touch_files(self):
        config = ServerConfig(dir=str(Path(self.temp.name) / 'disabled'))
        manager = AOFManager(config)
        executor = self.executor()
        self.assertEqual(await manager.replay(executor.execute), 0)
        self.assertFalse(Path(config.dir).exists())

    async def test_empty_replay(self):
        executor = self.executor()
        self.assertEqual(await self.manager.replay(executor.execute), 0)
        self.assertEqual(executor.store.keys('*'), [])

    async def test_multiple_commands_and_types_round_trip(self):
        executor = self.executor()
        commands = [
            ['SET', 'number', '1'], ['INCR', 'number'],
            ['RPUSH', 'list', 'a', 'b'], ['LPOP', 'list'],
            ['HSET', 'hash', 'a', '1', 'b', '2', '\udcff', '\x00'],
            ['HSET', 'hash', 'a', 'updated'], ['HDEL', 'hash', 'b'],
            ['SADD', 'members', 'a', 'b', '\udcff\x00'], ['SREM', 'members', 'a'],
            ['ZADD', 'scores', 'NX', '1', 'a', '2', 'b'],
            ['ZADD', 'scores', 'XX', '3', 'a', '4', 'missing'], ['ZREM', 'scores', 'a'],
            ['XADD', 'stream', '*', 'field', 'value'], ['SETBIT', 'bits', '0', '1'],
            ['BITOP', 'OR', 'copy', 'bits'], ['GEOADD', 'places', 'NX', '0', '0', 'origin'],
            ['GEOADD', 'places', 'XX', '1', '1', 'origin', '2', '2', 'missing'],
        ]
        session = ClientSession()
        for tokens in commands:
            self.assertFalse((await executor.execute(tokens, session)).is_error)
        before = self.manager.file.read_bytes()
        restored = self.executor()
        self.assertEqual(await self.manager.replay(restored.execute), len(commands))
        self.assertEqual(restored.store.snapshot(), executor.store.snapshot())
        self.assertEqual(self.manager.file.read_bytes(), before)
        self.assertEqual(restored.replication.offset, 0)
        await restored.execute(['INCR', 'number'], ClientSession())
        self.assertEqual(restored.store.get('number'), '3')
        self.assertEqual(self.manager.file.read_bytes(), before + encode_command(['INCR', 'number']))

    async def test_conditional_sorted_set_noops_are_not_logged(self):
        executor = self.executor()
        session = ClientSession()
        for command in [['ZADD', 'scores', 'XX', '1', 'a'],
                        ['GEOADD', 'places', 'XX', '0', '0', 'a']]:
            self.assertEqual((await executor.execute(command, session)).value, 0)
        self.assertEqual(self.manager.file.read_bytes(), b'')
        await executor.execute(['ZADD', 'scores', 'NX', '1', 'a'], session)
        before = self.manager.file.read_bytes()
        await executor.execute(['ZADD', 'scores', 'NX', '2', 'a'], session)
        self.assertEqual(self.manager.file.read_bytes(), before)

    async def test_binary_safe_replay(self):
        payload = bytes(range(256))
        executor = self.executor()
        await executor.execute([b'SET', b'key\xff', payload], ClientSession())
        restored = self.executor()
        await self.manager.replay(restored.execute)
        reply = await restored.execute([b'GET', b'key\xff'], ClientSession())
        self.assertEqual(reply.raw, b'$256\r\n' + payload + b'\r\n')

    async def test_ttl_becomes_absolute_and_does_not_restart(self):
        with patch('app.persistence.aof.time.time', return_value=1000):
            self.manager.write(['SET', 'expired', 'value', 'PX', '50'])
        self.assertEqual(self.manager.file.read_bytes(), encode_command(['SET', 'expired', 'value', 'PXAT', '1000050']))
        self.manager.write(['SET', 'future', 'value', 'PXAT', str(int(time.time() * 1000) + 60000)])
        restored = self.executor()
        await self.manager.replay(restored.execute)
        self.assertIsNone(restored.store.get('expired'))
        self.assertEqual(restored.store.get('future'), 'value')
        self.assertLess(restored.store._store['future'].expires_at - time.monotonic(), 61)
        self.assertGreater(restored.store._store['future'].expires_at - time.monotonic(), 58)

    async def test_replica_replay_does_not_propagate_or_append(self):
        self.manager.write(['SET', 'key', 'value'])
        before = self.manager.file.read_bytes()
        executor = self.executor(replica=True)
        await self.manager.replay(executor.execute)
        self.assertEqual(executor.store.get('key'), 'value')
        self.assertEqual(executor.replication.offset, 0)
        self.assertEqual(self.manager.file.read_bytes(), before)
        self.assertTrue((await executor.execute(['SET', 'key', 'external'], ClientSession())).is_error)

    async def test_manifest_order_and_active_file(self):
        first = self.manager.file
        second = first.parent / 'appendonly.aof.2.incr.aof'
        first.write_bytes(encode_command(['SET', 'number', '1']))
        second.write_bytes(encode_command(['INCR', 'number']))
        self.manager.manifest.write_text(
            f'file {first.name} seq 1 type i\nfile {second.name} seq 2 type i\n')
        restored = self.executor()
        self.assertEqual(await self.manager.replay(restored.execute), 2)
        self.assertEqual(restored.store.get('number'), '2')
        self.assertEqual(self.manager.file, second)
        await restored.execute(['INCR', 'number'], ClientSession())
        self.assertEqual(first.read_bytes(), encode_command(['SET', 'number', '1']))
        self.assertEqual(second.read_bytes(), encode_command(['INCR', 'number']) * 2)

    async def test_invalid_and_truncated_records_fail(self):
        for data in [b'?', b'*2\r\n$3\r\nSET\r\n', b'+PING\r\n',
                     encode_command(['BAD']), encode_command(['SET', 'key'])]:
            self.manager.file.write_bytes(data)
            with self.assertRaises(ValueError):
                await self.manager.replay(self.executor().execute)
            self.assertEqual(self.manager.file.read_bytes(), data)

    async def test_invalid_manifest_or_missing_file_fails(self):
        for manifest in ['', 'bad manifest\n', 'file ../escape seq 1 type i\n',
                         'file missing.aof seq 1 type i\n',
                         f'file {self.manager.file.name} seq 1 type b\n']:
            self.manager.manifest.write_text(manifest)
            with self.assertRaises((ValueError, FileNotFoundError)):
                await self.manager.replay(self.executor().execute)

    async def test_transactions_and_select_zero(self):
        commands = [['SELECT', '0'], ['MULTI'], ['SET', 'key', 'value'], ['EXEC']]
        self.manager.file.write_bytes(b''.join(encode_command(command) for command in commands))
        executor = self.executor()
        self.assertEqual(await self.manager.replay(executor.execute), 4)
        self.assertEqual(executor.store.get('key'), 'value')
        self.manager.file.write_bytes(encode_command(['MULTI']) + encode_command(['SET', 'key', 'value']))
        with self.assertRaises(ValueError):
            await self.manager.replay(self.executor().execute)
        self.manager.file.write_bytes(encode_command(['SELECT', '1']))
        with self.assertRaises(ValueError):
            await self.manager.replay(self.executor().execute)

    async def test_startup_uses_aof_instead_of_rdb(self):
        stale = RedisStore()
        stale.set('from-rdb', 'stale')
        (Path(self.temp.name) / 'dump.rdb').write_bytes(stale.snapshot())
        self.manager.write(['SET', 'number', '1'])
        self.manager.write(['INCR', 'number'])
        before = self.manager.file.read_bytes()
        serve = AsyncMock()
        with patch('sys.argv', ['redis', '--dir', self.temp.name, '--appendonly', 'yes']), patch('app.main.serve', serve):
            await main()
        executor = serve.call_args.args[0]
        self.assertEqual(executor.store.get('number'), '2')
        self.assertIsNone(executor.store.get('from-rdb'))
        self.assertEqual(executor.replication.offset, 0)
        self.assertEqual(self.manager.file.read_bytes(), before)

    async def test_replay_many_records(self):
        self.manager.file.write_bytes(encode_command(['INCR', 'number']) * 100)
        executor = self.executor()
        before = self.manager.file.stat().st_size
        self.assertEqual(await self.manager.replay(executor.execute), 100)
        self.assertEqual(executor.store.get('number'), '100')
        self.assertEqual(self.manager.file.stat().st_size, before)

    async def test_set_noops_and_reads_are_not_logged(self):
        executor = self.executor()
        session = ClientSession()
        await executor.execute(['SADD', 'members', 'a'], session)
        before = self.manager.file.read_bytes()
        for tokens in [['SADD', 'members', 'a', 'a'], ['SREM', 'members', 'missing'],
                       ['SREM', 'missing', 'a'], ['SCARD', 'members'],
                       ['SISMEMBER', 'members', 'a'], ['SMEMBERS', 'members'],
                       ['SINTER', 'members', 'missing'], ['SUNION', 'members', 'missing'],
                       ['SADD', 'members']]:
            await executor.execute(tokens, session)
        self.assertEqual(self.manager.file.read_bytes(), before)
        await executor.execute(['SREM', 'members', 'a'], session)
        restored = self.executor()
        self.assertEqual(await self.manager.replay(restored.execute), 2)
        self.assertIsNone(restored.store.type('members'))

    async def test_hash_noops_reads_and_deletion_replay(self):
        executor = self.executor()
        session = ClientSession()
        await executor.execute(['HSET', 'hash', 'a', '1'], session)
        before = self.manager.file.read_bytes()
        for tokens in [['HSET', 'hash', 'a', '1'], ['HDEL', 'hash', 'missing'],
                       ['HGET', 'hash', 'a'], ['HGETALL', 'hash'], ['HEXISTS', 'hash', 'a'],
                       ['HLEN', 'hash'], ['HSET', 'hash', 'a', 'bad', 'unpaired']]:
            await executor.execute(tokens, session)
        self.assertEqual(self.manager.file.read_bytes(), before)
        await executor.execute(['HSET', 'hash', 'a', 'updated'], session)
        restored = self.executor()
        self.assertEqual(await self.manager.replay(restored.execute), 2)
        self.assertEqual(restored.store.hget('hash', 'a'), 'updated')
        await executor.execute(['HDEL', 'hash', 'a'], session)
        restored = self.executor()
        self.assertEqual(await self.manager.replay(restored.execute), 3)
        self.assertIsNone(restored.store.type('hash'))
