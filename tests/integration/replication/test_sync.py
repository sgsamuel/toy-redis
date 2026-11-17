import unittest

from tests.support import following, make_executor, running_server, wait_until


class ReplicationTests(unittest.IsolatedAsyncioTestCase):
    async def test_replication_restores_mixed_snapshot_and_wait_offsets(self):
        executor = make_executor()
        executor.store.set('text', 'before', expiry_ms=60000)
        executor.store.rpush('list', 'a', 'b')
        executor.store.sadd('members', 'a', 'b')
        executor.store.expire('members', 60000)
        executor.store.zadd('scores', (1.0, 'member'))
        executor.store.xadd('stream', '1-0', [('f', 'value')])
        executor.store.setbit('bits', 0, 1)
        async with running_server(executor) as primary:
            writer = await primary.connect()
            await writer.command('SET', 'before', 'snapshot')
            async with following(primary) as replica:
                self.assertEqual(replica.store.get('text'), b'before')
                self.assertEqual(replica.store.lrange('list', 0, -1), ['a', 'b'])
                self.assertEqual(replica.store.zscore('scores', 'member'), 1.0)
                self.assertCountEqual(replica.store.smembers('members'), ['a', 'b'])
                self.assertAlmostEqual(replica.store._store['members'].expires_at,
                                       executor.store._store['members'].expires_at, delta=0.02)
                self.assertEqual(replica.store.xrange('stream', '-', '+'), executor.store.xrange('stream', '-', '+'))
                self.assertEqual(replica.store.get('bits'), b'\x80')
                self.assertAlmostEqual(replica.store._store['text'].expires_at, executor.store._store['text'].expires_at, delta=0.02)
                await writer.command('SADD', 'members', 'c')
                await writer.command('SREM', 'members', 'a', 'b')
                await writer.command('ZADD', 'scores', '0', 'member', '2', 'second')
                await writer.command('SET', 'after', 'new')
                await wait_until(lambda: replica.store.get('after') == 'new')
                self.assertEqual(replica.store.smembers('members'), ['c'])
                self.assertEqual(replica.store.zrange('scores', 0, -1, with_scores=True),
                                 [('member', 0.0), ('second', 2.0)])
                self.assertEqual((await writer.command('WAIT', '1', '1000')).value, 1)
                await wait_until(lambda: replica.replication.offset == executor.replication.offset)
                async with running_server(replica) as replica_server:
                    client = await replica_server.connect()
                    self.assertTrue((await client.command('SET', 'local', 'bad')).raw.startswith(b'-READONLY'))
                    for command in [('SADD', 'members', 'new'), ('SREM', 'members', 'c')]:
                        self.assertTrue((await client.command(*command)).raw.startswith(b'-READONLY'))
                    self.assertEqual((await client.command('SISMEMBER', 'members', 'c')).value, 1)
                    self.assertEqual((await client.command('SUNION', 'members', 'missing')).value, ['c'])
                    self.assertEqual((await client.command('SINTER', 'members', 'missing')).value, [])
                await writer.command('SREM', 'members', 'c')
                await wait_until(lambda: replica.store.type('members') is None)

    async def test_hash_snapshot_mutations_and_replica_reads(self):
        executor = make_executor()
        executor.store.hset('hash', ('a', 'before'), ('b', 'remove'), ('\udcff', '\x00'))
        executor.store.expire('hash', 60000)
        async with running_server(executor) as primary:
            writer = await primary.connect()
            async with following(primary) as replica:
                self.assertEqual(replica.store.hgetall('hash'), executor.store.hgetall('hash'))
                self.assertAlmostEqual(replica.store._store['hash'].expires_at,
                                       executor.store._store['hash'].expires_at, delta=0.02)
                self.assertEqual((await writer.command('HSET', 'hash', 'a', 'after')).value, 0)
                await writer.command('HDEL', 'hash', 'b')
                self.assertEqual((await writer.command('WAIT', '1', '1000')).value, 1)
                self.assertEqual(replica.store.hgetall('hash'), {'a': 'after', '\udcff': '\x00'})
                async with running_server(replica) as secondary:
                    client = await secondary.connect()
                    self.assertEqual((await client.command('HGET', 'hash', 'a')).value, 'after')
                    for tokens in [('HSET', 'hash', 'a', 'forbidden'), ('HDEL', 'hash', 'a')]:
                        self.assertTrue((await client.command(*tokens)).raw.startswith(b'-READONLY'))
                await writer.command('HDEL', 'hash', 'a', '\udcff')
                await wait_until(lambda: replica.store.type('hash') is None)
