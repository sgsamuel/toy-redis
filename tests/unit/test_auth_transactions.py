import asyncio
import unittest
from hashlib import sha256

from app.commands.common import ClientSession
from app.replication.primary import ReplicationState
from tests.support import make_executor


class AuthTransactionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.executor = make_executor()
        self.session = ClientSession()

    async def command(self, *tokens):
        return await self.executor.execute(list(tokens), self.session)

    async def test_default_identity_and_acl_reply(self):
        self.assertEqual((await self.command('ACL', 'WHOAMI')).value, 'default')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'default')).value,
                         ['flags', ['nopass'], 'passwords', []])
        self.assertIsNone((await self.command('ACL', 'GETUSER', 'missing')).value)

    async def test_password_hashes_and_authentication(self):
        await self.command('ACL', 'SETUSER', 'default', '>secret', '>second', '>secret')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'default')).value,
                         ['flags', [], 'passwords', [sha256(b'secret').hexdigest(), sha256(b'second').hexdigest()]])
        other = ClientSession()
        self.assertTrue((await self.executor.execute(['GET', 'key'], other)).raw.startswith(b'-NOAUTH'))
        self.assertTrue((await self.executor.execute(['AUTH', 'wrong'], other)).raw.startswith(b'-WRONGPASS'))
        self.assertEqual((await self.executor.execute(['AUTH', 'secret'], other)).value, 'OK')
        self.assertEqual((await self.executor.execute(['ACL', 'WHOAMI'], other)).value, 'default')

    async def test_named_users_and_nopass_reset(self):
        await self.command('ACL', 'SETUSER', 'alice', '>secret')
        other = ClientSession()
        self.assertEqual((await self.executor.execute(['AUTH', 'alice', 'secret'], other)).value, 'OK')
        self.assertEqual(other.username, 'alice')
        self.assertEqual((await self.command('ACL', 'WHOAMI')).value, 'default')
        await self.command('ACL', 'SETUSER', 'alice', 'nopass')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'alice')).value,
                         ['flags', ['nopass'], 'passwords', []])

    async def test_transaction_queue_exec_and_discard(self):
        await self.command('MULTI')
        self.assertEqual((await self.command('SET', 'key', 'value')).value, 'QUEUED')
        self.assertIsNone(self.executor.store.get('key'))
        self.assertEqual((await self.command('EXEC')).value, ['OK'])
        await self.command('MULTI')
        await self.command('SET', 'key', 'discarded')
        await self.command('DISCARD')
        self.assertEqual(self.executor.store.get('key'), 'value')

    async def test_queue_errors_abort_the_whole_transaction(self):
        for invalid in [['BOGUS'], ['SET', 'key'], ['ACL', 'BAD'], ['GEOADD', 'key', '0', '0']]:
            await self.command('MULTI')
            self.assertTrue((await self.executor.execute(invalid, self.session)).is_error)
            await self.command('SET', 'committed', 'yes')
            self.assertTrue((await self.command('EXEC')).raw.startswith(b'-EXECABORT'))
            self.assertIsNone(self.executor.store.get('committed'))

    async def test_runtime_errors_do_not_cancel_other_writes(self):
        self.executor.store.rpush('list', 'value')
        await self.command('MULTI')
        await self.command('INCR', 'list')
        await self.command('SET', 'key', 'value')
        reply = await self.command('EXEC')
        self.assertIn(b'WRONGTYPE', reply.raw)
        self.assertEqual(self.executor.store.get('key'), 'value')

    async def test_watch_changes_abort_exec(self):
        await self.command('WATCH', 'key')
        await self.command('MULTI')
        await self.command('SET', 'result', 'wrong')
        await self.executor.execute(['SET', 'key', 'changed'], ClientSession())
        self.assertEqual((await self.command('EXEC')).raw, b'*-1\r\n')
        self.assertIsNone(self.executor.store.get('result'))
        self.assertFalse(self.session.watched)

    async def test_blocking_operations_are_immediate_inside_exec(self):
        await self.command('MULTI')
        await self.command('BLPOP', 'empty', '0')
        await self.command('XREAD', 'BLOCK', '0', 'STREAMS', 'stream', '$')
        await self.command('SET', 'after', 'yes')
        result = await asyncio.wait_for(self.command('EXEC'), 1)
        self.assertEqual(result.value, [None, None, 'OK'])
        self.assertFalse(self.executor.store._list_waiters)
        self.assertFalse(self.executor.store._stream_waiters)

    async def test_readonly_and_internal_replication(self):
        replica = make_executor(replication=ReplicationState(replica_of=('localhost', 6379)))
        self.assertTrue((await replica.execute(['SET', 'key', 'external'], ClientSession())).raw.startswith(b'-READONLY'))
        self.assertEqual((await replica.apply_replication(['SET', 'key', 'internal'], ClientSession())).value, 'OK')
        self.assertEqual(replica.store.get('key'), 'internal')
        self.assertEqual(replica.replication.offset, 0)

    async def test_invalid_acl_rule_is_atomic(self):
        self.assertTrue((await self.command('ACL', 'SETUSER', 'default', '>secret', 'bad')).is_error)
        self.assertEqual(self.executor.acl.users['default'].passwords, [])
