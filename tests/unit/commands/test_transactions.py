import asyncio

from app.commands.common import ClientSession

from tests.support import CommandTestCase


class TransactionTests(CommandTestCase):
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
