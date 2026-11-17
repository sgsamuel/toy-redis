import asyncio

from tests.support import CommandTestCase, wait_until


class ListCommandTests(CommandTestCase):
    async def test_list_order_ranges_and_pop_counts(self):
        self.assertEqual((await self.command('RPUSH', 'list', 'a', 'b')).value, 2)
        self.assertEqual((await self.command('LPUSH', 'list', 'x', 'y')).value, 4)
        self.assertEqual((await self.command('LRANGE', 'list', '0', '-1')).value, ['y', 'x', 'a', 'b'])
        self.assertEqual((await self.command('LRANGE', 'list', '-2', '-1')).value, ['a', 'b'])
        self.assertEqual((await self.command('LPOP', 'list', '0')).value, [])
        self.assertEqual((await self.command('LPOP', 'list')).value, 'y')
        self.assertEqual((await self.command('LPOP', 'list', '10')).value, ['x', 'a', 'b'])
        self.assertIsNone(self.store.type('list'))

    async def test_blocking_list_delivery_timeout_and_cancellation(self):
        pending = asyncio.create_task(self.store.blpop('list', 0))
        await wait_until(lambda: bool(self.store._list_waiters))
        self.store.rpush('list', 'value')
        self.assertEqual(await pending, 'value')
        self.assertIsNone(await self.store.blpop('empty', 0.01))
        pending = asyncio.create_task(self.store.blpop('list', 0))
        await wait_until(lambda: bool(self.store._list_waiters))
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        self.assertFalse(self.store._list_waiters)
