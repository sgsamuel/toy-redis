import asyncio

from tests.support import CommandTestCase, wait_until


class StreamCommandTests(CommandTestCase):
    async def test_stream_ids_ranges_and_multiple_reads(self):
        self.assertEqual((await self.command('XADD', 'stream', '1-0', 'f', 'one')).value, '1-0')
        self.assertEqual((await self.command('XADD', 'stream', '1-*', 'f', 'two')).value, '1-1')
        self.assertTrue((await self.command('XADD', 'stream', '1-0', 'f', 'old')).is_error)
        self.assertEqual((await self.command('XRANGE', 'stream', '-', '+')).value,
                         [['1-0', ['f', 'one']], ['1-1', ['f', 'two']]])
        await self.command('XADD', 'other', '2-0', 'f', 'three')
        self.assertEqual((await self.command('XREAD', 'STREAMS', 'stream', 'other', '1-0', '0-0')).value,
                         [['stream', [['1-1', ['f', 'two']]]], ['other', [['2-0', ['f', 'three']]]]])

    async def test_blocking_stream_waiters(self):
        pending = asyncio.create_task(self.store.xread_blocking([('stream', '$')], 0))
        await wait_until(lambda: bool(self.store._stream_waiters))
        self.store.xadd('stream', '1-0', [('f', 'value')])
        result = await pending
        self.assertEqual(result[0][1][0].entry_id, (1, 0))
        self.assertFalse(self.store._stream_waiters)
        self.assertEqual(await self.store.xread_blocking([('missing', '0-0')], 10), [])
