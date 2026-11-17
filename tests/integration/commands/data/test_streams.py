import unittest

from tests.support import running_server, wait_until


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_removes_blocking_stream_waiters(self):
        async with running_server() as server:
            client = await server.connect()
            await client.send('XREAD', 'BLOCK', '0', 'STREAMS', 'stream', '$')
            await wait_until(lambda: bool(server.executor.store._stream_waiters))
            await client.close()
            await wait_until(lambda: not server.executor.store._stream_waiters)
