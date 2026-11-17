import unittest

from tests.support import running_server, wait_until


class ListTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_removes_blocking_list_waiters(self):
        async with running_server() as server:
            client = await server.connect()
            await client.send('BLPOP', 'jobs', '0')
            await wait_until(lambda: bool(server.executor.store._list_waiters))
            await client.close()
            await wait_until(lambda: not server.executor.store._list_waiters)
            producer = await server.connect()
            await producer.command('RPUSH', 'jobs', 'kept')
            self.assertEqual((await producer.command('LRANGE', 'jobs', '0', '-1')).value, ['kept'])
