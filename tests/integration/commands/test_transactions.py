import unittest

from tests.support import running_server


class TransactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_transactions_keep_order_and_abort_invalid_batches(self):
        async with running_server() as server:
            client = await server.connect()
            await client.command('MULTI')
            await client.command('INCR', 'number')
            await client.command('GET', 'number')
            self.assertEqual((await client.command('EXEC')).value, [1, '1'])
            await client.command('MULTI')
            self.assertTrue((await client.command('BAD')).is_error)
            await client.command('SET', 'discard', 'value')
            self.assertTrue((await client.command('EXEC')).raw.startswith(b'-EXECABORT'))
            self.assertIsNone(server.executor.store.get('discard'))
