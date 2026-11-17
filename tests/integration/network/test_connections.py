import asyncio
import unittest

from app.protocol import RedisResponse, encode_command

from tests.support import running_server


class ConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_crosses_old_queue_limit_without_losing_replies(self):
        async with running_server() as server:
            client = await server.connect()
            client.writer.write(encode_command(['INCR', 'counter']) * 1000)
            await client.writer.drain()
            replies = [(await client.receive()).value for _ in range(1000)]
            self.assertEqual(replies, list(range(1, 1001)))
            self.assertEqual(server.executor.store.get('counter'), '1000')

    async def test_fragmented_binary_commands_and_error_recovery(self):
        async with running_server() as server:
            client = await server.connect()
            payload = bytes(range(256))
            raw = encode_command([b'SET', b'key\xff', payload])
            for start in range(0, len(raw), 7):
                client.writer.write(raw[start:start + 7])
                await client.writer.drain()
                await asyncio.sleep(0)
            self.assertEqual((await client.receive()).value, 'OK')
            self.assertEqual((await client.command(b'GET', b'key\xff')).raw, RedisResponse.bulk(payload).raw)
            self.assertTrue((await client.command('BOGUS')).is_error)
            self.assertEqual((await client.command('PING')).value, 'PONG')

    async def test_protocol_error_is_sent_before_close(self):
        async with running_server() as server:
            client = await server.connect()
            client.writer.write(b'?')
            await client.writer.drain()
            self.assertTrue((await client.receive()).is_error)
            self.assertIsNone(await client.receive())
