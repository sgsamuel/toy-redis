import unittest

from app.protocol import encode_command

from tests.support import running_server


class HashTests(unittest.IsolatedAsyncioTestCase):
    async def test_hash_pipeline_and_binary_fields(self):
        async with running_server() as server:
            client = await server.connect()
            commands = [
                ['HSET', 'hash', 'a', '1', 'b', '2'], ['HSET', 'hash', 'a', 'updated'],
                ['HGET', 'hash', 'a'], ['HEXISTS', 'hash', 'b'], ['HLEN', 'hash'],
                ['HDEL', 'hash', 'a', 'a'], ['HGETALL', 'hash'],
                ['HDEL', 'hash', 'b'], ['TYPE', 'hash'],
            ]
            client.writer.write(b''.join(encode_command(tokens) for tokens in commands))
            await client.writer.drain()
            self.assertEqual([(await client.receive()).value for _ in commands],
                             [2, 0, 'updated', 1, 2, 1, ['b', '2'], 1, 'none'])
            await client.command(b'HSET', b'key\xff', b'field\xff', b'\x00\xff')
            self.assertEqual((await client.command(b'HGETALL', b'key\xff')).raw,
                             b'*2\r\n$6\r\nfield\xff\r\n$2\r\n\x00\xff\r\n')
