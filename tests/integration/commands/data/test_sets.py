import unittest

from app.protocol import encode_command

from tests.support import running_server


class SetTests(unittest.IsolatedAsyncioTestCase):
    async def test_set_commands_pipeline_and_binary_members(self):
        async with running_server() as server:
            client = await server.connect()
            commands = [
                ['SADD', 'members', 'a', 'b', 'a'], ['SISMEMBER', 'members', 'a'],
                ['SCARD', 'members'], ['SREM', 'members', 'a', 'a'],
                ['SMEMBERS', 'members'], ['SREM', 'members', 'b'], ['TYPE', 'members'],
            ]
            client.writer.write(b''.join(encode_command(tokens) for tokens in commands))
            await client.writer.drain()
            replies = [(await client.receive()).value for _ in commands]
            self.assertEqual(replies, [2, 1, 2, 1, ['b'], 1, 'none'])
            self.assertEqual((await client.command(b'SADD', b'key\xff', b'\xff\x00')).value, 1)
            reply = await client.command(b'SMEMBERS', b'key\xff')
            self.assertEqual(reply.raw, b'*1\r\n$2\r\n\xff\x00\r\n')
