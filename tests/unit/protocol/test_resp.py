import asyncio
import unittest

from app.commands.errors import CommandError
from app.commands.request import CommandRequest
from app.protocol import RESPParseError, RESPParser, RedisResponse, encode_command
from app.protocol.encoding import decode_text, encode_text


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_binary_round_trip_and_raw_mode(self):
        payload = bytes(range(256))
        frame = encode_command([b'SET', b'key\xff', payload])
        reader = asyncio.StreamReader()
        reader.feed_data(frame)
        parsed = await RESPParser(reader, raw_bulk=True).parse()
        self.assertEqual(parsed.value, [b'SET', b'key\xff', payload])
        self.assertEqual(parsed.raw, frame)
        request = CommandRequest.parse(parsed.value)
        self.assertEqual(request.name, 'SET')
        self.assertEqual(encode_text(request.tokens[2]), payload)
        self.assertEqual(encode_text(decode_text(payload)), payload)

    async def test_nested_arrays_nulls_and_multiple_frames(self):
        frames = [RedisResponse.array([RedisResponse.bulk('value'), RedisResponse.integer(3),
                                      RedisResponse.null(), RedisResponse.array(None)]),
                  RedisResponse.simple('OK')]
        reader = asyncio.StreamReader()
        reader.feed_data(b''.join(frame.raw for frame in frames))
        reader.feed_eof()
        parser = RESPParser(reader)
        self.assertEqual((await parser.parse()).value, ['value', 3, None, None])
        self.assertEqual((await parser.parse()).value, 'OK')
        self.assertIsNone(await parser.parse())

    async def test_partial_network_frame(self):
        reader = asyncio.StreamReader()
        parser = RESPParser(reader)
        reader.feed_data(b'$5\r\nhe')
        pending = asyncio.create_task(parser.parse())
        await asyncio.sleep(0)
        self.assertFalse(pending.done())
        reader.feed_data(b'llo\r\n')
        self.assertEqual((await pending).value, 'hello')

    async def test_malformed_and_truncated_frames_are_rejected(self):
        for frame in [b'?', b'$-2\r\n', b'*1\r\n', b'$3\r\nabcxx', b':x\r\n', b'$5\r\nhi']:
            with self.subTest(frame=frame):
                reader = asyncio.StreamReader()
                reader.feed_data(frame)
                reader.feed_eof()

                with self.assertRaises(RESPParseError):
                    await RESPParser(reader).parse()

    def test_request_validation(self):
        for tokens in [[], None, 'PING', [1], [None], [['PING']]]:
            with self.subTest(tokens=tokens), self.assertRaises(CommandError):
                CommandRequest.parse(tokens)

    def test_line_responses_reject_newlines(self):
        for factory in [RedisResponse.simple, RedisResponse.error]:
            with self.assertRaises(ValueError):
                factory('bad\r\nvalue')
