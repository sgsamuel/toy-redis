import asyncio
import unittest
from unittest.mock import patch

from app.commands.common import ClientSession
from tests.support import make_executor, wait_until


class DataCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.executor = make_executor()
        self.session = ClientSession()
        self.store = self.executor.store

    async def command(self, *tokens):
        return await self.executor.execute(list(tokens), self.session)

    async def test_strings_and_overwrite(self):
        self.assertIsNone((await self.command('GET', 'missing')).value)
        self.assertEqual((await self.command('SET', 'text', 'é')).value, 'OK')
        self.assertEqual((await self.command('STRLEN', 'text')).value, 2)
        self.store.rpush('list', 'a')
        await self.command('SET', 'list', 'replacement')
        self.assertEqual((await self.command('GET', 'list')).value, 'replacement')
        self.assertEqual((await self.command('TYPE', 'list')).value, 'string')

    async def test_expiration_and_invalid_ttl(self):
        for option, duration in [('EX', '60'), ('PX', '60000')]:
            await self.command('SET', 'key', 'value', option, duration)
            deadline = self.store._store['key'].expires_at

            with patch('app.data.common.time') as clock:
                clock.monotonic.return_value = deadline + 1
                self.assertIsNone((await self.command('GET', 'key')).value)
                self.assertEqual((await self.command('KEYS', '*')).value, [])

        for duration in ['0', '-1', 'bad']:
            self.assertTrue((await self.command('SET', 'bad', 'value', 'PX', duration)).is_error)
        self.assertIsNone(self.store.get('bad'))

    async def test_increment_bounds_preserve_failed_values(self):
        self.assertEqual((await self.command('INCR', 'counter')).value, 1)
        for original in [str(2 ** 63 - 1), str(2 ** 63), str(-(2 ** 63) - 1), 'bad']:
            self.store.set('number', original)
            version = self.store.get_key_version('number')
            self.assertTrue((await self.command('INCR', 'number')).is_error)
            self.assertEqual(self.store.get('number'), original)
            self.assertEqual(self.store.get_key_version('number'), version)
        self.store.set('number', str(-(2 ** 63)))
        self.assertEqual((await self.command('INCR', 'number')).value, -(2 ** 63) + 1)

    async def test_bitmap_boundaries_binary_get_and_padding(self):
        for offset in ['0', '7', '8', '23']:
            self.assertEqual((await self.command('SETBIT', 'bits', offset, '1')).value, 0)
            self.assertEqual((await self.command('GETBIT', 'bits', offset)).value, 1)
        self.assertEqual((await self.command('GET', 'bits')).raw, b'$3\r\n\x81\x80\x01\r\n')
        self.assertEqual((await self.command('SETBIT', 'bits', '0', '0')).value, 1)
        self.assertEqual((await self.command('GETBIT', 'absent', '100')).value, 0)
        self.assertIsNone(self.store.type('absent'))

    async def test_bitmap_validation_and_preserved_expiry(self):
        await self.command('SET', 'bits', 'a', 'PX', '60000')
        deadline = self.store._store['bits'].expires_at
        await self.command('SETBIT', 'bits', '0', '1')
        self.assertEqual(self.store._store['bits'].expires_at, deadline)
        for tokens in [('SETBIT', 'bits', '-1', '1'), ('SETBIT', 'bits', '0', '2'),
                       ('GETBIT', 'bits', str(2 ** 32)), ('GETBIT', 'bits', 'bad')]:
            self.assertTrue((await self.command(*tokens)).is_error)

    async def test_bitcount_ranges(self):
        await self.command('SET', 'text', 'foobar')
        for arguments, expected in [((), 26), (('0', '0'), 4), (('1', '1', 'BYTE'), 6),
                                    (('5', '30', 'BIT'), 17), (('-1', '-1'), 4),
                                    (('0', '-7'), 0), (('9', '10'), 0), (('4', '1'), 0)]:
            with self.subTest(arguments=arguments):
                self.assertEqual((await self.command('BITCOUNT', 'text', *arguments)).value, expected)
        self.assertEqual((await self.command('BITCOUNT', 'missing')).value, 0)

    async def test_bitop_padding_aliases_and_empty_destination(self):
        self.store.set('a', 'a')
        self.store.set('b', 'bc')
        self.assertEqual((await self.command('BITOP', 'AND', 'out', 'a', 'b')).value, 2)
        self.assertEqual(self.store.get('out'), b'`\x00')
        await self.command('BITOP', 'OR', 'a', 'a', 'b')
        self.assertEqual(self.store.get('a'), b'cc')
        await self.command('BITOP', 'OR', 'out', 'missing')
        self.assertIsNone(self.store.type('out'))

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

    async def test_sorted_set_updates_duplicates_and_ranks(self):
        self.assertEqual((await self.command('ZADD', 'scores', '3', 'b', '1', 'b', '1', 'a')).value, 2)
        self.assertEqual((await self.command('ZRANGE', 'scores', '0', '-1')).value, ['a', 'b'])
        self.assertEqual((await self.command('ZRANK', 'scores', 'a')).value, 0)
        self.assertEqual((await self.command('ZADD', 'scores', '0', 'b')).value, 0)
        self.assertEqual((await self.command('ZRANK', 'scores', 'b')).value, 0)
        self.assertEqual((await self.command('ZCARD', 'scores')).value, 2)
        self.assertEqual((await self.command('ZSCORE', 'scores', 'b')).value, '0')
        self.assertIsNone((await self.command('ZSCORE', 'scores', 'missing')).value)
        self.assertEqual((await self.command('ZREM', 'scores', 'a', 'a', 'b')).value, 2)
        self.assertIsNone(self.store.type('scores'))

    async def test_zrange_withscores_order_ranges_and_wire_format(self):
        await self.command('ZADD', 'scores', '2', 'b', '2', 'a', '-1.5', 'low',
                           'inf', 'high', '-inf', 'first')
        reply = await self.command('ZRANGE', 'scores', '1', '3', 'wItHsCoReS')
        self.assertEqual(reply.value, ['low', '-1.5', 'a', '2', 'b', '2'])
        self.assertEqual(reply.raw,
                         b'*6\r\n$3\r\nlow\r\n$4\r\n-1.5\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n2\r\n')
        self.assertEqual((await self.command('ZRANGE', 'scores', '-2', '-1', 'WITHSCORES')).value,
                         ['b', '2', 'high', 'inf'])
        self.assertEqual((await self.command('ZRANGE', 'scores', '0', '0', 'WITHSCORES')).value,
                         ['first', '-inf'])
        self.assertEqual((await self.command('ZRANGE', 'scores', '1', '3')).value,
                         ['low', 'a', 'b'])
        for key, start, stop in [('missing', '0', '-1'), ('scores', '9', '10'),
                                 ('scores', '2', '1')]:
            with self.subTest(key=key, start=start, stop=stop):
                self.assertEqual((await self.command('ZRANGE', key, start, stop, 'WITHSCORES')).raw,
                                 b'*0\r\n')

    async def test_zrange_withscores_validation_and_wrong_type(self):
        for args in [('scores', '0'), ('scores', '0', '-1', 'UNKNOWN'),
                     ('scores', '0', '-1', 'WITHSCORES', 'WITHSCORES'),
                     ('scores', 'bad', '-1', 'WITHSCORES')]:
            with self.subTest(args=args):
                self.assertTrue((await self.command('ZRANGE', *args)).is_error)
        await self.command('SET', 'string', 'value')
        reply = await self.command('ZRANGE', 'string', '0', '-1', 'WITHSCORES')
        self.assertTrue(reply.is_error)
        self.assertIn(b'WRONGTYPE', reply.raw)

    async def test_zadd_nx_xx_conditions_and_noop_versions(self):
        self.assertEqual((await self.command('ZADD', 'scores', 'XX', '1', 'a')).value, 0)
        self.assertIsNone(self.store.type('scores'))
        self.assertEqual(self.executor.replication.offset, 0)
        self.assertEqual((await self.command('ZADD', 'scores', 'nx', '1', 'a', '2', 'a')).value, 1)
        self.assertEqual(self.store.zscore('scores', 'a'), 1)
        version = self.store.get_key_version('scores')
        offset = self.executor.replication.offset
        self.assertEqual((await self.command('ZADD', 'scores', 'NX', '3', 'a')).value, 0)
        self.assertEqual(self.store.get_key_version('scores'), version)
        self.assertEqual(self.executor.replication.offset, offset)
        self.assertEqual((await self.command('ZADD', 'scores', 'NX', '3', 'a', '4', 'b')).value, 1)
        self.assertEqual((await self.command('ZADD', 'scores', 'xx', '5', 'a', '6', 'missing')).value, 0)
        self.assertEqual(self.store.zrange('scores', 0, -1, with_scores=True), [('b', 4), ('a', 5)])
        self.assertGreater(self.executor.replication.offset, offset)
        offset = self.executor.replication.offset
        await self.command('ZADD', 'scores', 'XX', '5', 'a', '6', 'missing')
        self.assertEqual(self.executor.replication.offset, offset)

    async def test_zadd_conditions_validate_atomically_and_preserve_expiry(self):
        await self.command('ZADD', 'scores', '1', 'a')
        await self.command('PEXPIRE', 'scores', '60000')
        deadline = self.store._store['scores'].expires_at
        version = self.store.get_key_version('scores')
        for options in [('NX', 'XX', '2', 'a'), ('XX', 'NX', '2', 'a'),
                        ('NX',), ('XX', '1'), ('NX', '2', 'b', 'nan', 'c'),
                        ('XX', 'bad', 'missing')]:
            with self.subTest(options=options):
                self.assertTrue((await self.command('ZADD', 'scores', *options)).is_error)
                self.assertEqual(self.store.get_key_version('scores'), version)
        await self.command('ZADD', 'scores', 'XX', '2', 'a')
        self.assertEqual(self.store._store['scores'].expires_at, deadline)
        await self.command('SET', 'text', 'value')
        for condition in ('NX', 'XX'):
            reply = await self.command('ZADD', 'text', condition, '1', 'a')
            self.assertTrue(reply.is_error)
            self.assertIn(b'WRONGTYPE', reply.raw)

    async def test_invalid_sorted_scores_do_not_partially_write(self):
        self.assertTrue((await self.command('ZADD', 'scores', '1', 'a', 'nan', 'b')).is_error)
        self.assertIsNone(self.store.type('scores'))
        self.assertEqual(self.executor.replication.offset, 0)

    async def test_wrong_types_are_errors(self):
        self.store.rpush('list', 'value')
        for tokens in [('GET', 'list'), ('INCR', 'list'), ('GETBIT', 'list', '0'),
                       ('ZCARD', 'list'), ('XRANGE', 'list', '-', '+')]:
            reply = await self.command(*tokens)
            self.assertTrue(reply.is_error)
            self.assertIn(b'WRONGTYPE', reply.raw)
        self.assertEqual(self.store.lrange('list', 0, -1), ['value'])
