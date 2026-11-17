from tests.support import CommandTestCase


class SortedSetCommandTests(CommandTestCase):
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
