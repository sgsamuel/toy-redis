from unittest.mock import patch

from app.commands.common import ClientSession

from tests.support import CommandTestCase


class SetCommandTests(CommandTestCase):
    async def test_missing_keys_behave_as_empty_sets(self):
        self.assertEqual((await self.command('SCARD', 'missing')).value, 0)
        self.assertEqual((await self.command('SISMEMBER', 'missing', 'a')).value, 0)
        self.assertEqual((await self.command('SMEMBERS', 'missing')).raw, b'*0\r\n')
        self.assertEqual((await self.command('SREM', 'missing', 'a')).value, 0)
        self.assertEqual(self.store.keys('*'), [])
        self.assertEqual(self.executor.replication.offset, 0)

    async def test_members_are_unique_and_case_sensitive(self):
        self.assertEqual((await self.command('sadd', 'members', 'a', 'a', 'A', '')).value, 3)
        self.assertEqual((await self.command('TYPE', 'members')).value, 'set')
        self.assertEqual((await self.command('SCARD', 'members')).value, 3)
        self.assertEqual((await self.command('SISMEMBER', 'members', 'a')).raw, b':1\r\n')
        self.assertEqual((await self.command('SISMEMBER', 'members', 'absent')).raw, b':0\r\n')
        self.assertCountEqual((await self.command('SMEMBERS', 'members')).value, ['a', 'A', ''])
        self.assertEqual((await self.command('SADD', 'members', 'a', 'b', 'b')).value, 1)
        self.assertEqual((await self.command('SCARD', 'members')).value, 4)

    async def test_removal_counts_unique_members_and_deletes_empty_key(self):
        await self.command('SADD', 'members', 'a', 'b')
        self.assertEqual((await self.command('SREM', 'members', 'a', 'a', 'missing')).value, 1)
        self.assertEqual((await self.command('SCARD', 'members')).value, 1)
        self.assertEqual((await self.command('SREM', 'members', 'b')).value, 1)
        self.assertEqual((await self.command('TYPE', 'members')).value, 'none')
        self.assertEqual((await self.command('KEYS', '*')).value, [])

    async def test_noops_preserve_versions_and_replication_offsets(self):
        await self.command('SADD', 'members', 'a')
        version = self.store.get_key_version('members')
        offset = self.executor.replication.offset
        for tokens in [('SADD', 'members', 'a', 'a'), ('SREM', 'members', 'missing'),
                       ('SCARD', 'members'), ('SISMEMBER', 'members', 'a'), ('SMEMBERS', 'members')]:
            await self.command(*tokens)
            self.assertEqual(self.store.get_key_version('members'), version)
            self.assertEqual(self.executor.replication.offset, offset)

    async def test_mutations_preserve_expiry_and_expired_keys_are_empty(self):
        await self.command('SADD', 'members', 'a')
        await self.command('PEXPIRE', 'members', '60000')
        deadline = self.store._store['members'].expires_at
        await self.command('SADD', 'members', 'b')
        await self.command('SREM', 'members', 'a')
        self.assertEqual(self.store._store['members'].expires_at, deadline)
        with patch('app.data.common.time') as clock:
            clock.monotonic.return_value = deadline + 1
            self.assertEqual((await self.command('SMEMBERS', 'members')).value, [])
            self.assertEqual((await self.command('SCARD', 'members')).value, 0)
            self.assertEqual((await self.command('SISMEMBER', 'members', 'b')).value, 0)
            self.assertEqual((await self.command('SREM', 'members', 'b')).value, 0)
        await self.command('SADD', 'members', 'new')
        self.assertEqual((await self.command('TTL', 'members')).value, -1)
        self.assertEqual((await self.command('SMEMBERS', 'members')).value, ['new'])

    async def test_wrong_types_are_errors_in_both_directions(self):
        self.store.set('string', 'value')
        self.store.rpush('list', 'value')
        self.store.zadd('sorted', (1.0, 'value'))
        self.store.xadd('stream', '1-0', [('field', 'value')])
        for key in ['string', 'list', 'sorted', 'stream']:
            for name, args in [('SADD', ('a',)), ('SISMEMBER', ('a',)), ('SCARD', ()),
                               ('SREM', ('a',)), ('SMEMBERS', ())]:
                with self.subTest(key=key, name=name):
                    reply = await self.command(name, key, *args)
                    self.assertTrue(reply.is_error)
                    self.assertIn(b'WRONGTYPE', reply.raw)
        await self.command('SADD', 'members', 'a')
        for tokens in [('GET', 'members'), ('ZADD', 'members', '1', 'a'),
                       ('LRANGE', 'members', '0', '-1'), ('XRANGE', 'members', '-', '+')]:
            reply = await self.command(*tokens)
            self.assertTrue(reply.is_error)
            self.assertIn(b'WRONGTYPE', reply.raw)
        self.assertEqual((await self.command('SET', 'members', 'replacement')).value, 'OK')
        self.assertEqual((await self.command('TYPE', 'members')).value, 'string')

    async def test_arity_validation_does_not_write(self):
        for tokens in [('SADD',), ('SADD', 'members'), ('SREM',), ('SREM', 'members'),
                       ('SISMEMBER', 'members'), ('SISMEMBER', 'members', 'a', 'b'),
                       ('SCARD',), ('SCARD', 'members', 'extra'),
                       ('SMEMBERS',), ('SMEMBERS', 'members', 'extra')]:
            with self.subTest(tokens=tokens):
                self.assertTrue((await self.command(*tokens)).is_error)
        self.assertEqual(self.store.keys('*'), [])
        self.assertEqual(self.executor.replication.offset, 0)

    async def test_binary_keys_and_members_round_trip(self):
        tokens = [b'SADD', b'key\xff', b'\xff\x00\r\n']
        self.assertEqual((await self.executor.execute(tokens, self.session)).value, 1)
        reply = await self.executor.execute([b'SMEMBERS', b'key\xff'], self.session)
        self.assertEqual(reply.raw, b'*1\r\n$4\r\n\xff\x00\r\n\r\n')
        self.assertEqual((await self.executor.execute(
            [b'SISMEMBER', b'key\xff', b'\xff\x00\r\n'], self.session)).value, 1)
        self.assertEqual((await self.executor.execute(
            [b'SREM', b'key\xff', b'\xff\x00\r\n'], self.session)).value, 1)

    async def test_watch_ignores_noops_and_aborts_on_real_changes(self):
        await self.command('SADD', 'members', 'a')
        other = ClientSession()
        for command, aborts in [(['SADD', 'members', 'a'], False),
                                (['SREM', 'members', 'missing'], False),
                                (['SADD', 'members', 'b'], True),
                                (['SREM', 'members', 'b'], True)]:
            await self.command('WATCH', 'members')
            await self.command('MULTI')
            await self.command('SCARD', 'members')
            await self.executor.execute(command, other)
            reply = await self.command('EXEC')
            self.assertEqual(reply.raw == b'*-1\r\n', aborts)

    async def test_set_commands_execute_in_transaction_order(self):
        await self.command('MULTI')
        commands = [('SADD', 'members', 'a', 'b'), ('SISMEMBER', 'members', 'a'),
                    ('SREM', 'members', 'a'), ('SCARD', 'members'), ('SMEMBERS', 'members')]
        for tokens in commands:
            self.assertEqual((await self.command(*tokens)).value, 'QUEUED')
        self.assertIsNone(self.store.type('members'))
        self.assertEqual((await self.command('EXEC')).value, [2, 1, 1, 1, ['b']])

    async def test_intersection_and_union_single_multiple_and_repeated_keys(self):
        self.store.sadd('first', 'a', 'b', 'c')
        self.store.sadd('second', 'b', 'c', 'd')
        self.store.sadd('third', 'c', 'e')
        for command in ['SINTER', 'SUNION']:
            self.assertCountEqual((await self.command(command, 'first')).value, ['a', 'b', 'c'])
            self.assertCountEqual((await self.command(command, 'first', 'first')).value, ['a', 'b', 'c'])
        self.assertCountEqual((await self.command('sinter', 'first', 'second')).value, ['b', 'c'])
        self.assertEqual((await self.command('SINTER', 'first', 'second', 'third')).value, ['c'])
        self.assertCountEqual((await self.command('sunion', 'first', 'second', 'third')).value,
                              ['a', 'b', 'c', 'd', 'e'])
        self.assertCountEqual((await self.command('SUNION', 'second', 'first', 'second')).value,
                              ['a', 'b', 'c', 'd'])

    async def test_set_operations_treat_missing_and_expired_keys_as_empty(self):
        self.store.sadd('live', 'a', 'b')
        self.store.sadd('expired', 'b', 'c')
        self.store.expire('expired', 60000)
        deadline = self.store._store['expired'].expires_at
        for command in ['SINTER', 'SUNION']:
            self.assertEqual((await self.command(command, 'missing')).raw, b'*0\r\n')
        for keys in [('live', 'missing'), ('missing', 'live')]:
            self.assertEqual((await self.command('SINTER', *keys)).value, [])
            self.assertCountEqual((await self.command('SUNION', *keys)).value, ['a', 'b'])
        with patch('app.data.common.time') as clock:
            clock.monotonic.return_value = deadline + 1
            self.assertEqual((await self.command('SINTER', 'live', 'expired')).value, [])
            self.assertCountEqual((await self.command('SUNION', 'expired', 'live')).value, ['a', 'b'])
        self.assertIsNone(self.store.type('missing'))
        self.assertIsNone(self.store.type('expired'))

    async def test_set_operations_validate_all_types_even_with_empty_intersections(self):
        self.store.sadd('first', 'a')
        self.store.sadd('second', 'b')
        self.store.set('string', 'value')
        self.store.hset('hash', ('field', 'value'))
        for command in ['SINTER', 'SUNION']:
            for key in ['string', 'hash']:
                for keys in [('first', key), (key, 'first'), ('missing', key),
                             (key, 'missing'), ('first', 'second', key)]:
                    with self.subTest(command=command, keys=keys):
                        reply = await self.command(command, *keys)
                        self.assertTrue(reply.is_error)
                        self.assertIn(b'WRONGTYPE', reply.raw)
            self.assertTrue((await self.command(command)).is_error)

    async def test_set_operations_preserve_inputs_versions_expiry_and_offsets(self):
        self.store.sadd('first', 'a', 'b')
        self.store.sadd('second', 'b', 'c')
        self.store.expire('first', 60000)
        version = self.store.get_key_version('first')
        deadline = self.store._store['first'].expires_at
        offset = self.executor.replication.offset
        self.store.sinter('first', 'second').append('external')
        self.store.sunion('first', 'second').clear()
        for command in ['SINTER', 'SUNION']:
            await self.command(command, 'first', 'second')
        self.assertEqual(self.store.get_key_version('first'), version)
        self.assertEqual(self.store._store['first'].expires_at, deadline)
        self.assertEqual(self.executor.replication.offset, offset)
        self.assertCountEqual(self.store.smembers('first'), ['a', 'b'])
        self.assertCountEqual(self.store.smembers('second'), ['b', 'c'])

    async def test_set_operations_support_binary_members_and_transactions(self):
        self.store.sadd('first', '\udcff\x00')
        self.store.sadd('second', '\udcff\x00')
        for command in ['SINTER', 'SUNION']:
            reply = await self.command(command, 'first', 'second')
            self.assertEqual(reply.raw, b'*1\r\n$2\r\n\xff\x00\r\n')
        await self.command('MULTI')
        for tokens in [('SADD', 'new', 'a'), ('SINTER', 'new', 'missing'),
                       ('SUNION', 'new', 'missing')]:
            self.assertEqual((await self.command(*tokens)).value, 'QUEUED')
        self.assertEqual((await self.command('EXEC')).value, [1, [], ['a']])
