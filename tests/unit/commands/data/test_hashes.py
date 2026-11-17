from unittest.mock import patch

from app.commands.common import ClientSession

from tests.support import CommandTestCase


class HashCommandTests(CommandTestCase):
    async def test_missing_keys_and_fields(self):
        self.assertEqual((await self.command('HGET', 'missing', 'field')).raw, b'$-1\r\n')
        self.assertEqual((await self.command('HGETALL', 'missing')).raw, b'*0\r\n')
        self.assertEqual((await self.command('HEXISTS', 'missing', 'field')).value, 0)
        self.assertEqual((await self.command('HLEN', 'missing')).value, 0)
        self.assertEqual((await self.command('HDEL', 'missing', 'field')).value, 0)
        self.assertEqual(self.store.keys('*'), [])
        self.assertEqual(self.executor.replication.offset, 0)

    async def test_multiple_fields_duplicates_overwrites_and_empty_values(self):
        self.assertEqual((await self.command('hset', 'hash', 'a', 'first', 'a', 'last',
                                            'A', 'upper', '', '')).value, 3)
        self.assertEqual((await self.command('TYPE', 'hash')).value, 'hash')
        self.assertEqual((await self.command('HLEN', 'hash')).value, 3)
        self.assertEqual((await self.command('HGET', 'hash', 'a')).value, 'last')
        self.assertEqual((await self.command('HGET', 'hash', 'A')).value, 'upper')
        self.assertEqual((await self.command('HGET', 'hash', '')).raw, b'$0\r\n\r\n')
        self.assertEqual((await self.command('HEXISTS', 'hash', '')).raw, b':1\r\n')
        self.assertEqual((await self.command('HEXISTS', 'hash', 'missing')).value, 0)
        reply = await self.command('HGETALL', 'hash')
        self.assertEqual(dict(zip(reply.value[::2], reply.value[1::2])),
                         {'a': 'last', 'A': 'upper', '': ''})
        offset = self.executor.replication.offset
        self.assertEqual((await self.command('HSET', 'hash', 'a', 'changed')).value, 0)
        self.assertGreater(self.executor.replication.offset, offset)
        self.assertEqual((await self.command('HGET', 'hash', 'a')).value, 'changed')

    async def test_delete_counts_unique_fields_and_removes_empty_hash(self):
        await self.command('HSET', 'hash', 'a', '1', 'b', '2')
        self.assertEqual((await self.command('HDEL', 'hash', 'a', 'a', 'missing')).value, 1)
        self.assertIsNone((await self.command('HGET', 'hash', 'a')).value)
        self.assertEqual((await self.command('HLEN', 'hash')).value, 1)
        self.assertEqual((await self.command('HDEL', 'hash', 'b')).value, 1)
        self.assertEqual((await self.command('TYPE', 'hash')).value, 'none')
        self.assertEqual(self.store.keys('*'), [])

    async def test_noops_and_reads_preserve_versions_and_offsets(self):
        await self.command('HSET', 'hash', 'a', '1')
        version = self.store.get_key_version('hash')
        offset = self.executor.replication.offset
        for tokens in [('HSET', 'hash', 'a', '1'), ('HDEL', 'hash', 'missing'),
                       ('HGET', 'hash', 'a'), ('HEXISTS', 'hash', 'a'),
                       ('HGETALL', 'hash'), ('HLEN', 'hash')]:
            await self.command(*tokens)
            self.assertEqual(self.store.get_key_version('hash'), version)
            self.assertEqual(self.executor.replication.offset, offset)
        fields = self.store.hgetall('hash')
        fields['a'] = 'external mutation'
        self.assertEqual(self.store.hget('hash', 'a'), '1')

    async def test_expiry_is_preserved_and_expired_hashes_are_missing(self):
        await self.command('HSET', 'hash', 'a', '1', 'b', '2')
        await self.command('PEXPIRE', 'hash', '60000')
        deadline = self.store._store['hash'].expires_at
        await self.command('HSET', 'hash', 'a', 'updated', 'c', '3')
        await self.command('HDEL', 'hash', 'b')
        self.assertEqual(self.store._store['hash'].expires_at, deadline)
        with patch('app.data.common.time') as clock:
            clock.monotonic.return_value = deadline + 1
            self.assertIsNone((await self.command('HGET', 'hash', 'a')).value)
            self.assertEqual((await self.command('HGETALL', 'hash')).value, [])
            self.assertEqual((await self.command('HEXISTS', 'hash', 'a')).value, 0)
            self.assertEqual((await self.command('HLEN', 'hash')).value, 0)
            self.assertEqual((await self.command('HDEL', 'hash', 'a')).value, 0)
        self.assertEqual((await self.command('HSET', 'hash', 'a', 'new')).value, 1)
        self.assertEqual((await self.command('TTL', 'hash')).value, -1)

    async def test_wrong_types(self):
        self.store.set('string', 'value')
        self.store.rpush('list', 'value')
        self.store.sadd('set', 'member')
        self.store.zadd('sorted', (1.0, 'member'))
        self.store.xadd('stream', '1-0', [('field', 'value')])
        for key in ['string', 'list', 'set', 'sorted', 'stream']:
            for name, args in [('HSET', ('a', '1')), ('HGET', ('a',)), ('HDEL', ('a',)),
                               ('HGETALL', ()), ('HEXISTS', ('a',)), ('HLEN', ())]:
                with self.subTest(key=key, name=name):
                    reply = await self.command(name, key, *args)
                    self.assertTrue(reply.is_error)
                    self.assertIn(b'WRONGTYPE', reply.raw)
        await self.command('HSET', 'hash', 'a', '1')
        for tokens in [('GET', 'hash'), ('SADD', 'hash', 'a'), ('ZADD', 'hash', '1', 'a'),
                       ('LRANGE', 'hash', '0', '-1'), ('XRANGE', 'hash', '-', '+')]:
            reply = await self.command(*tokens)
            self.assertTrue(reply.is_error)
            self.assertIn(b'WRONGTYPE', reply.raw)
        await self.command('SET', 'hash', 'replacement')
        self.assertEqual((await self.command('TYPE', 'hash')).value, 'string')

    async def test_invalid_argument_counts_are_atomic_and_abort_transactions(self):
        await self.command('HSET', 'hash', 'a', 'original')
        for tokens in [('HSET',), ('HSET', 'hash', 'a'), ('HSET', 'hash', 'a', '1', 'b'),
                       ('HGET', 'hash'), ('HGET', 'hash', 'a', 'extra'), ('HDEL', 'hash'),
                       ('HEXISTS', 'hash'), ('HEXISTS', 'hash', 'a', 'extra'),
                       ('HGETALL',), ('HGETALL', 'hash', 'extra'),
                       ('HLEN',), ('HLEN', 'hash', 'extra')]:
            with self.subTest(tokens=tokens):
                self.assertTrue((await self.command(*tokens)).is_error)
                self.assertEqual(self.store.hgetall('hash'), {'a': 'original'})
        await self.command('MULTI')
        self.assertTrue((await self.command('HSET', 'hash', 'a', '1', 'b')).is_error)
        await self.command('HSET', 'hash', 'a', 'changed')
        self.assertTrue((await self.command('EXEC')).raw.startswith(b'-EXECABORT'))
        self.assertEqual(self.store.hget('hash', 'a'), 'original')

    async def test_binary_keys_fields_and_values(self):
        async def command(*tokens):
            return await self.executor.execute(list(tokens), self.session)
        self.assertEqual((await command(b'HSET', b'key\xff', b'field\xff', b'\x00\xff\r\n')).value, 1)
        self.assertEqual((await command(b'HGET', b'key\xff', b'field\xff')).raw,
                         b'$4\r\n\x00\xff\r\n\r\n')
        self.assertEqual((await command(b'HGETALL', b'key\xff')).raw,
                         b'*2\r\n$6\r\nfield\xff\r\n$4\r\n\x00\xff\r\n\r\n')
        self.assertEqual((await command(b'HEXISTS', b'key\xff', b'field\xff')).value, 1)
        self.assertEqual((await command(b'HDEL', b'key\xff', b'field\xff')).value, 1)

    async def test_transactions_and_watch(self):
        await self.command('MULTI')
        for tokens in [('HSET', 'hash', 'a', '1', 'b', '2'), ('HGET', 'hash', 'a'),
                       ('HDEL', 'hash', 'b'), ('HEXISTS', 'hash', 'b'), ('HLEN', 'hash')]:
            self.assertEqual((await self.command(*tokens)).value, 'QUEUED')
        self.assertIsNone(self.store.type('hash'))
        self.assertEqual((await self.command('EXEC')).value, [2, '1', 1, 0, 1])
        for tokens, aborts in [(['HSET', 'hash', 'a', '1'], False),
                               (['HDEL', 'hash', 'missing'], False),
                               (['HSET', 'hash', 'a', 'changed'], True),
                               (['HDEL', 'hash', 'a'], True)]:
            await self.command('WATCH', 'hash')
            await self.command('MULTI')
            await self.command('HLEN', 'hash')
            await self.executor.execute(tokens, ClientSession())
            self.assertEqual((await self.command('EXEC')).raw == b'*-1\r\n', aborts)
