from unittest.mock import patch

from tests.support import CommandTestCase


class StringCommandTests(CommandTestCase):
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

    async def test_conditional_set(self):
        self.assertIsNone((await self.command("SET", "k", "v", "XX")).value)
        await self.command("SET", "k", "v", "nx", "PX", "60000")
        store = self.executor.store
        version = store.get_key_version("k")
        offset = self.executor.replication.offset
        self.assertIsNone((await self.command("SET", "k", "other", "NX")).value)
        self.assertEqual(store.get_key_version("k"), version)
        self.assertEqual(self.executor.replication.offset, offset)
        self.assertEqual(store.get("k"), "v")
        await self.command("SET", "k", "other", "XX")
        self.assertEqual((await self.command("TTL", "k")).value, -1)
        for options in [("NX", "XX"), ("PX", "1", "EX", "2"), ("PX",)]:
            self.assertTrue((await self.command("SET", "k", "bad", *options)).is_error)
        await self.command("PEXPIRE", "k", "0")
        self.assertEqual((await self.command("SET", "k", "new", "NX")).value, "OK")

    async def test_decrement_and_expiry_preserved(self):
        self.assertEqual((await self.command("DECR", "new")).value, -1)
        await self.command("SET", "k", "0", "PX", "60000")
        deadline = self.executor.store._store["k"].expires_at
        self.assertEqual((await self.command("DECR", "k")).value, -1)
        self.assertEqual(self.executor.store._store["k"].expires_at, deadline)
        for value in [str(-(2 ** 63)), str(2 ** 63), "bad"]:
            await self.command("SET", "k", value)
            self.assertTrue((await self.command("DECR", "k")).is_error)
            self.assertEqual(self.executor.store.get("k"), value)
