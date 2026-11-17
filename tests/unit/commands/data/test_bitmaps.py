from tests.support import CommandTestCase


class BitmapCommandTests(CommandTestCase):
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
