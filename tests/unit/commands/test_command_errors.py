from tests.support import CommandTestCase


class CommandErrorTests(CommandTestCase):
    async def test_wrong_types_are_errors(self):
        self.store.rpush('list', 'value')
        for tokens in [('GET', 'list'), ('INCR', 'list'), ('GETBIT', 'list', '0'),
                       ('ZCARD', 'list'), ('XRANGE', 'list', '-', '+')]:
            reply = await self.command(*tokens)
            self.assertTrue(reply.is_error)
            self.assertIn(b'WRONGTYPE', reply.raw)
        self.assertEqual(self.store.lrange('list', 0, -1), ['value'])
