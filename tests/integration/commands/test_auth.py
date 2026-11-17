import unittest

from tests.support import running_server


class AuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def test_authentication_is_per_connection(self):
        async with running_server() as server:
            admin = await server.connect()
            await admin.command('ACL', 'SETUSER', 'default', '>secret')
            other = await server.connect()
            self.assertTrue((await other.command('GET', 'key')).raw.startswith(b'-NOAUTH'))
            self.assertTrue((await other.command('AUTH', 'wrong')).raw.startswith(b'-WRONGPASS'))
            self.assertEqual((await other.command('AUTH', 'secret')).value, 'OK')
            self.assertEqual((await other.command('SET', 'key', 'value')).value, 'OK')
            self.assertEqual((await admin.command('GET', 'key')).value, 'value')
