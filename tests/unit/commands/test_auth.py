from hashlib import sha256

from app.commands.common import ClientSession

from tests.support import CommandTestCase


class AuthenticationTests(CommandTestCase):
    async def test_default_identity_and_acl_reply(self):
        self.assertEqual((await self.command('ACL', 'WHOAMI')).value, 'default')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'default')).value,
                         ['flags', ['nopass'], 'passwords', []])
        self.assertIsNone((await self.command('ACL', 'GETUSER', 'missing')).value)

    async def test_password_hashes_and_authentication(self):
        await self.command('ACL', 'SETUSER', 'default', '>secret', '>second', '>secret')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'default')).value,
                         ['flags', [], 'passwords', [sha256(b'secret').hexdigest(), sha256(b'second').hexdigest()]])
        other = ClientSession()
        self.assertTrue((await self.executor.execute(['GET', 'key'], other)).raw.startswith(b'-NOAUTH'))
        self.assertTrue((await self.executor.execute(['AUTH', 'wrong'], other)).raw.startswith(b'-WRONGPASS'))
        self.assertEqual((await self.executor.execute(['AUTH', 'secret'], other)).value, 'OK')
        self.assertEqual((await self.executor.execute(['ACL', 'WHOAMI'], other)).value, 'default')

    async def test_named_users_and_nopass_reset(self):
        await self.command('ACL', 'SETUSER', 'alice', '>secret')
        other = ClientSession()
        self.assertEqual((await self.executor.execute(['AUTH', 'alice', 'secret'], other)).value, 'OK')
        self.assertEqual(other.username, 'alice')
        self.assertEqual((await self.command('ACL', 'WHOAMI')).value, 'default')
        await self.command('ACL', 'SETUSER', 'alice', 'nopass')
        self.assertEqual((await self.command('ACL', 'GETUSER', 'alice')).value,
                         ['flags', ['nopass'], 'passwords', []])

    async def test_invalid_acl_rule_is_atomic(self):
        self.assertTrue((await self.command('ACL', 'SETUSER', 'default', '>secret', 'bad')).is_error)
        self.assertEqual(self.executor.acl.users['default'].passwords, [])
