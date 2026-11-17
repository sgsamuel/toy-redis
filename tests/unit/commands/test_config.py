from dataclasses import asdict
import unittest

from app.commands.common import ClientSession
from app.network.config import ServerConfig

from tests.support import make_executor


class ConfigCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_config_get_all_settings_and_patterns(self):
        config = ServerConfig()
        executor = make_executor(config=config)
        reply = await executor.execute(['CONFIG', 'GET', '*'], ClientSession())
        self.assertEqual(dict(zip(reply.value[::2], reply.value[1::2])), asdict(config))
        reply = await executor.execute(['CONFIG', 'GET', 'appendonly', 'appendfsync', 'appendonly'], ClientSession())
        self.assertEqual(reply.value, ['appendonly', 'no', 'appendfsync', 'everysec'])
        reply = await executor.execute(['CONFIG', 'GET', 'missing'], ClientSession())
        self.assertEqual(reply.value, [])
