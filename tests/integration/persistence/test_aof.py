import tempfile
import unittest

from app.network.config import ServerConfig
from app.protocol import encode_command

from tests.support import make_executor, running_server


class AOFRestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_aof_survives_server_restart_without_duplicate_records(self):
        with tempfile.TemporaryDirectory() as directory:
            config = ServerConfig(dir=directory, appendonly='yes')
            original = make_executor(config=config)
            async with running_server(original) as server:
                client = await server.connect()
                await client.command('SET', 'counter', '1')
                await client.command('INCR', 'counter')
            before = original.aof_manager.file.read_bytes()
            restored = make_executor(config=config)
            self.assertEqual(await restored.aof_manager.replay(restored.execute), 2)
            async with running_server(restored) as server:
                client = await server.connect()
                self.assertEqual((await client.command('GET', 'counter')).value, '2')
                self.assertEqual(restored.aof_manager.file.read_bytes(), before)
                await client.command('INCR', 'counter')
            self.assertEqual(restored.aof_manager.file.read_bytes(), before + encode_command(['INCR', 'counter']))
