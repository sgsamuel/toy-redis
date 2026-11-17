from pathlib import Path
from unittest.mock import AsyncMock, patch
import contextlib
import io
import tempfile
import unittest

from app.commands.common import ClientSession
from app.main import main, parse_args
from app.network.config import ServerConfig


class StartupTests(unittest.IsolatedAsyncioTestCase):
    def test_defaults_and_cli_validation(self):
        with patch('pathlib.Path.cwd', return_value=Path('/tmp/startup')), patch('sys.argv', ['redis']):
            config = ServerConfig()
            args = parse_args()
        self.assertEqual(config.dir, '/tmp/startup')
        self.assertEqual(args.dir, '/tmp/startup')
        self.assertEqual(config.appendonly, 'no')
        self.assertEqual(config.appendfsync, 'everysec')
        for option, value in [('--appendonly', 'maybe'), ('--appendfsync', 'sometimes')]:
            with patch('sys.argv', ['redis', option, value]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parse_args()

    async def test_main_wires_shared_state_and_data_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            serve = AsyncMock()
            with patch('sys.argv', ['redis', '--dir', directory]), patch('app.main.serve', serve):
                await main()
            executor = serve.call_args.args[0]
            self.assertIs(executor.aof_manager.config, executor.config)
            self.assertEqual((await executor.execute(['ACL', 'WHOAMI'], ClientSession())).value, 'default')
            self.assertEqual(list(Path(directory).iterdir()), [])
