"""Stress workloads for aof."""

import tempfile

from app.commands.common import ClientSession
from app.network.config import ServerConfig

from tests.support import make_executor


async def aof_restart(scale):
    with tempfile.TemporaryDirectory() as directory:
        config = ServerConfig(dir=directory, appendonly='yes')
        original = make_executor(config=config)
        count = 10000 * scale
        session = ClientSession()
        for _ in range(count):
            assert (await original.execute(['INCR', 'counter'], session)).value > 0
        before = original.aof_manager.file.read_bytes()
        restored = make_executor(config=config)
        replayed = await restored.aof_manager.replay(restored.execute)
        assert replayed == count
        assert restored.store.get('counter') == str(count)
        assert restored.aof_manager.file.read_bytes() == before
        assert restored.replication.offset == 0
        return {'writes': count, 'replayed': replayed, 'aof_bytes': len(before), 'duplicate_records': 0}
