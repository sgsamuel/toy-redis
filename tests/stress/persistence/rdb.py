"""Stress workloads for rdb."""

from pathlib import Path
import tempfile
import time


async def rdb_load(scale):
    from app.data.store import RedisStore

    count = 20000 * scale
    expired = int((time.time() - 60) * 1000).to_bytes(8, 'little')
    data = bytearray(b'REDIS0011\xfe\x00')
    for index in range(count):
        key = f'key:{index}'.encode()
        if index % 4 == 0:
            data.extend(b'\xfc' + expired)
        data.extend(b'\x00' + bytes([len(key)]) + key + b'\x40\x64' + b'x' * 100)
    data.extend(b'\xff' + bytes(8))
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'dump.rdb'
        path.write_bytes(data)
        store = RedisStore()
        store.load_rdb(path)
        assert len(store.keys('*')) == count - count // 4
        assert store.get('key:0') is None
        assert store.get(f'key:{count - 1}') == b'x' * 100
    return {'entries': count, 'live': count - count // 4, 'expired': count // 4, 'file_bytes': len(data)}
