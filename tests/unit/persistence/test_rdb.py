from pathlib import Path
from unittest.mock import patch
import tempfile
import time
import unittest

from app.data.store import RedisStore
from app.persistence.rdb import RDBReader, decompress_lzf


def length(value):
    if value < 64:
        return bytes([value])
    if value < 16384:
        return bytes([0x40 | (value >> 8), value & 255])
    return b'\x80' + value.to_bytes(4, 'big')


def string(data):
    return length(len(data)) + data


def entry(key, value):
    return b'\x00' + string(key) + string(value)


def snapshot(body):
    return b'REDIS0011\xfa' + string(b'redis-ver') + string(b'7.2.0') + b'\xfe\x00' + body + b'\xff' + bytes(8)


class RDBTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'dump.rdb'
        self.store = RedisStore()

    def test_multiple_values_and_length_encodings(self):
        self.path.write_bytes(snapshot(entry(b'one', b'first') + entry(b'two', b'second') +
                                       entry(b'long-' + b'x' * 100, b'y' * 17000)))
        self.store.load_rdb(self.path)
        self.assertEqual(self.store.get('one'), b'first')
        self.assertEqual(self.store.get('two'), b'second')
        self.assertEqual(self.store.get('long-' + 'x' * 100), b'y' * 17000)
        self.assertEqual(RDBReader(b'\x81' + (2 ** 32).to_bytes(8, 'big')).plain_length(), 2 ** 32)

    def test_integer_and_lzf_strings(self):
        compressed = b'\x02abc\xe0\x00\x02'
        self.assertEqual(decompress_lzf(compressed, 12), b'abcabcabcabc')
        body = b'\x00' + string(b'integer') + b'\xc1' + (-1234).to_bytes(2, 'little', signed=True)
        body += b'\x00' + string(b'compressed') + b'\xc3' + length(len(compressed)) + length(12) + compressed
        self.path.write_bytes(snapshot(body))
        self.store.load_rdb(self.path)
        self.assertEqual(self.store.get('integer'), b'-1234')
        self.assertEqual(self.store.get('compressed'), b'abcabcabcabc')

    def test_absolute_expirations_are_preserved(self):
        now = time.time()
        body = b'\xfc' + int((now - 60) * 1000).to_bytes(8, 'little') + entry(b'expired', b'gone')
        body += b'\xfd' + int(now + 60).to_bytes(4, 'little') + entry(b'future', b'value')
        body += entry(b'permanent', b'value')
        self.path.write_bytes(snapshot(body))
        self.store.load_rdb(self.path)
        self.assertIsNone(self.store.get('expired'))
        self.assertEqual(self.store.get('future'), b'value')
        deadline = self.store._store['future'].expires_at
        with patch('app.data.common.time') as clock:
            clock.monotonic.return_value = deadline + 1
            self.assertIsNone(self.store.get('future'))
            self.assertEqual(self.store.keys('*'), ['permanent'])

    def test_binary_keys_values_and_database_isolation(self):
        self.path.write_bytes(snapshot(entry(b'key\xff', b'\xff\x00') +
                                       b'\xfe\x01' + entry(b'other', b'hidden')))
        self.store.load_rdb(self.path)
        self.assertEqual(self.store.get('key\udcff'), b'\xff\x00')
        self.assertEqual(self.store.keys('*'), ['key\udcff'])

    def test_snapshot_round_trip_all_types_and_stale_key_removal(self):
        self.store.set('text', 'é', expiry_ms=60000)
        self.store.setbit('bits', 0, 1)
        self.store.rpush('list', 'a', 'b')
        self.store.zadd('scores', (1.0, 'a'), (2.0, 'b'))
        self.store.geoadd('places', (13.361389, 38.115556, 'Palermo'))
        self.store.xadd('stream', '1-0', [('field', 'value')])
        self.store.xadd('stream', '2-0', [('f' + str(i), 'v' * 300) for i in range(140)])
        restored = RedisStore()
        restored.set('stale', 'remove')
        restored.restore_snapshot(self.store.snapshot())
        self.assertEqual(restored.get('text'), 'é'.encode())
        self.assertEqual(restored.get('bits'), b'\x80')
        self.assertEqual(restored.lrange('list', 0, -1), ['a', 'b'])
        self.assertEqual(restored.zrange('scores', 0, -1), ['a', 'b'])
        self.assertEqual(restored.zscore('places', 'Palermo'), 3479099956230698)
        self.assertEqual(restored.xrange('stream', '-', '+'), self.store.xrange('stream', '-', '+'))
        self.assertIsNone(restored.get('stale'))
        self.assertAlmostEqual(restored._store['text'].expires_at, self.store._store['text'].expires_at, delta=0.01)

    def test_missing_file_and_corruption(self):
        self.store.load_rdb(self.path)
        self.assertEqual(self.store.keys('*'), [])
        for data in [b'', b'NOTREDIS!', snapshot(entry(b'valid', b'value'))[:-12], snapshot(b'\x99')]:
            self.path.write_bytes(data)
            with self.assertRaises(ValueError):
                self.store.load_rdb(self.path)
            self.assertEqual(self.store.keys('*'), [])

    def test_bad_lzf_input_is_rejected(self):
        for data, expected in [(b'\xe0\x00\x02', 12), (b'\x02a', 3), (b'\x00a', 9)]:
            with self.assertRaises(ValueError):
                decompress_lzf(data, expected)
