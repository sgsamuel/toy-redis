import unittest

from tests.support import following, make_executor, running_server, wait_until


class ReplicationTests(unittest.IsolatedAsyncioTestCase):
    async def test_replication_restores_mixed_snapshot_and_wait_offsets(self):
        executor = make_executor()
        executor.store.set('text', 'before', expiry_ms=60000)
        executor.store.rpush('list', 'a', 'b')
        executor.store.zadd('scores', (1.0, 'member'))
        executor.store.xadd('stream', '1-0', [('f', 'value')])
        executor.store.setbit('bits', 0, 1)
        async with running_server(executor) as primary:
            writer = await primary.connect()
            await writer.command('SET', 'before', 'snapshot')
            async with following(primary) as replica:
                self.assertEqual(replica.store.get('text'), b'before')
                self.assertEqual(replica.store.lrange('list', 0, -1), ['a', 'b'])
                self.assertEqual(replica.store.zscore('scores', 'member'), 1.0)
                self.assertEqual(replica.store.xrange('stream', '-', '+'), executor.store.xrange('stream', '-', '+'))
                self.assertEqual(replica.store.get('bits'), b'\x80')
                self.assertAlmostEqual(replica.store._store['text'].expires_at, executor.store._store['text'].expires_at, delta=0.02)
                await writer.command('SET', 'after', 'new')
                await wait_until(lambda: replica.store.get('after') == 'new')
                self.assertEqual((await writer.command('WAIT', '1', '1000')).value, 1)
                await wait_until(lambda: replica.replication.offset == executor.replication.offset)
                async with running_server(replica) as replica_server:
                    client = await replica_server.connect()
                    self.assertTrue((await client.command('SET', 'local', 'bad')).raw.startswith(b'-READONLY'))
