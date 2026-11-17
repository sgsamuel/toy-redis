import asyncio
import tempfile
import unittest

from app.network.config import ServerConfig
from app.protocol import RedisResponse, encode_command
from tests.support import following, make_executor, running_server, wait_until


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_crosses_old_queue_limit_without_losing_replies(self):
        async with running_server() as server:
            client = await server.connect()
            client.writer.write(encode_command(['INCR', 'counter']) * 1000)
            await client.writer.drain()
            replies = [(await client.receive()).value for _ in range(1000)]
            self.assertEqual(replies, list(range(1, 1001)))
            self.assertEqual(server.executor.store.get('counter'), '1000')

    async def test_fragmented_binary_commands_and_error_recovery(self):
        async with running_server() as server:
            client = await server.connect()
            payload = bytes(range(256))
            raw = encode_command([b'SET', b'key\xff', payload])
            for start in range(0, len(raw), 7):
                client.writer.write(raw[start:start + 7])
                await client.writer.drain()
                await asyncio.sleep(0)
            self.assertEqual((await client.receive()).value, 'OK')
            self.assertEqual((await client.command(b'GET', b'key\xff')).raw, RedisResponse.bulk(payload).raw)
            self.assertTrue((await client.command('BOGUS')).is_error)
            self.assertEqual((await client.command('PING')).value, 'PONG')

    async def test_protocol_error_is_sent_before_close(self):
        async with running_server() as server:
            client = await server.connect()
            client.writer.write(b'?')
            await client.writer.drain()
            self.assertTrue((await client.receive()).is_error)
            self.assertIsNone(await client.receive())

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

    async def test_pubsub_fanout_duplicates_and_counts(self):
        async with running_server() as server:
            first, second, publisher = [await server.connect() for _ in range(3)]
            await first.send('SUBSCRIBE', 'news', 'news', 'other')
            self.assertEqual((await first.receive()).value, ['subscribe', 'news', 1])
            self.assertEqual((await first.receive()).value, ['subscribe', 'news', 1])
            self.assertEqual((await first.receive()).value, ['subscribe', 'other', 2])
            await second.command('SUBSCRIBE', 'news')
            self.assertEqual((await publisher.command('PUBLISH', 'news', 'hello')).value, 2)
            for client in [first, second]:
                self.assertEqual((await client.receive()).value, ['message', 'news', 'hello'])
            self.assertEqual((await publisher.command('PUBLISH', 'absent', 'hello')).value, 0)

    async def test_subscribed_mode_error_ping_unsubscribe_and_reset(self):
        async with running_server() as server:
            client = await server.connect()
            await client.command('SUBSCRIBE', 'news')
            response = await client.command('ECHO', 'hello')
            self.assertEqual(response.raw, b"-ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
            self.assertEqual((await client.command('PING')).value, ['pong', ''])
            self.assertEqual((await client.command('PING', 'hello')).value, ['pong', 'hello'])
            self.assertEqual((await client.command('UNSUBSCRIBE')).value, ['unsubscribe', 'news', 0])
            self.assertEqual((await client.command('UNSUBSCRIBE')).value, ['unsubscribe', None, 0])
            await client.command('SUBSCRIBE', 'news')
            self.assertEqual((await client.command('RESET')).value, 'RESET')
            self.assertFalse(server.executor.pubsub.channels)
            self.assertEqual((await client.command('ECHO', 'hello')).value, 'hello')

    async def test_pattern_and_shard_delivery(self):
        async with running_server() as server:
            subscriber, publisher = [await server.connect() for _ in range(2)]
            await subscriber.command('PSUBSCRIBE', 'news.*')
            self.assertEqual((await publisher.command('PUBLISH', 'news.today', 'hello')).value, 1)
            self.assertEqual((await subscriber.receive()).value, ['pmessage', 'news.*', 'news.today', 'hello'])
            self.assertEqual((await subscriber.command('PUNSUBSCRIBE')).value, ['punsubscribe', 'news.*', 0])
            await subscriber.command('SSUBSCRIBE', 'news')
            self.assertEqual((await publisher.command('PUBLISH', 'news', 'normal')).value, 0)
            self.assertEqual((await publisher.command('SPUBLISH', 'news', 'shard')).value, 1)
            self.assertEqual((await subscriber.receive()).value, ['smessage', 'news', 'shard'])
            self.assertEqual((await subscriber.command('SUNSUBSCRIBE')).value, ['sunsubscribe', 'news', 0])

    async def test_quit_flushes_acknowledgment_and_cleans_subscriptions(self):
        async with running_server() as server:
            client = await server.connect()
            await client.command('SUBSCRIBE', 'news')
            self.assertEqual((await client.command('QUIT')).value, 'OK')
            self.assertIsNone(await client.receive())
            self.assertFalse(server.executor.pubsub.channels)

    async def test_disconnect_removes_blocking_list_waiters(self):
        async with running_server() as server:
            client = await server.connect()
            await client.send('BLPOP', 'jobs', '0')
            await wait_until(lambda: bool(server.executor.store._list_waiters))
            await client.close()
            await wait_until(lambda: not server.executor.store._list_waiters)
            producer = await server.connect()
            await producer.command('RPUSH', 'jobs', 'kept')
            self.assertEqual((await producer.command('LRANGE', 'jobs', '0', '-1')).value, ['kept'])

    async def test_disconnect_removes_blocking_stream_waiters(self):
        async with running_server() as server:
            client = await server.connect()
            await client.send('XREAD', 'BLOCK', '0', 'STREAMS', 'stream', '$')
            await wait_until(lambda: bool(server.executor.store._stream_waiters))
            await client.close()
            await wait_until(lambda: not server.executor.store._stream_waiters)

    async def test_transactions_keep_order_and_abort_invalid_batches(self):
        async with running_server() as server:
            client = await server.connect()
            await client.command('MULTI')
            await client.command('INCR', 'number')
            await client.command('GET', 'number')
            self.assertEqual((await client.command('EXEC')).value, [1, '1'])
            await client.command('MULTI')
            self.assertTrue((await client.command('BAD')).is_error)
            await client.command('SET', 'discard', 'value')
            self.assertTrue((await client.command('EXEC')).raw.startswith(b'-EXECABORT'))
            self.assertIsNone(server.executor.store.get('discard'))

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
