import unittest

from tests.support import running_server


class PubSubTests(unittest.IsolatedAsyncioTestCase):
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
