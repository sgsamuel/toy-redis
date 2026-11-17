import asyncio
import unittest

from app.commands.common import ClientSession
from app.network.auth import ACLState
from app.network.pubsub import PubSubState
from app.protocol import RESPParser, RedisResponse
from tests.support import make_executor


class PubSubStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_burst_batches_preserve_all_frames(self):
        session = ClientSession()
        for index in range(1000):
            self.assertTrue(session.send(RedisResponse.integer(index)))
        reader = asyncio.StreamReader()
        while not session.outgoing.empty():
            reader.feed_data(session.outgoing.get_nowait().raw)
            session.outgoing.task_done()
        parser = RESPParser(reader)
        for index in range(1000):
            self.assertEqual((await parser.parse()).value, index)
        await asyncio.wait_for(session.outgoing.join(), 1)
        self.assertFalse(session.disconnected.is_set())

    async def test_byte_limit_disconnects_and_unregisters_slow_consumer(self):
        state = PubSubState()
        session = ClientSession()
        state.subscribe(session, ['news'], 'channels')
        self.assertTrue(session.send(RedisResponse.bulk(b'x' * (4 * 1024 * 1024))))
        self.assertEqual(state.publish('news', 'y' * (4 * 1024 * 1024)), 0)
        self.assertTrue(session.disconnected.is_set())
        self.assertFalse(state.channels)
        self.assertFalse(session.channels)

    async def test_injected_auth_and_pubsub_state_can_be_shared(self):
        acl, pubsub = ACLState(), PubSubState()
        first = make_executor(acl=acl, pubsub=pubsub)
        second = make_executor(acl=acl, pubsub=pubsub)
        self.assertIs(first.acl, second.acl)
        self.assertIs(first.pubsub, second.pubsub)
        subscriber = ClientSession()
        await first.execute(['SUBSCRIBE', 'news'], subscriber)
        self.assertEqual((await second.execute(['PUBLISH', 'news', 'hello'], ClientSession())).value, 1)
        self.assertEqual(subscriber.outgoing.get_nowait().value, ['message', 'news', 'hello'])
