from app.commands.common import ClientSession
from app.replication.primary import ReplicationState

from tests.support import CommandTestCase, make_executor


class ReplicationTests(CommandTestCase):
    async def test_readonly_and_internal_replication(self):
        replica = make_executor(replication=ReplicationState(replica_of=('localhost', 6379)))
        self.assertTrue((await replica.execute(['SET', 'key', 'external'], ClientSession())).raw.startswith(b'-READONLY'))
        self.assertEqual((await replica.apply_replication(['SET', 'key', 'internal'], ClientSession())).value, 'OK')
        self.assertEqual(replica.store.get('key'), 'internal')
        self.assertEqual(replica.replication.offset, 0)
