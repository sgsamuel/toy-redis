from contextlib import asynccontextmanager, suppress
from functools import partial
import asyncio
import unittest

from app.commands.common import ClientSession
from app.commands.executor import CommandExecutor
from app.data.store import RedisStore
from app.network.auth import ACLState
from app.network.config import ServerConfig
from app.network.pubsub import PubSubState
from app.network.server import process_client
from app.persistence.aof import AOFManager
from app.protocol import RESPParser, encode_command
from app.replication.primary import ReplicationState
from app.replication.replica import follow_primary


def make_executor(*, config=None, replication=None, store=None, acl=None, pubsub=None):
    config = config if config is not None else ServerConfig()

    return CommandExecutor(
        store=store if store is not None else RedisStore(),
        replication=replication if replication is not None else ReplicationState(),
        config=config,
        acl=acl if acl is not None else ACLState(),
        pubsub=pubsub if pubsub is not None else PubSubState(),
        aof_manager=AOFManager(config),
    )


class CommandTestCase(unittest.IsolatedAsyncioTestCase):
    """Fresh command executor, store, and connection session for each test."""

    def setUp(self):
        self.executor = make_executor()
        self.session = ClientSession()
        self.store = self.executor.store

    async def command(self, *tokens):
        return await self.executor.execute(list(tokens), self.session)


async def wait_until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


class Client:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.parser = RESPParser(reader)

    async def send(self, *tokens):
        self.writer.write(encode_command(list(tokens)))
        await self.writer.drain()

    async def receive(self, timeout=3):
        return await asyncio.wait_for(self.parser.parse(), timeout)

    async def command(self, *tokens):
        await self.send(*tokens)

        return await self.receive()

    async def close(self):
        self.writer.close()

        with suppress(ConnectionError, OSError):
            await self.writer.wait_closed()


class Server:
    def __init__(self, executor):
        self.executor = executor
        self.tasks = set()
        self.clients = []
        self.errors = []
        self.port = None

    def accept(self, reader, writer):
        task = asyncio.create_task(process_client(reader, writer, self.executor))
        self.tasks.add(task)
        task.add_done_callback(self.finished)

    def finished(self, task):
        self.tasks.discard(task)

        if not task.cancelled() and task.exception() is not None:
            self.errors.append(task.exception())

    async def connect(self):
        reader, writer = await asyncio.open_connection('127.0.0.1', self.port)
        client = Client(reader, writer)
        self.clients.append(client)

        return client


@asynccontextmanager
async def running_server(executor=None):
    server = Server(executor if executor is not None else make_executor())
    listener = await asyncio.start_server(server.accept, '127.0.0.1', 0)
    server.port = listener.sockets[0].getsockname()[1]

    try:
        yield server
    finally:
        await asyncio.gather(*(client.close() for client in server.clients), return_exceptions=True)
        listener.close()
        await listener.wait_closed()
        pending = list(server.tasks)

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

    if server.errors:
        raise AssertionError(f'Unhandled server errors: {server.errors!r}')


@asynccontextmanager
async def following(primary, executor=None):
    executor = executor if executor is not None else make_executor()
    executor.replication.replica_of = ('127.0.0.1', primary.port)
    apply = partial(executor.apply_replication, session=ClientSession())
    task = asyncio.create_task(follow_primary(executor.replication, 0, apply, executor.store.restore_snapshot))

    try:
        await wait_until(lambda: executor.replication.master_link_up)
        yield executor
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
