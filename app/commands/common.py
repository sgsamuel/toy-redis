import asyncio

from dataclasses import dataclass, field

from app.network.auth import ACLState
from app.network.config import ServerConfig
from app.protocol import RedisResponse
from app.network.pubsub import PubSubState
from app.replication.primary import ReplicationState
from app.data.store import RedisStore


@dataclass(eq=False)
class ClientSession:
    username: str | None = None
    queued: list[list[str]] | None = None
    watched: dict[str, int] = field(default_factory=dict)
    replica_ack_offset: int | None = None
    last_write_offset: int = 0
    executing_transaction: bool = False
    transaction_error: bool = False
    trusted_replication: bool = False
    replaying_aof: bool = False
    channels: set[str] = field(default_factory=set)
    patterns: set[str] = field(default_factory=set)
    shard_channels: set[str] = field(default_factory=set)
    outgoing: asyncio.Queue[RedisResponse] = field(default_factory=lambda: asyncio.Queue(maxsize=256))
    disconnected: asyncio.Event = field(default_factory=asyncio.Event)
    closing: bool = False
    queued_bytes: int = 0

    @property
    def subscribed(self) -> bool:
        return bool(self.channels or self.patterns or self.shard_channels)

    def send(self, response: RedisResponse) -> bool:
        if self.disconnected.is_set():
            return False

        size = len(response.raw)

        if self.queued_bytes + size > 8 * 1024 * 1024:
            self.disconnected.set()
            return False

        if self.outgoing.full():
            queued = []

            while not self.outgoing.empty():
                queued.append(self.outgoing.get_nowait())

            response = RedisResponse(b"".join(item.raw for item in queued) + response.raw)
            self.outgoing.put_nowait(response)

            for _ in queued:
                self.outgoing.task_done()
        else:
            self.outgoing.put_nowait(response)

        self.queued_bytes += size

        return True

    async def enqueue(self, response: RedisResponse) -> None:
        await self.outgoing.put(response)
        self.queued_bytes += len(response.raw)


@dataclass
class CommandContext:
    store: RedisStore
    replication: ReplicationState
    session: ClientSession
    acl: ACLState
    pubsub: PubSubState
    config: ServerConfig


@dataclass
class CommandResult:
    response: RedisResponse
    mutation: list[str] | None = None
    register_replica: bool = False


@dataclass
class ExecuteTransaction:
    commands: list[list[str]]


def wrong_arguments(command: str) -> RedisResponse:
    return RedisResponse.error(
        f"Wrong number of arguments or syntax for '{command}'"
    )
