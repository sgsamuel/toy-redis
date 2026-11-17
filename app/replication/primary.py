from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.commands.errors import CommandError
from app.protocol import RedisResponse, encode_command

if TYPE_CHECKING:
    from app.commands.common import ClientSession


@dataclass
class ReplicationState:
    replica_of: tuple[str, int] | None = None
    replication_id: str = field(default_factory=lambda: secrets.token_hex(20))
    offset: int = 0
    replica_writers: dict[asyncio.StreamWriter, ClientSession] = field(default_factory=dict)
    ack_changed: asyncio.Event = field(default_factory=asyncio.Event)
    master_link_up: bool = False

    def validate_write(self, session: ClientSession, writes: bool) -> None:
        if not writes or self.replica_of is None or session.trusted_replication:
            return

        raise CommandError(RedisResponse.error(
            "You can't write against a read only replica.", prefix="READONLY"
        ))


def propagate_write(replication: ReplicationState, tokens: list[str]) -> None:
    frame = encode_command(tokens)

    replication.offset += len(frame)

    for writer, session in tuple(replication.replica_writers.items()):
        if writer.is_closing():
            replication.replica_writers.pop(writer, None)
        else:
            if not session.send(RedisResponse(frame)):
                writer.close()


def acknowledged_replicas(replication: ReplicationState, target: int) -> int:
    return sum(
        not writer.is_closing()
        and session.replica_ack_offset is not None
        and session.replica_ack_offset >= target
        for writer, session in replication.replica_writers.items()
    )


async def wait_for_replicas(
    replication: ReplicationState, target: int, required: int, timeout_ms: int
) -> int:
    count = acknowledged_replicas(replication, target)
    if count >= required:
        return count
    propagate_write(replication, ["REPLCONF", "GETACK", "*"])
    try:
        async with asyncio.timeout(timeout_ms / 1000 if timeout_ms else None):
            while True:
                replication.ack_changed.clear()
                count = acknowledged_replicas(replication, target)
                if count >= required:
                    return count
                await replication.ack_changed.wait()
    except TimeoutError:
        return acknowledged_replicas(replication, target)
