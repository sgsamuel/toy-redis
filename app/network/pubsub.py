from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING

from app.commands.errors import CommandError
from app.protocol import RedisResponse

if TYPE_CHECKING:
    from app.commands.common import ClientSession


@dataclass
class PubSubState:
    channels: dict[str, set[ClientSession]] = field(default_factory=dict)
    patterns: dict[str, set[ClientSession]] = field(default_factory=dict)
    shard_channels: dict[str, set[ClientSession]] = field(default_factory=dict)

    def validate(self, session: ClientSession, command_name: str) -> None:
        if not session.subscribed:
            return

        allowed = {
            "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE",
            "SUNSUBSCRIBE", "PING", "QUIT", "RESET",
        }

        if command_name in allowed:
            return

        raise CommandError(
            RedisResponse.error(
                f"Can't execute '{command_name.lower()}': only (P|S)SUBSCRIBE / "
                "(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            ),
            abort_transaction=False,
        )

    def subscribe(self, session: ClientSession, names: list[str], kind: str) -> RedisResponse:
        registry = getattr(self, kind)
        subscriptions = getattr(session, kind)
        label = {"channels": "subscribe", "patterns": "psubscribe", "shard_channels": "ssubscribe"}[kind]
        replies = []

        for name in names:
            registry.setdefault(name, set()).add(session)
            subscriptions.add(name)
            replies.append(self._acknowledgment(label, name, session))

        return RedisResponse(b"".join(reply.raw for reply in replies))

    def unsubscribe(self, session: ClientSession, names: list[str], kind: str) -> RedisResponse:
        registry = getattr(self, kind)
        subscriptions = getattr(session, kind)
        label = {"channels": "unsubscribe", "patterns": "punsubscribe", "shard_channels": "sunsubscribe"}[kind]
        names = names or sorted(subscriptions)
        replies = []

        for name in names:
            subscriptions.discard(name)
            subscribers = registry.get(name)

            if subscribers is not None:
                subscribers.discard(session)

                if not subscribers:
                    del registry[name]

            replies.append(self._acknowledgment(label, name, session))

        if not replies:
            replies.append(self._acknowledgment(label, None, session))

        return RedisResponse(b"".join(reply.raw for reply in replies))

    def remove(self, session: ClientSession) -> None:
        for kind in ("channels", "patterns", "shard_channels"):
            self.unsubscribe(session, [], kind)

    def publish(self, channel: str, message: str, shard: bool = False) -> int:
        registry = self.shard_channels if shard else self.channels
        label = "smessage" if shard else "message"
        response = RedisResponse.array([
            RedisResponse.bulk(label), RedisResponse.bulk(channel), RedisResponse.bulk(message),
        ])
        deliveries = [(session, response) for session in registry.get(channel, set()).copy()]

        if not shard:
            for pattern, subscribers in self.patterns.items():
                if fnmatchcase(channel, pattern):
                    response = RedisResponse.array([
                        RedisResponse.bulk("pmessage"), RedisResponse.bulk(pattern),
                        RedisResponse.bulk(channel), RedisResponse.bulk(message),
                    ])
                    deliveries.extend((session, response) for session in subscribers.copy())

        count = 0

        for session, response in deliveries:
            if session.send(response):
                count += 1
            else:
                self.remove(session)

        return count

    def _acknowledgment(self, label: str, name: str | None, session: ClientSession) -> RedisResponse:
        count = len(session.shard_channels) if label.startswith("s") and label != "subscribe" else len(session.channels) + len(session.patterns)

        return RedisResponse.array([
            RedisResponse.bulk(label), RedisResponse.bulk(name), RedisResponse.integer(count),
        ])
