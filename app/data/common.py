from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.protocol.encoding import encode_text

if TYPE_CHECKING:
    from .store import RedisStore


WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


@dataclass(slots=True)
class RedisString:
    value: str | bytes
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


@dataclass
class RedisList:
    values: deque[str]
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


@dataclass(frozen=True, slots=True)
class RedisStreamEntry:
    entry_id: tuple[int, int]
    fields: list[tuple[str, str]]


@dataclass
class RedisStream:
    entries: list[RedisStreamEntry]
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


@dataclass
class RedisHash:
    fields: dict[str, str] = field(default_factory=dict)
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


@dataclass
class RedisSet:
    members: set[str] = field(default_factory=set)
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


@dataclass
class RedisSortedSet:
    scores: dict[str, float] = field(default_factory=dict)
    ordered: list[tuple[float, str]] = field(default_factory=list)
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


def _get_string_locked(store: RedisStore, key: str) -> RedisString | None:
    entry = store._get_entry_locked(key)

    if entry is not None and not isinstance(entry, RedisString):
        raise ValueError(WRONG_TYPE)

    return entry


def _get_bytes_locked(store: RedisStore, key: str) -> bytes:
    entry = _get_string_locked(store, key)

    if entry is None:
        return b""

    return encode_text(entry.value) if isinstance(entry.value, str) else entry.value


def _get_list_locked(store: RedisStore, key: str) -> RedisList | None:
    entry = store._get_entry_locked(key)
    if entry is not None and not isinstance(entry, RedisList):
        raise ValueError(WRONG_TYPE)
    return entry


def _get_stream_locked(store: RedisStore, key: str) -> RedisStream | None:
    entry = store._get_entry_locked(key)
    if entry is not None and not isinstance(entry, RedisStream):
        raise ValueError(WRONG_TYPE)
    return entry


def _get_set_locked(store: RedisStore, key: str) -> RedisSet | None:
    entry = store._get_entry_locked(key)
    if entry is not None and not isinstance(entry, RedisSet):
        raise ValueError(WRONG_TYPE)
    return entry


def _get_sorted_set_locked(store: RedisStore, key: str) -> RedisSortedSet | None:
    entry = store._get_entry_locked(key)
    if entry is not None and not isinstance(entry, RedisSortedSet):
        raise ValueError(WRONG_TYPE)
    return entry


def _get_hash_locked(store: RedisStore, key: str) -> RedisHash | None:
    entry = store._get_entry_locked(key)
    if entry is not None and not isinstance(entry, RedisHash):
        raise ValueError(WRONG_TYPE)
    return entry
