from __future__ import annotations

from typing import TYPE_CHECKING

from ..common import RedisSet, _get_set_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def sadd(store: RedisStore, key: str, *members: str) -> int:
    with store._lock:
        entry = _get_set_locked(store, key)
        if entry is None:
            if not members:
                return 0
            entry = RedisSet()
            store._store[key] = entry
        before = len(entry.members)
        entry.members.update(members)
        added = len(entry.members) - before
        if added:
            store._mark_changed_locked(key)
        return added


def sismember(store: RedisStore, key: str, member: str) -> bool:
    with store._lock:
        entry = _get_set_locked(store, key)
        return entry is not None and member in entry.members


def scard(store: RedisStore, key: str) -> int:
    with store._lock:
        entry = _get_set_locked(store, key)
        return len(entry.members) if entry is not None else 0


def srem(store: RedisStore, key: str, *members: str) -> int:
    with store._lock:
        entry = _get_set_locked(store, key)
        if entry is None:
            return 0
        before = len(entry.members)
        entry.members.difference_update(members)
        removed = before - len(entry.members)
        if removed:
            if not entry.members:
                del store._store[key]
            store._mark_changed_locked(key)
        return removed


def smembers(store: RedisStore, key: str) -> list[str]:
    with store._lock:
        entry = _get_set_locked(store, key)
        return list(entry.members) if entry is not None else []
