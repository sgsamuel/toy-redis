from __future__ import annotations

from typing import TYPE_CHECKING

from ..common import RedisHash, _get_hash_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def hset(store: RedisStore, key: str, *pairs: tuple[str, str]) -> int:
    with store._lock:
        entry = _get_hash_locked(store, key)
        if entry is None:
            if not pairs:
                return 0
            entry = RedisHash()
            store._store[key] = entry
        added = 0
        changed = False
        for field, value in pairs:
            if field not in entry.fields:
                added += 1
            if entry.fields.get(field) != value:
                entry.fields[field] = value
                changed = True
        if changed:
            store._mark_changed_locked(key)
        return added


def hget(store: RedisStore, key: str, field: str) -> str | None:
    with store._lock:
        entry = _get_hash_locked(store, key)
        return entry.fields.get(field) if entry is not None else None


def hdel(store: RedisStore, key: str, *fields: str) -> int:
    with store._lock:
        entry = _get_hash_locked(store, key)
        if entry is None:
            return 0
        removed = 0
        for field in fields:
            if field in entry.fields:
                del entry.fields[field]
                removed += 1
        if removed:
            if not entry.fields:
                del store._store[key]
            store._mark_changed_locked(key)
        return removed


def hgetall(store: RedisStore, key: str) -> dict[str, str]:
    with store._lock:
        entry = _get_hash_locked(store, key)
        return entry.fields.copy() if entry is not None else {}


def hexists(store: RedisStore, key: str, field: str) -> bool:
    with store._lock:
        entry = _get_hash_locked(store, key)
        return entry is not None and field in entry.fields


def hlen(store: RedisStore, key: str) -> int:
    with store._lock:
        entry = _get_hash_locked(store, key)
        return len(entry.fields) if entry is not None else 0
