from __future__ import annotations

import time
from typing import TYPE_CHECKING

from ..common import RedisString, _get_bytes_locked, _get_string_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def set(store: RedisStore, key: str, value: str, expiry_ms: int | None = None, condition: str | None = None) -> bool:
    with store._lock:
        entry = store._get_entry_locked(key)
        if (condition == "NX" and entry is not None) or (condition == "XX" and entry is None):
            return False
        expires_at = time.monotonic() + expiry_ms / 1000 if expiry_ms is not None else None
        store._store[key] = RedisString(value=value, expires_at=expires_at)
        store._mark_changed_locked(key)
        return True


def get(store: RedisStore, key: str) -> str | bytes | None:
    with store._lock:
        entry = _get_string_locked(store, key)

        if entry is None:
            return None

        return entry.value


def incr(store: RedisStore, key: str, delta: int = 1) -> int:
    with store._lock:
        entry = _get_string_locked(store, key)

        if entry is None:
            entry = RedisString(value="0")
            store._store[key] = entry

        try:
            value = int(entry.value)
        except ValueError:
            raise ValueError("value is not an integer or out of range")

        if not -(2 ** 63) <= value < 2 ** 63:
            raise ValueError("value is not an integer or out of range")

        if not -(2 ** 63) <= value + delta < 2 ** 63:
            raise ValueError("increment or decrement would overflow")

        value += delta
        entry.value = str(value)
        store._mark_changed_locked(key)
        return value


def strlen(store: RedisStore, key: str) -> int:
    with store._lock:
        data = _get_bytes_locked(store, key)

        return len(data)
