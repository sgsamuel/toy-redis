from __future__ import annotations

import asyncio
import itertools
from collections import deque
from typing import TYPE_CHECKING

from ..common import RedisList, _get_list_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def _notify_list_waiters_locked(store: RedisStore, key: str, entry: RedisList) -> None:
    queue = store._list_waiters.get(key)
    if queue is None:
        return

    while queue and entry.values:
        waiter = queue.popleft()
        if not waiter.done():
            waiter.set_result(entry.values.popleft())
            store._mark_changed_locked(key)

    if not queue:
        del store._list_waiters[key]
    if not entry.values:
        del store._store[key]


def rpush(store: RedisStore, key: str, *values: str) -> int:
    with store._lock:
        entry = _get_list_locked(store, key)
        if entry is None:
            entry = RedisList(values=deque())
            store._store[key] = entry
        entry.values.extend(values)
        store._mark_changed_locked(key)
        length = len(entry.values)
        _notify_list_waiters_locked(store, key, entry)
        return length


def lpush(store: RedisStore, key: str, *values: str) -> int:
    with store._lock:
        entry = _get_list_locked(store, key)
        if entry is None:
            entry = RedisList(values=deque())
            store._store[key] = entry
        entry.values.extendleft(values)
        store._mark_changed_locked(key)
        length = len(entry.values)
        _notify_list_waiters_locked(store, key, entry)
        return length


def llen(store: RedisStore, key: str) -> int:
    with store._lock:
        entry = _get_list_locked(store, key)
        return len(entry.values) if entry else 0


def lrange(store: RedisStore, key: str, start: int, stop: int) -> list[str]:
    with store._lock:
        entry = _get_list_locked(store, key)
        if not entry:
            return []

        num_values = len(entry.values)
        if start < 0:
            start += num_values
        if stop < 0:
            stop += num_values

        start = max(0, start)
        stop = min(num_values - 1, stop)

        if start > stop or start >= num_values:
            return []

        return list(itertools.islice(entry.values, start, stop + 1))


def _lpop_locked(store: RedisStore, key: str, count: int = 1) -> list[str]:
    if count < 0:
        raise ValueError("Count must be non-negative")
    entry = _get_list_locked(store, key)
    if not entry:
        return []
    actual_count = min(count, len(entry.values))
    values = [entry.values.popleft() for _ in range(actual_count)]

    if not entry.values:
        del store._store[key]

    if values:
        store._mark_changed_locked(key)
    return values


def lpop(store: RedisStore, key: str, count: int = 1) -> list[str]:
    with store._lock:
        return _lpop_locked(store, key, count)


async def blpop(store: RedisStore, key: str, timeout: float) -> str | None:
    with store._lock:
        values = _lpop_locked(store, key)
        if values:
            return values[0]

        waiter: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        queue = store._list_waiters.setdefault(key, deque())
        queue.append(waiter)

    try:
        async with asyncio.timeout(timeout or None):
            return await waiter
    except TimeoutError:
        return None
    finally:
        with store._lock:
            if waiter in queue:
                queue.remove(waiter)
                if not queue:
                    del store._list_waiters[key]
