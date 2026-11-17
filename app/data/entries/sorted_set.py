from __future__ import annotations

from typing import TYPE_CHECKING

from ..common import RedisSortedSet, RedisSortedSetEntry, _get_sorted_set_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def zadd(store: RedisStore, key: str, *pairs: tuple[float, str]) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        if entry is None:
            entry = RedisSortedSet(entries=[])
            store._store[key] = entry

        members = {item.value: item for item in entry.entries}
        added = 0
        changed = False

        for score, member in pairs:
            existing = members.get(member)
            if existing is None:
                members[member] = RedisSortedSetEntry(value=member, score=score)
                added += 1
                changed = True
            elif existing.score != score:
                existing.score = score
                changed = True

        if changed:
            entry.entries = sorted(members.values(), key=lambda item: (item.score, item.value))
            store._mark_changed_locked(key)

        return added


def zrank(store: RedisStore, key: str, member: str) -> int | None:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return None

        for rank, item in enumerate(entry.entries):
            if item.value == member:
                return rank

        return None


def zrange(store: RedisStore, key: str, start: int, stop: int) -> list[str]:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return []

        length = len(entry.entries)
        start = max(0, start + length if start < 0 else start)
        stop = stop + length if stop < 0 else stop

        if stop < start:
            return []

        return [item.value for item in entry.entries[start:stop + 1]]


def zcard(store: RedisStore, key: str) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        return len(entry.entries) if entry is not None else 0


def zscore(store: RedisStore, key: str, member: str) -> float | None:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return None

        for item in entry.entries:
            if item.value == member:
                return item.score

        return None


def zrem(store: RedisStore, key: str, *members: str) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return 0

        removed_members = set(members)
        remaining = [item for item in entry.entries if item.value not in removed_members]
        removed = len(entry.entries) - len(remaining)

        if removed:
            if remaining:
                entry.entries = remaining
            else:
                del store._store[key]

            store._mark_changed_locked(key)

        return removed
