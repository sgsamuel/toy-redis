from __future__ import annotations

from bisect import bisect_left, insort
from heapq import merge
from typing import TYPE_CHECKING

from ..common import RedisSortedSet, _get_sorted_set_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def zadd(
    store: RedisStore, key: str, *pairs: tuple[float, str], condition: str | None = None
) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        if entry is None:
            if condition == "XX" or not pairs:
                return 0
            entry = RedisSortedSet()
            store._store[key] = entry

        updates = {}
        added = 0
        for score, member in pairs:
            previous = updates.get(member, entry.scores.get(member))
            if ((condition == "NX" and previous is not None)
                    or (condition == "XX" and previous is None)):
                continue
            if previous != score:
                added += previous is None
                updates[member] = score

        if not updates:
            return 0

        if len(updates) == 1:
            member, score = next(iter(updates.items()))
            if member in entry.scores:
                entry.ordered.pop(bisect_left(entry.ordered, (entry.scores[member], member)))
            insort(entry.ordered, (score, member))
        else:
            unchanged = (item for item in entry.ordered if item[1] not in updates)
            changed = sorted((score, member) for member, score in updates.items())
            entry.ordered = list(merge(unchanged, changed))

        entry.scores.update(updates)
        store._mark_changed_locked(key)
        return added


def zrank(store: RedisStore, key: str, member: str) -> int | None:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        score = entry.scores.get(member) if entry is not None else None
        return bisect_left(entry.ordered, (score, member)) if score is not None else None


def zrange(
    store: RedisStore, key: str, start: int, stop: int, *, with_scores: bool = False
) -> list[str] | list[tuple[str, float]]:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        if entry is None:
            return []
        length = len(entry.scores)
        start = max(0, start + length if start < 0 else start)
        stop = stop + length if stop < 0 else stop
        if stop < start:
            return []
        items = entry.ordered[start:stop + 1]
        if with_scores:
            return [(member, score) for score, member in items]
        return [member for _, member in items]


def zcard(store: RedisStore, key: str) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        return len(entry.scores) if entry is not None else 0


def zscore(store: RedisStore, key: str, member: str) -> float | None:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        return entry.scores.get(member) if entry is not None else None


def zrem(store: RedisStore, key: str, *members: str) -> int:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)
        if entry is None:
            return 0
        removed_members = entry.scores.keys() & members
        removed = len(removed_members)
        if removed == 1:
            member = next(iter(removed_members))
            entry.ordered.pop(bisect_left(entry.ordered, (entry.scores[member], member)))
        elif removed:
            entry.ordered = [item for item in entry.ordered if item[1] not in removed_members]
        for member in removed_members:
            del entry.scores[member]
        if removed:
            if not entry.scores:
                del store._store[key]
            store._mark_changed_locked(key)
        return removed
