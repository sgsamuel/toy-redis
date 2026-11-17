import asyncio
from collections import deque
from threading import Lock
from fnmatch import fnmatchcase
from pathlib import Path

from app.persistence.rdb import dump_rdb, load_rdb, parse_rdb
from .common import RedisString, RedisList, RedisStream, RedisStreamEntry, RedisSortedSet
from .entries import strings, lists, streams, sorted_set, bitmap, geo


class RedisStore:
    def __init__(self):
        self._store: dict[str, RedisString | RedisList | RedisStream | RedisSortedSet] = {}

        self._list_waiters: dict[str, deque[asyncio.Future[str]]] = {}
        self._stream_waiters: dict[str, deque[asyncio.Future[None]]] = {}
        self._lock = Lock()

        self._key_versions: dict[str, int] = {}

    def _mark_changed_locked(self, key: str) -> None:
        """Increment the key version; the caller must hold _lock."""
        self._key_versions[key] = self._key_versions.get(key, 0) + 1

    def _get_entry_locked(self, key: str) -> RedisString | RedisList | RedisStream | RedisSortedSet | None:
        """Read and expire a key; the caller must hold _lock."""
        entry = self._store.get(key)
        if entry is not None and entry.is_expired():
            del self._store[key]
            self._mark_changed_locked(key)
            return None
        return entry

    def get_key_version(self, key: str) -> int:
        with self._lock:
            self._get_entry_locked(key)
            return self._key_versions.get(key, 0)

    def type(self, key: str) -> str | None:
        with self._lock:
            entry = self._get_entry_locked(key)
            if entry is None:
                return None
            if isinstance(entry, RedisString):
                return "string"
            if isinstance(entry, RedisList):
                return "list"
            if isinstance(entry, RedisStream):
                return "stream"
            if isinstance(entry, RedisSortedSet):
                return "zset"

        raise TypeError(f"Unsupported entry type: {type(entry).__name__}")

    def load_rdb(self, path: Path) -> None:
        entries = load_rdb(path)

        with self._lock:
            self._store.update(entries)

            for key in entries:
                self._mark_changed_locked(key)

    def snapshot(self) -> bytes:
        with self._lock:
            return dump_rdb(self._store)

    def restore_snapshot(self, data: bytes) -> None:
        entries = parse_rdb(data)

        with self._lock:
            changed = self._store.keys() | entries.keys()
            self._store = entries

            for key in changed:
                self._mark_changed_locked(key)

    def keys(self, pattern: str) -> list[str]:
        with self._lock:
            return [
                key for key in list(self._store)
                if self._get_entry_locked(key) is not None and fnmatchcase(key, pattern)
            ]

    def set(self, key: str, value: str, px: int | None = None) -> None:
        strings.set(self, key, value, px)

    def get(self, key: str) -> str | bytes | None:
        return strings.get(self, key)

    def incr(self, key: str) -> int:
        return strings.incr(self, key)

    def setbit(self, key: str, offset: int, value: int) -> int:
        return bitmap.setbit(self, key, offset, value)

    def getbit(self, key: str, offset: int) -> int:
        return bitmap.getbit(self, key, offset)

    def strlen(self, key: str) -> int:
        return strings.strlen(self, key)

    def bitcount(self, key: str, start: int = 0, end: int = -1, unit: str = "BYTE") -> int:
        return bitmap.bitcount(self, key, start, end, unit)

    def bitop(self, operation: str, destination: str, *keys: str) -> int:
        return bitmap.bitop(self, operation, destination, *keys)

    def rpush(self, key: str, *values: str) -> int:
        return lists.rpush(self, key, *values)

    def lpush(self, key: str, *values: str) -> int:
        return lists.lpush(self, key, *values)

    def llen(self, key: str) -> int:
        return lists.llen(self, key)

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        return lists.lrange(self, key, start, stop)

    def lpop(self, key: str, count: int = 1) -> list[str]:
        return lists.lpop(self, key, count)

    async def blpop(self, key: str, timeout: float) -> str | None:
        return await lists.blpop(self, key, timeout)

    def xadd(self, key: str, raw_id: str, fields: list[tuple[str, str]]) -> str:
        return streams.xadd(self, key, raw_id, fields)

    def xrange(self, key: str, start: str, end: str) -> list[RedisStreamEntry]:
        return streams.xrange(self, key, start, end)

    def xread(
        self, requests: list[tuple[str, str]]
    ) -> list[tuple[str, list[RedisStreamEntry]]]:
        return streams.xread(self, requests)

    async def xread_blocking(
        self, requests: list[tuple[str, str]], block_ms: int
    ) -> list[tuple[str, list[RedisStreamEntry]]]:
        return await streams.xread_blocking(self, requests, block_ms)

    def zadd(self, key: str, *pairs: tuple[float, str]) -> int:
        return sorted_set.zadd(self, key, *pairs)

    def zrank(self, key: str, member: str) -> int | None:
        return sorted_set.zrank(self, key, member)

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        return sorted_set.zrange(self, key, start, stop)

    def zcard(self, key: str) -> int:
        return sorted_set.zcard(self, key)

    def zscore(self, key: str, member: str) -> float | None:
        return sorted_set.zscore(self, key, member)

    def zrem(self, key: str, *members: str) -> int:
        return sorted_set.zrem(self, key, *members)

    def geoadd(self, key: str, *locations: tuple[float, float, str]) -> int:
        return geo.geoadd(self, key, *locations)

    def geopos(self, key: str, *members: str) -> list[tuple[float, float] | None]:
        return geo.geopos(self, key, *members)

    def geodist(self, key: str, member1: str, member2: str) -> float | None:
        return geo.geodist(self, key, member1, member2)

    def geosearch(
        self,
        key: str,
        center: str | tuple[float, float],
        shape: tuple[float, ...],
        order: str | None = None,
        count: int | None = None,
        any_results: bool = False,
    ) -> list[tuple[str, float, float, tuple[float, float]]]:
        return geo.geosearch(self, key, center, shape, order, count, any_results)
