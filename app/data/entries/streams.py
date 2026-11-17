from __future__ import annotations

import asyncio
import time
from bisect import bisect_left, bisect_right
from collections import deque
from typing import TYPE_CHECKING

from ..common import RedisStream, RedisStreamEntry, _get_stream_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def _notify_stream_waiters_locked(store: RedisStore, key: str) -> None:
    queue = store._stream_waiters.get(key)
    if queue is None:
        return

    for waiter in queue:
        if not waiter.done():
            waiter.set_result(None)


def _resolve_stream_id(
    raw_id: str, last_id: tuple[int, int]
) -> tuple[int, int]:
    last_ms, last_seq = last_id
    max_part = (1 << 64) - 1

    if raw_id == "*":
        now_ms = time.time_ns() // 1_000_000
        if now_ms > last_ms:
            return now_ms, 0
        if last_seq < max_part:
            return last_ms, last_seq + 1
        if last_ms < max_part:
            return last_ms + 1, 0
        raise ValueError(
            "The stream has exhausted the last possible ID, unable to add more items"
        )

    ms_text, separator, seq_text = raw_id.partition("-")
    auto_sequence = seq_text == "*"

    if not separator:
        seq_text = "0"

    parts = (ms_text,) if auto_sequence else (ms_text, seq_text)
    if any(
        len(part) > 20
        or not part.isascii()
        or not part.isdecimal()
        or int(part) > max_part
        for part in parts
    ):
        raise ValueError("Invalid stream ID specified as stream command argument")

    ms = int(ms_text)
    if auto_sequence:
        seq = last_seq + 1 if ms == last_ms else 0
    else:
        seq = int(seq_text)

    entry_id = (ms, seq)
    if entry_id == (0, 0) and not auto_sequence:
        raise ValueError("The ID specified in XADD must be greater than 0-0")

    if seq > max_part or entry_id <= last_id:
        raise ValueError(
            "The ID specified in XADD is equal or smaller than the target stream top item"
        )

    return entry_id


def _parse_stream_id(raw_id: str, default_seq: int = 0) -> tuple[int, int]:
    ms, separator, seq = raw_id.partition("-")
    if not separator:
        seq = str(default_seq)

    if any(
        len(part) > 20 or not part.isascii() or not part.isdecimal()
        for part in (ms, seq)
    ):
        raise ValueError("Invalid stream ID specified as stream command argument")

    entry_id = (int(ms), int(seq))
    if max(entry_id) >= 1 << 64:
        raise ValueError("Invalid stream ID specified as stream command argument")
    return entry_id


def xadd(store: RedisStore, key: str, raw_id: str, fields: list[tuple[str, str]]) -> str:
    with store._lock:
        stream = _get_stream_locked(store, key)
        last_id = stream.entries[-1].entry_id if stream and stream.entries else (0, 0)
        entry_id = _resolve_stream_id(raw_id, last_id)

        if stream is None:
            stream = RedisStream(entries=[])
            store._store[key] = stream

        stream.entries.append(RedisStreamEntry(entry_id=entry_id, fields=fields))
        store._mark_changed_locked(key)
        _notify_stream_waiters_locked(store, key)
        ms, seq = entry_id
        return f"{ms}-{seq}"


def xrange(store: RedisStore, key: str, start: str, end: str) -> list[RedisStreamEntry]:
    max_id_part = (1 << 64) - 1
    start_id = (0, 0) if start == "-" else _parse_stream_id(start)
    end_id = (
        (max_id_part, max_id_part) if end == "+"
        else _parse_stream_id(end, max_id_part)
    )

    with store._lock:
        stream = _get_stream_locked(store, key)
        if stream is None:
            return []

        first = bisect_left(stream.entries, start_id, key=lambda entry: entry.entry_id)
        last = bisect_right(stream.entries, end_id, key=lambda entry: entry.entry_id)
        return stream.entries[first:last]


def _xread_locked(
    store: RedisStore, locations: list[tuple[str, tuple[int, int]]]
) -> list[tuple[str, list[RedisStreamEntry]]]:
    result = []

    for key, after_id in locations:
        stream = _get_stream_locked(store, key)
        if stream is None:
            continue

        first = bisect_right(stream.entries, after_id, key=lambda entry: entry.entry_id)
        if first < len(stream.entries):
            result.append((key, stream.entries[first:]))
    return result


def xread(
    store: RedisStore, requests: list[tuple[str, str]]
) -> list[tuple[str, list[RedisStreamEntry]]]:
    with store._lock:
        locations = [(key, _parse_stream_id(raw_id)) for key, raw_id in requests]
        return _xread_locked(store, locations)


async def xread_blocking(
    store: RedisStore, requests: list[tuple[str, str]], block_ms: int
) -> list[tuple[str, list[RedisStreamEntry]]]:
    keys = {key for key, _ in requests}
    loop = asyncio.get_running_loop()
    timeout = None if block_ms == 0 else block_ms / 1000

    with store._lock:
        locations = []
        for key, raw_id in requests:
            if raw_id == "$":
                stream = _get_stream_locked(store, key)
                after_id = (
                    stream.entries[-1].entry_id
                    if stream is not None and stream.entries
                    else (0, 0)
                )
            else:
                after_id = _parse_stream_id(raw_id)

            locations.append((key, after_id))

    try:
        async with asyncio.timeout(timeout):
            while True:
                with store._lock:
                    result = _xread_locked(store, locations)
                    if result:
                        return result

                    waiter: asyncio.Future[None] = loop.create_future()
                    for key in keys:
                        store._stream_waiters.setdefault(key, deque()).append(waiter)

                try:
                    await waiter
                finally:
                    with store._lock:
                        for key in keys:
                            queue = store._stream_waiters[key]
                            queue.remove(waiter)
                            if not queue:
                                del store._stream_waiters[key]
    except TimeoutError:
        return []
