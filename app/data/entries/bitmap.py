from __future__ import annotations

from typing import TYPE_CHECKING

from ..common import RedisString, _get_bytes_locked

if TYPE_CHECKING:
    from ..store import RedisStore


def setbit(store: RedisStore, key: str, offset: int, value: int) -> int:
    with store._lock:
        data = bytearray(_get_bytes_locked(store, key))
        entry = store._store.get(key)

        if entry is None:
            entry = RedisString(value=b"")
            store._store[key] = entry

        byte_index, bit_index = divmod(offset, 8)

        if byte_index >= len(data):
            data.extend(bytes(byte_index + 1 - len(data)))

        mask = 1 << (7 - bit_index)
        previous = int(bool(data[byte_index] & mask))

        if value:
            data[byte_index] |= mask
        else:
            data[byte_index] &= ~mask

        entry.value = bytes(data)
        store._mark_changed_locked(key)

        return previous


def getbit(store: RedisStore, key: str, offset: int) -> int:
    with store._lock:
        data = _get_bytes_locked(store, key)
        byte_index, bit_index = divmod(offset, 8)

        if byte_index >= len(data):
            return 0

        return (data[byte_index] >> (7 - bit_index)) & 1


def bitcount(
    store: RedisStore, key: str, start: int = 0, end: int = -1, unit: str = "BYTE"
) -> int:
    with store._lock:
        data = _get_bytes_locked(store, key)
        length = len(data) * 8 if unit == "BIT" else len(data)
        start = max(0, start + length if start < 0 else start)
        end = min(length - 1, end + length if end < 0 else end)

        if start > end:
            return 0

        if unit == "BYTE":
            return sum(value.bit_count() for value in data[start:end + 1])

        first_byte = start // 8
        last_byte, last_bit = divmod(end, 8)
        selected = int.from_bytes(data[first_byte:last_byte + 1], "big")
        selected >>= 7 - last_bit
        selected &= (1 << (end - start + 1)) - 1

        return selected.bit_count()


def bitop(store: RedisStore, operation: str, destination: str, *keys: str) -> int:
    with store._lock:
        sources = [_get_bytes_locked(store, key) for key in keys]
        length = max(len(data) for data in sources)
        result = int.from_bytes(sources[0].ljust(length, b"\x00"), "big")

        for data in sources[1:]:
            value = int.from_bytes(data.ljust(length, b"\x00"), "big")

            if operation == "AND":
                result &= value
            else:
                result |= value

        if length:
            store._store[destination] = RedisString(value=result.to_bytes(length, "big"))
            store._mark_changed_locked(destination)
        elif store._get_entry_locked(destination) is not None:
            del store._store[destination]
            store._mark_changed_locked(destination)

        return length
