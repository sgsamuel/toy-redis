from __future__ import annotations

import time
import struct
from collections import deque
from io import BytesIO
from pathlib import Path

from app.protocol.encoding import decode_text, encode_text

from app.data.common import RedisString, RedisList, RedisSortedSet, RedisSortedSetEntry, RedisStream, RedisStreamEntry


class RDBReader:
    def __init__(self, data: bytes):
        self.source = BytesIO(data)

    def read(self, length: int) -> bytes:
        data = self.source.read(length)

        if len(data) != length:
            raise ValueError("Truncated RDB file")

        return data

    def length(self) -> tuple[int, bool]:
        first = self.read(1)[0]
        kind = first >> 6

        if kind == 0:
            return first & 63, False

        if kind == 1:
            return ((first & 63) << 8) | self.read(1)[0], False

        if first in (0x80, 0x81):
            return int.from_bytes(self.read(4 if first == 0x80 else 8), "big"), False

        if kind == 3:
            return first & 63, True

        raise ValueError("Invalid RDB length encoding")

    def plain_length(self) -> int:
        length, encoded = self.length()

        if encoded:
            raise ValueError("Unexpected encoded RDB length")

        return length

    def string(self) -> bytes:
        length, encoded = self.length()

        if not encoded:
            return self.read(length)

        if length in (0, 1, 2):
            value = int.from_bytes(self.read(1 << length), "little", signed=True)

            return str(value).encode()

        if length == 3:
            compressed_length = self.plain_length()
            original_length = self.plain_length()

            return decompress_lzf(self.read(compressed_length), original_length)

        raise ValueError("Unsupported RDB string encoding")


def decompress_lzf(data: bytes, expected_length: int) -> bytes:
    reader = RDBReader(data)
    result = bytearray()

    while reader.source.tell() < len(data):
        control = reader.read(1)[0]

        if control < 32:
            result.extend(reader.read(control + 1))
        else:
            length = control >> 5
            offset = (control & 31) << 8

            if length == 7:
                length += reader.read(1)[0]

            offset += reader.read(1)[0] + 1

            if offset > len(result):
                raise ValueError("Invalid RDB LZF back reference")

            for _ in range(length + 2):
                result.append(result[-offset])

        if len(result) > expected_length:
            raise ValueError("Invalid RDB LZF length")

    if len(result) != expected_length:
        raise ValueError("Invalid RDB LZF length")

    return bytes(result)


def load_rdb(path: Path) -> dict[str, RedisString]:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return {}

    return parse_rdb(data)


def parse_rdb(data: bytes) -> dict:
    reader = RDBReader(data)
    header = reader.read(9)

    if not header.startswith(b"REDIS") or not header[5:].isdigit():
        raise ValueError("Invalid RDB header")

    entries = {}
    database = 0
    expires_at = None
    wall_time = time.time()
    monotonic_time = time.monotonic()

    while True:
        opcode = reader.read(1)[0]

        if opcode == 0xFF:
            if int(header[5:]) >= 5:
                reader.read(8)  # CRC64 footer; checksum validation is not implemented.

            break

        if opcode == 0xFA:
            reader.string()
            reader.string()
        elif opcode == 0xFE:
            database = reader.plain_length()
        elif opcode == 0xFB:
            reader.plain_length()
            reader.plain_length()
        elif opcode == 0xFC:
            expires_at = int.from_bytes(reader.read(8), "little") / 1000
        elif opcode == 0xFD:
            expires_at = int.from_bytes(reader.read(4), "little")
        elif opcode == 0xF8:
            reader.plain_length()
        elif opcode == 0xF9:
            reader.read(1)
        elif opcode in (0, 1, 5, 15):
            key = decode_text(reader.string())
            entry = read_entry(reader, opcode)

            if database == 0 and (expires_at is None or expires_at > wall_time):
                deadline = monotonic_time + expires_at - wall_time if expires_at is not None else None
                entry.expires_at = deadline
                entries[key] = entry

            expires_at = None
        else:
            raise ValueError(f"Unsupported RDB value type or opcode: {opcode}")

    return entries


def read_entry(reader: RDBReader, opcode: int):
    if opcode == 0:
        return RedisString(value=reader.string())

    if opcode == 1:
        return RedisList(values=deque(decode_text(reader.string()) for _ in range(reader.plain_length())))

    if opcode == 5:
        entries = []

        for _ in range(reader.plain_length()):
            member = decode_text(reader.string())
            score = struct.unpack("<d", reader.read(8))[0]
            entries.append(RedisSortedSetEntry(value=member, score=score))

        return RedisSortedSet(entries=sorted(entries, key=lambda item: (item.score, item.value)))

    entries = []

    for _ in range(reader.plain_length()):
        master = reader.string()

        if len(master) != 16:
            raise ValueError("Invalid RDB stream ID")

        master_ms = int.from_bytes(master[:8], "big")
        master_seq = int.from_bytes(master[8:], "big")
        values = iter(read_listpack(reader.string()))
        live, deleted, field_count = int(next(values)), int(next(values)), int(next(values))
        fields = [decode_text(next(values)) for _ in range(field_count)]
        next(values)  # Master entry terminator.

        for _ in range(live + deleted):
            flags, delta_ms, delta_seq = int(next(values)), int(next(values)), int(next(values))

            if flags & 2:
                pairs = [(field, decode_text(next(values))) for field in fields]
            else:
                pairs = [(decode_text(next(values)), decode_text(next(values))) for _ in range(int(next(values)))]

            next(values)  # Reverse traversal count.

            if not flags & 1:
                entries.append(RedisStreamEntry((master_ms + delta_ms, master_seq + delta_seq), pairs))

    count, last_ms, last_seq, groups = (reader.plain_length() for _ in range(4))

    if groups or count != len(entries):
        raise ValueError("Unsupported RDB stream consumer groups or invalid entry count")

    # This server has no stream trimming or deletion, so the last ID is the last entry.
    if entries and entries[-1].entry_id != (last_ms, last_seq):
        raise ValueError("Unsupported RDB stream last ID")

    return RedisStream(entries=entries)


def encode_length(length: int) -> bytes:
    if length < 64:
        return bytes([length])

    if length < 16384:
        return bytes([0x40 | (length >> 8), length & 255])

    if length < 2 ** 32:
        return b"\x80" + length.to_bytes(4, "big")

    return b"\x81" + length.to_bytes(8, "big")


def encode_string(value: str | bytes) -> bytes:
    data = encode_text(value) if isinstance(value, str) else value

    return encode_length(len(data)) + data


def back_length(length: int) -> bytes:
    parts = [length & 127]
    length >>= 7

    while length:
        parts.append(length & 127)
        length >>= 7

    parts.reverse()

    return bytes([parts[0], *(part | 128 for part in parts[1:])])


def write_listpack(values: list[int | str]) -> bytes:
    body = bytearray()

    for value in values:
        if isinstance(value, int):
            encoded = bytes([value]) if 0 <= value < 128 else b"\xf4" + value.to_bytes(8, "little", signed=True)
        else:
            data = encode_text(value)
            encoded = b"\xf0" + len(data).to_bytes(4, "little") + data

        body.extend(encoded + back_length(len(encoded)))

    return (len(body) + 7).to_bytes(4, "little") + min(len(values), 65535).to_bytes(2, "little") + body + b"\xff"


def read_listpack(data: bytes) -> list[bytes]:
    reader = RDBReader(data)
    total = int.from_bytes(reader.read(4), "little")
    reader.read(2)

    if total != len(data):
        raise ValueError("Invalid RDB listpack length")

    values = []

    while True:
        start = reader.source.tell()
        first = reader.read(1)[0]

        if first == 255:
            return values

        if first < 128:
            value = str(first).encode()
        elif first < 192:
            value = reader.read(first & 63)
        elif first < 224:
            number = ((first & 31) << 8) | reader.read(1)[0]
            value = str(number - 8192 if number & 4096 else number).encode()
        elif first < 240:
            length = ((first & 15) << 8) | reader.read(1)[0]
            value = reader.read(length)
        elif first == 240:
            value = reader.read(int.from_bytes(reader.read(4), "little"))
        elif first in (241, 242, 243, 244):
            size = {241: 2, 242: 3, 243: 4, 244: 8}[first]
            value = str(int.from_bytes(reader.read(size), "little", signed=True)).encode()
        else:
            raise ValueError("Invalid RDB listpack encoding")

        size = reader.source.tell() - start
        reader.read(len(back_length(size)))
        values.append(value)


def dump_rdb(entries: dict) -> bytes:
    output = bytearray(b"REDIS0011\xfe\x00")
    monotonic_time, wall_time = time.monotonic(), time.time()

    for key, entry in entries.items():
        if entry.expires_at is not None:
            if entry.expires_at <= monotonic_time:
                continue

            timestamp = int((wall_time + entry.expires_at - monotonic_time) * 1000)
            output.extend(b"\xfc" + timestamp.to_bytes(8, "little"))

        if isinstance(entry, RedisString):
            opcode, payload = 0, encode_string(entry.value)
        elif isinstance(entry, RedisList):
            opcode = 1
            payload = encode_length(len(entry.values)) + b"".join(encode_string(value) for value in entry.values)
        elif isinstance(entry, RedisSortedSet):
            opcode = 5
            payload = encode_length(len(entry.entries)) + b"".join(
                encode_string(item.value) + struct.pack("<d", item.score) for item in entry.entries
            )
        elif isinstance(entry, RedisStream):
            opcode = 15
            parts = [encode_length(len(entry.entries))]

            for item in entry.entries:
                ms, seq = item.entry_id
                fields = [field for field, _ in item.fields]
                values = [value for _, value in item.fields]
                pack = write_listpack([1, 0, len(fields), *fields, 0, 2, 0, 0, *values, 3 + len(values)])
                parts.extend([encode_string(ms.to_bytes(8, "big") + seq.to_bytes(8, "big")), encode_string(pack)])

            last_ms, last_seq = entry.entries[-1].entry_id if entry.entries else (0, 0)
            parts.extend(encode_length(value) for value in (len(entry.entries), last_ms, last_seq, 0))
            payload = b"".join(parts)
        else:
            raise ValueError("Unsupported snapshot entry type")

        output.extend(bytes([opcode]) + encode_string(key) + payload)

    output.extend(b"\xff" + bytes(8))  # Zero disables Redis CRC64 checking.

    return bytes(output)
