import asyncio

from .response import RedisResponse


class RESPParseError(Exception):
    """Raised when incoming bytes do not conform to the RESP2 protocol."""


class RESPParser:
    def __init__(self, reader: asyncio.StreamReader, *, raw_bulk: bool = False):
        self._reader = reader
        self._raw_bulk = raw_bulk

    async def parse(self) -> RedisResponse | None:
        """Read a RESP value, returning None only at a clean end of stream."""
        try:
            prefix = await self._reader.readexactly(1)
        except asyncio.IncompleteReadError:
            return None

        try:
            match prefix:
                case b"+":
                    return RedisResponse.simple((await self._readline()).decode())
                case b"-":
                    error = (await self._readline()).decode()
                    prefix_str, _, message = error.partition(" ")
                    return RedisResponse.error(
                        message or prefix_str,
                        prefix=prefix_str if message else "ERR",
                    )
                case b":":
                    return RedisResponse.integer(await self._read_integer())
                case b"$":
                    return await self._parse_bulk_string()
                case b"*":
                    return await self._parse_array()
                case _:
                    raise RESPParseError(f"Unknown RESP type prefix: {prefix!r}")
        except ValueError as exc:
            raise RESPParseError(str(exc)) from exc

    async def _readline(self) -> bytes:
        try:
            return (await self._reader.readuntil(b"\r\n"))[:-2]
        except asyncio.IncompleteReadError as exc:
            raise RESPParseError("Connection closed mid-line") from exc
        except asyncio.LimitOverrunError as exc:
            raise RESPParseError("RESP line exceeds the reader limit") from exc

    async def _read_integer(self) -> int:
        raw = await self._readline()
        digits = raw[1:] if raw.startswith((b"-", b"+")) else raw
        if not digits or not digits.isdigit():
            raise RESPParseError("Invalid RESP integer")
        return int(raw)

    async def _read_length(self) -> int:
        length = await self._read_integer()
        if length < -1:
            raise RESPParseError("RESP length must be non-negative or -1")
        return length

    async def _parse_bulk_string(self) -> RedisResponse:
        length = await self._read_length()
        if length == -1:
            return RedisResponse.null()

        try:
            data = await self._reader.readexactly(length + 2)
        except asyncio.IncompleteReadError as exc:
            raise RESPParseError("Connection closed mid-bulk-string") from exc
        if not data.endswith(b"\r\n"):
            raise RESPParseError("Bulk string missing trailing CRLF")
        response = RedisResponse.bulk(data[:-2])

        if self._raw_bulk:
            response.value = data[:-2]

        return response

    async def _parse_array(self) -> RedisResponse:
        count = await self._read_length()
        if count == -1:
            return RedisResponse.array(None)

        items = []
        for _ in range(count):
            item = await self.parse()
            if item is None:
                raise RESPParseError("Connection closed mid-array")
            items.append(item)
        return RedisResponse.array(items)
