import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress

from app.protocol import RESPParseError, RESPParser, RedisResponse, encode_command

from .primary import ReplicationState
from app.protocol.encoding import decode_text


async def _send_command(writer: asyncio.StreamWriter, *tokens: str) -> None:
    writer.write(encode_command(tokens))
    await writer.drain()


ApplyCommand = Callable[[list[str]], Awaitable[RedisResponse]]


async def follow_primary(
    replication: ReplicationState, listen_port: int, apply_command: ApplyCommand,
    restore_snapshot: Callable[[bytes], None],
) -> None:
    if replication.replica_of is None:
        return

    writer = None
    try:
        reader, writer = await asyncio.open_connection(*replication.replica_of)

        send_handshake = [
            (("PING",), b"+PONG\r\n"),
            (("REPLCONF", "listening-port", str(listen_port)), b"+OK\r\n"),
            (("REPLCONF", "capa", "psync2"), b"+OK\r\n")
        ]
        for command, expected in send_handshake:
            await _send_command(writer, *command)
            if await reader.readuntil(b"\r\n") != expected:
                raise ValueError(f"Unexpected response to {command[0]}")

        await _send_command(writer, "PSYNC", "?", "-1")

        reply = (await reader.readuntil(b"\r\n")).split()
        if len(reply) != 3 or reply[0] != b"+FULLRESYNC":
            raise ValueError(f"Expected FULLRESYNC, got {reply!r}")

        replid = reply[1].decode("ascii")
        offset = int(reply[2])

        header = await reader.readline()
        while header == b"\n":
            header = await reader.readline()

        if not header.startswith(b"$") or not header.endswith(b"\r\n"):
            raise ValueError("Expected an RDB length header")

        size = int(header[1:-2])
        if size <= 0:
            raise ValueError("Invalid RDB length")

        snapshot = await reader.readexactly(size)
        restore_snapshot(snapshot)

        replication.replication_id = replid
        replication.offset = offset
        replication.master_link_up = True

        parser = RESPParser(reader, raw_bulk=True)
        while True:
            parsed = await parser.parse()
            if parsed is None:
                break

            tokens = parsed.value

            if isinstance(tokens, list):
                tokens = [decode_text(token) if isinstance(token, bytes) else token for token in tokens]

            if not isinstance(tokens, list) or not all(isinstance(t, str) for t in tokens):
                raise RESPParseError("Expected array of bulk strings")
            reply = await apply_command(tokens)

            if reply.is_error:
                raise ValueError(f"Failed to apply replication command: {reply.value}")

            replication.offset += len(parsed.raw)

            if (
                len(tokens) == 3
                and tokens[0].upper() == "REPLCONF"
                and tokens[1].upper() == "GETACK"
                and tokens[2] == "*"
                and reply.raw
            ):
                writer.write(reply.raw)
                await writer.drain()
    except (OSError, EOFError, ValueError, RESPParseError) as err:
        logging.getLogger(__name__).warning("Replication link failed: %s", err)
    finally:
        replication.master_link_up = False
        if writer is not None:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
