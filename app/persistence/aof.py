import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from shlex import quote, split

from app.commands.common import ClientSession
from app.network.config import ServerConfig
from app.protocol import RESPParseError, RESPParser, RedisResponse, encode_command


class AOFManager:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.directory = Path(config.dir) / config.appenddirname
        self.file = self.directory / f"{config.appendfilename}.1.incr.aof"
        self.manifest = self.directory / f"{config.appendfilename}.manifest"

        if config.appendonly != "yes":
            return

        self.directory.mkdir(parents=True, exist_ok=True)

        if self.manifest.exists():
            if not self.manifest.is_file():
                raise ValueError(f"AOF manifest is not a file: {self.manifest}")

            return

        with self.file.open("ab"):
            pass

        # Exclusive creation preserves a manifest written by another startup.
        try:
            with self.manifest.open("x", encoding="utf-8") as output:
                output.write(f"file {quote(self.file.name)} seq 1 type i\n")
        except FileExistsError:
            if not self.manifest.is_file():
                raise ValueError(f"AOF manifest is not a file: {self.manifest}")

    def write(self, command: list[str]) -> None:
        if self.config.appendonly != "yes":
            return

        if command[0].upper() == "SET" and len(command) == 5 and command[3].upper() in {"EX", "PX"}:
            duration = int(command[4]) * (1000 if command[3].upper() == "EX" else 1)
            deadline = int(time.time() * 1000) + duration
            command = [*command[:3], "PXAT", str(deadline)]

        with self.file.open("ab") as output:
            output.write(encode_command(command))

    async def replay(
        self, execute: Callable[[list[str | bytes], ClientSession], Awaitable[RedisResponse]]
    ) -> int:
        if self.config.appendonly != "yes":
            return 0

        files = []
        previous_sequence = 0

        for line in self.manifest.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue

            fields = split(line)

            if len(fields) != 6 or fields[::2] != ["file", "seq", "type"]:
                raise ValueError("Invalid AOF manifest entry")

            filename, sequence, kind = fields[1::2]

            if kind == "h":
                continue

            if kind != "i" or Path(filename).name != filename or int(sequence) <= previous_sequence:
                raise ValueError("Unsupported AOF manifest entry or invalid sequence")

            previous_sequence = int(sequence)
            files.append(self.directory / filename)

        if not files:
            raise ValueError("AOF manifest contains no incremental files")

        session = ClientSession(username="default", trusted_replication=True, replaying_aof=True)
        count = 0

        for path in files:
            reader = asyncio.StreamReader()
            reader.feed_data(path.read_bytes())
            reader.feed_eof()
            parser = RESPParser(reader, raw_bulk=True)

            try:
                while (record := await parser.parse()) is not None:
                    tokens = record.value

                    if not isinstance(tokens, list) or not tokens or not all(isinstance(token, bytes) for token in tokens):
                        raise ValueError(f"Invalid AOF command in {path}")

                    if tokens[0].upper() == b"SELECT":
                        if tokens[1:] != [b"0"]:
                            raise ValueError("Only database 0 is supported during AOF replay")

                        count += 1
                        continue

                    response = await execute(tokens, session)
                    errors = isinstance(response.value, list) and any(isinstance(value, Exception) for value in response.value)

                    if response.is_error or errors:
                        raise ValueError(f"AOF replay failed in {path}: {response.value}")

                    count += 1
            except RESPParseError as error:
                raise ValueError(f"Invalid or truncated AOF file {path}: {error}") from error

        if session.queued is not None:
            raise ValueError("Incomplete transaction at the end of AOF")

        self.file = files[-1]

        return count
