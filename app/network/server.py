import asyncio
from contextlib import suppress

from app.commands.executor import CommandExecutor
from app.protocol import RESPParseError, RESPParser, RedisResponse
from app.replication.replica import follow_primary
from app.commands.common import ClientSession


async def process_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    executor: CommandExecutor,
) -> None:
    parser = RESPParser(reader, raw_bulk=True)
    default = executor.acl.users["default"]
    session = ClientSession(username="default" if "nopass" in default.flags else None)
    incoming: asyncio.Queue[list | RedisResponse] = asyncio.Queue(maxsize=256)

    async def read_commands() -> None:
        try:
            while True:
                parsed = await parser.parse()

                if parsed is None:
                    return

                tokens = parsed.value

                if not isinstance(tokens, list) or not tokens:
                    await incoming.put(RedisResponse.error("Expected array of bulk strings"))
                else:
                    await incoming.put(tokens)
        except RESPParseError as err:
            await incoming.put(RedisResponse.error(str(err)))
            await incoming.join()
            await session.outgoing.join()

    async def execute_commands() -> None:
        while not session.closing:
            command = await incoming.get()

            try:
                response = command if isinstance(command, RedisResponse) else await executor.execute(command, session)
                await session.enqueue(response)

                if session.replica_ack_offset is not None:
                    executor.replication.replica_writers[writer] = session
            finally:
                incoming.task_done()

        await session.outgoing.join()

    async def write_responses() -> None:
        while True:
            response = await session.outgoing.get()

            try:
                writer.write(response.raw)
                await writer.drain()
            finally:
                session.queued_bytes -= len(response.raw)
                session.outgoing.task_done()

    tasks = [
        asyncio.create_task(read_commands()),
        asyncio.create_task(execute_commands()),
        asyncio.create_task(write_responses()),
        asyncio.create_task(session.disconnected.wait()),
    ]

    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            task.result()
    except (ConnectionError, OSError):
        pass
    finally:
        session.disconnected.set()
        executor.pubsub.remove(session)
        executor.replication.replica_writers.pop(writer, None)
        executor.replication.ack_changed.set()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        writer.close()

        with suppress(ConnectionError, OSError):
            await writer.wait_closed()


async def serve(executor: CommandExecutor, host: str, port: int) -> None:
    server = await asyncio.start_server(
        lambda reader, writer: process_client(reader, writer, executor), host, port
    )
    async with server, asyncio.TaskGroup() as tasks:
        if executor.replication.replica_of is not None:
            listen_port = server.sockets[0].getsockname()[1]
            session = ClientSession()

            async def apply_command(tokens: list[str]) -> RedisResponse:
                return await executor.apply_replication(tokens, session)

            tasks.create_task(
                follow_primary(executor.replication, listen_port, apply_command, executor.store.restore_snapshot)
            )
        await server.serve_forever()
