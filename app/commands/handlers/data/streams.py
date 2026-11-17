from app.protocol import RedisResponse
from app.data.common import RedisStreamEntry

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_xadd(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 4 or len(args) % 2:
        return CommandResult(wrong_arguments("XADD"))

    key, raw_id, *field_values = args
    fields = list(zip(field_values[::2], field_values[1::2]))
    entry_id = context.store.xadd(key, raw_id, fields)

    return CommandResult(
        RedisResponse.bulk(entry_id),
        mutation=["XADD", key, entry_id, *field_values],
    )


async def handle_xrange(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 3:
        return CommandResult(wrong_arguments("XRANGE"))

    key, start, end = args

    return CommandResult(_stream_entries_response(context.store.xrange(key, start, end)))


async def handle_xread(context: CommandContext, args: list[str]) -> CommandResult:
    block_ms: int | None = None

    if args and args[0].upper() == "BLOCK":
        if len(args) < 5:
            return CommandResult(wrong_arguments("XREAD"))

        try:
            block_ms = int(args[1])
        except ValueError:
            return CommandResult(RedisResponse.error("Invalid timeout for 'XREAD'"))

        if block_ms < 0:
            return CommandResult(RedisResponse.error("Timeout is negative"))

        args = args[2:]

    if len(args) < 3 or args[0].upper() != "STREAMS":
        return CommandResult(wrong_arguments("XREAD"))

    streams = args[1:]

    if len(streams) % 2:
        return CommandResult(RedisResponse.error("XREAD requires one ID per stream key"))

    midpoint = len(streams) // 2
    requests = list(zip(streams[:midpoint], streams[midpoint:]))

    if block_ms is None or context.session.executing_transaction:
        if block_ms is not None:
            requests = [(key, "18446744073709551615-18446744073709551615" if entry_id == "$" else entry_id)
                        for key, entry_id in requests]

        result = context.store.xread(requests)
    else:
        result = await context.store.xread_blocking(requests, block_ms)

    if not result:
        return CommandResult(RedisResponse.array(None))

    return CommandResult(RedisResponse.array([
        RedisResponse.array([RedisResponse.bulk(key), _stream_entries_response(entries)])
        for key, entries in result
    ]))


def _stream_entries_response(entries: list[RedisStreamEntry]) -> RedisResponse:
    replies = []

    for entry in entries:
        ms, seq = entry.entry_id
        fields = [
            RedisResponse.bulk(item)
            for field, value in entry.fields
            for item in (field, value)
        ]
        replies.append(RedisResponse.array([
            RedisResponse.bulk(f"{ms}-{seq}"),
            RedisResponse.array(fields),
        ]))

    return RedisResponse.array(replies)
