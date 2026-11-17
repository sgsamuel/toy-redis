from math import isfinite

from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_rpush(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments("RPUSH"))

    key, *values = args

    return CommandResult(
        RedisResponse.integer(context.store.rpush(key, *values)),
        mutation=["RPUSH", *args],
    )


async def handle_lpush(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments("LPUSH"))

    key, *values = args

    return CommandResult(
        RedisResponse.integer(context.store.lpush(key, *values)),
        mutation=["LPUSH", *args],
    )


async def handle_lrange(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 3:
        return CommandResult(wrong_arguments("LRANGE"))

    key, start, stop = args

    try:
        start_index, stop_index = (int(start), int(stop))
    except ValueError:
        return CommandResult(RedisResponse.error("Invalid index for 'LRANGE'"))

    values = context.store.lrange(key, start_index, stop_index)

    return CommandResult(RedisResponse.array([RedisResponse.bulk(value) for value in values]))


async def handle_llen(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("LLEN"))

    return CommandResult(RedisResponse.integer(context.store.llen(args[0])))


async def handle_lpop(context: CommandContext, args: list[str]) -> CommandResult:
    match args:
        case [key]:
            values = context.store.lpop(key, 1)

            return CommandResult(
                RedisResponse.bulk(values[0] if values else None),
                mutation=["LPOP", key] if values else None,
            )
        case [key, count]:
            try:
                limit = int(count)
            except ValueError:
                return CommandResult(RedisResponse.error("Invalid count for 'LPOP'"))

            values = context.store.lpop(key, limit)

            return CommandResult(
                RedisResponse.array([RedisResponse.bulk(value) for value in values]),
                mutation=["LPOP", key, str(len(values))] if values else None,
            )
        case _:
            return CommandResult(wrong_arguments("LPOP"))


async def handle_blpop(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("BLPOP"))

    key, raw_timeout = args

    try:
        timeout = float(raw_timeout)
    except ValueError:
        return CommandResult(RedisResponse.error("Invalid timeout for 'BLPOP'"))

    if not isfinite(timeout) or timeout < 0:
        return CommandResult(RedisResponse.error("Invalid timeout for 'BLPOP'"))

    if context.session.executing_transaction:
        values = context.store.lpop(key)
        value = values[0] if values else None
    else:
        value = await context.store.blpop(key, timeout)

    if value is None:
        return CommandResult(RedisResponse.array(None))

    return CommandResult(
        RedisResponse.array([RedisResponse.bulk(key), RedisResponse.bulk(value)]),
        mutation=["LPOP", key],
    )
