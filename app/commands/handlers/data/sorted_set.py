from math import isnan

from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_zadd(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 3 or len(args) % 2 != 1:
        return CommandResult(wrong_arguments("ZADD"))

    key, *score_members = args
    pairs = []

    for raw_score, member in zip(score_members[::2], score_members[1::2]):
        try:
            score = float(raw_score)
        except ValueError:
            return CommandResult(RedisResponse.error("value is not a valid float"))

        if isnan(score):
            return CommandResult(RedisResponse.error("value is not a valid float"))

        pairs.append((score, member))

    added = context.store.zadd(key, *pairs)

    return CommandResult(
        RedisResponse.integer(added),
        mutation=["ZADD", *args],
    )


async def handle_zrank(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("ZRANK"))

    key, member = args
    rank = context.store.zrank(key, member)

    if rank is None:
        return CommandResult(RedisResponse.null())

    return CommandResult(RedisResponse.integer(rank))


async def handle_zrange(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 3:
        return CommandResult(wrong_arguments("ZRANGE"))

    key, start, stop = args

    try:
        start_index, stop_index = (int(start), int(stop))
    except ValueError:
        return CommandResult(RedisResponse.error("Invalid index for 'ZRANGE'"))

    values = context.store.zrange(key, start_index, stop_index)

    return CommandResult(RedisResponse.array([RedisResponse.bulk(value) for value in values]))


async def handle_zcard(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("ZCARD"))

    return CommandResult(RedisResponse.integer(context.store.zcard(args[0])))


async def handle_zscore(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("ZSCORE"))

    key, member = args
    score = context.store.zscore(key, member)

    if score is None:
        return CommandResult(RedisResponse.null())

    value = str(int(score)) if score.is_integer() else str(score)

    return CommandResult(RedisResponse.bulk(value))


async def handle_zrem(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments("ZREM"))

    key, *members = args
    removed = context.store.zrem(key, *members)

    return CommandResult(
        RedisResponse.integer(removed),
        mutation=["ZREM", *args] if removed else None,
    )
