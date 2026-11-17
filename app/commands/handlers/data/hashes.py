from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult


async def handle_hset(context: CommandContext, args: list[str]) -> CommandResult:
    key, *fields = args
    version = context.store.get_key_version(key)
    added = context.store.hset(key, *zip(fields[::2], fields[1::2]))
    return CommandResult(
        RedisResponse.integer(added),
        mutation=["HSET", *args] if context.store.get_key_version(key) != version else None,
    )


async def handle_hget(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(RedisResponse.bulk(context.store.hget(*args)))


async def handle_hdel(context: CommandContext, args: list[str]) -> CommandResult:
    key, *fields = args
    removed = context.store.hdel(key, *fields)
    return CommandResult(
        RedisResponse.integer(removed),
        mutation=["HDEL", *args] if removed else None,
    )


async def handle_hgetall(context: CommandContext, args: list[str]) -> CommandResult:
    fields = context.store.hgetall(args[0])
    return CommandResult(RedisResponse.array([
        RedisResponse.bulk(value) for pair in fields.items() for value in pair
    ]))


async def handle_hexists(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(RedisResponse.integer(int(context.store.hexists(*args))))


async def handle_hlen(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(RedisResponse.integer(context.store.hlen(args[0])))
