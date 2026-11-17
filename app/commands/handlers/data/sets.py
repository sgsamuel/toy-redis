from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult


async def handle_sadd(context: CommandContext, args: list[str]) -> CommandResult:
    key, *members = args
    added = context.store.sadd(key, *members)
    return CommandResult(
        RedisResponse.integer(added),
        mutation=["SADD", *args] if added else None,
    )


async def handle_sismember(context: CommandContext, args: list[str]) -> CommandResult:
    key, member = args
    return CommandResult(RedisResponse.integer(int(context.store.sismember(key, member))))


async def handle_scard(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(RedisResponse.integer(context.store.scard(args[0])))


async def handle_srem(context: CommandContext, args: list[str]) -> CommandResult:
    key, *members = args
    removed = context.store.srem(key, *members)
    return CommandResult(
        RedisResponse.integer(removed),
        mutation=["SREM", *args] if removed else None,
    )


async def handle_smembers(context: CommandContext, args: list[str]) -> CommandResult:
    members = context.store.smembers(args[0])
    return CommandResult(RedisResponse.array([RedisResponse.bulk(member) for member in members]))


async def handle_sinter(context: CommandContext, args: list[str]) -> CommandResult:
    members = context.store.sinter(*args)
    return CommandResult(RedisResponse.array([RedisResponse.bulk(member) for member in members]))


async def handle_sunion(context: CommandContext, args: list[str]) -> CommandResult:
    members = context.store.sunion(*args)
    return CommandResult(RedisResponse.array([RedisResponse.bulk(member) for member in members]))
