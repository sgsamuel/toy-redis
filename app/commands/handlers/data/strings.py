import time

from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_get(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("GET"))

    return CommandResult(RedisResponse.bulk(context.store.get(args[0])))


async def handle_set(context: CommandContext, args: list[str]) -> CommandResult:
    match args:
        case [key, value]:
            context.store.set(key, value)
        case [key, value, flag, ttl] if (unit := flag.upper()) in ("EX", "PX", "PXAT"):
            try:
                duration = int(ttl)
            except ValueError:
                return CommandResult(RedisResponse.error("Invalid expiration for 'SET'"))

            if duration <= 0:
                return CommandResult(RedisResponse.error("Invalid expiration for 'SET'"))

            milliseconds = duration * 1000 if unit == "EX" else duration

            if unit == "PXAT":
                milliseconds -= int(time.time() * 1000)
            context.store.set(key, value, px=milliseconds)
        case _:
            return CommandResult(wrong_arguments("SET"))

    return CommandResult(RedisResponse.simple("OK"), mutation=["SET", *args])


async def handle_incr(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("INCR"))

    key = args[0]
    value = context.store.incr(key)

    return CommandResult(RedisResponse.integer(value), mutation=["INCR", *args])


async def handle_strlen(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("STRLEN"))

    return CommandResult(RedisResponse.integer(context.store.strlen(args[0])))
