import time

from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_get(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("GET"))

    return CommandResult(RedisResponse.bulk(context.store.get(args[0])))


async def handle_set(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments("SET"))
    key, value, *options = args
    condition = None
    milliseconds = None
    deadline = None
    index = 0
    while index < len(options):
        option = options[index].upper()
        if option in ("NX", "XX") and condition is None:
            condition = option
        elif option in ("EX", "PX", "EXAT", "PXAT") and milliseconds is None and index + 1 < len(options):
            index += 1
            try:
                duration = int(options[index])
            except ValueError:
                return CommandResult(RedisResponse.error("Invalid expiration for 'SET'"))
            if duration <= 0:
                return CommandResult(RedisResponse.error("Invalid expiration for 'SET'"))
            duration *= 1000 if option in ("EX", "EXAT") else 1
            deadline = duration if option in ("EXAT", "PXAT") else int(time.time() * 1000) + duration
            milliseconds = deadline - int(time.time() * 1000)
        else:
            return CommandResult(RedisResponse.error("syntax error"))
        index += 1
    if not context.store.set(key, value, px=milliseconds, condition=condition):
        return CommandResult(RedisResponse.bulk(None))
    mutation = ["SET", key, value]
    if deadline is not None:
        mutation.extend(["PXAT", str(deadline)])
    return CommandResult(RedisResponse.simple("OK"), mutation=mutation)


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


async def handle_decr(context: CommandContext, args: list[str]) -> CommandResult:
    value = context.store.incr(args[0], -1)
    return CommandResult(RedisResponse.integer(value), mutation=["DECR", *args])
