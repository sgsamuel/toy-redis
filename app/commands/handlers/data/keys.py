from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_keys(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("KEYS"))

    keys = context.store.keys(args[0])

    return CommandResult(RedisResponse.array([RedisResponse.bulk(key) for key in keys]))


def ttl_handler(*, milliseconds: bool = False, absolute: bool = False):
    async def handle(context: CommandContext, args: list[str]) -> CommandResult:
        return CommandResult(RedisResponse.integer(
            context.store.ttl(args[0], milliseconds=milliseconds, absolute=absolute)))
    return handle


def expire_handler(*, milliseconds: bool = False, absolute: bool = False):
    async def handle(context: CommandContext, args: list[str]) -> CommandResult:
        import time

        try:
            duration = int(args[1])
        except ValueError:
            return CommandResult(RedisResponse.error("value is not an integer or out of range"))
        if not -(2 ** 63) <= duration < 2 ** 63:
            return CommandResult(RedisResponse.error("value is not an integer or out of range"))
        condition = args[2].upper() if len(args) == 3 else None
        if condition is not None and condition not in ("NX", "XX", "GT", "LT"):
            return CommandResult(RedisResponse.error("syntax error"))
        duration *= 1 if milliseconds else 1000
        now = int(time.time() * 1000)
        deadline = duration if absolute else now + duration
        changed = context.store.expire(args[0], deadline - now, condition)
        return CommandResult(RedisResponse.integer(int(changed)),
                             mutation=["PEXPIREAT", args[0], str(deadline)] if changed else None)
    return handle


async def handle_persist(context: CommandContext, args: list[str]) -> CommandResult:
    changed = context.store.persist(args[0])
    return CommandResult(RedisResponse.integer(int(changed)),
                         mutation=["PERSIST", *args] if changed else None)
