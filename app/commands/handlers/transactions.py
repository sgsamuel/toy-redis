from app.protocol import RedisResponse

from ..common import CommandContext, CommandResult, ExecuteTransaction, wrong_arguments


async def handle_multi(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("MULTI"))

    if context.session.queued is not None:
        return CommandResult(RedisResponse.error("MULTI calls can not be nested"))

    context.session.queued = []
    context.session.transaction_error = False

    return CommandResult(RedisResponse.simple("OK"))


async def handle_exec(
    context: CommandContext, args: list[str]
) -> CommandResult | ExecuteTransaction:
    if args:
        return CommandResult(wrong_arguments("EXEC"))

    session = context.session
    queued = session.queued

    if queued is None:
        return CommandResult(RedisResponse.error("EXEC without MULTI"))

    changed = any(
        context.store.get_key_version(key) != version
        for key, version in session.watched.items()
    )
    session.queued = None
    session.watched.clear()
    invalid = session.transaction_error
    session.transaction_error = False

    if invalid:
        return CommandResult(RedisResponse.error(
            "Transaction discarded because of previous errors.", prefix="EXECABORT"
        ))

    if changed:
        return CommandResult(RedisResponse.array(None))

    return ExecuteTransaction(queued)


async def handle_discard(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("DISCARD"))

    if context.session.queued is None:
        return CommandResult(RedisResponse.error("DISCARD without MULTI"))

    context.session.queued = None
    context.session.transaction_error = False
    context.session.watched.clear()

    return CommandResult(RedisResponse.simple("OK"))


async def handle_watch(context: CommandContext, args: list[str]) -> CommandResult:
    if not args:
        return CommandResult(wrong_arguments("WATCH"))

    if context.session.queued is not None:
        return CommandResult(RedisResponse.error("WATCH inside MULTI is not allowed"))

    for key in args:
        context.session.watched.setdefault(key, context.store.get_key_version(key))

    return CommandResult(RedisResponse.simple("OK"))


async def handle_unwatch(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("UNWATCH"))

    if context.session.queued is not None:
        return CommandResult(RedisResponse.error("UNWATCH inside MULTI is not allowed"))

    context.session.watched.clear()

    return CommandResult(RedisResponse.simple("OK"))
