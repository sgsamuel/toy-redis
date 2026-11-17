from app.protocol import RedisResponse

from ..common import CommandContext, CommandResult, wrong_arguments


async def handle_ping(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) > 1:
        return CommandResult(wrong_arguments("PING"))

    if context.session.subscribed:
        return CommandResult(RedisResponse.array([
            RedisResponse.bulk("pong"), RedisResponse.bulk(args[0] if args else ""),
        ]))

    if args:
        return CommandResult(RedisResponse.bulk(args[0]))

    return CommandResult(RedisResponse.simple("PONG"))


async def handle_echo(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("ECHO"))

    return CommandResult(RedisResponse.bulk(args[0]))


async def handle_type(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("TYPE"))

    key_type = context.store.type(args[0])

    return CommandResult(RedisResponse.simple("none" if key_type is None else key_type))


async def handle_quit(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("QUIT"))

    context.pubsub.remove(context.session)
    context.session.closing = True

    return CommandResult(RedisResponse.simple("OK"))


async def handle_reset(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("RESET"))

    context.pubsub.remove(context.session)
    context.session.queued = None
    context.session.transaction_error = False
    context.session.watched.clear()
    context.session.username = None

    return CommandResult(RedisResponse.simple("RESET"))
