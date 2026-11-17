from app.protocol import RedisResponse

from ..common import CommandContext, CommandResult, wrong_arguments


def _subscribe(context: CommandContext, args: list[str], kind: str, command: str) -> CommandResult:
    if not args:
        return CommandResult(wrong_arguments(command))

    return CommandResult(context.pubsub.subscribe(context.session, args, kind))


async def handle_subscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return _subscribe(context, args, "channels", "SUBSCRIBE")


async def handle_psubscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return _subscribe(context, args, "patterns", "PSUBSCRIBE")


async def handle_ssubscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return _subscribe(context, args, "shard_channels", "SSUBSCRIBE")


async def handle_unsubscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(context.pubsub.unsubscribe(context.session, args, "channels"))


async def handle_punsubscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(context.pubsub.unsubscribe(context.session, args, "patterns"))


async def handle_sunsubscribe(context: CommandContext, args: list[str]) -> CommandResult:
    return CommandResult(context.pubsub.unsubscribe(context.session, args, "shard_channels"))


async def handle_publish(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("PUBLISH"))

    channel, message = args
    count = context.pubsub.publish(channel, message)

    return CommandResult(RedisResponse.integer(count))


async def handle_spublish(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("SPUBLISH"))

    channel, message = args
    count = context.pubsub.publish(channel, message, shard=True)

    return CommandResult(RedisResponse.integer(count))
