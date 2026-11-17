from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_keys(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("KEYS"))

    keys = context.store.keys(args[0])

    return CommandResult(RedisResponse.array([RedisResponse.bulk(key) for key in keys]))
