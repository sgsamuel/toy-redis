from dataclasses import asdict
from fnmatch import fnmatchcase

from app.protocol import RedisResponse

from ..common import CommandContext, CommandResult, wrong_arguments


async def handle_config_get(context: CommandContext, args: list[str]) -> CommandResult:
    if not args:
        return CommandResult(wrong_arguments("CONFIG GET"))

    settings = asdict(context.config)
    replies = []

    for name, value in settings.items():
        if any(fnmatchcase(name, pattern) for pattern in args):
            replies.extend([RedisResponse.bulk(name), RedisResponse.bulk(value)])

    return CommandResult(RedisResponse.array(replies))
