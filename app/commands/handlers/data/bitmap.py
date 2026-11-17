from app.protocol import RedisResponse

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_setbit(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 3:
        return CommandResult(wrong_arguments("SETBIT"))

    key, raw_offset, raw_value = args

    try:
        offset = int(raw_offset)
    except ValueError:
        return CommandResult(RedisResponse.error("bit offset is not an integer or out of range"))

    if offset < 0 or offset >= 2 ** 32:
        return CommandResult(RedisResponse.error("bit offset is not an integer or out of range"))

    if raw_value not in ("0", "1"):
        return CommandResult(RedisResponse.error("bit is not an integer or out of range"))

    previous = context.store.setbit(key, offset, int(raw_value))

    return CommandResult(
        RedisResponse.integer(previous),
        mutation=["SETBIT", *args],
    )


async def handle_getbit(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("GETBIT"))

    key, raw_offset = args

    try:
        offset = int(raw_offset)
    except ValueError:
        return CommandResult(RedisResponse.error("bit offset is not an integer or out of range"))

    if offset < 0 or offset >= 2 ** 32:
        return CommandResult(RedisResponse.error("bit offset is not an integer or out of range"))

    return CommandResult(RedisResponse.integer(context.store.getbit(key, offset)))


async def handle_bitcount(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) not in (1, 3, 4):
        return CommandResult(wrong_arguments("BITCOUNT"))

    key = args[0]
    start, end = 0, -1
    unit = args[3].upper() if len(args) == 4 else "BYTE"

    if unit not in ("BYTE", "BIT"):
        return CommandResult(wrong_arguments("BITCOUNT"))

    if len(args) >= 3:
        try:
            start, end = (int(args[1]), int(args[2]))
        except ValueError:
            return CommandResult(RedisResponse.error("Invalid index for 'BITCOUNT'"))

    count = context.store.bitcount(key, start, end, unit)

    return CommandResult(RedisResponse.integer(count))


async def handle_bitop_and(context: CommandContext, args: list[str]) -> CommandResult:
    return _handle_bitop(context, args, "AND")


async def handle_bitop_or(context: CommandContext, args: list[str]) -> CommandResult:
    return _handle_bitop(context, args, "OR")


def _handle_bitop(context: CommandContext, args: list[str], operation: str) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments(f"BITOP {operation}"))

    destination, *keys = args
    length = context.store.bitop(operation, destination, *keys)

    return CommandResult(
        RedisResponse.integer(length),
        mutation=["BITOP", operation, destination, *keys],
    )
