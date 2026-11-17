from app.protocol import RedisResponse
from app.replication.primary import acknowledged_replicas, wait_for_replicas

from ..common import CommandContext, CommandResult, wrong_arguments


async def handle_info(context: CommandContext, args: list[str]) -> CommandResult:
    replication = context.replication

    if args and "replication" not in {arg.lower() for arg in args}:
        return CommandResult(RedisResponse.bulk(""))

    lines = ["# Replication"]

    if replication.replica_of is None:
        lines.append("role:master")
    else:
        host, port = replication.replica_of
        lines.extend([
            "role:slave",
            f'master_host:{host}',
            f'master_port:{port}',
            f"master_link_status:{"up" if replication.master_link_up else "down"}",
        ])

    lines.extend([
        f'master_replid:{replication.replication_id}',
        f'master_repl_offset:{replication.offset}',
    ])

    return CommandResult(RedisResponse.bulk("\r\n".join(lines) + "\r\n"))


async def handle_replconf_ack(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("REPLCONF ACK"))

    try:
        offset = int(args[0])
    except ValueError:
        return CommandResult(RedisResponse.error("Invalid replication offset"))

    if offset < 0:
        return CommandResult(RedisResponse.error("Invalid replication offset"))

    previous = context.session.replica_ack_offset

    if previous is not None:
        context.session.replica_ack_offset = max(previous, offset)
        context.replication.ack_changed.set()

    return CommandResult(RedisResponse(b''))


async def handle_replconf_getack(context: CommandContext, args: list[str]) -> CommandResult:
    if args != ["*"]:
        return CommandResult(wrong_arguments("REPLCONF GETACK"))

    if context.replication.replica_of is None:
        return CommandResult(RedisResponse(b''))

    return CommandResult(RedisResponse.array([
        RedisResponse.bulk("REPLCONF"),
        RedisResponse.bulk("ACK"),
        RedisResponse.bulk(str(context.replication.offset)),
    ]))


async def handle_replconf_option(context: CommandContext, args: list[str]) -> CommandResult:
    if not args or len(args) % 2 != 1:
        return CommandResult(wrong_arguments("REPLCONF"))

    return CommandResult(RedisResponse.simple("OK"))


async def handle_psync(context: CommandContext, args: list[str]) -> CommandResult:
    replication = context.replication
    session = context.session

    if len(args) != 2:
        return CommandResult(wrong_arguments("PSYNC"))

    if session.queued is not None:
        return CommandResult(RedisResponse.error("PSYNC is not allowed inside MULTI"))

    snapshot = context.store.snapshot()
    status = RedisResponse.simple(f'FULLRESYNC {replication.replication_id} {replication.offset}')

    return CommandResult(
        RedisResponse(status.raw + f'${len(snapshot)}\r\n'.encode() + snapshot),
        register_replica=True,
    )


async def handle_wait(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 2:
        return CommandResult(wrong_arguments("WAIT"))

    try:
        required, timeout_ms = map(int, args)
    except ValueError:
        return CommandResult(RedisResponse.error("WAIT expects integer arguments"))

    if required < 0 or timeout_ms < 0:
        return CommandResult(RedisResponse.error("WAIT arguments must be non-negative"))

    if context.replication.replica_of is not None:
        return CommandResult(RedisResponse.error("WAIT cannot be used with replicas"))

    target = context.session.last_write_offset

    if context.session.executing_transaction:
        count = acknowledged_replicas(context.replication, target)
    else:
        count = await wait_for_replicas(context.replication, target, required, timeout_ms)

    return CommandResult(RedisResponse.integer(count))
