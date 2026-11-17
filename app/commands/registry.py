from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.commands.common import CommandContext, CommandResult, ExecuteTransaction, wrong_arguments
from app.commands.errors import CommandError
from app.commands.request import CommandRequest
from app.protocol import RedisResponse
from app.commands.handlers import auth, config, connection, pubsub, replication, transactions
from app.commands.handlers.data import bitmaps, geo, hashes, keys, lists, sets, sorted_sets, streams, strings


Handler = Callable[[CommandContext, list[str]], Awaitable[CommandResult | ExecuteTransaction]]


@dataclass(frozen=True)
class Command:
    handler: Handler | None = None
    writes: bool = False
    transaction_control: bool = False
    subcommands: dict[str, "Command"] | None = None
    min_args: int = 0
    max_args: int | None = None
    argument_step: int = 1
    argument_counts: tuple[int, ...] | None = None


COMMANDS: dict[str, Command] = {
    "CONFIG": Command(subcommands={
        "GET": Command(config.handle_config_get, min_args=1),
    }),
    "KEYS": Command(keys.handle_keys, min_args=1, max_args=1),
    "ACL": Command(subcommands={
        "WHOAMI": Command(auth.handle_acl_whoami, max_args=0),
        "GETUSER": Command(auth.handle_acl_getuser, min_args=1, max_args=1),
        "SETUSER": Command(auth.handle_acl_setuser, min_args=1),
    }),
    "AUTH": Command(auth.handle_auth, min_args=1, max_args=2),
    "PING": Command(connection.handle_ping, max_args=1),
    "QUIT": Command(connection.handle_quit, transaction_control=True, max_args=0),
    "RESET": Command(connection.handle_reset, transaction_control=True, max_args=0),
    "SUBSCRIBE": Command(pubsub.handle_subscribe, min_args=1),
    "UNSUBSCRIBE": Command(pubsub.handle_unsubscribe),
    "PSUBSCRIBE": Command(pubsub.handle_psubscribe, min_args=1),
    "PUNSUBSCRIBE": Command(pubsub.handle_punsubscribe),
    "SSUBSCRIBE": Command(pubsub.handle_ssubscribe, min_args=1),
    "SUNSUBSCRIBE": Command(pubsub.handle_sunsubscribe),
    "PUBLISH": Command(pubsub.handle_publish, min_args=2, max_args=2),
    "SPUBLISH": Command(pubsub.handle_spublish, min_args=2, max_args=2),
    "ECHO": Command(connection.handle_echo, min_args=1, max_args=1),
    "GET": Command(strings.handle_get, min_args=1, max_args=1),
    "SET": Command(strings.handle_set, writes=True, min_args=2),
    "INCR": Command(strings.handle_incr, writes=True, min_args=1, max_args=1),
    "DECR": Command(strings.handle_decr, writes=True, min_args=1, max_args=1),
    "TTL": Command(keys.ttl_handler(), min_args=1, max_args=1),
    "PTTL": Command(keys.ttl_handler(milliseconds=True), min_args=1, max_args=1),
    "EXPIRETIME": Command(keys.ttl_handler(absolute=True), min_args=1, max_args=1),
    "PEXPIRETIME": Command(keys.ttl_handler(milliseconds=True, absolute=True), min_args=1, max_args=1),
    "EXPIRE": Command(keys.expire_handler(), writes=True, min_args=2, max_args=3),
    "PEXPIRE": Command(keys.expire_handler(milliseconds=True), writes=True, min_args=2, max_args=3),
    "EXPIREAT": Command(keys.expire_handler(absolute=True), writes=True, min_args=2, max_args=3),
    "PEXPIREAT": Command(keys.expire_handler(milliseconds=True, absolute=True), writes=True, min_args=2, max_args=3),
    "PERSIST": Command(keys.handle_persist, writes=True, min_args=1, max_args=1),
    "SETBIT": Command(bitmaps.handle_setbit, writes=True, min_args=3, max_args=3),
    "GETBIT": Command(bitmaps.handle_getbit, min_args=2, max_args=2),
    "STRLEN": Command(strings.handle_strlen, min_args=1, max_args=1),
    "BITCOUNT": Command(bitmaps.handle_bitcount, min_args=1, max_args=4, argument_counts=(1, 3, 4)),
    "BITOP": Command(subcommands={
        "AND": Command(bitmaps.handle_bitop_and, writes=True, min_args=2),
        "OR": Command(bitmaps.handle_bitop_or, writes=True, min_args=2),
    }),
    "RPUSH": Command(lists.handle_rpush, writes=True, min_args=2),
    "LPUSH": Command(lists.handle_lpush, writes=True, min_args=2),
    "LPOP": Command(lists.handle_lpop, writes=True, min_args=1, max_args=2),
    "BLPOP": Command(lists.handle_blpop, writes=True, min_args=2, max_args=2),
    "LLEN": Command(lists.handle_llen, min_args=1, max_args=1),
    "LRANGE": Command(lists.handle_lrange, min_args=3, max_args=3),
    "TYPE": Command(connection.handle_type, min_args=1, max_args=1),
    "XADD": Command(streams.handle_xadd, writes=True, min_args=4, argument_step=2),
    "XRANGE": Command(streams.handle_xrange, min_args=3, max_args=3),
    "XREAD": Command(streams.handle_xread, min_args=3),
    "INFO": Command(replication.handle_info),
    "REPLCONF": Command(subcommands={
        "ACK": Command(replication.handle_replconf_ack, min_args=1, max_args=1),
        "GETACK": Command(replication.handle_replconf_getack, min_args=1, max_args=1),
        "LISTENING-PORT": Command(replication.handle_replconf_option, min_args=1),
        "IP-ADDRESS": Command(replication.handle_replconf_option, min_args=1),
        "CAPA": Command(replication.handle_replconf_option, min_args=1),
    }),
    "PSYNC": Command(replication.handle_psync, min_args=2, max_args=2),
    "WAIT": Command(replication.handle_wait, min_args=2, max_args=2),
    "MULTI": Command(transactions.handle_multi, transaction_control=True, max_args=0),
    "EXEC": Command(transactions.handle_exec, transaction_control=True, max_args=0),
    "DISCARD": Command(transactions.handle_discard, transaction_control=True, max_args=0),
    "WATCH": Command(transactions.handle_watch, transaction_control=True, min_args=1),
    "UNWATCH": Command(transactions.handle_unwatch, transaction_control=True, max_args=0),
    "GEOADD": Command(geo.handle_geoadd, writes=True, min_args=4),
    "GEOPOS": Command(geo.handle_geopos, min_args=2),
    "GEODIST": Command(geo.handle_geodist, min_args=3, max_args=4),
    "GEOSEARCH": Command(geo.handle_geosearch, min_args=6),
    "HSET": Command(hashes.handle_hset, writes=True, min_args=3, argument_step=2),
    "HGET": Command(hashes.handle_hget, min_args=2, max_args=2),
    "HDEL": Command(hashes.handle_hdel, writes=True, min_args=2),
    "HGETALL": Command(hashes.handle_hgetall, min_args=1, max_args=1),
    "HEXISTS": Command(hashes.handle_hexists, min_args=2, max_args=2),
    "HLEN": Command(hashes.handle_hlen, min_args=1, max_args=1),
    "SADD": Command(sets.handle_sadd, writes=True, min_args=2),
    "SISMEMBER": Command(sets.handle_sismember, min_args=2, max_args=2),
    "SCARD": Command(sets.handle_scard, min_args=1, max_args=1),
    "SREM": Command(sets.handle_srem, writes=True, min_args=2),
    "SMEMBERS": Command(sets.handle_smembers, min_args=1, max_args=1),
    "SINTER": Command(sets.handle_sinter, min_args=1),
    "SUNION": Command(sets.handle_sunion, min_args=1),
    "ZADD": Command(sorted_sets.handle_zadd, writes=True, min_args=3),
    "ZRANK": Command(sorted_sets.handle_zrank, min_args=2, max_args=2),
    "ZRANGE": Command(sorted_sets.handle_zrange, min_args=3, max_args=4),
    "ZCARD": Command(sorted_sets.handle_zcard, min_args=1, max_args=1),
    "ZSCORE": Command(sorted_sets.handle_zscore, min_args=2, max_args=2),
    "ZREM": Command(sorted_sets.handle_zrem, writes=True, min_args=2),
}


@dataclass(frozen=True)
class ResolvedCommand:
    name: str
    definition: Command
    handler: Handler
    args: list[str]


def resolve_command(request: CommandRequest) -> ResolvedCommand:
    name, *args = request.tokens
    command_name = request.name
    command = COMMANDS.get(command_name)

    if command is None:
        raise CommandError(RedisResponse.error(f"Unknown command {name!r}"))

    while command.subcommands is not None:
        if not args:
            raise CommandError(RedisResponse.error(f"Missing subcommand for '{command_name}'"))

        subcommand, *args = args
        subcommand = subcommand.upper()
        parent_name = command_name
        command_name = f"{command_name} {subcommand}"
        command = command.subcommands.get(subcommand)

        if command is None:
            raise CommandError(RedisResponse.error(f"Unknown {parent_name} subcommand {subcommand!r}"))

    if command.handler is None:
        raise CommandError(RedisResponse.error(f"Missing handler for '{command_name}'"))

    count = len(args)
    valid = count >= command.min_args
    valid = valid and (command.max_args is None or count <= command.max_args)
    valid = valid and (count - command.min_args) % command.argument_step == 0
    valid = valid and (command.argument_counts is None or count in command.argument_counts)

    if not valid:
        raise CommandError(wrong_arguments(command_name))

    return ResolvedCommand(command_name, command, command.handler, args)
