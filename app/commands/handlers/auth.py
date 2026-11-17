from app.network.auth import ACLUser
from app.protocol import RedisResponse

from ..common import CommandContext, CommandResult, wrong_arguments


async def handle_acl_whoami(context: CommandContext, args: list[str]) -> CommandResult:
    if args:
        return CommandResult(wrong_arguments("ACL WHOAMI"))

    return CommandResult(RedisResponse.bulk(context.session.username))


async def handle_acl_getuser(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) != 1:
        return CommandResult(wrong_arguments("ACL GETUSER"))

    user = context.acl.users.get(args[0])

    if user is None:
        return CommandResult(RedisResponse.null())

    return CommandResult(RedisResponse.array([
        RedisResponse.bulk("flags"),
        RedisResponse.array([RedisResponse.bulk(flag) for flag in user.flags]),
        RedisResponse.bulk("passwords"),
        RedisResponse.array([RedisResponse.bulk(password) for password in user.passwords]),
    ]))


async def handle_acl_setuser(context: CommandContext, args: list[str]) -> CommandResult:
    if not args:
        return CommandResult(wrong_arguments("ACL SETUSER"))

    username, *rules = args

    for rule in rules:
        if rule != "nopass" and not rule.startswith(">"):
            return CommandResult(RedisResponse.error("Unsupported ACL SETUSER rule"))

    user = context.acl.users.get(username)

    if user is None:
        user = ACLUser(flags=[])
        context.acl.users[username] = user

    for rule in rules:
        if rule == "nopass":
            user.passwords.clear()

            if "nopass" not in user.flags:
                user.flags.append("nopass")
        else:
            user.add_password(rule[1:])

    return CommandResult(RedisResponse.simple("OK"))


async def handle_auth(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) not in (1, 2):
        return CommandResult(wrong_arguments("AUTH"))

    username, password = ("default", args[0]) if len(args) == 1 else args
    user = context.acl.users.get(username)

    if len(args) == 1 and user is not None and "nopass" in user.flags:
        return CommandResult(RedisResponse.error(
            "AUTH <password> called without any password configured for the default user"
        ))

    if user is None or not user.accepts_password(password):
        return CommandResult(RedisResponse.error(
            "invalid username-password pair or user is disabled.", prefix="WRONGPASS"
        ))

    context.session.username = username

    return CommandResult(RedisResponse.simple("OK"))
