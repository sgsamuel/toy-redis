import asyncio

from app.commands.common import ClientSession, CommandContext, ExecuteTransaction
from app.commands.errors import CommandError
from app.commands.registry import resolve_command
from app.commands.request import CommandRequest
from app.network.auth import ACLState
from app.network.config import ServerConfig
from app.network.pubsub import PubSubState
from app.persistence.aof import AOFManager
from app.protocol import RedisResponse
from app.replication.primary import ReplicationState, propagate_write
from app.data.store import RedisStore


class CommandExecutor:
    def __init__(
        self,
        store: RedisStore,
        replication: ReplicationState,
        config: ServerConfig,
        acl: ACLState,
        pubsub: PubSubState,
        aof_manager: AOFManager | None = None,
    ):
        self.config = config
        self.store = store
        self.replication = replication
        self.aof_manager = aof_manager
        self.acl = acl
        self.pubsub = pubsub

    async def apply_replication(self, tokens: list[str | bytes], session: ClientSession) -> RedisResponse:
        session.trusted_replication = True

        return await self.execute(tokens, session)

    async def execute(self, tokens: list[str | bytes], session: ClientSession) -> RedisResponse:
        try:
            request = CommandRequest.parse(tokens)

            if not session.executing_transaction:
                await asyncio.sleep(0)

            self.acl.validate(session, request.name)
            self.pubsub.validate(session, request.name)
            command = resolve_command(request)
            self.replication.validate_write(session, command.definition.writes)

            if session.queued is not None and not command.definition.transaction_control:
                session.queued.append(request.tokens)

                return RedisResponse.simple("QUEUED")

            context = CommandContext(self.store, self.replication, session, self.acl, self.pubsub, self.config)

            try:
                result = await command.handler(context, command.args)
            except ValueError as error:
                return RedisResponse.error(str(error))

            if isinstance(result, ExecuteTransaction):
                session.executing_transaction = True

                try:
                    replies = [await self.execute(tokens, session) for tokens in result.commands]
                finally:
                    session.executing_transaction = False

                return RedisResponse.array(replies)

            if result.response.is_error:
                return result.response

            if result.register_replica:
                session.replica_ack_offset = 0

            if command.definition.writes and result.mutation is not None and not session.replaying_aof:
                if self.aof_manager is not None:
                    self.aof_manager.write(result.mutation)

                if self.replication.replica_of is None:
                    propagate_write(self.replication, result.mutation)
                    session.last_write_offset = self.replication.offset

            return result.response
        except CommandError as error:
            if session.queued is not None and error.abort_transaction:
                session.transaction_error = True

            return error.response
