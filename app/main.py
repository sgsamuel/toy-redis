import argparse
import asyncio
from pathlib import Path

from app.network.config import ServerConfig
from app.network.auth import ACLState
from app.network.pubsub import PubSubState
from app.commands.executor import CommandExecutor
from app.replication.primary import ReplicationState
from app.network.server import serve
from app.persistence.aof import AOFManager
from app.data.store import RedisStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=6379, help="Port to bind to")
    parser.add_argument("--replicaof", nargs="+", metavar="PRIMARY", help="Primary host and port")
    parser.add_argument("--dir", default=str(Path.cwd()), help="Base directory for data files")
    parser.add_argument("--dbfilename", default="dump.rdb", help="RDB snapshot filename")
    parser.add_argument("--appendonly", type=str.lower, choices=("yes", "no"), default="no", help="Enable AOF write logging and startup replay")
    parser.add_argument("--appenddirname", default="appendonlydir", help="AOF subdirectory under the data directory")
    parser.add_argument("--appendfilename", default="appendonly.aof", help="AOF base filename")
    parser.add_argument("--appendfsync", type=str.lower, choices=("always", "everysec", "no"), default="everysec", help="AOF fsync policy (configuration only)")
    args = parser.parse_args()

    if args.replicaof is not None:
        primary = args.replicaof
        if len(primary) == 1:
            primary = primary[0].split()
        if len(primary) != 2:
            parser.error("--replicaof expects a host and port")

        host, raw_port = primary
        try:
            port = int(raw_port)
        except ValueError:
            parser.error("--replicaof expects an integer port")
        if not host or not 1 <= port <= 65535:
            parser.error("--replicaof expects a host and a port between 1 and 65535")
        args.replicaof = (host, port)

    return args


async def main() -> None:
    args = parse_args()
    config = ServerConfig(
        dir=args.dir,
        dbfilename=args.dbfilename,
        appendonly=args.appendonly,
        appenddirname=args.appenddirname,
        appendfilename=args.appendfilename,
        appendfsync=args.appendfsync,
    )
    store = RedisStore()
    if config.appendonly != "yes":
        store.load_rdb(Path(config.dir) / config.dbfilename)

    aof_manager = AOFManager(config)
    acl = ACLState()
    pubsub = PubSubState()
    executor = CommandExecutor(
        store=store,
        replication=ReplicationState(replica_of=args.replicaof),
        config=config,
        acl=acl,
        pubsub=pubsub,
        aof_manager=aof_manager,
    )
    await aof_manager.replay(executor.execute)
    await serve(executor, args.host, args.port)


if __name__ == "__main__":
    asyncio.run(main())
