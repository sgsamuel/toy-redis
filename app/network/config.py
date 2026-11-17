from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ServerConfig:
    dir: str = field(default_factory=lambda: str(Path.cwd()))
    dbfilename: str = "dump.rdb"
    appendonly: str = "no"
    appenddirname: str = "appendonlydir"
    appendfilename: str = "appendonly.aof"
    appendfsync: str = "everysec"
