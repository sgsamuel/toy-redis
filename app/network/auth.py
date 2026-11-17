from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from hmac import compare_digest
from typing import TYPE_CHECKING

from app.commands.errors import CommandError
from app.protocol import RedisResponse
from app.protocol.encoding import encode_text

if TYPE_CHECKING:
    from app.commands.common import ClientSession


@dataclass
class ACLUser:
    flags: list[str] = field(default_factory=lambda: ["nopass"])
    passwords: list[str] = field(default_factory=list)

    def add_password(self, password: str) -> None:
        digest = sha256(encode_text(password)).hexdigest()

        if digest not in self.passwords:
            self.passwords.append(digest)

        if "nopass" in self.flags:
            self.flags.remove("nopass")

    def accepts_password(self, password: str) -> bool:
        if "nopass" in self.flags:
            return True

        digest = sha256(encode_text(password)).hexdigest()

        return any(compare_digest(digest, stored) for stored in self.passwords)


@dataclass
class ACLState:
    users: dict[str, ACLUser] = field(default_factory=lambda: {"default": ACLUser()})

    def validate(self, session: ClientSession, command_name: str) -> None:
        if session.username is None and "nopass" in self.users["default"].flags:
            session.username = "default"

        if session.username is not None or session.trusted_replication:
            return

        if command_name in {"AUTH", "QUIT", "RESET"}:
            return

        raise CommandError(
            RedisResponse.error("Authentication required.", prefix="NOAUTH"),
            abort_transaction=False,
        )
