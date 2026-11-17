from dataclasses import dataclass

from app.protocol import RedisResponse
from app.protocol.encoding import decode_text

from .errors import CommandError


@dataclass(frozen=True)
class CommandRequest:
    name: str
    tokens: list[str]

    @classmethod
    def parse(cls, tokens: list[str | bytes]) -> "CommandRequest":
        if not isinstance(tokens, list) or not all(isinstance(token, (str, bytes)) for token in tokens):
            raise CommandError(RedisResponse.error("Expected array of bulk strings"))

        if not tokens:
            raise CommandError(RedisResponse.error("Empty command"))

        decoded = [decode_text(token) if isinstance(token, bytes) else token for token in tokens]

        return cls(name=decoded[0].upper(), tokens=decoded)
