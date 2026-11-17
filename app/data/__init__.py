"""In-memory storage and data operations."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import RedisStore

__all__ = ["RedisStore"]


def __getattr__(name: str):
    if name == "RedisStore":
        from .store import RedisStore

        return RedisStore

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
