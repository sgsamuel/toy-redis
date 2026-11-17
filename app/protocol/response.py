from typing import Any

from .encoding import decode_text, encode_text


class RedisResponse:
    def __init__(self, raw: bytes, value: Any = None, *, is_error: bool = False):
        self.raw = raw
        self.value = value
        self.is_error = is_error

    @classmethod
    def simple(cls, message: str) -> "RedisResponse":
        if "\r" in message or "\n" in message:
            raise ValueError("Simple strings cannot contain CR or LF characters")
        return cls(f"+{message}\r\n".encode(), message)

    @classmethod
    def error(cls, message: str, prefix: str = "ERR") -> "RedisResponse":
        if "\r" in message or "\n" in message:
            raise ValueError("Error messages cannot contain CR or LF characters")
        return cls(
            f"-{prefix} {message}\r\n".encode(),
            Exception(f"{prefix} {message}"),
            is_error=True,
        )

    @classmethod
    def integer(cls, value: int) -> "RedisResponse":
        return cls(f":{value}\r\n".encode(), value)

    @classmethod
    def bulk(cls, data: str | bytes | None) -> "RedisResponse":
        if data is None:
            return cls.null()
        payload = encode_text(data) if isinstance(data, str) else data
        decoded = data if isinstance(data, str) else decode_text(data)
        return cls(f"${len(payload)}\r\n".encode() + payload + b"\r\n", decoded)

    @classmethod
    def null(cls) -> "RedisResponse":
        return cls(b"$-1\r\n", None)

    @classmethod
    def array(cls, items: list["RedisResponse"] | None) -> "RedisResponse":
        if items is None:
            return cls(b"*-1\r\n", None)
        header = f"*{len(items)}\r\n".encode()
        body = b"".join(item.raw for item in items)
        return cls(header + body, [item.value for item in items])
