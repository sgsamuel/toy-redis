"""Lossless conversion at the existing text-key and command-handler boundary."""


def encode_text(value: str) -> bytes:
    return value.encode("utf-8", errors="surrogateescape")


def decode_text(value: bytes) -> str:
    return value.decode("utf-8", errors="surrogateescape")
