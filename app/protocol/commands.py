from .response import RedisResponse


def encode_command(tokens: list[str] | tuple[str, ...]) -> bytes:
    """Encode command tokens as an array of bulk strings."""
    return RedisResponse.array([RedisResponse.bulk(token) for token in tokens]).raw
