from __future__ import annotations

from math import asin, cos, isfinite, radians, sin, sqrt
from typing import TYPE_CHECKING

from ..common import _get_sorted_set_locked
from . import sorted_sets

if TYPE_CHECKING:
    from ..store import RedisStore


MIN_LONGITUDE = -180.0
MAX_LONGITUDE = 180.0
MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
EARTH_RADIUS_IN_METERS = 6372797.560856
METERS_PER_UNIT = {"m": 1.0, "km": 1000.0, "ft": 0.3048, "mi": 1609.34}


def spread_int32_to_int64(value: int) -> int:
    value &= 0xFFFFFFFF
    value = (value | (value << 16)) & 0x0000FFFF0000FFFF
    value = (value | (value << 8)) & 0x00FF00FF00FF00FF
    value = (value | (value << 4)) & 0x0F0F0F0F0F0F0F0F
    value = (value | (value << 2)) & 0x3333333333333333
    value = (value | (value << 1)) & 0x5555555555555555

    return value


def interleave(latitude: int, longitude: int) -> int:
    return spread_int32_to_int64(latitude) | (spread_int32_to_int64(longitude) << 1)


def encode_coordinates(longitude: float, latitude: float) -> float:
    normalized_latitude = int(2 ** 26 * (latitude - MIN_LATITUDE) / (MAX_LATITUDE - MIN_LATITUDE))
    normalized_longitude = int(2 ** 26 * (longitude - MIN_LONGITUDE) / (MAX_LONGITUDE - MIN_LONGITUDE))

    return float(interleave(normalized_latitude, normalized_longitude))


def compact_int64_to_int32(value: int) -> int:
    value &= 0x5555555555555555
    value = (value | (value >> 1)) & 0x3333333333333333
    value = (value | (value >> 2)) & 0x0F0F0F0F0F0F0F0F
    value = (value | (value >> 4)) & 0x00FF00FF00FF00FF
    value = (value | (value >> 8)) & 0x0000FFFF0000FFFF
    value = (value | (value >> 16)) & 0x00000000FFFFFFFF

    return value


def decode_coordinates(score: float) -> tuple[float, float]:
    if not isfinite(score) or score < 0 or score > 2 ** 54:
        raise ValueError("Invalid geospatial score")

    latitude_grid = compact_int64_to_int32(int(score))
    longitude_grid = compact_int64_to_int32(int(score) >> 1)
    latitude_min = MIN_LATITUDE + (MAX_LATITUDE - MIN_LATITUDE) * (latitude_grid / 2 ** 26)
    latitude_max = MIN_LATITUDE + (MAX_LATITUDE - MIN_LATITUDE) * ((latitude_grid + 1) / 2 ** 26)
    longitude_min = MIN_LONGITUDE + (MAX_LONGITUDE - MIN_LONGITUDE) * (longitude_grid / 2 ** 26)
    longitude_max = MIN_LONGITUDE + (MAX_LONGITUDE - MIN_LONGITUDE) * ((longitude_grid + 1) / 2 ** 26)
    longitude = min(MAX_LONGITUDE, max(MIN_LONGITUDE, (longitude_min + longitude_max) / 2))
    latitude = min(MAX_LATITUDE, max(MIN_LATITUDE, (latitude_min + latitude_max) / 2))

    return longitude, latitude


def geoadd(
    store: RedisStore, key: str, *locations: tuple[float, float, str], condition: str | None = None
) -> int:
    pairs = [
        (encode_coordinates(longitude, latitude), member)
        for longitude, latitude, member in locations
    ]

    return sorted_sets.zadd(store, key, *pairs, condition=condition)


def geopos(store: RedisStore, key: str, *members: str) -> list[tuple[float, float] | None]:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return [None for _ in members]

        scores = entry.scores

        return [
            decode_coordinates(scores[member]) if member in scores else None
            for member in members
        ]


def haversine_distance(
    longitude1: float, latitude1: float, longitude2: float, latitude2: float
) -> float:
    longitude1, longitude2 = radians(longitude1), radians(longitude2)
    latitude1, latitude2 = radians(latitude1), radians(latitude2)
    longitude_sine = sin((longitude2 - longitude1) / 2)

    if longitude_sine == 0:
        return EARTH_RADIUS_IN_METERS * abs(latitude2 - latitude1)

    latitude_sine = sin((latitude2 - latitude1) / 2)
    value = latitude_sine ** 2 + cos(latitude1) * cos(latitude2) * longitude_sine ** 2

    return 2 * EARTH_RADIUS_IN_METERS * asin(sqrt(min(1.0, max(0.0, value))))


def geodist(store: RedisStore, key: str, member1: str, member2: str) -> float | None:
    position1, position2 = geopos(store, key, member1, member2)

    if position1 is None or position2 is None:
        return None

    return haversine_distance(*position1, *position2)


def geosearch(
    store: RedisStore,
    key: str,
    center: str | tuple[float, float],
    shape: tuple[float, ...],
    order: str | None = None,
    count: int | None = None,
    any_results: bool = False,
) -> list[tuple[str, float, float, tuple[float, float]]]:
    with store._lock:
        entry = _get_sorted_set_locked(store, key)

        if entry is None:
            return []

        if isinstance(center, str):
            origin = entry.scores.get(center)

            if origin is None:
                raise ValueError("could not decode requested zset member")

            center = decode_coordinates(origin)

        longitude, latitude = center
        results = []

        for score, member in entry.ordered:
            position = decode_coordinates(score)
            distance = haversine_distance(longitude, latitude, *position)

            if len(shape) == 1:
                if distance > shape[0]:
                    continue
            else:
                width, height = shape
                member_longitude, member_latitude = position
                latitude_distance = EARTH_RADIUS_IN_METERS * abs(radians(member_latitude) - radians(latitude))
                longitude_distance = haversine_distance(member_longitude, member_latitude, longitude, member_latitude)

                if latitude_distance > height / 2 or longitude_distance > width / 2:
                    continue

            results.append((member, distance, score, position))

            if any_results and count is not None and len(results) >= count:
                break

        if order is not None or (count is not None and not any_results):
            results.sort(key=lambda result: result[1], reverse=order == "DESC")

        return results[:count] if count is not None else results
