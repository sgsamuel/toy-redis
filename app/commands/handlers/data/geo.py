from math import isfinite

from app.protocol import RedisResponse
from app.data.entries.geo import (
    MAX_LATITUDE, MAX_LONGITUDE, MIN_LATITUDE, MIN_LONGITUDE, METERS_PER_UNIT,
)

from ...common import CommandContext, CommandResult, wrong_arguments


async def handle_geoadd(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 4:
        return CommandResult(wrong_arguments("GEOADD"))

    key, *coordinates = args
    condition = None
    while coordinates and coordinates[0].upper() in ("NX", "XX"):
        option = coordinates.pop(0).upper()
        if condition is not None and condition != option:
            return CommandResult(RedisResponse.error("NX and XX options are mutually exclusive"))
        condition = option
    if not coordinates or len(coordinates) % 3:
        return CommandResult(wrong_arguments("GEOADD"))
    locations = []

    for raw_longitude, raw_latitude, member in zip(
        coordinates[::3], coordinates[1::3], coordinates[2::3]
    ):
        try:
            longitude, latitude = (float(raw_longitude), float(raw_latitude))
        except ValueError:
            return CommandResult(RedisResponse.error("value is not a valid float"))

        if (
            not isfinite(longitude)
            or not isfinite(latitude)
            or not MIN_LONGITUDE <= longitude <= MAX_LONGITUDE
            or not MIN_LATITUDE <= latitude <= MAX_LATITUDE
        ):
            return CommandResult(RedisResponse.error(
                f"invalid longitude,latitude pair {longitude},{latitude}"
            ))

        locations.append((longitude, latitude, member))

    version = context.store.get_key_version(key)
    added = context.store.geoadd(key, *locations, condition=condition)

    return CommandResult(
        RedisResponse.integer(added),
        mutation=["GEOADD", *args] if context.store.get_key_version(key) != version else None,
    )


async def handle_geopos(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 2:
        return CommandResult(wrong_arguments("GEOPOS"))

    key, *members = args
    positions = context.store.geopos(key, *members)
    replies = []

    for position in positions:
        if position is None:
            replies.append(RedisResponse.array(None))
        else:
            longitude, latitude = position
            replies.append(RedisResponse.array([
                RedisResponse.bulk(str(longitude)),
                RedisResponse.bulk(str(latitude)),
            ]))

    return CommandResult(RedisResponse.array(replies))


async def handle_geodist(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) not in (3, 4):
        return CommandResult(wrong_arguments("GEODIST"))

    key, member1, member2 = args[:3]
    unit = args[3].lower() if len(args) == 4 else "m"

    if unit not in METERS_PER_UNIT:
        return CommandResult(RedisResponse.error("unsupported unit provided. please use M, KM, FT, MI"))

    distance = context.store.geodist(key, member1, member2)

    if distance is None:
        return CommandResult(RedisResponse.null())

    return CommandResult(RedisResponse.bulk(f"{distance / METERS_PER_UNIT[unit]:.4f}"))


async def handle_geosearch(context: CommandContext, args: list[str]) -> CommandResult:
    if len(args) < 6:
        return CommandResult(wrong_arguments("GEOSEARCH"))

    key = args[0]
    center = None
    shape = None
    unit = "m"
    order = None
    count = None
    any_results = False
    extras = set()
    index = 1

    while index < len(args):
        option = args[index].upper()
        index += 1

        if option == "FROMMEMBER" and center is None and index < len(args):
            center = args[index]
            index += 1
        elif option == "FROMLONLAT" and center is None and index + 1 < len(args):
            try:
                longitude, latitude = (float(args[index]), float(args[index + 1]))
            except ValueError:
                return CommandResult(RedisResponse.error("value is not a valid float"))

            if (
                not isfinite(longitude)
                or not isfinite(latitude)
                or not MIN_LONGITUDE <= longitude <= MAX_LONGITUDE
                or not MIN_LATITUDE <= latitude <= MAX_LATITUDE
            ):
                return CommandResult(RedisResponse.error(
                    f"invalid longitude,latitude pair {longitude},{latitude}"
                ))

            center = (longitude, latitude)
            index += 2
        elif option in ("BYRADIUS", "BYBOX") and shape is None:
            dimensions = 1 if option == "BYRADIUS" else 2

            if index + dimensions >= len(args):
                return CommandResult(wrong_arguments("GEOSEARCH"))

            try:
                shape = tuple(float(value) for value in args[index:index + dimensions])
            except ValueError:
                return CommandResult(RedisResponse.error("value is not a valid float"))

            if any(not isfinite(value) or value < 0 for value in shape):
                return CommandResult(RedisResponse.error("radius, width and height must be non-negative finite numbers"))

            unit = args[index + dimensions].lower()

            if unit not in METERS_PER_UNIT:
                return CommandResult(RedisResponse.error("unsupported unit provided. please use M, KM, FT, MI"))

            shape = tuple(value * METERS_PER_UNIT[unit] for value in shape)
            index += dimensions + 1
        elif option in ("ASC", "DESC") and order is None:
            order = option
        elif option == "COUNT" and count is None and index < len(args):
            try:
                count = int(args[index])
            except ValueError:
                return CommandResult(RedisResponse.error("COUNT must be a positive integer"))

            if count <= 0:
                return CommandResult(RedisResponse.error("COUNT must be a positive integer"))

            index += 1

            if index < len(args) and args[index].upper() == "ANY":
                any_results = True
                index += 1
        elif option in ("WITHCOORD", "WITHDIST", "WITHHASH"):
            extras.add(option)
        else:
            return CommandResult(wrong_arguments("GEOSEARCH"))

    if center is None or shape is None:
        return CommandResult(wrong_arguments("GEOSEARCH"))

    results = context.store.geosearch(key, center, shape, order, count, any_results)
    replies = []

    for member, distance, score, position in results:
        if not extras:
            replies.append(RedisResponse.bulk(member))
            continue

        fields = [RedisResponse.bulk(member)]

        if "WITHDIST" in extras:
            fields.append(RedisResponse.bulk(f"{distance / METERS_PER_UNIT[unit]:.4f}"))

        if "WITHHASH" in extras:
            fields.append(RedisResponse.integer(int(score)))

        if "WITHCOORD" in extras:
            longitude, latitude = position
            fields.append(RedisResponse.array([
                RedisResponse.bulk(str(longitude)),
                RedisResponse.bulk(str(latitude)),
            ]))

        replies.append(RedisResponse.array(fields))

    return CommandResult(RedisResponse.array(replies))
