"""Stress workloads for storage."""

import random


async def randomized_storage(scale):
    from app.data.store import RedisStore

    store = RedisStore()
    rng = random.Random(42)
    members, values, bitmap = {}, [], bytearray(128)
    count = 10000 * scale
    for _ in range(count):
        operation = rng.randrange(6)
        if operation == 0:
            member, score = str(rng.randrange(200)), float(rng.randrange(100))
            expected = int(member not in members)
            members[member] = score
            assert store.zadd('scores', (score, member)) == expected
        elif operation == 1:
            member = str(rng.randrange(200))
            expected = int(member in members)
            members.pop(member, None)
            assert store.zrem('scores', member) == expected
        elif operation == 2:
            value = str(rng.randrange(100))
            values.append(value)
            assert store.rpush('list', value) == len(values)
        elif operation == 3:
            size = rng.randrange(4)
            expected, values = values[:size], values[size:]
            assert store.lpop('list', size) == expected
        else:
            offset, bit = rng.randrange(1024), rng.randrange(2)
            previous = (bitmap[offset // 8] >> (7 - offset % 8)) & 1
            assert store.setbit('bitmap', offset, bit) == previous
            mask = 1 << (7 - offset % 8)
            bitmap[offset // 8] = (bitmap[offset // 8] & ~mask) | (bit << (7 - offset % 8))
        assert store.zrange('scores', 0, -1) == sorted(members, key=lambda m: (members[m], m))
        assert store.lrange('list', 0, -1) == values
        assert store.bitcount('bitmap') == sum(value.bit_count() for value in bitmap)
    return {'operations': count, 'seed': 42, 'reference_model_matches': True}
