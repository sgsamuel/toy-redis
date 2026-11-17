"""Bounded local stress workloads; exit nonzero when any check fails."""
import argparse
import asyncio
import json
import random
import tempfile
import time
from functools import partial
from pathlib import Path

from app.commands.common import ClientSession
from app.network.config import ServerConfig
from app.protocol import encode_command
from tests.support import following, make_executor, running_server, wait_until


async def concurrent_clients(scale, *, authenticate=False):
    async with running_server() as server:
        if authenticate:
            admin = await server.connect()
            await admin.command('ACL', 'SETUSER', 'default', '>secret')

        async def worker():
            client = await server.connect()
            if authenticate:
                assert (await client.command('PING')).raw.startswith(b'-NOAUTH')
                assert (await client.command('AUTH', 'wrong')).raw.startswith(b'-WRONGPASS')
                assert (await client.command('AUTH', 'secret')).value == 'OK'
            for _ in range(200 * scale):
                response = await client.command('INCR', 'counter')
                assert response is not None and not response.is_error

        clients = 32 * scale
        await asyncio.gather(*(worker() for _ in range(clients)))
        expected = clients * 200 * scale
        actual = int(server.executor.store.get('counter'))
        assert actual == expected, (actual, expected)
        return {'clients': clients, 'commands': expected, 'stored': actual}


async def pipeline(scale, *, size):
    size *= scale
    async with running_server() as server:
        client = await server.connect()
        client.writer.write(encode_command(['INCR', 'counter']) * size)
        await client.writer.drain()
        for index in range(size):
            response = await client.receive()
            assert response is not None and response.value == index + 1, index
        assert server.executor.store.get('counter') == str(size)
        return {'sent': size, 'replies': size, 'stored': size}


async def pubsub_fanout(scale, *, atomic=False):
    subscribers = 24 * scale
    messages = (1000 if atomic else 200) * scale
    async with running_server() as server:
        clients = [await server.connect() for _ in range(subscribers)]
        for client in clients:
            await client.command('SUBSCRIBE', 'news')
            for pattern in ['n*', '*ews', 'new?', '*']:
                await client.command('PSUBSCRIBE', pattern)
        publisher = await server.connect()

        async def consume(client):
            for number in range(messages):
                kinds = []
                for _ in range(5):
                    response = await client.receive()
                    assert response is not None and response.value[-1] == str(number)
                    kinds.append(response.value[0])
                assert kinds.count('message') == 1 and kinds.count('pmessage') == 4

        consumers = [asyncio.create_task(consume(client)) for client in clients]
        try:
            if atomic:
                await publisher.command('MULTI')
                for number in range(messages):
                    assert (await publisher.command('PUBLISH', 'news', str(number))).value == 'QUEUED'
                response = await publisher.command('EXEC')
                assert response.value == [subscribers * 5] * messages
            else:
                publisher.writer.write(b''.join(encode_command(['PUBLISH', 'news', str(i)]) for i in range(messages)))
                await publisher.writer.drain()
                for _ in range(messages):
                    response = await publisher.receive()
                    assert response is not None and response.value == subscribers * 5
            await asyncio.gather(*consumers)
        finally:
            for task in consumers:
                task.cancel()
            await asyncio.gather(*consumers, return_exceptions=True)
        assert len(server.executor.pubsub.channels['news']) == subscribers
        return {'subscribers': subscribers, 'publications': messages, 'delivered_frames': subscribers * messages * 5}


async def disconnect_waiters(scale):
    count = 32 * scale
    async with running_server() as server:
        lists = [await server.connect() for _ in range(count)]
        streams = [await server.connect() for _ in range(count)]
        for client in lists:
            await client.send('BLPOP', 'jobs', '0')
        for client in streams:
            await client.send('XREAD', 'BLOCK', '0', 'STREAMS', 'events', '$')
        store = server.executor.store
        await wait_until(lambda: sum(map(len, store._list_waiters.values())) == count
                         and sum(map(len, store._stream_waiters.values())) == count)
        await asyncio.gather(*(client.close() for client in lists + streams))
        await wait_until(lambda: not store._list_waiters and not store._stream_waiters)
        producer = await server.connect()
        await producer.command('RPUSH', 'jobs', *map(str, range(count)))
        assert store.llen('jobs') == count
        return {'disconnected_clients': count * 2, 'stale_waiters': 0, 'jobs_preserved': count}


async def replication_burst(scale):
    async with running_server() as primary:
        producer = await primary.connect()
        for index in range(100 * scale):
            await producer.command('SET', f'before:{index}', str(index))
        store = primary.executor.store
        store.rpush('list', 'a', 'b')
        store.zadd('scores', (1.0, 'member'))
        store.xadd('stream', '1-0', [('field', 'value')])
        store.setbit('bits', 0, 1)
        store.set('expiring', 'value', expiry_ms=60000)
        async with following(primary) as replica:
            assert len(replica.store.keys('before:*')) == 100 * scale
            assert replica.store.lrange('list', 0, -1) == ['a', 'b']
            assert replica.store.zscore('scores', 'member') == 1.0
            assert replica.store.xrange('stream', '-', '+') == store.xrange('stream', '-', '+')
            assert replica.store.get('bits') == b'\x80'
            assert abs(replica.store._store['expiring'].expires_at - store._store['expiring'].expires_at) < 0.02
            clients = [await primary.connect() for _ in range(8 * scale)]
            for client in clients:
                client.writer.write(encode_command(['INCR', 'counter']) * (64 * scale))
            async def collect(client):
                await client.writer.drain()
                for _ in range(64 * scale):
                    assert await client.receive() is not None
            await asyncio.gather(*(collect(client) for client in clients))
            expected = 8 * scale * 64 * scale
            await wait_until(lambda: replica.store.get('counter') == str(expected))
            assert (await clients[0].command('WAIT', '1', '3000')).value == 1
            await wait_until(lambda: replica.replication.offset == primary.executor.replication.offset)
            assert replica.replication.master_link_up
            return {'restored_keys': 100 * scale, 'burst_writes': expected,
                    'primary': int(store.get('counter')), 'replica': int(replica.store.get('counter')),
                    'matching_offset': replica.replication.offset}


async def aof_restart(scale):
    with tempfile.TemporaryDirectory() as directory:
        config = ServerConfig(dir=directory, appendonly='yes')
        original = make_executor(config=config)
        count = 10000 * scale
        session = ClientSession()
        for _ in range(count):
            assert (await original.execute(['INCR', 'counter'], session)).value > 0
        before = original.aof_manager.file.read_bytes()
        restored = make_executor(config=config)
        replayed = await restored.aof_manager.replay(restored.execute)
        assert replayed == count
        assert restored.store.get('counter') == str(count)
        assert restored.aof_manager.file.read_bytes() == before
        assert restored.replication.offset == 0
        return {'writes': count, 'replayed': replayed, 'aof_bytes': len(before), 'duplicate_records': 0}


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


async def rdb_load(scale):
    from app.data.store import RedisStore

    count = 20000 * scale
    expired = int((time.time() - 60) * 1000).to_bytes(8, 'little')
    data = bytearray(b'REDIS0011\xfe\x00')
    for index in range(count):
        key = f'key:{index}'.encode()
        if index % 4 == 0:
            data.extend(b'\xfc' + expired)
        data.extend(b'\x00' + bytes([len(key)]) + key + b'\x40\x64' + b'x' * 100)
    data.extend(b'\xff' + bytes(8))
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'dump.rdb'
        path.write_bytes(data)
        store = RedisStore()
        store.load_rdb(path)
        assert len(store.keys('*')) == count - count // 4
        assert store.get('key:0') is None
        assert store.get(f'key:{count - 1}') == b'x' * 100
    return {'entries': count, 'live': count - count // 4, 'expired': count // 4, 'file_bytes': len(data)}


SCENARIOS = {
    'concurrent-clients': concurrent_clients,
    'authenticated-clients': partial(concurrent_clients, authenticate=True),
    'pipeline-128': partial(pipeline, size=128),
    'pipeline-257': partial(pipeline, size=257),
    'pipeline-1000': partial(pipeline, size=1000),
    'pipeline-5000': partial(pipeline, size=5000),
    'pubsub-pattern-fanout': pubsub_fanout,
    'pubsub-atomic-burst': partial(pubsub_fanout, atomic=True),
    'disconnect-cleanup': disconnect_waiters,
    'replication-burst': replication_burst,
    'aof-restart': aof_restart,
    'randomized-storage': randomized_storage,
    'rdb-load': rdb_load,
}


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scale', type=int, choices=range(1, 11), default=1, metavar='1..10')
    parser.add_argument('--scenario', action='append', choices=SCENARIOS, help='Run selected scenarios; repeat to select more')
    parser.add_argument('--output', type=Path, help='Write the complete JSON report to this file')
    args = parser.parse_args()
    results = []
    for name in args.scenario or SCENARIOS:
        start = time.perf_counter()
        try:
            async with asyncio.timeout(30 * args.scale):
                metrics = await SCENARIOS[name](args.scale)
            result = {'scenario': name, 'pass': True, **metrics}
        except Exception as error:
            result = {'scenario': name, 'pass': False, 'error': repr(error)}
        result['seconds'] = round(time.perf_counter() - start, 4)
        results.append(result)
        print(json.dumps(result), flush=True)
    failures = sum(not result['pass'] for result in results)
    report = {'scale': args.scale, 'passed': len(results) - failures, 'failed': failures, 'results': results}
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'failed': failures}))
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
