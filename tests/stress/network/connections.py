"""Stress workloads for connections."""

import asyncio

from app.protocol import encode_command

from tests.support import running_server, wait_until


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
