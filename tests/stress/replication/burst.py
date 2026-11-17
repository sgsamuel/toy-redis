"""Stress workloads for replication."""

import asyncio

from app.protocol import encode_command

from tests.support import following, running_server, wait_until


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
