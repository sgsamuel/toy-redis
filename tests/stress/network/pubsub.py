"""Stress workloads for pubsub."""

import asyncio

from app.protocol import encode_command

from tests.support import running_server


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
