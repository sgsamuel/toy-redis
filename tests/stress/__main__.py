"""Run bounded local stress workloads; exit nonzero when any check fails."""

from functools import partial
from pathlib import Path
import argparse
import asyncio
import json
import time

from tests.stress.data.storage import randomized_storage
from tests.stress.network.connections import concurrent_clients, disconnect_waiters, pipeline
from tests.stress.network.pubsub import pubsub_fanout
from tests.stress.persistence.aof import aof_restart
from tests.stress.persistence.rdb import rdb_load
from tests.stress.replication.burst import replication_burst


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
