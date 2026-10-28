"""Read-only SQL QPS against Loams Postgres directly and through PgDog.

Set BENCH_PG_DIRECT_DSN and BENCH_PG_ROUTER_DSN externally. Never writes DSNs.
"""
import asyncio
import json
import math
import os
import time
from pathlib import Path

import psycopg

OUT = Path('results/2026-10-28-postgres')


async def run(dsn, topology, clients, repetition):
    connections = [await psycopg.AsyncConnection.connect(dsn, autocommit=True, prepare_threshold=None)
                   for _ in range(clients)]
    latencies, errors = [], []
    for connection in connections:
        row = await (await connection.execute('SELECT value FROM loams_bench_points WHERE id = 42')).fetchone()
        assert row == (84,), row
    start = time.perf_counter()
    deadline = start + 5

    async def worker(connection, index):
        request = index
        while time.perf_counter() < deadline:
            key = (request * 7919) % 100000
            before = time.perf_counter()
            try:
                row = await (await connection.execute(
                    'SELECT value FROM loams_bench_points WHERE id = %s', (key,))).fetchone()
                assert row == (key * 2,), row
                latencies.append((time.perf_counter() - before) * 1000)
            except Exception as error:
                errors.append(type(error).__name__)
            request += clients
    try:
        await asyncio.gather(*(worker(connection, index) for index, connection in enumerate(connections)))
        elapsed = time.perf_counter() - start
    finally:
        await asyncio.gather(*(connection.close() for connection in connections))
    ordered = sorted(latencies)
    def p(q):
        return ordered[math.ceil(q * len(ordered)) - 1] if ordered else None
    result = {'topology': topology, 'workload': 'read-only-primary-key-lookup', 'clients': clients,
              'repetition': repetition, 'successful_queries': len(latencies), 'errors': len(errors),
              'error_types': sorted(set(errors)), 'elapsed_seconds': elapsed,
              'successful_qps': len(latencies) / elapsed, 'p50_ms': p(.5), 'p95_ms': p(.95),
              'p99_ms': p(.99), 'latencies_ms': latencies}
    (OUT / f'{topology}-c{clients}-r{repetition}.json').write_text(json.dumps(result))
    print(json.dumps({k: v for k, v in result.items() if k != 'latencies_ms'}), flush=True)


async def main():
    OUT.mkdir(parents=True, exist_ok=True)
    direct, routed = os.environ['BENCH_PG_DIRECT_DSN'], os.environ['BENCH_PG_ROUTER_DSN']
    async with await psycopg.AsyncConnection.connect(direct, autocommit=True) as connection:
        await connection.execute('CREATE TABLE IF NOT EXISTS loams_bench_points (id bigint PRIMARY KEY, value bigint NOT NULL)')
        await connection.execute('INSERT INTO loams_bench_points SELECT i, i*2 FROM generate_series(0,99999) i ON CONFLICT DO NOTHING')
        await connection.execute('ANALYZE loams_bench_points')
        cursor = await connection.execute('SELECT count(*),sum(value) FROM loams_bench_points')
        assert await cursor.fetchone() == (100000, 9999900000)
        cursor = await connection.execute('SELECT version()')
        version = (await cursor.fetchone())[0]
        settings = {}
        for setting in ['fsync', 'synchronous_commit', 'shared_buffers', 'max_connections']:
            cursor = await connection.execute('SHOW ' + setting)
            settings[setting] = (await cursor.fetchone())[0]
        (OUT / 'environment.json').write_text(json.dumps({'version': version, 'settings': settings,
            'method': 'local shared-host closed-loop; 100000 rows; one correctness-checking warmup query per connection; 3 x 5-second runs per concurrency; autocommit read-only SQL QPS, not multi-statement transaction TPS; client prepared statements disabled',
            'psycopg_version': psycopg.__version__}, indent=2))
    for topology, dsn in [('direct', direct), ('pgdog', routed)]:
        for clients in [1, 8, 32]:
            for repetition in [1, 2, 3]:
                await run(dsn, topology, clients, repetition)


if __name__ == '__main__':
    asyncio.run(main())
