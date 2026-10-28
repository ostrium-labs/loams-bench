"""Local development baselines; closed-loop HTTP throughput, never a capacity claim."""
import asyncio
import hashlib
import io
import json
import math
import os
import platform
import random
import time
from pathlib import Path

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path('results/2026-10-28-development')
OUT.mkdir(parents=True, exist_ok=True)
HOUSE = 'http://127.0.0.1:28226'
HYBRID = 'http://127.0.0.1:28280'


async def post(session, url, body, headers=None):
    async with session.post(url, data=body, headers=headers) as response:
        text = await response.text()
        if response.status != 200:
            raise RuntimeError(f'HTTP {response.status}: {text[:400]}')
        return text


async def rpc(session, service, method, data):
    return json.loads(await post(session, f'{HYBRID}/loams.collection.v1.{service}/{method}',
                                 json.dumps(data), {'Content-Type': 'application/json'}))


async def prepare(session):
    n = 100000
    sink = io.BytesIO()
    pq.write_table(pa.table({'id': pa.array(range(n), type=pa.uint64()),
                            'bucket': pa.array([i % 100 for i in range(n)], type=pa.uint64())}), sink)
    await post(session, HOUSE + '/loams/seed?database=default&table=bench&engine=MergeTree',
               sink.getvalue(), {'X-Loams-Columns': json.dumps([['id', 'UInt64'], ['bucket', 'UInt64']])})
    schema = {'fields': [{'name': 'body', 'source_path': 'body',
                         'kind': {'text': {'analyzer': 'standard', 'positions': True}},
                         'indexed': True, 'fast': False}],
              'vectors': [{'name': 'embedding', 'dim': 32, 'distance': 'cosine'}],
              'sparse_vectors': [], 'dynamic': 'ignore', 'max_fields': 1000}
    await rpc(session, 'CollectionService', 'CreateCollection',
              {'namespace': 'enginebench', 'name': 'baseline', 'schema': schema})
    rng = random.Random(42)
    for base in range(0, 1000, 100):
        ops = []
        for i in range(base, base + 100):
            vector = [1.0] + [0.0] * 31 if i == 0 else [0.0] + [rng.random() for _ in range(31)]
            ops.append({'upsert': {'id': {'uint': str(i + 1)},
                                   'source': {'body': 'benchmarktarget' if i == 0 else f'ordinary document {i}'},
                                   'vectors': {'embedding': vector}}})
        await rpc(session, 'DocumentService', 'WriteDocuments',
                  {'namespace': 'enginebench', 'collection': 'baseline', 'ops': ops})
    query = {'namespace': 'enginebench', 'collection': 'baseline',
             'retrievers': [{'text': {'query': {'match': {'field': 'body', 'text': 'benchmarktarget'}}}},
                            {'vector': {'field': 'embedding', 'query': [1.0] + [0.0] * 31, 'k': 10}}],
             'fusion': {'rrf': {'k': 60}}, 'limit': 10}
    answer = await rpc(session, 'QueryService', 'Search', query)
    (OUT / 'hybrid-correctness.json').write_text(json.dumps(answer, indent=2))
    return query


def percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, math.ceil(q * len(values)) - 1)] if values else None


async def measure(session, name, url, body, validate, concurrency, repetition):
    if (OUT / f'{name}-c{concurrency}-r{repetition}.json').exists():
        return
    # Correctness is checked on every completed response, including warmup.
    for _ in range(10):
        validate(await post(session, url, body, {'Content-Type': 'application/json'}))
    start = time.perf_counter()
    deadline = start + 5
    latency, errors = [], []

    async def worker():
        while time.perf_counter() < deadline:
            began = time.perf_counter()
            try:
                validate(await post(session, url, body, {'Content-Type': 'application/json'}))
                latency.append((time.perf_counter() - began) * 1000)
            except Exception as error:
                errors.append(str(error)[:400])
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - start
    result = {'workload': name, 'concurrency': concurrency, 'repetition': repetition,
              'successful_queries': len(latency), 'errors': len(errors), 'error_examples': errors[:3],
              'elapsed_seconds': elapsed, 'successful_qps': len(latency) / elapsed,
              'p50_ms': percentile(latency, .5), 'p95_ms': percentile(latency, .95),
              'p99_ms': percentile(latency, .99), 'latencies_ms': latency}
    (OUT / f'{name}-c{concurrency}-r{repetition}.json').write_text(json.dumps(result))
    print(json.dumps({k: v for k, v in result.items() if k != 'latencies_ms'}), flush=True)


async def main():
    binaries = {}
    for name in ['loams', 'loams-house-engine-dev']:
        path = Path('/mnt/Projects/rust-cache/target/debug') / name
        binaries[name] = {'sha256': hashlib.file_digest(path.open('rb'), 'sha256').hexdigest(),
                          'size_bytes': path.stat().st_size, 'build': 'pre-existing debug executable; exact source SHA unverified'}
    (OUT / 'environment.json').write_text(json.dumps({'platform': platform.platform(),
        'cpu_count': os.cpu_count(), 'python': platform.python_version(), 'binaries': binaries,
        'method': 'closed-loop, local loopback; co-located driver; shared machine; warm in-memory data; 10 warmup queries, 5-second repetitions',
        'house_rows': 100000, 'hybrid_documents': 1000, 'hybrid_dimensions': 32}, indent=2))
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        query = await prepare(session)
        def house_check(text):
            rows = [json.loads(line) for line in text.strip().splitlines()]
            assert rows[-1] == [100000, 4999950000], rows
        def hybrid_check(text):
            answer = json.loads(text)
            hits = answer.get('hits', [])
            assert hits and str(hits[0].get('pk', {}).get('uint')) == '1', answer
        workloads = [('house-aggregate', HOUSE + '/',
                      'SELECT count(), sum(id) FROM bench FORMAT JSONCompactEachRowWithNamesAndTypes', house_check),
                     ('hybrid-rrf', HYBRID + '/loams.collection.v1.QueryService/Search', json.dumps(query), hybrid_check)]
        for name, url, body, check in workloads:
            for concurrency in [1, 8, 32]:
                for repetition in [1, 2, 3]:
                    await measure(session, name, url, body, check, concurrency, repetition)


if __name__ == '__main__':
    asyncio.run(main())
