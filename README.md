# Loams benchmarks

Raw benchmark results and trace exports now live in [loams-traces](https://github.com/ostrium-labs/loams-traces), with a Vite+ / DuckDB-Wasm browser explorer and a SHA-256 provenance manifest. This repository owns the reproducible drivers. Historical measurements below retain their original conditions.

Measured results and reproducible drivers. The **1 billion QPS goal has not been reached**.

## 2026-10-28 development baseline

These are exploratory measurements from pre-existing **debug** executables on a shared laptop. Exact executable source commits are unverified; binary SHA-256 hashes are in `environment.json`. Other processes were running. Cloud image downloads and three parallel WAN ingestion checks overlapped part of the hybrid run. These results do not establish production capacity or a scaling curve.

| Workload | Clients | QPS median (min–max across 3 runs) | p99 ms range | Errors |
|---|---:|---:|---:|---:|
| house-aggregate | 1 | 20.3 (20.2–20.5) | 50.7–65.1 | 0 |
| house-aggregate | 8 | 164.7 (164.7–165.3) | 63.4–63.9 | 0 |
| house-aggregate | 32 | 647.1 (647.1–647.4) | 64.7–65.1 | 0 |
| hybrid-rrf | 1 | 46.8 (2.9–48.2) | 34.5–2565.8 | 0 |
| hybrid-rrf | 8 | 445.7 (433.9–457.6) | 28.3–31.0 | 0 |
| hybrid-rrf | 32 | 642.9 (619.9–655.8) | 86.3–98.2 | 0 |

House: count and sum over a seeded 100,000-row in-memory Parquet table, through the engine development HTTP server. Every successful response must equal count=100000 and sum=4999950000. This bypasses production House authentication, storage and gateway layers.

Hybrid: 1,000 synthetic documents, 32 dimensions, dense cosine and text retrieval fused with RRF k=60, candidate k=10, limit 10, strong default consistency, repeated fixed query. Every successful response must return the known target first. This checks a constructed top-1 invariant, not general recall@10 or real-world relevance. No filter. The first single-client repetition includes a large latency outlier; it is retained.

Method: closed loop HTTP, colocated asyncio driver, concurrency 1/8/32, ten warmup requests and three five-second repetitions per setting. The timer includes draining in-flight requests; successful QPS = verified completions / elapsed time. Individual latencies and all repetitions are retained. Dataset setup completed before timing.

## Confluent Cloud ingestion — separate from query QPS

Basic clusters in AWS ap-south-1, GCP asia-south1 and Azure centralindia. Three concurrent runs, each sending at most 16 MiB of 1,024-byte messages with acks=all and three partitions. WAN laptop measurements; shared uplink was contended by image downloads. These are burst-drain observations, not steady-state cloud capacity.

| Cloud | Acked records/s | Acked | Consumed before drain deadline | Missing at deadline |
|---|---:|---:|---:|---:|
| aws | 287.3 | 16384 | 16384 | 0 |
| azure | 368.8 | 16384 | 14688 | 1696 |
| gcp | 289.4 | 16384 | 16384 | 0 |

Azure did not consume 1,696 acknowledged messages before the configured 15-second drain expired. The raw field `lost_after_ack` counts this observation; it is **not proof of permanent message loss**. The driver deleted its test topic afterward, so this run cannot establish later delivery. The run is incomplete for end-to-end delivery validation. SigNoz receipt was verified by querying spans with this exact run resource attribute: 492 spans each for AWS and GCP, and 475 for Azure. Counts by span name are in `signoz-span-counts.json`. The collector uses a separate ClickHouse instance; this does not demonstrate SigNoz on Loams House.

## Scope limits

These are single-compute, unsharded development runs. Several unrelated Loams PRs remain open or conflicted. The source pin and executable hashes identify what was actually measured; this report does not claim all PRs are merged. No billion-QPS, durable-write, large-corpus relevance, failover, multi-node or cross-region SQL capacity has been demonstrated.

## Reproduce

Start `loams-house-engine-dev --listen 127.0.0.1:28226` and `loams dev --data-dir <fresh-cache-dir> --listen 127.0.0.1:28280 --no-flight-sql --no-qdrant --no-es --no-live --no-durable --no-graph --no-console`. Create namespace `enginebench` through `loams.collection.v1.NamespaceService/CreateNamespace`, then install `requirements.txt` and run `python run.py`. Existing result filenames are skipped: use a fresh checkout/output directory for new measurements. Source binaries are not included.

The Confluent driver is copied from the Loams CFB worktree `bench/confluent/driver.py` (Apache-2.0); this repository retains the actual driver used. It additionally requires confluent-kafka and the optional OpenTelemetry packages. Credentials stay outside the repository. See sanitized raw JSON parameters for the exact run settings.

## Budget

Authorized ceiling: $2,400, including the earlier $400 Confluent allocation. No new cloud compute or Confluent clusters were provisioned for these runs. Local image downloads, Python dependencies and existing-cluster traffic were used. Additional Kafka payload was 48 MiB sent plus at most 48 MiB consumed; using the earlier conservative $0.05/GB each direction gives less than $0.01 payload transfer (estimate, excludes protocol overhead). Existing cluster running charges continue under the original campaign guard; this is not reconciled billing.

## House transport sensitivity

The same seeded aggregate was also measured with a fresh HTTP connection for every query (`python house_connections.py` after `run.py` seeds the server). This is a separate measurement and retains all earlier results.

| Clients | Fresh-connection QPS median | QPS range | Errors |
|---|---:|---:|---:|
| 1 | 158.4 | 153.7–161.3 | 0 |
| 8 | 855.3 | 851.3–870.3 | 0 |
| 32 | 1385.3 | 1305.2–1406.0 | 0 |

The single-client difference is consistent with overhead in the test HTTP persistent-connection path. It does not identify a particular TCP mechanism or establish production engine capacity.

## Postgres runner

The manually dispatched `Postgres and PgDog benchmark` workflow uses an isolated GitHub-hosted Ubuntu 24.04 runner, pinned Loams source and pinned PostgreSQL storage/compute and PgDog images. It compares direct SQL point reads with the same reads through PgDog. This is separate hardware from the laptop. Every returned value is validated. The fixture uses development credentials on loopback ports, with no cloud credentials. That workflow was queued and canceled once the local deployment succeeded; no hosted-runner result is claimed. All Postgres measurements below came from the local laptop.

## Loams Postgres 17.11 with PgDog 0.1.60

Pinned compute/storage images, one compute, one pageserver and one safekeeper, local-folder backing store. Scratch compose topology and image digests are recorded under `results/2026-10-28-postgres/`. Disabled RustFS-only services and optional dependencies in the local compose copy to accommodate podman-compose 1.6.0. Own House and hybrid processes were stopped before these runs.

**Development settings: fsync=off, synchronous_commit=on, shared_buffers=128MB, max_connections=100. All timed workloads are read-only, with data warmed by initialization and validation. No durable-write throughput is claimed.**

### Verified point reads from the host Python client

100,000 rows, deterministic primary-key permutation, autocommit, client prepared statements disabled, one warmup per connection, three five-second repetitions. Every returned value is checked. All 18 repetitions had zero errors.

| Path | Clients | Median QPS | QPS range | p99 ms range |
|---|---:|---:|---:|---:|
| direct | 1 | 7,428 | 6,351–7,559 | 0.678–0.756 |
| direct | 8 | 24,240 | 23,949–24,949 | 0.928–0.961 |
| direct | 32 | 30,031 | 29,982–30,426 | 1.489–1.690 |
| pgdog | 1 | 4,460 | 4,412–5,061 | 0.904–0.961 |
| pgdog | 8 | 22,311 | 21,943–22,440 | 1.115–1.152 |
| pgdog | 32 | 28,901 | 28,828–29,389 | 2.140–2.347 |

### Native pgbench select-only

PostgreSQL 17.11 `pgbench`, scale 1 (100,000 accounts), simple query protocol, clients 1/8/32/64, up to four client threads, three ten-second repetitions. The client runs inside the compute container: direct connects by TCP loopback; PgDog crosses the container bridge. This topology differs from the host Python driver, so their rates are not a controlled client-only comparison. The built-in select-only script performs one SELECT per counted transaction; its reported TPS is therefore query throughput for this specific workload.

All 24 repetitions exited successfully with zero failed transactions. pgbench discards SELECT result values; separate pre/post fixture checks verify table cardinality and contents. Raw stdout/stderr and exact commands are retained. These short cached-read runs do not establish maximum capacity.

| Path | Clients | Median select-only QPS | QPS range |
|---|---:|---:|---:|
| direct | 1 | 19,824 | 19,780–19,844 |
| direct | 8 | 200,565 | 199,630–201,565 |
| direct | 32 | 289,343 | 285,682–290,421 |
| direct | 64 | 296,010 | 295,875–298,845 |
| pgdog | 1 | 17,433 | 17,118–17,620 |
| pgdog | 8 | 96,740 | 93,695–97,239 |
| pgdog | 32 | 125,492 | 122,961–126,566 |
| pgdog | 64 | 121,792 | 121,377–125,562 |

PgDog uses transaction pooling, two workers and the default maximum pool size of ten server connections. This is a default-configuration baseline, not a tuning sweep. The documented knobs are [workers and default_pool_size](https://docs.pgdog.dev/configuration/pgdog.toml/general/). There is one backend and no sharding in this run.

Reproduce the verified point-read suite with `BENCH_PG_DIRECT_DSN` and `BENCH_PG_ROUTER_DSN` supplied externally, then `python postgres.py`. `python pgbench.py` initializes the **scratch database** with scale-1 pgbench tables and runs the native suite; it defaults to the local Podman container names recorded in its source. Never point it at an existing application database. Docker users can set `BENCH_CONTAINER_ENGINE` and `BENCH_COMPUTE_CONTAINER`.

## Rust release-client results

[Loams Live function-query baseline](https://github.com/ostrium-labs/loams-traces): nine runs, no query failures. This measures a ping function and does not cover Live table access or subscriptions.
