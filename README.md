# Loams benchmarks

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

Azure did not consume 1,696 acknowledged messages before the configured 15-second drain expired. The raw field `lost_after_ack` counts this observation; it is **not proof of permanent message loss**. The driver deleted its test topic afterward, so this run cannot establish later delivery. The run is incomplete for end-to-end delivery validation. Kafka telemetry was configured for the local OTLP collector; trace receipt was not independently verified.

## Not yet measured

Loams Postgres (Neon fork) plus PostgreSQL routing: pinned storage/compute image deployment is still being prepared. No Postgres or router number is claimed. `loams-sqlrouter` is currently a sans-I/O control-plane library, not a runnable SQL proxy; the exact requested routing topology still needs to be established. Several unrelated Loams PRs remain open or conflicted. These runs do not claim that all PRs have merged or that the executables include their changes.

## Reproduce

Start `loams-house-engine-dev --listen 127.0.0.1:28226` and `loams dev --data-dir <fresh-cache-dir> --listen 127.0.0.1:28280 --no-flight-sql --no-qdrant --no-es --no-live --no-durable --no-graph --no-console`. Create namespace `enginebench` through `loams.collection.v1.NamespaceService/CreateNamespace`, then install `requirements.txt` and run `python run.py`. Existing result filenames are skipped: use a fresh checkout/output directory for new measurements. Source binaries are not included.

The Confluent driver is copied from the Loams CFB worktree `bench/confluent/driver.py` (Apache-2.0); this repository retains the actual driver used. It additionally requires confluent-kafka and the optional OpenTelemetry packages. Credentials stay outside the repository. See sanitized raw JSON parameters for the exact run settings.

## Budget

Authorized ceiling: $2,400, including the earlier $400 Confluent allocation. No new cloud compute or Confluent clusters were provisioned for these runs. Local image downloads, Python dependencies and existing-cluster traffic were used. Additional Kafka payload was 48 MiB sent plus at most 48 MiB consumed; using the earlier conservative $0.05/GB each direction gives less than $0.01 payload transfer (estimate, excludes protocol overhead). Existing cluster running charges continue under the original campaign guard; this is not reconciled billing.
