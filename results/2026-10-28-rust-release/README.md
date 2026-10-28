# Loams Live Rust-client baseline

A release-built Rust client queried a Loams 0.1.2 release image on the shared laptop. The function returns `pong`; every response is checked. This measures function dispatch and excludes Live table reads, mutations and subscriptions. Three five-second closed-loop repetitions per concurrency, with ten warmup requests per client. Elapsed time includes draining in-flight requests.

| Clients | Median validated queries/s | Range | Failures |
|---|---:|---:|---:|
| 1 | 933.7 | 926.0–935.3 | 0 |
| 8 | 943.0 | 940.7–944.0 | 0 |
| 32 | 940.6 | 936.5–946.8 | 0 |

The engine was a previously published image, not the fresh engine build now underway. It is a single-host microbenchmark, not a distributed capacity or billion-QPS result. Exact hashes, version and per-run histograms are in this directory.
