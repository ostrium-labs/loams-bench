# Release benchmark suite

Requested scope and truthful per-engine status are in `engines.json`. The new benchmark clients are Rust and refuse debug builds. Existing Python development baselines remain historical measurements; they are not release-suite results.

The owner selected RisingWave for streaming SQL and Sail for Spark. Their results do not prove Flink or Apache Spark API conformance. Airflow integration and CQL require working gateway/executor deployments before any throughput can be claimed. Git metrics separate ref discovery, fetch, clone and acknowledged push. Obscura metrics require actual browser work and page-content checks. Event Fabric requires delivery through the configured connector pipeline, not broker-only traffic.

Neither the billion-QPS target nor the billion-Git-operations/s target has been achieved. Workloads with different units cannot be added.

Rust client output counts verified successful completions over elapsed time including draining in-flight operations, retains failures, and stores a latency histogram. Credentials are supplied through named environment variables; output excludes their values. A warmup or connection failure is recorded as blocked with no rate. The client refuses to overwrite an existing measurement.

Build and run: `cargo build --release --locked --manifest-path rust/Cargo.toml`, then invoke `loams-bench scenarios.json <fresh-output-directory>`. The release workflow records compiler and source identities and uploads binary hashes. Release-suite rates remain unverified until the binaries are built and the corresponding engine scenarios complete.
