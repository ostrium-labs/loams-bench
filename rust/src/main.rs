//! Release Rust clients. Only validated completed operations count as successes.
use anyhow::{Context, Result, bail};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinSet;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    id: String,
    engine: String,
    unit: String,
    #[serde(default = "http")]
    protocol: String,
    url: Option<String>,
    #[serde(default = "post")]
    method: String,
    body: Option<Value>,
    /// Only an environment variable name is stored; credentials never enter output.
    authorization_env: Option<String>,
    dsn_env: Option<String>,
    sql: Option<String>,
    redis_url_env: Option<String>,
    redis_command: Option<Vec<String>>,
    expected_i64: Option<i64>,
    expected_pointer: Option<String>,
    expected_value: Option<Value>,
    expected_text: Option<String>,
    #[serde(default = "concurrencies")]
    concurrency: Vec<usize>,
    #[serde(default = "seconds")]
    seconds: u64,
    #[serde(default = "repetitions")]
    repetitions: u32,
}
fn http() -> String {
    "http".into()
}
fn post() -> String {
    "POST".into()
}
fn concurrencies() -> Vec<usize> {
    vec![1, 8, 32]
}
fn seconds() -> u64 {
    10
}
fn repetitions() -> u32 {
    3
}

#[derive(Serialize)]
struct Measurement {
    schema_version: u32,
    scenario: String,
    engine: String,
    unit: String,
    build_profile: &'static str,
    client_revision: &'static str,
    started_unix_ms: u128,
    clients: usize,
    repetition: u32,
    successful_completed_operations: u64,
    failed_operations: u64,
    elapsed_seconds_including_drain: f64,
    successful_operations_per_second: f64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_p99_us: u64,
    /// Histogram preserves all samples without unbounded per-query allocation.
    latency_buckets_us: Vec<(u64, u64)>,
}

enum Client {
    Http(reqwest::Client),
    Postgres(tokio_postgres::Client),
    Redis(redis::aio::MultiplexedConnection),
}
impl Client {
    async fn connect(s: &Scenario) -> Result<Self> {
        match s.protocol.as_str() {
            "redis" => {
                let name = s
                    .redis_url_env
                    .as_deref()
                    .context("redis_url_env required")?;
                let url = std::env::var(name).context("Redis URL environment variable absent")?;
                let connection = redis::Client::open(url)?
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("Redis connection failed; credentials suppressed")
                    })?;
                Ok(Self::Redis(connection))
            }
            "http" => Ok(Self::Http(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()?,
            )),
            "postgres" => {
                let dsn = std::env::var(s.dsn_env.as_deref().context("dsn_env required")?)
                    .context("DSN environment variable is absent")?;
                // NoTls is limited to local fixtures; remote deployments require a TLS client.
                anyhow::ensure!(
                    dsn.contains("127.0.0.1") || dsn.contains("localhost"),
                    "Postgres NoTls client requires loopback"
                );
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("Postgres connection failed; credential details suppressed")
                    })?;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                Ok(Self::Postgres(client))
            }
            _ => bail!("unsupported protocol"),
        }
    }

    async fn operation(&self, s: &Scenario) -> Result<()> {
        match self {
            Self::Redis(connection) => {
                let arguments = s.redis_command.as_ref().context("redis_command required")?;
                let mut command = redis::cmd(arguments.first().context("empty Redis command")?);
                for argument in &arguments[1..] {
                    command.arg(argument);
                }
                let answer: String = tokio::time::timeout(
                    Duration::from_secs(30),
                    command.query_async(&mut connection.clone()),
                )
                .await??;
                anyhow::ensure!(
                    Some(&answer) == s.expected_text.as_ref(),
                    "Redis correctness check failed"
                );
            }
            Self::Http(client) => {
                let method = s.method.parse::<reqwest::Method>()?;
                let mut request = client.request(method, s.url.as_deref().context("url required")?);
                if let Some(name) = &s.authorization_env {
                    request = request.header(
                        "Authorization",
                        std::env::var(name).context("authorization variable absent")?,
                    );
                }
                if let Some(body) = &s.body {
                    request = request.json(body);
                }
                let response = request.send().await?;
                anyhow::ensure!(response.status().is_success(), "non-success HTTP status");
                let bytes = response.bytes().await?;
                if let Some(pointer) = &s.expected_pointer {
                    let json: Value = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        json.pointer(pointer) == s.expected_value.as_ref(),
                        "JSON correctness check failed"
                    );
                } else if let Some(expected) = &s.expected_text {
                    anyhow::ensure!(
                        std::str::from_utf8(&bytes)?.contains(expected),
                        "text correctness check failed"
                    );
                } else {
                    bail!("correctness validator required");
                }
            }
            Self::Postgres(client) => {
                let rows = tokio::time::timeout(
                    Duration::from_secs(30),
                    client.query(s.sql.as_deref().context("sql required")?, &[]),
                )
                .await??;
                anyhow::ensure!(rows.len() == 1, "expected exactly one row");
                let got: i64 = rows[0].try_get(0)?;
                anyhow::ensure!(Some(got) == s.expected_i64, "SQL correctness check failed");
            }
        }
        Ok(())
    }
}

async fn measure(s: Arc<Scenario>, clients: usize, repetition: u32) -> Result<Measurement> {
    anyhow::ensure!((1..=4096).contains(&clients), "clients outside 1..4096");
    anyhow::ensure!((1..=3600).contains(&s.seconds), "seconds outside 1..3600");
    let mut connections = Vec::new();
    for _ in 0..clients {
        let client = Client::connect(&s).await?;
        for _ in 0..10 {
            client
                .operation(&s)
                .await
                .context("warmup/correctness check failed")?;
        }
        connections.push(client);
    }
    let started_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(s.seconds);
    let mut tasks = JoinSet::new();
    for client in connections {
        let s = s.clone();
        tasks.spawn(async move {
            let mut latency = Histogram::<u64>::new(3).expect("valid precision");
            let mut failed = 0;
            while Instant::now() < deadline {
                let before = Instant::now();
                match client.operation(&s).await {
                    Ok(()) => {
                        latency
                            .record(before.elapsed().as_micros().max(1) as u64)
                            .expect("histogram autoresizes");
                    }
                    Err(_) => {
                        failed += 1;
                    }
                }
            }
            (latency, failed)
        });
    }
    let mut latency = Histogram::<u64>::new(3)?;
    let mut failed = 0;
    while let Some(result) = tasks.join_next().await {
        let (histogram, errors) = result?;
        latency.add(&histogram)?;
        failed += errors;
    }
    let elapsed = start.elapsed().as_secs_f64();
    Ok(Measurement {
        schema_version: 1,
        scenario: s.id.clone(),
        engine: s.engine.clone(),
        unit: s.unit.clone(),
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        client_revision: option_env!("LOAMS_BENCH_REVISION").unwrap_or("unverified"),
        started_unix_ms,
        clients,
        repetition,
        successful_completed_operations: latency.len(),
        failed_operations: failed,
        elapsed_seconds_including_drain: elapsed,
        successful_operations_per_second: latency.len() as f64 / elapsed,
        latency_p50_us: latency.value_at_quantile(0.5),
        latency_p95_us: latency.value_at_quantile(0.95),
        latency_p99_us: latency.value_at_quantile(0.99),
        latency_buckets_us: latency
            .iter_recorded()
            .map(|v| (v.value_iterated_to(), v.count_since_last_iteration()))
            .collect(),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        args.len() == 3,
        "usage: loams-bench SCENARIOS.json OUTPUT_DIRECTORY"
    );
    anyhow::ensure!(
        !cfg!(debug_assertions),
        "benchmark requires cargo build --release"
    );
    let scenarios: Vec<Scenario> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let output = Path::new(&args[2]);
    std::fs::create_dir_all(output)?;
    for s in scenarios {
        anyhow::ensure!(
            !s.id.is_empty()
                && s.id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "invalid scenario ID"
        );
        anyhow::ensure!(
            s.expected_pointer.is_none() || s.expected_value.is_some(),
            "expected_pointer requires expected_value"
        );
        anyhow::ensure!(
            s.expected_text.as_ref().is_none_or(|v| !v.is_empty()),
            "expected_text must not be empty"
        );
        let s = Arc::new(s);
        for &clients in &s.concurrency {
            for repetition in 1..=s.repetitions {
                let filename = output.join(format!("{}-c{}-r{}.json", s.id, clients, repetition));
                anyhow::ensure!(!filename.exists(), "refusing to overwrite a measurement");
                let result = match measure(s.clone(), clients, repetition).await {
                    Ok(result) => result,
                    Err(error) => {
                        let status = serde_json::json!({"scenario":s.id,"engine":s.engine,"status":"blocked","reason":error.to_string(),"successful_operations_per_second":null});
                        std::fs::write(filename, serde_json::to_vec_pretty(&status)?)?;
                        eprintln!("{} blocked; no throughput claimed", s.id);
                        break;
                    }
                };
                std::fs::write(filename, serde_json::to_vec_pretty(&result)?)?;
                println!(
                    "{} c{} r{}: {:.1} {} ({} failures)",
                    s.id,
                    clients,
                    repetition,
                    result.successful_operations_per_second,
                    s.unit,
                    result.failed_operations
                );
            }
        }
    }
    Ok(())
}
