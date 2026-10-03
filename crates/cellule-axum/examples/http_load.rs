//! Steady-state, closed-loop HTTP reads with bounded histograms and full validation.
//! The RustFS runner supplies a JSON configuration containing acknowledged orders.

use std::{path::PathBuf, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{sync::Barrier, task::JoinSet, time::Instant};

type LoadResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
// Ten-microsecond upper bounds through one second, plus an overflow bucket.
const BUCKET_US: u64 = 10;
const BUCKETS: usize = 100_002;

#[derive(Deserialize)]
struct Config {
    address: std::net::SocketAddr,
    concurrency: usize,
    seconds: u64,
    warmup_seconds: u64,
    orders: Vec<Observation>,
}

#[derive(Deserialize)]
struct Observation {
    output: Order,
    receipt: Receipt,
}

#[derive(Deserialize, PartialEq)]
struct Order {
    id: i64,
    total_cents: i64,
}

#[derive(Deserialize)]
struct Receipt {
    cell: String,
    incarnation: String,
    commit_sequence: u64,
}

#[derive(Serialize)]
struct Measurements {
    warmup_successes: u64,
    attempts: u64,
    successes: u64,
    errors: u64,
    payload_bytes: u64,
    seconds: f64,
    successful_requests_per_second: f64,
    payload_bytes_per_second: f64,
    latency_ms_all_attempts: Percentiles,
    max_latency_ms: f64,
    histogram_resolution_us: u64,
    histogram_overflow: u64,
    per_order_successes: Vec<u64>,
    first_error: Option<String>,
}

#[derive(Serialize)]
struct Percentiles {
    p50: Option<f64>,
    p95: Option<f64>,
    p99: Option<f64>,
}

struct Counts {
    warmup_successes: u64,
    histogram: Vec<u64>,
    successes: Vec<u64>,
    errors: u64,
    bytes: u64,
    max_us: u64,
    first_error: Option<String>,
}

impl Counts {
    fn new(orders: usize) -> Self {
        Self {
            warmup_successes: 0,
            histogram: vec![0; BUCKETS],
            successes: vec![0; orders],
            errors: 0,
            bytes: 0,
            max_us: 0,
            first_error: None,
        }
    }

    fn merge(&mut self, other: Self) {
        self.warmup_successes += other.warmup_successes;
        for (total, count) in self.histogram.iter_mut().zip(other.histogram) {
            *total += count;
        }
        for (total, count) in self.successes.iter_mut().zip(other.successes) {
            *total += count;
        }
        self.errors += other.errors;
        self.bytes += other.bytes;
        self.max_us = self.max_us.max(other.max_us);
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }

    fn percentile(&self, attempts: u64, percent: u64) -> Option<f64> {
        let rank = (attempts * percent).div_ceil(100).max(1);
        let mut count = 0;
        for (index, bucket) in self.histogram.iter().enumerate() {
            count += bucket;
            if count >= rank {
                return (index < BUCKETS - 1).then_some(index as f64 * BUCKET_US as f64 / 1000.0);
            }
        }
        None
    }
}

async fn request(client: &reqwest::Client, url: &str, expected: &Observation) -> LoadResult<usize> {
    let response = client.get(url).send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if status != reqwest::StatusCode::OK {
        return Err(format!("GET returned {status}: {}", String::from_utf8_lossy(&bytes)).into());
    }
    let actual: Observation = serde_json::from_slice(&bytes)?;
    if actual.output != expected.output
        || actual.receipt.cell != expected.receipt.cell
        || actual.receipt.incarnation != expected.receipt.incarnation
        || actual.receipt.commit_sequence < expected.receipt.commit_sequence
    {
        return Err("GET violated acknowledged order or minimum receipt".into());
    }
    Ok(bytes.len())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> LoadResult<()> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("configuration path required")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    if !config.address.ip().is_loopback()
        || !(1..=128).contains(&config.concurrency)
        || !(1..=3600).contains(&config.seconds)
        || !(1..=60).contains(&config.warmup_seconds)
        || config.orders.is_empty()
        || config.orders.len() > 100_000
    {
        return Err("invalid bounded loopback workload".into());
    }
    let client = reqwest::Client::builder()
        .http1_only()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?;
    let concurrency = config.concurrency;
    let seconds = config.seconds;
    let config = Arc::new(config);
    let urls: Arc<Vec<_>> = Arc::new(
        config
            .orders
            .iter()
            .map(|order| format!("http://{}/orders/{}", config.address, order.output.id))
            .collect(),
    );
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = JoinSet::new();
    // Every client visits every order; rotating offsets avoid stride/modulo aliasing.
    for worker in 0..concurrency {
        let config = config.clone();
        let client = client.clone();
        let barrier = barrier.clone();
        let urls = urls.clone();
        tasks.spawn(async move {
            let mut index = worker % config.orders.len();
            let warmup_until = Instant::now() + Duration::from_secs(config.warmup_seconds);
            let mut warmup_successes = 0;
            while Instant::now() < warmup_until {
                request(&client, &urls[index], &config.orders[index]).await?;
                warmup_successes += 1;
                index = (index + 1) % config.orders.len();
            }
            let mut counts = Counts::new(config.orders.len());
            counts.warmup_successes = warmup_successes;
            barrier.wait().await;
            let end = Instant::now() + Duration::from_secs(seconds);
            while Instant::now() < end {
                let started = Instant::now();
                let result = request(&client, &urls[index], &config.orders[index]).await;
                let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                counts.max_us = counts.max_us.max(micros);
                let bucket = usize::try_from(micros.div_ceil(BUCKET_US))
                    .unwrap_or(usize::MAX)
                    .min(BUCKETS - 1);
                counts.histogram[bucket] += 1;
                match result {
                    Ok(bytes) => {
                        counts.successes[index] += 1;
                        counts.bytes += bytes as u64;
                    }
                    Err(error) => {
                        counts.errors += 1;
                        if counts.first_error.is_none() {
                            counts.first_error = Some(error.to_string());
                        }
                    }
                }
                index = (index + 1) % config.orders.len();
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(counts)
        });
    }
    // A warmup failure must not strand the other clients at the barrier.
    tokio::select! {
        _ = barrier.wait() => {}
        failed = tasks.join_next() => {
            return Err(format!("warmup client terminated: {:?}", failed.map(|result| result.map(|inner| inner.err().map(|error| error.to_string())))).into());
        }
    }
    let started = Instant::now();
    let mut counts = Counts::new(config.orders.len());
    while let Some(result) = tasks.join_next().await {
        counts.merge(result??);
    }
    let elapsed = started.elapsed().as_secs_f64();
    let successes: u64 = counts.successes.iter().sum();
    let attempts = successes + counts.errors;
    let measurement = Measurements {
        warmup_successes: counts.warmup_successes,
        attempts,
        successes,
        errors: counts.errors,
        payload_bytes: counts.bytes,
        seconds: elapsed,
        successful_requests_per_second: successes as f64 / elapsed,
        payload_bytes_per_second: counts.bytes as f64 / elapsed,
        latency_ms_all_attempts: Percentiles {
            p50: counts.percentile(attempts, 50),
            p95: counts.percentile(attempts, 95),
            p99: counts.percentile(attempts, 99),
        },
        max_latency_ms: counts.max_us as f64 / 1000.0,
        histogram_resolution_us: BUCKET_US,
        histogram_overflow: counts.histogram[BUCKETS - 1],
        per_order_successes: counts.successes,
        first_error: counts.first_error,
    };
    println!("{}", serde_json::to_string(&measurement)?);
    if measurement.errors != 0 || measurement.per_order_successes.contains(&0) {
        return Err("steady-state reads failed validation or coverage".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograms_merge_and_report_upper_bounds_and_overflow() {
        let mut counts = Counts::new(1);
        counts.histogram[1] = 2;
        counts.successes[0] = 2;
        let mut other = Counts::new(1);
        other.histogram[2] = 1;
        other.histogram[BUCKETS - 1] = 1;
        other.successes[0] = 1;
        other.errors = 1;
        counts.merge(other);
        assert_eq!(counts.percentile(4, 50), Some(0.01));
        assert_eq!(counts.percentile(4, 75), Some(0.02));
        assert_eq!(counts.percentile(4, 99), None);
        assert_eq!(counts.successes, [3]);
        assert_eq!(counts.errors, 1);
    }

    #[tokio::test]
    async fn driver_rejects_incorrect_data_and_receipts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/orders/{id}",
            axum::routing::get(|axum::extract::Path(id): axum::extract::Path<i64>| async move {
                axum::Json(serde_json::json!({
                    "output": {"id": 1, "total_cents": if id == 4 { 100 } else { 112 }},
                    "receipt": {
                        "cell": if id == 2 { "wrong-cell" } else { "cell" },
                        "incarnation": if id == 3 { "wrong-incarnation" } else { "incarnation" },
                        "commit_sequence": if id == 5 { 0 } else { 1 }
                    }
                }))
            }),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let expected: Observation = serde_json::from_value(serde_json::json!({
            "output": {"id": 1, "total_cents": 112},
            "receipt": {"cell": "cell", "incarnation": "incarnation", "commit_sequence": 1}
        }))
        .unwrap();
        assert!(
            request(&client, &format!("http://{address}/orders/1"), &expected)
                .await
                .is_ok()
        );
        for id in 2..=5 {
            assert!(
                request(&client, &format!("http://{address}/orders/{id}"), &expected)
                    .await
                    .is_err()
            );
        }
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}
