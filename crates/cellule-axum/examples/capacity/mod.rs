//! Bounded arrival queue and retained, validated outcomes for a local capacity probe.
use std::{io::Write as _, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::Instant,
};

mod metrics;
mod request;
use metrics::Metrics;
use request::{Observation, WriteRequest};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    address: std::net::SocketAddr,
    cells: usize,
    concurrency: usize,
    queue_capacity: usize,
    write_rate: u64,
    read_rate: u64,
    warmup_seconds: u64,
    seconds: u64,
    evidence_directory: std::path::PathBuf,
    seed_file: Option<std::path::PathBuf>,
    #[serde(default)]
    write_offset: u64,
    #[serde(default)]
    metrics_urls: Vec<String>,
    hot_read_cells: Option<usize>,
    metrics_tls_directory: Option<std::path::PathBuf>,
}

impl Config {
    fn validate(&self) -> Result<()> {
        if !self.address.ip().is_loopback()
            || !(1..=2_000).contains(&self.cells)
            || !(1..=1_024).contains(&self.concurrency)
            || !(1..=16_384).contains(&self.queue_capacity)
            || self.write_rate > 100_000
            || self.read_rate > 100_000
            || self.write_rate + self.read_rate == 0
            || !(1..=60).contains(&self.warmup_seconds)
            || !(1..=3_600).contains(&self.seconds)
            || !self.write_offset.is_multiple_of(self.cells as u64)
            || self
                .hot_read_cells
                .is_some_and(|cells| cells == 0 || cells > self.cells)
            || self.metrics_urls.len() > 3
            || self.metrics_urls.iter().any(|url| !valid_metrics_url(url))
            || (self
                .metrics_urls
                .iter()
                .any(|url| url.starts_with("https:"))
                && self.metrics_tls_directory.is_none())
        {
            return Err("invalid bounded loopback capacity configuration".into());
        }
        Ok(())
    }
}

fn valid_metrics_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some_and(|host| {
                host.parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
            })
            && url.username().is_empty()
            && url.password().is_none()
    })
}

async fn sample_metrics(
    client: &reqwest::Client,
    urls: &[String],
    destination: &std::path::Path,
) -> Result<()> {
    let mut observations = Vec::new();
    for url in urls {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let result: serde_json::Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let finished = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        observations.push(serde_json::json!({ "url": url, "request_started_ms": started, "request_finished_ms": finished, "metrics": result }));
    }
    std::fs::write(destination, serde_json::to_vec(&observations)?)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Kind {
    Write,
    Read,
}

struct Job {
    kind: Kind,
    index: u64,
    due: Instant,
    measured: bool,
}

#[derive(Serialize)]
struct Record {
    request: Option<WriteRequest>,
    read_id: Option<i64>,
    measured: bool,
    status: u16,
    response: Option<Observation>,
    error: Option<String>,
    scheduled_seconds: f64,
    completed_seconds: f64,
    request_latency_ms: f64,
    scheduled_latency_ms: f64,
    payload_bytes: usize,
}

fn journal(path: &std::path::Path) -> Result<std::io::BufWriter<std::fs::File>> {
    Ok(std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?,
    ))
}

fn retain(writer: &mut impl std::io::Write, record: &Record) -> Result<()> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

pub async fn run(config: Config) -> Result<()> {
    config.validate()?;
    std::fs::create_dir(&config.evidence_directory)?;
    std::fs::write(
        config.evidence_directory.join("config.json"),
        serde_json::to_vec_pretty(&config)?,
    )?;
    let mut metrics_client = reqwest::Client::builder()
        .http1_only()
        .no_proxy()
        .timeout(Duration::from_secs(5));
    if let Some(path) = &config.metrics_tls_directory {
        metrics_client = metrics_client.add_root_certificate(reqwest::Certificate::from_pem(
            &std::fs::read(path.join("ca.crt"))?,
        )?);
        let mut identity = std::fs::read(path.join("node-0.crt"))?;
        identity.extend(std::fs::read(path.join("node-0.key"))?);
        metrics_client = metrics_client.identity(reqwest::Identity::from_pem(&identity)?);
    }
    let metrics_client = metrics_client.build()?;
    let client = reqwest::Client::builder()
        .http1_only()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?;
    let url = format!("http://{}", config.address);
    let mut seeds: Vec<Observation> = Vec::with_capacity(config.cells);
    let mut setup = journal(&config.evidence_directory.join("setup.jsonl"))?;
    let setup_started = Instant::now();
    if let Some(path) = &config.seed_file {
        seeds = serde_json::from_slice(&std::fs::read(path)?)?;
        if seeds.len() != config.cells
            || seeds.iter().enumerate().any(|(index, seed)| {
                seed.output
                    .as_ref()
                    .is_none_or(|order| order.id != index as i64)
            })
        {
            return Err("seed file does not contain one ordered acknowledged row per Cell".into());
        }
    }
    for index in 0..if config.seed_file.is_none() {
        config.cells
    } else {
        0
    } {
        let id = i64::try_from(index)?;
        let initial: Observation = client
            .get(format!("{url}/orders/{id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if initial.output.is_some()
            || initial.receipt.commit_sequence != 0
            || seeds
                .iter()
                .any(|seed: &Observation| seed.receipt.cell == initial.receipt.cell)
        {
            return Err("probe requires fresh, distinct, sequence-zero Cells".into());
        }
        let body = WriteRequest::new(id)?;
        let started = Instant::now();
        let (status, response, bytes) =
            request::post(&client, &url, &body, &initial.receipt).await?;
        retain(
            &mut setup,
            &Record {
                request: Some(body),
                read_id: None,
                measured: false,
                status,
                response: Some(response.clone()),
                error: None,
                scheduled_seconds: started.duration_since(setup_started).as_secs_f64(),
                completed_seconds: setup_started.elapsed().as_secs_f64(),
                request_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                scheduled_latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
                payload_bytes: bytes,
            },
        )?;
        seeds.push(response);
    }
    setup.flush()?;
    setup.get_ref().sync_all()?;
    let config = Arc::new(config);
    let seeds = Arc::new(seeds);
    if !config.metrics_urls.is_empty() {
        // Warm exporter allocations and the dedicated mTLS sampling client
        // before scheduled arrivals. Priming is recorded outside the window.
        sample_metrics(
            &metrics_client,
            &config.metrics_urls,
            &config.evidence_directory.join("metrics-prime.json"),
        )
        .await?;
    }
    let start = Instant::now();
    let warm_end = start + Duration::from_secs(config.warmup_seconds);
    let end = warm_end + Duration::from_secs(config.seconds);
    let metrics_task = {
        let config = config.clone();
        let client = metrics_client;
        tokio::spawn(async move {
            if !config.metrics_urls.is_empty() {
                tokio::time::sleep_until(warm_end).await;
                sample_metrics(
                    &client,
                    &config.metrics_urls,
                    &config.evidence_directory.join("metrics-window-start.json"),
                )
                .await?;
                for minute in 1..=(config.seconds.saturating_sub(1) / 60) {
                    tokio::time::sleep_until(warm_end + Duration::from_secs(minute * 60)).await;
                    sample_metrics(
                        &client,
                        &config.metrics_urls,
                        &config
                            .evidence_directory
                            .join(format!("metrics-window-minute-{minute}.json")),
                    )
                    .await?;
                }
                tokio::time::sleep_until(end).await;
                sample_metrics(
                    &client,
                    &config.metrics_urls,
                    &config.evidence_directory.join("metrics-window-end.json"),
                )
                .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
    };
    let (sender, receiver) = mpsc::channel::<Job>(config.queue_capacity);
    let receiver = Arc::new(Mutex::new(receiver));
    let mut tasks = JoinSet::new();
    for index in 0..config.concurrency {
        let (config, seeds, receiver, client, url) = (
            config.clone(),
            seeds.clone(),
            receiver.clone(),
            client.clone(),
            url.clone(),
        );
        tasks.spawn(async move {
            let mut writes = Metrics::new(config.cells);
            let mut reads = Metrics::new(config.cells);
            let mut evidence = journal(
                &config
                    .evidence_directory
                    .join(format!("client-{index}.jsonl")),
            )?;
            loop {
                let job = receiver.lock().await.recv().await;
                let Some(job) = job else { break };
                let shard = usize::try_from(
                    job.index
                        % match job.kind {
                            Kind::Read => config.hot_read_cells.unwrap_or(config.cells),
                            Kind::Write => config.cells,
                        } as u64,
                )?;
                let started = Instant::now();
                let (body, read_id, result) = match job.kind {
                    Kind::Write => {
                        let id = i64::try_from(
                            (config.cells as u64)
                                .checked_add(job.index)
                                .and_then(|id| id.checked_add(config.write_offset))
                                .ok_or("write ID overflow")?,
                        )?;
                        let body = WriteRequest::new(id)?;
                        let result =
                            request::post(&client, &url, &body, &seeds[shard].receipt).await;
                        (Some(body), None, result)
                    }
                    Kind::Read => (
                        None,
                        Some(seeds[shard].order()?.id),
                        request::get(&client, &url, &seeds[shard]).await,
                    ),
                };
                let completed = Instant::now();
                let (status, response, payload_bytes, error) = match result {
                    Ok((status, response, bytes)) => (status, Some(response), bytes, None),
                    Err(error) => (0, None, 0, Some(error.to_string())),
                };
                let scheduled_latency = completed.duration_since(job.due);
                let metrics = match job.kind {
                    Kind::Write => &mut writes,
                    Kind::Read => &mut reads,
                };
                if job.measured {
                    metrics.observe(
                        shard,
                        error.is_none(),
                        completed < end,
                        scheduled_latency,
                        completed.duration_since(started),
                        payload_bytes,
                    );
                } else {
                    metrics.warmup_attempts += 1;
                    metrics.warmup_errors += u64::from(error.is_some());
                }
                retain(
                    &mut evidence,
                    &Record {
                        request: body,
                        read_id,
                        measured: job.measured,
                        status,
                        response,
                        error,
                        scheduled_seconds: job.due.duration_since(start).as_secs_f64(),
                        completed_seconds: completed.duration_since(start).as_secs_f64(),
                        request_latency_ms: completed.duration_since(started).as_secs_f64()
                            * 1_000.0,
                        scheduled_latency_ms: scheduled_latency.as_secs_f64() * 1_000.0,
                        payload_bytes,
                    },
                )?;
            }
            evidence.flush()?;
            evidence.get_ref().sync_all()?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((writes, reads))
        });
    }
    let mut indexes = [0_u64; 2];
    let mut offered = [0_u64; 2];
    let mut dropped = [0_u64; 2];
    let mut warmup_dropped = [0_u64; 2];
    let rates = [config.write_rate, config.read_rate];
    loop {
        let write_due = (indexes[0] * 1_000_000_000)
            .checked_div(rates[0])
            .map_or(end, |nanos| start + Duration::from_nanos(nanos));
        let read_due = (indexes[1] * 1_000_000_000)
            .checked_div(rates[1])
            .map_or(end, |nanos| start + Duration::from_nanos(nanos));
        let kind = usize::from(read_due < write_due);
        let due = if kind == 0 { write_due } else { read_due };
        if due >= end || Instant::now() >= end {
            break;
        }
        tokio::time::sleep_until(due).await;
        let measured = due >= warm_end;
        offered[kind] += u64::from(measured);
        let job = Job {
            kind: if kind == 0 { Kind::Write } else { Kind::Read },
            index: indexes[kind],
            due,
            measured,
        };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if measured {
                    dropped[kind] += 1
                } else {
                    warmup_dropped[kind] += 1
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
        indexes[kind] += 1;
    }
    drop(sender);
    let mut writes = Metrics::new(config.cells);
    let mut reads = Metrics::new(config.cells);
    let mut task_errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok((worker_writes, worker_reads))) => {
                writes.merge(worker_writes);
                reads.merge(worker_reads);
            }
            Ok(Err(error)) => task_errors.push(error.to_string()),
            Err(error) => task_errors.push(error.to_string()),
        }
    }
    match metrics_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => task_errors.push(format!("measurement sampling failed: {error}")),
        Err(error) => task_errors.push(format!("measurement sampling join failed: {error}")),
    }
    let summary = serde_json::json!({
        "setup_seconds": setup_started.elapsed().as_secs_f64() - start.elapsed().as_secs_f64(),
        "window_seconds": config.seconds, "drain_seconds": Instant::now().saturating_duration_since(end).as_secs_f64(),
        "writes": writes.summary(config.seconds, rates[0], offered[0], dropped[0], warmup_dropped[0]),
        "reads": reads.summary(config.seconds, rates[1], offered[1], dropped[1], warmup_dropped[1]),
        "task_errors": task_errors,
        "scope": "offered-load development probe; cold recovery is a separate required gate"
    });
    std::fs::write(
        config.evidence_directory.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string(&summary)?);
    if writes.errors + reads.errors + writes.warmup_errors + reads.warmup_errors != 0
        || !task_errors.is_empty()
    {
        return Err(
            "capacity probe failed; retained outcomes and summary contain the evidence".into(),
        );
    }
    Ok(())
}
