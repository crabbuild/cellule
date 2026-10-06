//! Sparse, receipt-bound owner reads include the generator in the owner budget.

use super::*;
use crate::performance_fixture::DurabilityRecorder;
use cellule_runtime::{Receipt, primitives::sql::SqlCell};
use tokio::task::JoinSet;

mod export;

pub(super) struct Seed {
    pub(super) sql: SqlCell<ReferenceSql>,
    pub(super) minimum: Receipt,
    pub(super) key: u64,
    pub(super) digest: [u8; 32],
    pub(super) value: Vec<u8>,
}

struct Sample {
    // Arrival, entity, scheduled, generated, dispatched, terminal, minimum
    // sequence, observed sequence, outcome (1 success, 2 error, 3 client full).
    fields: [u64; 9],
    digest: [u8; 32],
    error: Option<String>,
}

async fn query(cells: Arc<Vec<Seed>>, mut sample: Sample, start: Instant) -> Sample {
    let seed = &cells[sample.fields[1] as usize];
    match seed
        .sql
        .query(
            Some(seed.minimum),
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT payload, digest FROM write_values WHERE key = ?1".into(),
                    parameters: vec![SqlValue::Integer(seed.key as i64)],
                }],
            },
        )
        .await
    {
        Ok(observed) => {
            assert_eq!(observed.receipt, seed.minimum);
            let [SqlValue::Blob(value), SqlValue::Blob(digest)] =
                observed.output[0].rows[0].as_slice()
            else {
                panic!("invalid owner point-read result")
            };
            assert_eq!(observed.output[0].rows.len(), 1);
            assert_eq!(value, &seed.value);
            assert_eq!(digest.as_slice(), blake3::hash(value).as_bytes());
            sample.digest = digest.as_slice().try_into().unwrap();
            sample.fields[7] = observed.receipt.commit_sequence;
            sample.fields[8] = 1;
        }
        Err(error) => {
            sample.fields[8] = 2;
            sample.error = Some(format!("{error:?}"));
        }
    }
    sample.fields[5] = start.elapsed().as_micros() as u64;
    sample
}

pub(super) async fn run(
    sync: &Path,
    host: &cellule_host::CellNode,
    cells: Vec<Seed>,
    observations: observation::NodeObservations,
    storage: Arc<observation::StorageCounters>,
    telemetry: Arc<DurabilityRecorder>,
) -> observation::NodeObservations {
    const SECONDS: usize = 60;
    const CONCURRENCY: usize = 1024;
    assert_eq!(cells.len(), 2000);
    let cells = Arc::new(cells);
    let mut exporter = export::Writer::new(sync, observations, storage, telemetry);
    let mut seeds = BufWriter::new(File::create(sync.join("read-seeds.tsv")).unwrap());
    writeln!(
        seeds,
        "entity\tminimum_sequence\tkey\tdigest\tpayload_bytes"
    )
    .unwrap();
    for (entity, seed) in cells.iter().enumerate() {
        let digest = seed
            .digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        writeln!(
            seeds,
            "{entity}\t{}\t{}\t{digest}\t{}",
            seed.minimum.commit_sequence,
            seed.key,
            seed.value.len()
        )
        .unwrap();
    }
    seeds.flush().unwrap();
    let mut windows = BufWriter::new(File::create(sync.join("read-windows.tsv")).unwrap());
    writeln!(windows, "window\tphase\trate\tseconds\tconcurrency\tcount\telapsed_us\tcpu_us\tprocess_rss_bytes\tmemory_current_bytes\tmemory_peak_bytes\tdescriptors\tsqlite_descriptors\tactive_cells\tstart_at_ms\tend_at_ms\texport_requests\texport_accepted\texport_failed\texport_completed\texport_drain_us\texport_drain_cpu_us\tfully_served").unwrap();
    let mut points = vec![(0, "warmup", 5000)];
    points.extend(
        [5000, 10_000, 25_000, 50_000]
            .into_iter()
            .enumerate()
            .map(|(index, rate)| (index + 1, "measure", rate)),
    );
    for (id, phase, rate) in points {
        let start_at_ms = now_ms();
        let start = Instant::now() + Duration::from_millis(20);
        let before_cpu = cpu_us();
        let mut next_trace_flush = start + Duration::from_secs(1);
        let (mut export_requests, mut export_accepted, mut export_failed) = (0, 0, 0);
        let (mut arrivals, pacer) =
            driver::pacing::paced_arrivals(start, rate * SECONDS, rate, CONCURRENCY);
        let mut jobs = JoinSet::new();
        let mut samples = Vec::with_capacity(rate * SECONDS);
        while let Some((arrival, generated)) = arrivals.recv().await {
            if Instant::now() >= next_trace_flush {
                // Never pause arrival dispatch for filesystem I/O. A full
                // bounded writer queue is retained as a failed evidence gate.
                export_requests += 1;
                if exporter.request(id, export_requests) {
                    export_accepted += 1;
                } else {
                    export_failed += 1;
                }
                next_trace_flush = Instant::now() + Duration::from_secs(1);
            }
            while let Some(result) = jobs.try_join_next() {
                samples.push(result.unwrap());
            }
            let entity = arrival % cells.len();
            let dispatched = start.elapsed().as_micros() as u64;
            let mut sample = Sample {
                fields: [
                    arrival as u64,
                    entity as u64,
                    arrival as u64 * 1_000_000 / rate as u64,
                    generated,
                    dispatched,
                    0,
                    cells[entity].minimum.commit_sequence,
                    0,
                    0,
                ],
                digest: [0; 32],
                error: None,
            };
            if jobs.len() == CONCURRENCY {
                sample.fields[5] = dispatched;
                sample.fields[8] = 3;
                samples.push(sample);
            } else {
                jobs.spawn(query(cells.clone(), sample, start));
            }
        }
        pacer.await.unwrap();
        tokio::time::sleep_until((start + Duration::from_secs(SECONDS as u64)).into()).await;
        while let Some(result) = jobs.join_next().await {
            samples.push(result.unwrap());
        }
        let elapsed = start.elapsed().as_micros() as u64;
        let used_cpu = cpu_us() - before_cpu;
        let drain_started = Instant::now();
        let before_drain_cpu = cpu_us();
        let export_completed = exporter.finish_window(id).await;
        let export_drain_us = drain_started.elapsed().as_micros();
        let export_drain_cpu_us = cpu_us() - before_drain_cpu;
        let end_at_ms = now_ms();
        assert_eq!(export_completed, export_accepted);
        assert_eq!(samples.len(), rate * SECONDS);
        let mut latency = samples
            .iter()
            .map(|sample| sample.fields[5] - sample.fields[2])
            .collect::<Vec<_>>();
        let mut generator = samples
            .iter()
            .map(|sample| sample.fields[3] - sample.fields[2])
            .collect::<Vec<_>>();
        latency.sort_unstable();
        generator.sort_unstable();
        let p99 = |values: &[u64]| values[(values.len() * 99).div_ceil(100) - 1];
        let fully_served = samples.iter().all(|sample| sample.fields[8] == 1)
            && p99(&latency) <= 50_000
            && p99(&generator) <= 5000
            && elapsed <= 62_000_000
            && export_failed == 0;
        let (fds, sqlite) = descriptors();
        assert_eq!(host.stats().active_cells(), 2000);
        writeln!(windows, "{id}\t{phase}\t{rate}\t{SECONDS}\t{CONCURRENCY}\t{}\t{elapsed}\t{used_cpu}\t{}\t{}\t{}\t{fds}\t{sqlite}\t2000\t{start_at_ms}\t{end_at_ms}\t{export_requests}\t{export_accepted}\t{export_failed}\t{export_completed}\t{export_drain_us}\t{export_drain_cpu_us}\t{fully_served}", samples.len(), proc_memory("VmRSS"), memory("memory.current"), memory("memory.peak")).unwrap();
        // These are terminal records, including failed admission and late
        // arrivals. Export happens after timing and cannot convert failures.
        samples.sort_unstable_by_key(|sample| sample.fields[0]);
        let mut raw = BufWriter::new(File::create(sync.join(format!("read-{id}.bin"))).unwrap());
        let mut errors =
            BufWriter::new(File::create(sync.join(format!("read-{id}-errors.tsv"))).unwrap());
        writeln!(errors, "arrival\terror").unwrap();
        for sample in samples {
            for field in sample.fields {
                raw.write_all(&field.to_be_bytes()).unwrap();
            }
            raw.write_all(&sample.digest).unwrap();
            if let Some(error) = sample.error {
                writeln!(
                    errors,
                    "{}\t{}",
                    sample.fields[0],
                    error.replace(['\t', '\n', '\r'], " ")
                )
                .unwrap();
            }
        }
        raw.flush().unwrap();
        errors.flush().unwrap();
        windows.flush().unwrap();
        println!(
            "OWNER_READ window={id} phase={phase} rate={rate} fully_served={fully_served} scheduled_p99_us={}",
            p99(&latency)
        );
        if phase == "measure" && !fully_served {
            break;
        }
    }
    exporter.finish()
}
