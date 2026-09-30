//! Local publication attribution over a throttled provider.
//!
//! The recorded capacity runs measure the phases but run only in CI with a real
//! provider. This model drives one Cell at several offered concurrency levels
//! through the actor, prices each provider operation with the p95 latencies the
//! capacity run recorded, and reports where an acknowledged command's time goes:
//! queue wait behind earlier roots, root preparation, authority CAS, and total.
//!
//! Run manually:
//!
//! ```sh
//! cargo test -p cellule-runtime --features test-support \
//!   --test protocol client::publication -- --ignored --nocapture
//! ```
//!
//! Recorded result (2026-09-30, one Cell, in-memory provider, 2 ms/GET and
//! 7 ms/PUT):
//!
//! | Offered concurrency | commands/s | PUTs per publish | prep p50 | CAS p50 | total p50 |
//! | ---: | ---: | ---: | ---: | ---: | ---: |
//! | 1 | 29.7 | 6.1 | 15.7 ms | 9.1 ms | 25.9 ms |
//! | 4 | 27.4 | 6.3 | 17.5 ms | 9.1 ms | 28.3 ms |
//! | 16 | 27.0 | 6.3 | 18.1 ms | 9.2 ms | 28.7 ms |
//!
//! Two probes explain the shape:
//!
//! * Doubling the PUT latency (7 -> 14 ms) adds ~6 ms to preparation but ~7 ms
//!   to the single control CAS, so the six immutable uploads already fly in one
//!   parallel wave rather than serialized batches. There is no wave to merge.
//! * `queue_wait` stays at microseconds even at concurrency 16, because the Cell
//!   is busy from admission until its durability proof lands: a second
//!   publication cannot be queued behind the first. Publication coalescing
//!   therefore has nothing to coalesce until the actor may execute ahead of an
//!   in-flight root, and that pipelining is the release-gate change the plan
//!   defers to a fault-injection lane.
//!
//! What is left per acknowledged command is ~7 ms of uploads, ~9 ms of control
//! CAS, and ~9 ms of local preparation work (capture index inspection, chain
//! and directory validation, root encoding). Raising concurrency does not help:
//! it only grows p95 while throughput falls.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, PublicationTiming};
use cellule_runtime::identity::CellId;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Provider p95 latencies recorded by the 2026-09-29 write-capacity run:
/// PUT 6.7-7.5 ms, GET 1.9-2.1 ms.
const GET_MS: u64 = 2;

/// Provider PUT latency for the model; override to probe upload concurrency.
///
/// If preparation scales with this value, the publish path serializes provider
/// writes into waves; if it stays flat, the remaining time is local work.
fn put_ms() -> u64 {
    std::env::var("CELLULE_MODEL_PUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(7)
}

#[derive(Default)]
struct PublicationRecorder {
    timings: Mutex<Vec<PublicationTiming>>,
    objects: AtomicUsize,
    bytes: AtomicUsize,
}

impl CellTelemetry for PublicationRecorder {
    fn publication_completed(&self, _cell: CellId, timing: PublicationTiming) {
        self.timings.lock().unwrap().push(timing);
    }

    fn publication_cost(&self, objects: u64, bytes: u64) {
        self.objects.fetch_add(objects as usize, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as usize, Ordering::Relaxed);
    }
}

fn percentile(mut samples: Vec<u128>, percent: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    let index = (samples.len() * percent / 100).min(samples.len() - 1);
    Duration::from_micros(samples[index] as u64)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual publication phase attribution over a throttled provider"]
async fn publication_phase_attribution_over_a_throttled_provider() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(GET_MS),
        wait_put_per_call: Duration::from_millis(put_ms()),
        ..ThrottleConfig::default()
    };
    let provider = Arc::new(ThrottledStore::new(Arc::clone(&counted), config));
    let fixture = fixture_with_store(Limits::default(), Store::new(provider)).await;
    let recorder = Arc::new(PublicationRecorder::default());
    fixture
        .runtime
        .as_ref()
        .expect("fixture runtime")
        .install_telemetry(recorder.clone())
        .unwrap();
    let client = CellClient::local(Arc::clone(&fixture.registry), fixture.handle().clone());

    // Warm the route and the description so the measurement covers the write
    // path rather than the first-call discovery.
    client
        .command::<CreateComment>(&fixture.target, mutation_identity(1), b"warm".to_vec())
        .await
        .unwrap();

    // Override to probe how preparation scales with the segment count.
    let commands: usize = std::env::var("CELLULE_MODEL_COMMANDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(48);
    // Identities must be unique across every level: the same request ID with a
    // fresh issue time is a conflicting reuse, not a replay.
    let mut next_identity = 10_u8;
    for concurrency in [1_usize, 4, 16] {
        recorder.timings.lock().unwrap().clear();
        recorder.objects.store(0, Ordering::Relaxed);
        let puts_before = counted.put_requests();
        let writes_before = recorder.objects.load(Ordering::Relaxed);
        let started = Instant::now();

        let mut tasks = Vec::new();
        let per_task = commands / concurrency;
        for _lane in 0..concurrency {
            let client = client.clone();
            let target = fixture.target.clone();
            let identities: Vec<u8> = (0..per_task)
                .map(|_| {
                    let byte = next_identity;
                    next_identity = next_identity.wrapping_add(1);
                    byte
                })
                .collect();
            tasks.push(tokio::spawn(async move {
                for byte in identities {
                    client
                        .command::<CreateComment>(
                            &target,
                            mutation_identity(byte),
                            b"attributed".to_vec(),
                        )
                        .await
                        .unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = started.elapsed();
        let _ = writes_before;

        let timings = recorder.timings.lock().unwrap().clone();
        let queue: Vec<u128> = timings
            .iter()
            .map(|timing| timing.queue_wait.as_micros())
            .collect();
        let preparation: Vec<u128> = timings
            .iter()
            .map(|timing| timing.preparation.as_micros())
            .collect();
        let authority: Vec<u128> = timings
            .iter()
            .map(|timing| timing.authority.as_micros())
            .collect();
        let total: Vec<u128> = timings
            .iter()
            .map(|timing| timing.total.as_micros())
            .collect();
        let puts = counted.put_requests() - puts_before;
        println!(
            "concurrency={concurrency:2} commands={commands} elapsed={elapsed:?} \
             commands/s={:.1} publications={} puts={} puts/publish={:.2}",
            commands as f64 / elapsed.as_secs_f64(),
            timings.len(),
            puts,
            puts as f64 / timings.len().max(1) as f64,
        );
        println!(
            "  queue_wait p50={:?} p95={:?} | preparation p50={:?} p95={:?} | \
             authority p50={:?} p95={:?} | total p50={:?} p95={:?}",
            percentile(queue.clone(), 50),
            percentile(queue, 95),
            percentile(preparation.clone(), 50),
            percentile(preparation, 95),
            percentile(authority.clone(), 50),
            percentile(authority, 95),
            percentile(total.clone(), 50),
            percentile(total, 95),
        );
    }

    fixture.handle().drain().await.unwrap();
}
