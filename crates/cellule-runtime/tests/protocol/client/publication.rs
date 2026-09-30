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
//! Recorded result (2026-09-30, one Cell, in-memory provider, 24 commands per
//! level). Provider latencies come from the 2026-09-29 write-capacity run
//! (PUT 6.7-7.5 ms); zeroing them separates local work from provider round
//! trips.
//!
//! | Model | prep p50 | control CAS p50 | total p50 | commands/s |
//! | --- | ---: | ---: | ---: | ---: |
//! | PUT 7 ms, GET/HEAD 2 ms | 14.9 ms | 9.1 ms | 24.7 ms | 32.6 |
//! | PUT 0 ms, GET/HEAD 0 ms | 3.8 ms | 0.1 ms | 5.0 ms | 121.7 |
//!
//! So one acknowledged command is provider-dominated: ~11 ms of preparation is
//! provider time (a two-HEAD predecessor wave plus one parallel six-PUT upload
//! wave) and only ~3.8 ms is local work, with ~9 ms of control CAS after it.
//! Step timings taken with temporary instrumentation agreed: the captured
//! segment open and index resolve cost ~0.3 ms, chain validation ~10 us, and
//! the directory phase ~0.1 ms. An earlier reading of "~9 ms of local
//! preparation" was an artifact of zeroing only the PUT latency while GET and
//! HEAD stayed at 2 ms.
//!
//! Two probes settle the next step:
//!
//! * Doubling the PUT latency (7 -> 14 ms) adds ~6 ms to preparation but ~7 ms
//!   to the single control CAS, so the six immutable uploads already fly in one
//!   parallel wave rather than serialized batches. There is no wave to merge.
//! * `queue_wait` stays at microseconds even at concurrency 16, because the Cell
//!   is busy from admission until its durability proof lands: a second
//!   publication cannot be queued behind the first.
//!
//! The remaining levers are therefore provider round trips per commit, not CPU:
//! the control CAS (~7 ms) and the shared directory/page/root uploads are only
//! amortized by publishing one root for several commits, and coalescing has
//! nothing to merge until the actor may execute ahead of an in-flight root.
//! Skipping the predecessor wave entirely saves ~3 ms of the ~25 ms commit and
//! removes the only positive presence check, which is why the predecessor-graph
//! cache stays unimplemented.
//!
//! Raising offered concurrency does not raise throughput (29.7 -> 27.0
//! commands/s from 1 to 16 at 48 commands); it only grows p95.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, PublicationTiming};
use cellule_runtime::identity::CellId;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Provider p95 latencies recorded by the 2026-09-29 write-capacity run:
/// PUT 6.7-7.5 ms, GET 1.9-2.1 ms.
/// Provider latencies for the model; override to separate local work from
/// provider round trips. With both set to zero the harness reports the pure
/// local cost of one publish.
fn provider_ms(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Default)]
struct PublicationRecorder {
    timings: Mutex<Vec<PublicationTiming>>,
    phases: Mutex<Vec<(cellule_ltx::LtxPhase, Duration)>>,
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

    fn ltx_phase(&self, phase: cellule_ltx::LtxPhase, elapsed: Duration, _succeeded: bool) {
        self.phases.lock().unwrap().push((phase, elapsed));
    }
}

/// A provider wrapper that holds the control CAS until the test releases it.
///
/// Immutable Cell objects use `PutMode::Create`, so gating `PutMode::Update`
/// gates exactly the authority transition. The gate stays disarmed until the
/// fixture is up, because provisioning publishes its own control successor.
#[derive(Debug)]
struct HeldControlCasStore {
    inner: Arc<InMemory>,
    armed: AtomicBool,
    cas_reached: Arc<tokio::sync::Notify>,
    cas_release: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Display for HeldControlCasStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("held-control-cas-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldControlCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.armed.load(Ordering::SeqCst) && matches!(&options.mode, PutMode::Update(_)) {
            // Wake the test, then block the CAS until it releases the gate.
            self.cas_reached.notify_one();
            let permit =
                self.cas_release
                    .acquire()
                    .await
                    .map_err(|_| object_store::Error::Generic {
                        store: "held-control-cas-store",
                        source: Box::new(std::io::Error::other("control CAS gate closed")),
                    })?;
            permit.forget();
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A read must never observe a commit whose root has not been published.
///
/// The lane holds the control CAS, so the commit is recorded and uploaded but
/// not yet authoritative. A read may be rejected or wait, but it must never
/// report the unproven mutation; once the CAS lands, the same read sees it.
///
/// Today the requirement is enforced by keeping the Cell busy until the proof
/// lands. Pipelining removes that gate, so this test is the contract the
/// explicit proof watermark must keep satisfying.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_never_observe_a_commit_before_its_root_publishes() {
    let cas_reached = Arc::new(tokio::sync::Notify::new());
    let cas_release = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(HeldControlCasStore {
        inner: Arc::new(InMemory::new()),
        armed: AtomicBool::new(false),
        cas_reached: Arc::clone(&cas_reached),
        cas_release: Arc::clone(&cas_release),
    });
    let fixture = fixture_with_store(Limits::default(), Store::new(store.clone())).await;
    let client = CellClient::local(Arc::clone(&fixture.registry), fixture.handle().clone());
    assert_eq!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap()
            .output,
        0
    );

    store.armed.store(true, Ordering::SeqCst);
    let command = {
        let client = client.clone();
        let target = fixture.target.clone();
        tokio::spawn(async move {
            client
                .command::<CreateComment>(&target, mutation_identity(41), b"gated".to_vec())
                .await
        })
    };
    // The publication path is now blocked inside the control CAS.
    cas_reached.notified().await;
    assert!(
        !command.is_finished(),
        "the control CAS did not hold the publication, so this lane proves nothing"
    );

    let mut read = {
        let client = client.clone();
        let target = fixture.target.clone();
        tokio::spawn(async move { client.query::<CountComments>(&target, None, ()).await })
    };
    let mut read_finished_early = false;
    tokio::select! {
        finished = &mut read => {
            read_finished_early = true;
            let error = finished
                .unwrap()
                .expect_err("a read served a commit before its root published");
            assert!(
                matches!(error, InvocationError::NotStarted(_)),
                "unexpected read failure: {error:?}"
            );
        }
        () = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    cas_release.add_permits(1);
    command.await.unwrap().unwrap();
    let observed = if read_finished_early {
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap()
    } else {
        read.await.unwrap().unwrap()
    };
    assert_eq!(observed.output, 1, "the published commit must be readable");
}

/// Summarizes one LTX phase over the commands of a measurement level.
fn phase_summary(
    phases: &[(cellule_ltx::LtxPhase, Duration)],
    wanted: cellule_ltx::LtxPhase,
) -> (usize, Duration, Duration) {
    let mut samples: Vec<u128> = phases
        .iter()
        .filter(|(phase, _)| *phase == wanted)
        .map(|(_, elapsed)| elapsed.as_micros())
        .collect();
    if samples.is_empty() {
        return (0, Duration::ZERO, Duration::ZERO);
    }
    let p95 = percentile(samples.clone(), 95);
    let total: u128 = samples.iter().sum();
    let count = samples.len();
    samples.clear();
    (
        count,
        Duration::from_micros((total / count as u128) as u64),
        p95,
    )
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
        wait_get_per_call: Duration::from_millis(provider_ms("CELLULE_MODEL_GET_MS", 2)),
        wait_put_per_call: Duration::from_millis(provider_ms("CELLULE_MODEL_PUT_MS", 7)),
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
        recorder.phases.lock().unwrap().clear();
        recorder.objects.store(0, Ordering::Relaxed);
        let puts_before = counted.put_requests();
        let writes_before = recorder.objects.load(Ordering::Relaxed);
        let started = Instant::now();

        let mut tasks = Vec::new();
        // Distribute the remainder so every level issues exactly `commands`.
        let per_task = commands.div_ceil(concurrency);
        let mut planned = commands;
        for _lane in 0..concurrency {
            let client = client.clone();
            let target = fixture.target.clone();
            let lane_commands = per_task.min(planned);
            planned -= lane_commands;
            if lane_commands == 0 {
                continue;
            }
            let identities: Vec<u8> = (0..lane_commands)
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
        let phases = recorder.phases.lock().unwrap().clone();
        for phase in [
            cellule_ltx::LtxPhase::RootPreparation,
            cellule_ltx::LtxPhase::Directory,
            cellule_ltx::LtxPhase::Verification,
            cellule_ltx::LtxPhase::Encode,
            cellule_ltx::LtxPhase::Capture,
        ] {
            let (count, mean, p95) = phase_summary(&phases, phase);
            if count > 0 {
                println!("  phase {phase:?}: count={count} mean={mean:?} p95={p95:?}");
            }
        }
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
