//! Finite, nonblocking runtime and provider measurements for this application.
use cellule_runtime::fleet::telemetry::{
    CellTelemetry, CommandResponseSource, DurabilitySubmissionOutcome, PrimitiveOperationKind,
    PrimitiveOperationOutcome, PublicationTiming,
};
use cellule_store::{StorageObservation, StorageObserver, StorageOperation, StorageOutcome};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

mod capture;
mod storage;
use capture::CaptureMetrics;

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(super) struct QueryMetrics {
    queries: AtomicU64,
    failures: AtomicU64,
    queue_ns: AtomicU64,
    worker_ns: AtomicU64,
    primitives: AtomicU64,
    primitive_ns: AtomicU64,
    writes: WriteMetrics,
    capture: CaptureMetrics,
    storage: storage::Accounting,
    peer: [Histogram; PeerPhase::COUNT],
    host_capacity: serde_json::Value,
    response_sources: [AtomicU64; 3],
    response_elapsed: [Histogram; 3],
    response_confirmation: Histogram,
    proof_wait: [Histogram; 2],
    submission_sources: [AtomicU64; 4],
    log_append_successes: AtomicU64,
    log_append_failures: AtomicU64,
    log_append_bytes: AtomicU64,
    selected_roots: AtomicU64,
    materialized_commits: AtomicU64,
    follower_frames: AtomicU64,
    follower_sync_calls: AtomicU64,
    follower_failures: AtomicU64,
    sample_origin: std::sync::OnceLock<std::time::Instant>,
    sample_session: std::sync::OnceLock<uuid::Uuid>,
}

// Application-owned instrumentation: fixed histograms, 100-us upper
// bounds through two seconds. No request or Cell labels and no actor locks.
const WRITE_BUCKET_US: u64 = 100;
const WRITE_BUCKETS: usize = 20_002;

struct Histogram {
    buckets: Vec<AtomicU64>,
    total_ns: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: (0..WRITE_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            total_ns: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    fn observe(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = usize::try_from(micros.div_ceil(WRITE_BUCKET_US))
            .unwrap_or(usize::MAX)
            .min(WRITE_BUCKETS - 1);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    fn snapshot(&self) -> serde_json::Value {
        let buckets: Vec<_> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let count: u64 = buckets.iter().sum();
        let percentile = |percent: u64| {
            let rank = (count * percent).div_ceil(100).max(1);
            let mut seen = 0;
            for (index, value) in buckets.iter().enumerate() {
                seen += value;
                if seen >= rank {
                    return (index < WRITE_BUCKETS - 1)
                        .then_some(index as f64 * WRITE_BUCKET_US as f64 / 1000.0);
                }
            }
            None
        };
        serde_json::json!({
            "count": count,
            "total_ns": self.total_ns.load(Ordering::Relaxed),
            "mean_ms": (count > 0).then(|| self.total_ns.load(Ordering::Relaxed) as f64 / count as f64 / 1_000_000.0),
            "p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99),
            "overflow": buckets[WRITE_BUCKETS - 1], "resolution_us": WRITE_BUCKET_US
        })
    }

    fn raw(&self) -> serde_json::Value {
        serde_json::json!({
            "resolution_us": WRITE_BUCKET_US,
            "bucket_count": WRITE_BUCKETS,
            "total_ns": self.total_ns.load(Ordering::Relaxed),
            // Preserve every cumulative count and its original bucket index.
            // Serializing hundreds of thousands of empty JSON values on an
            // async service thread perturbs the workload being measured.
            "nonzero_buckets": self.buckets.iter().enumerate().filter_map(|(index, bucket)| {
                let count = bucket.load(Ordering::Relaxed);
                (count != 0).then_some((index, count))
            }).collect::<Vec<_>>()
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum PeerPhase {
    OwnerEnrollment,
    RequestSign,
    RoundTrip,
    ReplyVerify,
    RequestVerify,
    ReceiverEnrollment,
    DurableAppend,
    FollowerWorkerQueue,
    FollowerWorker,
    FollowerDataSync,
}

impl PeerPhase {
    const COUNT: usize = 10;
    const ALL: [Self; Self::COUNT] = [
        Self::OwnerEnrollment,
        Self::RequestSign,
        Self::RoundTrip,
        Self::ReplyVerify,
        Self::RequestVerify,
        Self::ReceiverEnrollment,
        Self::DurableAppend,
        Self::FollowerWorkerQueue,
        Self::FollowerWorker,
        Self::FollowerDataSync,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::OwnerEnrollment => "owner_enrollment",
            Self::RequestSign => "request_sign",
            Self::RoundTrip => "round_trip",
            Self::ReplyVerify => "reply_verify",
            Self::RequestVerify => "request_verify",
            Self::ReceiverEnrollment => "receiver_enrollment",
            Self::DurableAppend => "durable_append",
            Self::FollowerWorkerQueue => "follower_worker_queue",
            Self::FollowerWorker => "follower_worker",
            Self::FollowerDataSync => "follower_data_sync",
        }
    }
}

#[derive(Default)]
struct WriteMetrics {
    queue: Histogram,
    worker: Histogram,
    preparation: Histogram,
    authority: Histogram,
    publication: Histogram,
    compaction: Histogram,
    root_admission: Histogram,
    root_preparation: Histogram,
    root_preparation_work: Histogram,
    root_open: Histogram,
    directory: Histogram,
    dirty_admission: Histogram,
    recovery_admission: Histogram,
    publication_failures: AtomicU64,
    objects: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Default)]
struct StorageMetrics {
    started: AtomicU64,
    duration: Histogram,
    outcomes: [AtomicU64; StorageOutcome::ALL.len()],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl StorageObserver for QueryMetrics {
    fn for_object(
        &self,
        location: Option<&object_store::path::Path>,
    ) -> Option<std::sync::Arc<dyn StorageObserver>> {
        Some(self.storage.observer(location))
    }

    fn started(&self, operation: StorageOperation) {
        self.storage.started(operation);
    }
    fn finished(&self, observation: StorageObservation) {
        self.storage.finished(observation);
    }
}

fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

impl CellTelemetry for QueryMetrics {
    fn follower_append(&self, timing: cellule_runtime::fleet::telemetry::FollowerAppendTiming) {
        self.peer_phase(PeerPhase::FollowerWorkerQueue, timing.worker_queue);
        self.peer_phase(PeerPhase::FollowerWorker, timing.worker);
        self.peer_phase(PeerPhase::FollowerDataSync, timing.data_sync);
        self.follower_frames
            .fetch_add(timing.frames, Ordering::Relaxed);
        self.follower_sync_calls
            .fetch_add(timing.data_sync_calls, Ordering::Relaxed);
        self.follower_failures
            .fetch_add(u64::from(!timing.succeeded), Ordering::Relaxed);
    }
    fn ltx_capture(&self, timing: &cellule_ltx::CaptureTiming, succeeded: bool) {
        self.capture.observe(timing, succeeded);
    }

    fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: Duration,
        confirmation: Duration,
    ) {
        let index = match source {
            CommandResponseSource::Recorded => 0,
            CommandResponseSource::Fleet => 1,
            CommandResponseSource::Object => 2,
        };
        self.response_sources[index].fetch_add(1, Ordering::Relaxed);
        self.response_elapsed[index].observe(elapsed);
        self.response_confirmation.observe(confirmation);
    }
    fn durability_proof(
        &self,
        source: cellule_runtime::node::log::DurabilitySource,
        waited: Duration,
    ) {
        let index = match source {
            cellule_runtime::node::log::DurabilitySource::Fleet => 0,
            cellule_runtime::node::log::DurabilitySource::Object => 1,
        };
        self.proof_wait[index].observe(waited);
    }
    fn durability_submission(&self, outcome: DurabilitySubmissionOutcome) {
        let index = match outcome {
            DurabilitySubmissionOutcome::Fleet => 0,
            DurabilitySubmissionOutcome::Unsupported => 1,
            DurabilitySubmissionOutcome::Unavailable => 2,
            DurabilitySubmissionOutcome::Rejected => 3,
        };
        self.submission_sources[index].fetch_add(1, Ordering::Relaxed);
    }
    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        if acknowledged {
            self.log_append_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.log_append_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.log_append_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn command_execution(&self, queue: Duration, worker: Duration, _succeeded: bool) {
        self.writes.queue.observe(queue);
        self.writes.worker.observe(worker);
    }
    fn publication_completed(&self, _cell: cellule_runtime::CellId, timing: PublicationTiming) {
        if timing.succeeded {
            self.selected_roots.fetch_add(1, Ordering::Relaxed);
            self.materialized_commits
                .fetch_add(timing.covered_commits, Ordering::Relaxed);
        }
        self.writes.preparation.observe(timing.preparation);
        self.writes.authority.observe(timing.authority);
        self.writes.publication.observe(timing.total);
        self.writes
            .publication_failures
            .fetch_add(u64::from(!timing.succeeded), Ordering::Relaxed);
    }
    fn publication_cost(&self, objects: u64, bytes: u64) {
        self.writes.objects.fetch_add(objects, Ordering::Relaxed);
        self.writes.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn ltx_phase(&self, phase: cellule_ltx::LtxPhase, elapsed: Duration, _succeeded: bool) {
        use cellule_ltx::LtxPhase;
        match phase {
            LtxPhase::Compaction => self.writes.compaction.observe(elapsed),
            LtxPhase::RootOpen => self.writes.root_open.observe(elapsed),
            LtxPhase::Directory => self.writes.directory.observe(elapsed),
            LtxPhase::RootAdmission => self.writes.root_admission.observe(elapsed),
            LtxPhase::RootPreparation => self.writes.root_preparation.observe(elapsed),
            LtxPhase::RootPreparationWork => self.writes.root_preparation_work.observe(elapsed),
            LtxPhase::DirtyAdmission => self.writes.dirty_admission.observe(elapsed),
            LtxPhase::RecoveryAdmission => self.writes.recovery_admission.observe(elapsed),
            _ => {}
        }
    }
    fn query_execution(&self, queue: Duration, worker: Duration, succeeded: bool) {
        self.queue_ns.fetch_add(nanos(queue), Ordering::Relaxed);
        self.worker_ns.fetch_add(nanos(worker), Ordering::Relaxed);
        self.failures
            .fetch_add(u64::from(!succeeded), Ordering::Relaxed);
        self.queries.fetch_add(1, Ordering::Relaxed);
    }
    fn primitive_operation(
        &self,
        _module: &'static str,
        kind: PrimitiveOperationKind,
        _outcome: PrimitiveOperationOutcome,
        elapsed: Duration,
    ) {
        if kind == PrimitiveOperationKind::Query {
            self.primitive_ns
                .fetch_add(nanos(elapsed), Ordering::Relaxed);
            self.primitives.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl QueryMetrics {
    pub(super) fn peer_phase(&self, phase: PeerPhase, elapsed: Duration) {
        self.peer[phase as usize].observe(elapsed);
    }

    pub(super) async fn enrollment<T>(
        &self,
        receiver: bool,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        let started = std::time::Instant::now();
        let result = storage::enrollment(receiver, future).await;
        self.peer_phase(
            if receiver {
                PeerPhase::ReceiverEnrollment
            } else {
                PeerPhase::OwnerEnrollment
            },
            started.elapsed(),
        );
        result
    }

    pub(super) fn window_snapshot(&self) -> serde_json::Value {
        let mut value = self.snapshot();
        let mut histograms = serde_json::Map::new();
        for (label, histogram) in [
            ("actor_queue", &self.writes.queue),
            ("worker", &self.writes.worker),
            ("publication", &self.writes.publication),
            ("compaction", &self.writes.compaction),
            ("dirty_admission", &self.writes.dirty_admission),
        ] {
            histograms.insert(label.into(), histogram.raw());
        }
        for phase in PeerPhase::ALL {
            histograms.insert(phase.label().into(), self.peer[phase as usize].raw());
        }
        for (name, histogram) in [
            ("response_recorded", &self.response_elapsed[0]),
            ("response_fleet", &self.response_elapsed[1]),
            ("response_object", &self.response_elapsed[2]),
            ("response_confirmation", &self.response_confirmation),
            ("proof_fleet", &self.proof_wait[0]),
            ("proof_object", &self.proof_wait[1]),
        ] {
            histograms.insert(name.into(), histogram.raw());
        }
        histograms.extend(self.capture.window_snapshot());
        value["histograms"] = histograms.into();
        value
    }
    pub(super) fn new(host: &cellule_ltx::Host) -> Self {
        Self {
            host_capacity: serde_json::json!({
                "io": host.io_capacity(), "jobs": host.job_capacity(),
                "dirty": host.dirty_capacity(), "recovery": host.recovery_capacity(),
                "scratch_mib": host.scratch_capacity(),
                "local_disk_bytes": host.local_disk_capacity()
            }),
            ..Self::default()
        }
    }

    // Atomic diagnostics can include in-flight background work. The final
    // snapshot after HTTP and runtime drain has stable totals.
    pub(super) fn snapshot(&self) -> serde_json::Value {
        let queries = self.queries.load(Ordering::Relaxed);
        let primitives = self.primitives.load(Ordering::Relaxed);
        let mean_us = |total: &AtomicU64, count: u64| {
            if count == 0 {
                None
            } else {
                Some(total.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0)
            }
        };
        // Derive aggregate counters from this one family snapshot. Reading
        // separate live totals before families introduces false reconciliation
        // gaps while callbacks complete. Duration histograms remain independent.
        let storage_families = self.storage.snapshot();
        let sum = |operation: &str, field: &str, outcome: Option<&str>| -> u64 {
            storage_families
                .as_object()
                .into_iter()
                .flat_map(|families| families.values())
                .map(|family| {
                    let value = &family[operation][field];
                    outcome.map_or_else(
                        || value.as_u64().unwrap_or(0),
                        |outcome| value[outcome].as_u64().unwrap_or(0),
                    )
                })
                .sum()
        };
        let storage: serde_json::Map<String, serde_json::Value> = StorageOperation::ALL
            .iter()
            .map(|operation| {
                let metrics = &self.storage.total[operation.index()];
                let outcomes: serde_json::Map<String, serde_json::Value> = StorageOutcome::ALL
                    .iter()
                    .map(|outcome| {
                        (
                            outcome.label().into(),
                            sum(operation.label(), "outcomes", Some(outcome.label())).into(),
                        )
                    })
                    .collect();
                (
                    operation.label().into(),
                    serde_json::json!({
                        "started": sum(operation.label(), "started", None),
                        "duration": metrics.duration.snapshot(), "outcomes": outcomes,
                        "bytes_read": sum(operation.label(), "bytes_read", None),
                        "bytes_written": sum(operation.label(), "bytes_written", None)
                    }),
                )
            })
            .collect();
        serde_json::json!({
            "schema_version": 3,
            "sample_session": self.sample_session.get_or_init(uuid::Uuid::now_v7).to_string(),
            "sample_elapsed_ns": nanos(self.sample_origin.get_or_init(std::time::Instant::now).elapsed()),
            "host_capacity": self.host_capacity,
            "response_sources": {
                "recorded": self.response_sources[0].load(Ordering::Relaxed),
                "fleet": self.response_sources[1].load(Ordering::Relaxed),
                "object": self.response_sources[2].load(Ordering::Relaxed)
            },
            "submission_sources": {
                "fleet": self.submission_sources[0].load(Ordering::Relaxed),
                "unsupported": self.submission_sources[1].load(Ordering::Relaxed),
                "unavailable": self.submission_sources[2].load(Ordering::Relaxed),
                "rejected": self.submission_sources[3].load(Ordering::Relaxed)
            },
            "node_log_append": {
                "successes": self.log_append_successes.load(Ordering::Relaxed),
                "failures": self.log_append_failures.load(Ordering::Relaxed),
                "bytes": self.log_append_bytes.load(Ordering::Relaxed)
            },
            "follower_append": {
                "input_frames": self.follower_frames.load(Ordering::Relaxed),
                "data_sync_calls": self.follower_sync_calls.load(Ordering::Relaxed),
                "failures": self.follower_failures.load(Ordering::Relaxed)
            },
            "storage_operations": storage,
            "storage_families": storage_families,
            "peer_phases": PeerPhase::ALL.iter().map(|phase| (phase.label().to_owned(), self.peer[*phase as usize].snapshot())).collect::<serde_json::Map<_, _>>(),
            "queries": queries,
            "failures": self.failures.load(Ordering::Relaxed),
            "mean_actor_queue_us": mean_us(&self.queue_ns, queries),
            "mean_worker_round_trip_us": mean_us(&self.worker_ns, queries),
            "primitive_queries": primitives,
            "mean_primitive_query_us": mean_us(&self.primitive_ns, primitives),
            "capture": self.capture.snapshot(),
            "writes": {
                "actor_queue": self.writes.queue.snapshot(),
                "worker": self.writes.worker.snapshot(),
                "preparation": self.writes.preparation.snapshot(),
                "authority": self.writes.authority.snapshot(),
                "publication": self.writes.publication.snapshot(),
                "compaction": self.writes.compaction.snapshot(),
                "root_open": self.writes.root_open.snapshot(),
                "directory": self.writes.directory.snapshot(),
                "root_admission": self.writes.root_admission.snapshot(),
                "root_preparation": self.writes.root_preparation.snapshot(),
                "root_preparation_work": self.writes.root_preparation_work.snapshot(),
                "dirty_admission": self.writes.dirty_admission.snapshot(),
                "recovery_admission": self.writes.recovery_admission.snapshot(),
                "publication_failures": self.writes.publication_failures.load(Ordering::Relaxed),
                "selected_roots": self.selected_roots.load(Ordering::Relaxed),
                "materialized_commits": self.materialized_commits.load(Ordering::Relaxed),
                "uploaded_objects": self.writes.objects.load(Ordering::Relaxed),
                "uploaded_bytes": self.writes.bytes.load(Ordering::Relaxed)
            }
        })
    }
}
