//! Finite, nonblocking read-path measurements for this example application.
use cellule_runtime::fleet::telemetry::{
    CellTelemetry, PrimitiveOperationKind, PrimitiveOperationOutcome, PublicationTiming,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Default)]
pub(super) struct QueryMetrics {
    queries: AtomicU64,
    failures: AtomicU64,
    queue_ns: AtomicU64,
    worker_ns: AtomicU64,
    primitives: AtomicU64,
    primitive_ns: AtomicU64,
    writes: WriteMetrics,
}

// Application-owned instrumentation: six fixed histograms, 100-us upper
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
            "mean_ms": (count > 0).then(|| self.total_ns.load(Ordering::Relaxed) as f64 / count as f64 / 1_000_000.0),
            "p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99),
            "overflow": buckets[WRITE_BUCKETS - 1], "resolution_us": WRITE_BUCKET_US
        })
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
    publication_failures: AtomicU64,
    objects: AtomicU64,
    bytes: AtomicU64,
}

fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

impl CellTelemetry for QueryMetrics {
    fn command_execution(&self, queue: Duration, worker: Duration, _succeeded: bool) {
        self.writes.queue.observe(queue);
        self.writes.worker.observe(worker);
    }
    fn publication_completed(&self, _cell: cellule_runtime::CellId, timing: PublicationTiming) {
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
        if phase == cellule_ltx::LtxPhase::Compaction {
            self.writes.compaction.observe(elapsed);
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
    // Called after HTTP and runtime drain, when the totals are stable.
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
        serde_json::json!({
            "queries": queries,
            "failures": self.failures.load(Ordering::Relaxed),
            "mean_actor_queue_us": mean_us(&self.queue_ns, queries),
            "mean_worker_round_trip_us": mean_us(&self.worker_ns, queries),
            "primitive_queries": primitives,
            "mean_primitive_query_us": mean_us(&self.primitive_ns, primitives),
            "writes": {
                "actor_queue": self.writes.queue.snapshot(),
                "worker": self.writes.worker.snapshot(),
                "preparation": self.writes.preparation.snapshot(),
                "authority": self.writes.authority.snapshot(),
                "publication": self.writes.publication.snapshot(),
                "compaction": self.writes.compaction.snapshot(),
                "publication_failures": self.writes.publication_failures.load(Ordering::Relaxed),
                "uploaded_objects": self.writes.objects.load(Ordering::Relaxed),
                "uploaded_bytes": self.writes.bytes.load(Ordering::Relaxed)
            }
        })
    }
}
