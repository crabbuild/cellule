//! Finite native-capture observations, including unsuccessful attempts.
use super::{AtomicU64, Duration, Histogram, Ordering};
use cellule_ltx::CaptureTiming;

const PHASES: [&str; 13] = [
    "total",
    "preparation",
    "schema_check",
    "wal_existence",
    "position_resolution",
    "wal_read",
    "page_collection",
    "verification",
    "encode",
    "local_write",
    "fsync",
    "parent_sync",
    "checkpoint",
];

#[derive(Default)]
pub(super) struct CaptureMetrics {
    phases: [Histogram; PHASES.len()],
    failures: AtomicU64,
    wal_bytes: AtomicU64,
    wal_read_bytes: AtomicU64,
    ltx_bytes: AtomicU64,
    largest_wal_image_bytes: AtomicU64,
    sparse_reads: AtomicU64,
    full_reads: AtomicU64,
    snapshot_reads: AtomicU64,
    fallback_reads: AtomicU64,
    checkpoint_runs: AtomicU64,
    checkpoint_busy: AtomicU64,
}

impl CaptureMetrics {
    pub(super) fn observe(&self, timing: &CaptureTiming, succeeded: bool) {
        // Preserve CaptureTiming boundaries. A phase not visited by an attempt
        // contributes zero; this is one bounded histogram set, not Cell labels.
        for (phase, nanos) in self.phases.iter().zip([
            timing.total_nanos,
            timing.preparation_nanos,
            timing.schema_check_nanos,
            timing.wal_existence_nanos,
            timing.position_resolution_nanos,
            timing.wal_read_nanos,
            timing.page_collection_nanos,
            timing.verification_nanos,
            timing.encode_nanos,
            timing.local_write_nanos,
            timing.fsync_nanos,
            timing.parent_sync_nanos,
            timing.checkpoint_nanos,
        ]) {
            phase.observe(Duration::from_nanos(nanos));
        }
        self.failures
            .fetch_add(u64::from(!succeeded), Ordering::Relaxed);
        self.wal_bytes
            .fetch_add(timing.wal_bytes, Ordering::Relaxed);
        self.wal_read_bytes
            .fetch_add(timing.wal_read_bytes, Ordering::Relaxed);
        self.ltx_bytes
            .fetch_add(timing.ltx_bytes, Ordering::Relaxed);
        self.largest_wal_image_bytes
            .fetch_max(timing.wal_image_bytes, Ordering::Relaxed);
        self.sparse_reads
            .fetch_add(u64::from(timing.wal_sparse_reads), Ordering::Relaxed);
        self.full_reads
            .fetch_add(u64::from(timing.wal_full_reads), Ordering::Relaxed);
        self.snapshot_reads
            .fetch_add(u64::from(timing.wal_snapshot_reads), Ordering::Relaxed);
        self.fallback_reads
            .fetch_add(u64::from(timing.wal_fallback_reads), Ordering::Relaxed);
        self.checkpoint_runs
            .fetch_add(u64::from(timing.checkpoint_runs), Ordering::Relaxed);
        self.checkpoint_busy
            .fetch_add(u64::from(timing.checkpoint_busy), Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> serde_json::Value {
        let phases: serde_json::Map<_, _> = PHASES
            .iter()
            .zip(&self.phases)
            .map(|(name, phase)| ((*name).to_owned(), phase.snapshot()))
            .collect();
        serde_json::json!({
            "phases": phases,
            "failures": self.failures.load(Ordering::Relaxed),
            "wal_bytes": self.wal_bytes.load(Ordering::Relaxed),
            "wal_read_bytes": self.wal_read_bytes.load(Ordering::Relaxed),
            "ltx_bytes": self.ltx_bytes.load(Ordering::Relaxed),
            "largest_wal_image_bytes": self.largest_wal_image_bytes.load(Ordering::Relaxed),
            "sparse_reads": self.sparse_reads.load(Ordering::Relaxed),
            "full_reads": self.full_reads.load(Ordering::Relaxed),
            "snapshot_reads": self.snapshot_reads.load(Ordering::Relaxed),
            "fallback_reads": self.fallback_reads.load(Ordering::Relaxed),
            "checkpoint_runs": self.checkpoint_runs.load(Ordering::Relaxed),
            "checkpoint_busy": self.checkpoint_busy.load(Ordering::Relaxed),
        })
    }
}
