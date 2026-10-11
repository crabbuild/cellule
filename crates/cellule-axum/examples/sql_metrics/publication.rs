//! Managed bundle phases and retained-work pressure, with finite labels.
use super::*;
use cellule_runtime::fleet::telemetry::{
    AdmissionRefusal, NodePublicationCohort, NodePublicationPhase, PublicationPhaseOutcome,
    PublicationPipelineFallback,
};

#[derive(Default)]
struct Phase {
    active: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    elapsed: Histogram,
}

#[derive(Default)]
pub(super) struct Metrics {
    phases: [Phase; NodePublicationPhase::ALL.len()],
    http_failures: [[AtomicU64; FAILURE_LABELS.len()]; 2],
    refusals: [AtomicU64; AdmissionRefusal::ALL.len()],
    fallbacks: [AtomicU64; 4],
    cohorts: AtomicU64,
    captures: AtomicU64,
    frames: AtomicU64,
    bytes: AtomicU64,
    checkpoints: AtomicU64,
    max_frames: AtomicU64,
    max_bytes: AtomicU64,
}

const FAILURE_LABELS: [&str; 13] = [
    "publication_backlog",
    "pending_publication_bytes",
    "node_retained_bytes",
    "mailbox_requests",
    "mailbox_bytes",
    "other_capacity",
    "fenced",
    "draining",
    "closed",
    "deadline",
    "pending_publication",
    "other",
    "outcome_unknown",
];

fn failure_class(error: &(dyn std::error::Error + 'static)) -> usize {
    let mut source = Some(error);
    for _ in 0..16 {
        let Some(error) = source else {
            break;
        };
        if let Some(runtime) = error.downcast_ref::<cellule_runtime::Error>() {
            use cellule_runtime::Error;
            if let Error::Shared(original) = runtime {
                return failure_class(original.as_ref());
            }
            return match runtime {
                Error::Capacity("publication backlog") => 0,
                Error::Capacity("pending publication bytes") => 1,
                Error::Capacity("node retained bytes") => 2,
                Error::Capacity("Cell mailbox requests") => 3,
                Error::Capacity("Cell mailbox bytes") => 4,
                Error::Capacity(_) => 5,
                Error::Fenced => 6,
                Error::CellDraining => 7,
                Error::RuntimeClosed => 8,
                Error::Deadline => 9,
                Error::PendingPublication => 10,
                _ => 11,
            };
        }
        source = error.source();
    }
    11
}

impl Metrics {
    pub(super) fn http_failure(&self, error: &cellule_axum::HttpError, write: bool) {
        let reason = if error.code() == "outcome_unknown" {
            12
        } else {
            failure_class(error)
        };
        self.http_failures[usize::from(write)][reason].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn started(&self, phase: NodePublicationPhase) {
        self.phases[phase as usize]
            .active
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn completed(
        &self,
        phase: NodePublicationPhase,
        elapsed: Duration,
        outcome: PublicationPhaseOutcome,
    ) {
        let phase = &self.phases[phase as usize];
        let counter = match outcome {
            PublicationPhaseOutcome::Succeeded => &phase.succeeded,
            PublicationPhaseOutcome::Failed => &phase.failed,
            PublicationPhaseOutcome::Cancelled => &phase.cancelled,
        };
        phase.elapsed.observe(elapsed);
        counter.fetch_add(1, Ordering::Relaxed);
        phase.active.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn refused(&self, reason: AdmissionRefusal) {
        self.refusals[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn fallback(&self, reason: PublicationPipelineFallback) {
        self.fallbacks[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn cohort(&self, cohort: NodePublicationCohort) {
        self.cohorts.fetch_add(1, Ordering::Relaxed);
        self.captures
            .fetch_add(cohort.captures as u64, Ordering::Relaxed);
        self.frames
            .fetch_add(cohort.frames as u64, Ordering::Relaxed);
        self.bytes.fetch_add(cohort.bytes as u64, Ordering::Relaxed);
        self.checkpoints
            .fetch_add(cohort.checkpoints as u64, Ordering::Relaxed);
        self.max_frames
            .fetch_max(cohort.frames as u64, Ordering::Relaxed);
        self.max_bytes
            .fetch_max(cohort.bytes as u64, Ordering::Relaxed);
    }

    pub(super) fn histograms(&self) -> serde_json::Map<String, serde_json::Value> {
        NodePublicationPhase::ALL
            .iter()
            .map(|phase| {
                (
                    format!("node_publication_{}", phase.label()),
                    self.phases[*phase as usize].elapsed.raw(),
                )
            })
            .collect()
    }

    pub(super) fn snapshot(&self) -> serde_json::Value {
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let phases = NodePublicationPhase::ALL
            .iter()
            .map(|phase| {
                let value = &self.phases[*phase as usize];
                (
                    phase.label().to_owned(),
                    serde_json::json!({
                        "active": read(&value.active), "succeeded": read(&value.succeeded),
                        "failed": read(&value.failed), "cancelled": read(&value.cancelled),
                        "elapsed": value.elapsed.snapshot(),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let http_failures = ["reads", "writes"]
            .iter()
            .enumerate()
            .map(|(kind, label)| {
                (
                    (*label).to_owned(),
                    FAILURE_LABELS
                        .iter()
                        .enumerate()
                        .map(|(reason, label)| {
                            (
                                (*label).to_owned(),
                                read(&self.http_failures[kind][reason]).into(),
                            )
                        })
                        .collect::<serde_json::Map<_, _>>()
                        .into(),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        serde_json::json!({
            "phases": phases,
            "http_failures": http_failures,
            "admission_refusals": AdmissionRefusal::ALL.iter().map(|reason| (
                reason.label().to_owned(), read(&self.refusals[*reason as usize]).into()
            )).collect::<serde_json::Map<_, _>>(),
            "pipeline_fallbacks": {"checkpoints": read(&self.fallbacks[0]),
                "capacity": read(&self.fallbacks[1]), "no_successor": read(&self.fallbacks[2]),
                "headroom": read(&self.fallbacks[3])},
            "cohorts": {"count": read(&self.cohorts), "captures": read(&self.captures),
                "frames": read(&self.frames), "native_bytes": read(&self.bytes),
                "checkpoints": read(&self.checkpoints), "max_frames": read(&self.max_frames),
                "max_native_bytes": read(&self.max_bytes)},
        })
    }
}
