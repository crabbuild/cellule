//! Bounded application-driven reconciliation. Every effect follows journal CAS.

use std::{future::Future, sync::Arc, time::Duration};

use cellule_runtime::fleet::operations::AttemptId;
use cellule_runtime::fleet::operations::{
    DrainBlocker, FleetAction, FleetInspectionObservation, FleetInspectionRequest, FleetProfile,
    FleetScope, JournalTransition, MaintenancePhase, OperationError,
};
use cellule_runtime::identity::SessionId;
use cellule_runtime::{Error, Result};
use tokio::time::{Instant, timeout_at};

use super::actions::operation;
use super::{FleetActionCompletion, FleetAdapterFuture, FleetJournal, FleetJournalSnapshot};

mod maintenance;
mod movement;
mod observation;
mod planning;
pub use observation::{FleetObservation, FleetOwnedCell};

/// Application-owned complete roster and authenticated paginated observation.
///
/// Implementations compare membership and registry revisions before and after
/// collecting pages, pin boot identities and signing keys, and include busy and
/// transitioning ownership. `complete` must not be inferred from a filtered live
/// directory. Bounded pages retain their original sample times. HTTP and product
/// authorization remain in the application.
pub trait FleetObserver: Send + Sync + 'static {
    /// Captures advisory inputs at the exact journal/registry barrier. Complete
    /// coverage is required for count balancing; partial pressure observations
    /// still require authenticated source and receiver evidence.
    fn observe<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        now_ms: i64,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation>;
}

/// Authenticated management transport. A timeout never means definite refusal.
pub trait FleetTransport: Send + Sync + 'static {
    /// Dispatches to the exact action endpoint. Dropping this waiter does not
    /// cancel node-owned work or free its journal permit.
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>>;

    /// Captures current authority and actor evidence for the entire request.
    /// Cached effect receipts cannot satisfy this boundary.
    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>>;
}

/// One endpoint failure retained without suppressing healthy sibling progress.
#[derive(Debug)]
pub struct FleetAttemptFailure {
    /// Exact still-charged attempt whose endpoint call could not be confirmed.
    pub attempt: AttemptId,
    /// Original source chain, including transport and waiter timeout errors.
    pub error: Arc<Error>,
}

/// Bounded progress from one pass; detailed adapter errors preserve their source.
#[derive(Debug)]
pub struct FleetReconcileReport {
    /// Latest committed snapshot observed by this pass.
    pub snapshot: FleetJournalSnapshot,
    /// New journal-backed permits allocated across all donors.
    pub allocated: usize,
    /// Effect waiters started; their effects remain owned by target nodes.
    pub dispatched: usize,
    /// Fresh current-state captures consumed.
    pub inspected: usize,
    /// Permits atomically retired with immutable history.
    pub retired: usize,
    /// Clean source releases newly committed during this pass.
    pub released: usize,
    /// Fresh successor activations newly committed during this pass.
    pub activated: usize,
    /// Canonical failed-source recovery completions newly committed this pass.
    pub recovered: usize,
    /// Proven pre-release cancellations newly committed during this pass.
    pub cancelled: usize,
    /// Bounded distinct conditions preventing further optional work.
    pub blockers: Vec<DrainBlocker>,
    /// Endpoint failures retained independently of healthy sibling progress.
    /// At most one entry per previously charged attempt; permits remain charged.
    pub failures: Vec<FleetAttemptFailure>,
    /// Original maintenance endpoint error, independent of movement failures.
    /// Its durable intent and phase remain retained for a later pass.
    pub maintenance_failure: Option<Arc<Error>>,
    /// Suggested logical wake time; an application event may wake sooner.
    pub next_wake_at_ms: i64,
}

impl FleetReconcileReport {
    fn blocked(&mut self, reason: DrainBlocker) {
        if !self.blockers.contains(&reason) {
            self.blockers.push(reason);
        }
    }
}

/// One caller-driven controller facade; it starts no scheduler or runtime.
///
/// The current path cordons maintenance nodes and executes settled movement.
/// Busy maintenance and node role
/// finalization require their host barriers; an unfinished maintenance operation
/// remains visible and cannot be reported complete by this facade.
pub struct FleetReconciler {
    scope: FleetScope,
    claimant: SessionId,
    profile: FleetProfile,
    journal: Arc<dyn FleetJournal>,
    observer: Arc<dyn FleetObserver>,
    transport: Arc<dyn FleetTransport>,
}

impl FleetReconciler {
    /// Validates scope, identity and bounds before any adapter call or task.
    pub fn new(
        scope: FleetScope,
        claimant: SessionId,
        profile: FleetProfile,
        journal: Arc<dyn FleetJournal>,
        observer: Arc<dyn FleetObserver>,
        transport: Arc<dyn FleetTransport>,
    ) -> Result<Self> {
        let profile = profile.validate().map_err(operation)?;
        cellule_runtime::fleet::operations::FleetHead::new(scope, 0).map_err(operation)?;
        if claimant.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(operation(OperationError::Invalid(
                "zero controller claimant",
            )));
        }
        Ok(Self {
            scope,
            claimant,
            profile,
            journal,
            observer,
            transport,
        })
    }

    /// Reconciles every existing permit before choosing new movement. Each pass
    /// performs at most one movement step per previously charged attempt and
    /// allocates no more than the shared profile allows. The supplied clock reads
    /// the same logical domain as observations and node evidence at each boundary.
    /// It must be nonnegative and never regress during a pass.
    /// A stopped scheduling policy still permits settling accepted work.
    pub async fn reconcile_once(
        &self,
        now: impl Fn() -> Result<i64> + Send + Sync,
        deadline: Instant,
    ) -> Result<FleetReconcileReport> {
        let clock = PassClock::new(&now, deadline)?;
        let now_ms = clock.now()?;
        let initial = call(
            deadline,
            "fleet-journal",
            self.journal.load_snapshot(self.scope),
        )
        .await?;
        if initial.head().scope() != self.scope {
            return Err(operation(OperationError::Conflict));
        }
        let snapshot = call(
            deadline,
            "fleet-journal",
            self.journal.claim_controller(
                self.scope,
                initial.head().revision(),
                self.claimant,
                clock.now()?,
            ),
        )
        .await?;
        let claimed_epoch = self.controller_epoch(&snapshot, clock.now()?)?;
        let mut report = FleetReconcileReport {
            snapshot,
            allocated: 0,
            dispatched: 0,
            inspected: 0,
            retired: 0,
            released: 0,
            activated: 0,
            recovered: 0,
            cancelled: 0,
            blockers: Vec::new(),
            failures: Vec::new(),
            maintenance_failure: None,
            next_wake_at_ms: now_ms
                .checked_add(self.profile.reconcile_interval_ms)
                .ok_or(Error::Control("fleet wake time overflow"))?,
        };
        let claimed_revision = report.snapshot.head().revision();
        let ids = report
            .snapshot
            .head()
            .attempts()
            .iter()
            .map(|a| a.spec().id)
            .collect::<Vec<_>>();
        let count = ids.len();
        for (index, id) in ids.into_iter().enumerate() {
            if !report
                .snapshot
                .head()
                .attempts()
                .iter()
                .any(|attempt| attempt.spec().id == id)
            {
                continue;
            }
            let step_clock = clock.partition(count - index + 1);
            if let Err(error) = self.advance(id, &step_clock, &mut report).await {
                let timed_out = matches!(
                    &error,
                    Error::Facility {
                        name: "fleet-controller-deadline",
                        ..
                    }
                );
                let endpoint_failed = matches!(
                    &error,
                    Error::Facility {
                        name: "fleet-transport",
                        ..
                    }
                );
                if !timed_out && !endpoint_failed {
                    return Err(error);
                }
                if timed_out {
                    // A journal CAS may have committed after its waiter expired.
                    // Re-read at the outer deadline before any dependent action.
                    let snapshot = call(
                        clock.deadline,
                        "fleet-journal",
                        self.journal.load_snapshot(self.scope),
                    )
                    .await?;
                    if self.controller_epoch(&snapshot, clock.now()?)? != claimed_epoch {
                        return Err(operation(OperationError::Fenced));
                    }
                    report.snapshot = snapshot;
                }
                report.blocked(DrainBlocker::OutcomeUnknown);
                report.failures.push(FleetAttemptFailure {
                    attempt: id,
                    error: Arc::new(error),
                });
            }
        }
        if let Err(error) = self
            .advance_maintenance(&clock.partition(2), &mut report)
            .await
        {
            let timed_out = matches!(
                &error,
                Error::Facility {
                    name: "fleet-controller-deadline",
                    ..
                }
            );
            let endpoint_failed = matches!(
                &error,
                Error::Facility {
                    name: "fleet-transport",
                    ..
                }
            );
            if !timed_out && !endpoint_failed {
                return Err(error);
            }
            if timed_out {
                // Acceptance or phase publication may have outlived its waiter.
                let snapshot = call(
                    clock.deadline,
                    "fleet-journal",
                    self.journal.load_snapshot(self.scope),
                )
                .await?;
                if self.controller_epoch(&snapshot, clock.now()?)? != claimed_epoch {
                    return Err(operation(OperationError::Fenced));
                }
                report.snapshot = snapshot;
            }
            report.blocked(DrainBlocker::OutcomeUnknown);
            report.maintenance_failure = Some(Arc::new(error));
        }
        if report.snapshot.registry().scheduling_enabled() {
            self.plan(&clock, &mut report).await?;
        }
        // Proven cancellation keeps the periodic retry interval, avoiding an
        // immediate allocation/refusal loop when a receiver cannot admit work.
        if report.snapshot.head().revision() != claimed_revision
            && report.cancelled == 0
            && report.failures.is_empty()
            && report.maintenance_failure.is_none()
        {
            // A multi-phase move must not spend one periodic interval between
            // every action and expire its admission deadline before release.
            report.next_wake_at_ms = clock.now()?;
        }
        Ok(report)
    }

    async fn commit(
        &self,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
        transition: JournalTransition,
    ) -> Result<()> {
        let now_ms = clock.now()?;
        let epoch = self.controller_epoch(&report.snapshot, now_ms)?;
        report.snapshot = call(
            clock.deadline,
            "fleet-journal",
            self.journal
                .compare_exchange(&report.snapshot, epoch, now_ms, &transition),
        )
        .await?;
        Ok(())
    }

    fn controller_epoch(&self, snapshot: &FleetJournalSnapshot, now_ms: i64) -> Result<u64> {
        if snapshot.head().scope() != self.scope {
            return Err(operation(OperationError::Conflict));
        }
        let lease = snapshot
            .head()
            .controller()
            .ok_or_else(|| operation(OperationError::Fenced))?;
        if lease.claimant != self.claimant || now_ms >= lease.expires_at_ms {
            return Err(operation(OperationError::Fenced));
        }
        Ok(lease.epoch)
    }
}

struct PassClock<'a> {
    read: &'a (dyn Fn() -> Result<i64> + Send + Sync),
    deadline: Instant,
    last_ms: Arc<std::sync::atomic::AtomicI64>,
}
impl<'a> PassClock<'a> {
    fn new(read: &'a (dyn Fn() -> Result<i64> + Send + Sync), deadline: Instant) -> Result<Self> {
        let now_ms = read()?;
        if now_ms < 0 {
            return Err(operation(OperationError::Invalid(
                "negative controller time",
            )));
        }
        if deadline <= Instant::now() {
            return Err(operation(OperationError::Deadline));
        }
        Ok(Self {
            read,
            deadline,
            last_ms: Arc::new(std::sync::atomic::AtomicI64::new(now_ms)),
        })
    }
    fn now(&self) -> Result<i64> {
        let now_ms = (self.read)()?;
        let previous = self
            .last_ms
            .fetch_max(now_ms, std::sync::atomic::Ordering::SeqCst);
        if now_ms < previous {
            return Err(Error::Control("fleet controller clock regressed"));
        }
        Ok(now_ms)
    }
    fn partition(&self, units: usize) -> Self {
        // FleetHead bounds attempts to two, and one share remains for planning.
        let now = Instant::now();
        let share = self.deadline.saturating_duration_since(now) / units as u32;
        Self {
            read: self.read,
            last_ms: Arc::clone(&self.last_ms),
            deadline: now + share,
        }
    }
    fn capture_deadline_ms(&self) -> Result<i64> {
        // No inspection may outlive this pass or its controller lease.
        let remaining = self
            .deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(30));
        self.now()?
            .checked_add(i64::try_from(remaining.as_millis()).map_err(|source| {
                Error::Facility {
                    name: "fleet-controller-clock",
                    source: Box::new(source),
                }
            })?)
            .ok_or(Error::Control("fleet inspection deadline overflow"))
    }
}

async fn call<T>(
    deadline: Instant,
    name: &'static str,
    future: impl Future<Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>>,
) -> Result<T> {
    timeout_at(deadline, future)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-controller-deadline",
            source: Box::new(source),
        })?
        .map_err(|source| Error::Facility { name, source })
}

fn nonce() -> cellule_runtime::identity::Digest {
    cellule_runtime::identity::Digest::from_bytes(
        *blake3::hash(uuid::Uuid::now_v7().as_bytes()).as_bytes(),
    )
}
