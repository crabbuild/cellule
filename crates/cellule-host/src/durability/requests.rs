//! Bounded epoch requests, linearized with automatic rotation.
use super::*;
use cellule_runtime::cell::actor::NodeByteReservation;
use cellule_runtime::node::durability::NodeDurability;
use cellule_runtime::node::log::NodeLogRetirementProof;
use std::sync::Weak;
use tokio::sync::Notify;

const REQUEST_BYTES: usize = 4 * 1024;

/// Local progress of an accepted request; no phase alone settles a fleet role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeLogRotationPhase {
    /// Accepted before retirement starts.
    Queued,
    /// Waiting for object coverage or every original member's append fence.
    Retiring,
    /// The old epoch is closed; replacement recruitment remains outstanding.
    Recruiting,
    /// Confirmed retirement and a replacement binding were installed.
    Completed,
    /// Host drain or supervisor failure interrupted the request; inspect authority.
    Interrupted,
}

/// Opaque local completion of confirmed old-epoch retirement and replacement.
/// It does not establish durable journal settlement or replacement policy.
#[derive(Clone, Debug)]
pub struct NodeLogRotationCompletion {
    retirement: Arc<NodeLogRetirementProof>,
    replacement_epoch: u64,
}
impl NodeLogRotationCompletion {
    /// Returns confirmation of every original member's exact append fence.
    #[must_use]
    pub fn retirement(&self) -> &Arc<NodeLogRetirementProof> {
        &self.retirement
    }
    /// Returns the newer epoch installed through canonical runtime replacement.
    #[must_use]
    pub const fn replacement_epoch(&self) -> u64 {
        self.replacement_epoch
    }
}

/// One coherent request observation with the original source failure retained.
#[derive(Clone, Debug)]
pub struct NodeLogRotationObservation {
    phase: NodeLogRotationPhase,
    first_failure: Option<Arc<dyn std::error::Error + Send + Sync>>,
    latest_failure: Option<Arc<dyn std::error::Error + Send + Sync>>,
    retirement: Option<Arc<NodeLogRetirementProof>>,
    completion: Option<Arc<NodeLogRotationCompletion>>,
}
impl NodeLogRotationObservation {
    /// Returns progress; Interrupted is never completion or proof of absence.
    #[must_use]
    pub const fn phase(&self) -> NodeLogRotationPhase {
        self.phase
    }
    /// Returns the first original failure, independently of subsequent retries.
    #[must_use]
    pub fn first_failure(&self) -> Option<&Arc<dyn std::error::Error + Send + Sync>> {
        self.first_failure.as_ref()
    }
    /// Returns the latest original failure; successful retry does not erase it.
    #[must_use]
    pub fn latest_failure(&self) -> Option<&Arc<dyn std::error::Error + Send + Sync>> {
        self.latest_failure.as_ref()
    }
    /// Returns member confirmation after successful canonical old-epoch closure.
    #[must_use]
    pub fn retirement(&self) -> Option<&Arc<NodeLogRetirementProof>> {
        self.retirement.as_ref()
    }
    /// Returns local completion only after the replacement binding is installed.
    #[must_use]
    pub fn completion(&self) -> Option<&Arc<NodeLogRotationCompletion>> {
        self.completion.as_ref()
    }
}

/// Weak, epoch-bound inspection handle. Dropping it cannot cancel accepted work.
/// The host retains one pending and one most-recent completed request.
#[derive(Clone, Debug)]
pub struct NodeLogRotationRequest {
    epoch: u64,
    record: Weak<RotationRecord>,
}
impl NodeLogRotationRequest {
    /// Returns the exact original epoch named at acceptance.
    #[must_use]
    pub const fn log_epoch(&self) -> u64 {
        self.epoch
    }
    /// Observes retained local progress. Eviction is not proof of completion.
    pub fn observe(&self) -> cellule_runtime::Result<NodeLogRotationObservation> {
        let record = self.record.upgrade().ok_or(Error::Control(
            "node-log rotation receipt no longer retained",
        ))?;
        Ok(record.lock()?.clone())
    }
}

pub(super) struct RotationRecord {
    pub(super) epoch: u64,
    pub(super) durability: Arc<NodeDurability>,
    progress: Mutex<NodeLogRotationObservation>,
    _bytes: NodeByteReservation,
}
impl RotationRecord {
    fn lock(
        &self,
    ) -> cellule_runtime::Result<std::sync::MutexGuard<'_, NodeLogRotationObservation>> {
        self.progress
            .lock()
            .map_err(|_| Error::Control("node-log rotation receipt lock poisoned"))
    }
    fn handle(self: &Arc<Self>) -> NodeLogRotationRequest {
        NodeLogRotationRequest {
            epoch: self.epoch,
            record: Arc::downgrade(self),
        }
    }
    pub(super) fn phase(&self, phase: NodeLogRotationPhase) -> cellule_runtime::Result<()> {
        self.lock()?.phase = phase;
        Ok(())
    }
    pub(super) fn failed(
        &self,
        error: Arc<dyn std::error::Error + Send + Sync>,
    ) -> cellule_runtime::Result<()> {
        let mut progress = self.lock()?;
        progress
            .first_failure
            .get_or_insert_with(|| Arc::clone(&error));
        progress.latest_failure = Some(error);
        Ok(())
    }
    pub(super) fn retired(
        &self,
        proof: Arc<NodeLogRetirementProof>,
    ) -> cellule_runtime::Result<()> {
        let mut progress = self.lock()?;
        progress.retirement = Some(proof);
        progress.phase = NodeLogRotationPhase::Recruiting;
        Ok(())
    }
    fn completed(&self, replacement_epoch: u64) -> cellule_runtime::Result<()> {
        let mut progress = self.lock()?;
        let retirement = progress.retirement.clone().ok_or(Error::Control(
            "node-log rotation lacks member confirmation",
        ))?;
        progress.completion = Some(Arc::new(NodeLogRotationCompletion {
            retirement,
            replacement_epoch,
        }));
        progress.phase = NodeLogRotationPhase::Completed;
        Ok(())
    }
}

#[derive(Default)]
struct RequestBank {
    pending: Option<Arc<RotationRecord>>,
    completed: Option<Arc<RotationRecord>>,
    running: Option<u64>,
    stopped: bool,
}

pub(crate) struct RotationRequests {
    application: ApplicationId,
    bank: Mutex<RequestBank>,
    pub(super) wake: Notify,
}
impl RotationRequests {
    pub(super) fn new(application: ApplicationId) -> Self {
        Self {
            application,
            bank: Mutex::new(RequestBank::default()),
            wake: Notify::new(),
        }
    }
    fn lock(&self) -> cellule_runtime::Result<std::sync::MutexGuard<'_, RequestBank>> {
        self.bank
            .lock()
            .map_err(|_| Error::Control("node-log rotation request lock poisoned"))
    }
    pub(crate) fn request(
        &self,
        runtime: &CellRuntime,
        epoch: u64,
        cancellation: &CancellationToken,
    ) -> cellule_runtime::Result<NodeLogRotationRequest> {
        let mut bank = self.lock()?;
        if let Some(record) = bank
            .pending
            .as_ref()
            .filter(|record| record.epoch == epoch)
            .or_else(|| {
                bank.completed
                    .as_ref()
                    .filter(|record| record.epoch == epoch)
            })
        {
            return Ok(record.handle());
        }
        if bank.stopped || cancellation.is_cancelled() || runtime.is_shutting_down() {
            return Err(Error::CellDraining);
        }
        if bank.pending.is_some() {
            return Err(Error::Capacity("node-log rotation request already pending"));
        }
        let (application, durability) = runtime
            .node_durability()
            .ok_or(Error::Control("node-log durability is not enrolled"))?;
        if application != self.application || durability.log_epoch()? != epoch {
            return Err(Error::Control("node-log rotation request has stale scope"));
        }
        // A request cannot retroactively strengthen a best-effort rotation.
        // This lock linearizes acceptance with the supervisor's epoch claim.
        if bank.running.is_some() {
            return Err(Error::Control(
                "node-log automatic rotation already running",
            ));
        }
        let record = Arc::new(RotationRecord {
            epoch,
            durability,
            _bytes: runtime.try_reserve_node_bytes(REQUEST_BYTES)?,
            progress: Mutex::new(NodeLogRotationObservation {
                phase: NodeLogRotationPhase::Queued,
                first_failure: None,
                latest_failure: None,
                retirement: None,
                completion: None,
            }),
        });
        let handle = record.handle();
        bank.pending = Some(record);
        self.wake.notify_one();
        Ok(handle)
    }
    pub(crate) fn lookup(
        &self,
        epoch: u64,
    ) -> cellule_runtime::Result<Option<NodeLogRotationRequest>> {
        let bank = self.lock()?;
        Ok(bank
            .pending
            .as_ref()
            .filter(|record| record.epoch == epoch)
            .or_else(|| {
                bank.completed
                    .as_ref()
                    .filter(|record| record.epoch == epoch)
            })
            .map(RotationRecord::handle))
    }
    pub(super) fn claim(
        &self,
        runtime: &CellRuntime,
        max_frames: u64,
    ) -> cellule_runtime::Result<Option<RotationWork>> {
        let mut bank = self.lock()?;
        if bank.stopped || bank.running.is_some() {
            return Ok(None);
        }
        let Some((application, current)) = runtime.node_durability() else {
            return Ok(None);
        };
        if application != self.application {
            return Err(Error::Control(
                "CellNode node durability application changed during rotation",
            ));
        }
        let record = bank.pending.clone();
        if let Some(record) = &record {
            if !Arc::ptr_eq(&current, &record.durability) {
                return Err(Error::Control(
                    "requested node-log binding changed outside supervisor",
                ));
            }
        } else if !current.needs_rotation(max_frames) {
            return Ok(None);
        }
        let epoch = current.log_epoch()?;
        bank.running = Some(epoch);
        Ok(Some(RotationWork {
            epoch,
            durability: current,
            record,
        }))
    }
    pub(super) fn complete(
        &self,
        work: &RotationWork,
        replacement_epoch: u64,
    ) -> cellule_runtime::Result<()> {
        let mut bank = self.lock()?;
        if bank.running != Some(work.epoch) {
            return Err(Error::Control("node-log rotation claim changed"));
        }
        if let Some(record) = &work.record {
            bank.completed = bank.pending.take();
            // Publish completion after old local history is released. A caller
            // observing Completed can rely on the bounded bank having settled.
            record.completed(replacement_epoch)?;
        }
        bank.running = None;
        Ok(())
    }
    pub(super) fn stop(
        &self,
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
    ) -> cellule_runtime::Result<()> {
        let mut bank = self.lock()?;
        bank.stopped = true;
        if let Some(record) = &bank.pending {
            if let Some(source) = source {
                record.failed(source)?;
            }
            record.phase(NodeLogRotationPhase::Interrupted)?;
        }
        bank.running = None;
        Ok(())
    }
}

pub(super) struct RotationWork {
    pub(super) epoch: u64,
    pub(super) durability: Arc<NodeDurability>,
    pub(super) record: Option<Arc<RotationRecord>>,
}
