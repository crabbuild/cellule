//! Pending-before-CAS ownership in the existing durability supervisor and drain lane.
use super::*;
use crate::fleet::{FleetEnrollmentAcceptance, FleetJournal};
use cellule_runtime::cell::actor::NodeByteReservation;
use cellule_runtime::fleet::operations::{
    EnrollmentEndpoint, EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec,
    EnrollmentStatus, FleetScope, MAX_RECORD_BYTES,
};
use cellule_runtime::identity::{Digest, NodeId};
use cellule_runtime::node::durability::NodeLogAuthority;
use cellule_runtime::node::lease::NodeLeaseGuard;
use cellule_runtime::node::log::NodeLogRetirementObservation;
use cellule_runtime::node::log_transport::NodeLogTransport;
use cellule_runtime::node::{
    NodeDirectory, NodeLogEnrollmentAttempt, NodeLogEnrollmentProof, NodeLogEnrollmentRefusalProof,
};
use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex as AsyncMutex;

pub(crate) const MAX_FOLLOWER_ENROLLMENT_EPOCHS: usize = 32;

mod authority;
mod inventory;
mod maintenance;
mod protocol;
pub use inventory::{
    FollowerEnrollmentInventoryCursor, FollowerEnrollmentInventoryPage, FollowerEnrollmentProgress,
};
pub use maintenance::FollowerEvacuation;

/// Read-only input provider for journal-bound follower recruitment.
/// Applications own signed directory/transport/authority construction and unique
/// advancing epochs. Preparation must perform no enrollment CAS or frame append.
pub trait FleetNodeDurabilityProvider: Send + Sync + 'static {
    /// Selects one exact ensemble and fresh immutable CAS attempt. None leaves
    /// the existing object proof path available until the next supervisor tick.
    fn prepare(
        self: Arc<Self>,
        limits: ReplicaLimits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<FleetNodeLogRecruitment>>> + Send>>;

    /// Reports whether provider-owned enrollment state requires epoch rotation.
    ///
    /// An error defers the decision to a later supervisor tick; it does not
    /// stop the current object-durable publication path.
    fn rotation_required(
        self: Arc<Self>,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<bool>> + Send>>;

    /// Observes the existing supervisor's rotation events.
    fn rotation_event(&self, _event: NodeDurabilityRotation) {}
}

/// Provider-owned transport and authority for one opaque prepared enrollment.
/// Construction does not start a shipper or mutate directory authority.
pub struct FleetNodeLogRecruitment {
    directory: NodeDirectory,
    attempt: NodeLogEnrollmentAttempt,
    transport: Arc<dyn NodeLogTransport>,
    authority: Arc<dyn NodeLogAuthority>,
    lease: NodeLeaseGuard,
    telemetry: cellule_runtime::fleet::telemetry::CellTelemetryHandle,
}
impl FleetNodeLogRecruitment {
    /// Binds one prepared attempt to application-owned authenticated transports.
    /// The transport must address the attempt's exact original follower boots;
    /// resolving a replacement physical boot does not authorize a new enrollment.
    /// The authority must fresh-load and reconcile this exact source boot/epoch
    /// after host enrollment; a pre-enrollment ETag cannot authorize later writes.
    /// An ambiguous close requires its original checked close receipt, never an
    /// arbitrary absent or newer log record.
    pub fn new(
        directory: NodeDirectory,
        attempt: NodeLogEnrollmentAttempt,
        transport: Arc<dyn NodeLogTransport>,
        authority: Arc<dyn NodeLogAuthority>,
        lease: NodeLeaseGuard,
        telemetry: cellule_runtime::fleet::telemetry::CellTelemetryHandle,
    ) -> cellule_runtime::Result<Self> {
        if directory.fleet() != attempt.prepared().source().fleet() {
            return Err(Error::Fenced);
        }
        Ok(Self {
            directory,
            attempt,
            transport,
            authority,
            lease,
            telemetry,
        })
    }
    fn config(
        &self,
        limits: ReplicaLimits,
        authority: Arc<dyn NodeLogAuthority>,
    ) -> cellule_runtime::Result<NodeDurabilityConfig> {
        let prepared = self.attempt.prepared();
        NodeDurabilityConfig::new(
            prepared.source().session(),
            prepared.source().node(),
            prepared.log().epoch(),
            prepared.log().members().to_vec(),
            self.transport.clone(),
            authority,
            self.lease.clone(),
            limits,
            self.telemetry.clone(),
        )
    }
}

/// Original durable request and retained result for one selected follower boot.
#[derive(Clone)]
pub struct FollowerEnrollmentMember {
    /// Immutable request, including both intent revisions and the original epoch.
    pub spec: EnrollmentSpec,
    /// Original accepted/tombstoned request, or None while its reply is unknown.
    pub accepted: Option<EnrollmentRecord>,
    /// Original checked native or joined-nonexecution event being published.
    pub event: Option<EnrollmentEvent>,
    /// Whether the journal confirmed this exact event and immutable request.
    pub published: bool,
}
/// Read-only inventory of an owned follower enrollment, including unknown work.
/// Completed retirement removes local inventory; absence proves no fleet fact.
#[derive(Clone)]
pub struct FollowerEnrollmentCompletion {
    /// Immutable original source version and complete signed selected ensemble.
    pub attempt: NodeLogEnrollmentAttempt,
    /// Every original selected member, including missing acceptance replies.
    pub members: Vec<FollowerEnrollmentMember>,
    /// Whether this owner's one enrollment CAS started.
    pub native_started: bool,
    /// Checked canonical enrollment for the original epoch/member set.
    pub enrollment: Option<NodeLogEnrollmentProof>,
    /// Checked original-token refusal fence, if it won against an ambiguous CAS.
    pub refusal: Option<NodeLogEnrollmentRefusalProof>,
    /// Every original confirmed native retirement response, before authority close.
    pub retirement: Option<Arc<NodeLogRetirementObservation>>,
    /// Whether the original authority callback confirmed canonical closure.
    pub native_closed: bool,
    /// Original native failure, independent of publication/acceptance failures.
    pub execution_error: Option<Arc<Error>>,
    /// Original journal failure, preserved even after successful replay.
    pub journal_error: Option<Arc<Error>>,
}

#[derive(Default)]
struct Bank {
    draining: bool,
    last_epoch: u64,
    pending: Option<Arc<Responsibility>>,
    epochs: BTreeMap<u64, Arc<Responsibility>>,
}
pub(crate) struct FleetFollowerEnrollment {
    scope: FleetScope,
    node: NodeId,
    session: SessionId,
    provider: Arc<dyn FleetNodeDurabilityProvider>,
    journal: Arc<dyn FleetJournal>,
    runtime: CellRuntime,
    interval: Duration,
    cancellation: CancellationToken,
    bank: Arc<StdMutex<Bank>>,
    protocol: AsyncMutex<()>,
}
struct Responsibility {
    inputs: FleetNodeLogRecruitment,
    limits: ReplicaLimits,
    data: StdMutex<Progress>,
    closing: AsyncMutex<()>,
    bank: std::sync::Weak<StdMutex<Bank>>,
}
struct Progress {
    members: Vec<FollowerEnrollmentMember>,
    native_started: bool,
    no_effect: bool,
    delivered: bool,
    enrollment: Option<NodeLogEnrollmentProof>,
    refusal: Option<NodeLogEnrollmentRefusalProof>,
    retirement: Option<Arc<NodeLogRetirementObservation>>,
    native_closed: bool,
    execution_error: Option<Arc<Error>>,
    journal_error: Option<Arc<Error>>,
    reservation: Option<NodeByteReservation>,
    cleanup: Option<Arc<cellule_runtime::node::durability::NodeDurability>>,
}
impl Responsibility {
    fn progress(&self) -> cellule_runtime::Result<std::sync::MutexGuard<'_, Progress>> {
        self.data
            .lock()
            .map_err(|_| Error::Node("follower enrollment progress lock poisoned"))
    }
    fn remember(&self, error: Error, journal: bool) -> cellule_runtime::Result<Arc<Error>> {
        let error = Arc::new(error);
        let mut progress = self.progress()?;
        let slot = if journal {
            &mut progress.journal_error
        } else {
            &mut progress.execution_error
        };
        if slot.is_none() {
            *slot = Some(error.clone());
        }
        Ok(error)
    }
    fn remember_unrecorded(&self, error: Error) -> cellule_runtime::Result<Arc<Error>> {
        let error = Arc::new(error);
        let mut progress = self.progress()?;
        // Journal and native calls already retain their original source. Only
        // record otherwise unreported validation/clock failures here.
        if progress.execution_error.is_none() && progress.journal_error.is_none() {
            progress.execution_error = Some(error.clone());
        }
        Ok(error)
    }
    fn finished(&self) -> cellule_runtime::Result<()> {
        self.progress()?.reservation.take();
        if let Some(bank) = self.bank.upgrade() {
            bank.lock()
                .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?
                .epochs
                .remove(&self.inputs.attempt.prepared().log().epoch());
        }
        Ok(())
    }
}

fn journal_error(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-enrollment-journal",
        source,
    }
}
fn operation(error: cellule_runtime::fleet::operations::OperationError) -> Error {
    Error::FleetOperation(Box::new(error))
}
#[derive(Debug)]
struct RetainedError(Arc<Error>);
impl std::fmt::Display for RetainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: Arc<Error>) -> Error {
    Error::Facility {
        name: "fleet-follower-enrollment",
        source: Box::new(RetainedError(error)),
    }
}
fn now_ms() -> cellule_runtime::Result<i64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Node("follower enrollment clock precedes epoch"))?;
    i64::try_from(now.as_millis()).map_err(|_| Error::Node("follower enrollment clock overflow"))
}

impl FleetFollowerEnrollment {
    pub(crate) fn new(
        scope: FleetScope,
        node: NodeId,
        session: SessionId,
        provider: Arc<dyn FleetNodeDurabilityProvider>,
        journal: Arc<dyn FleetJournal>,
        runtime: CellRuntime,
        interval: Duration,
        cancellation: CancellationToken,
    ) -> cellule_runtime::Result<Self> {
        cellule_runtime::fleet::operations::FleetHead::new(scope, 0).map_err(operation)?;
        if node.as_bytes().iter().all(|byte| *byte == 0) || interval.is_zero() {
            return Err(Error::Fenced);
        }
        Ok(Self {
            scope,
            node,
            session,
            provider,
            journal,
            runtime,
            interval,
            cancellation,
            bank: Arc::new(StdMutex::new(Bank::default())),
            protocol: AsyncMutex::new(()),
        })
    }
    pub(crate) fn completion(
        &self,
        epoch: u64,
    ) -> cellule_runtime::Result<Option<FollowerEnrollmentCompletion>> {
        let record = self
            .bank
            .lock()
            .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?
            .epochs
            .get(&epoch)
            .cloned();
        let Some(record) = record else {
            return Ok(None);
        };
        let progress = record.progress()?;
        Ok(Some(FollowerEnrollmentCompletion {
            attempt: record.inputs.attempt.clone(),
            members: progress.members.clone(),
            native_started: progress.native_started,
            enrollment: progress.enrollment.clone(),
            refusal: progress.refusal.clone(),
            retirement: progress.retirement.clone(),
            native_closed: progress.native_closed,
            execution_error: progress.execution_error.clone(),
            journal_error: progress.journal_error.clone(),
        }))
    }
}

impl NodeDurabilityProvider for FleetFollowerEnrollment {
    fn recruit(
        self: Arc<Self>,
        limits: ReplicaLimits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        Box::pin(async move {
            self.recruit_owned(limits, required_follower_bytes, live_node_limit)
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
    fn drain(self: Arc<Self>) -> Pin<Box<dyn Future<Output = FacilityResult> + Send>> {
        Box::pin(async move {
            self.drain_owned()
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
    fn rotation_required(
        self: Arc<Self>,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<bool>> + Send>> {
        Arc::clone(&self.provider).rotation_required(live_node_limit)
    }
    fn rotation_event(&self, event: NodeDurabilityRotation) {
        self.provider.rotation_event(event);
    }
}

fn evidence(
    record: &Responsibility,
    member: &FollowerEnrollmentMember,
    tag: &[u8],
) -> cellule_runtime::Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet.follower-enrollment.v1\0");
    hash.update(tag);
    hash.update(&member.spec.to_bytes().map_err(operation)?);
    hash.update(record.inputs.attempt.evidence_digest()?.as_bytes());
    let progress = record.progress()?;
    if let Some(proof) = &progress.enrollment {
        hash.update(proof.evidence_digest()?.as_bytes());
    }
    if let Some(proof) = &progress.refusal {
        hash.update(proof.evidence_digest()?.as_bytes());
    }
    if let Some(retirement) = &progress.retirement {
        hash.update(&retirement.barrier().covered_through().to_le_bytes());
        for member in retirement.members() {
            let receipt = member.result().map_err(retained)?;
            hash.update(member.member().as_bytes());
            hash.update(&receipt.base_sequence.to_le_bytes());
            hash.update(&receipt.durable_through.to_le_bytes());
        }
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
