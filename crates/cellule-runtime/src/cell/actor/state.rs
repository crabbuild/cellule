//! Actor state: queues, admissions, activations, and task results.

use super::*;

pub(super) struct RuntimeInner {
    pub(super) sender: mpsc::Sender<Message>,
    pub(super) publications: broadcast::Sender<CatalogEntry>,
    pub(super) resources: ResourceLedger,
    pub(super) primitive_jobs: Arc<Semaphore>,
    pub(super) shutting_down: AtomicBool,
    pub(super) node_admission: NodeAdmission,
    pub(super) session: SessionId,
    pub(super) pool: SqlWorkerPool,
    pub(super) replica_host: cellule_ltx::Host,
    pub(super) receivers: std::sync::Mutex<receiver::ReceiverRegistry>,
    pub(super) application_limits: OnceLock<HashMap<crate::identity::NamespaceId, (u64, u64)>>,
    pub(super) node_lease: Arc<RuntimeNodeLease>,
    pub(super) node_durability: NodeDurabilitySlot,
    pub(super) telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    pub(super) unpublished_node_log_bytes: Arc<AtomicU64>,
    pub(super) shared_publication: Arc<crate::publication::SharedPublication>,
}

pub(super) enum RuntimeNodeLease {
    ObjectOnly,
    Required(OnceLock<NodeLeaseGuard>),
}

impl RuntimeNodeLease {
    pub(super) fn check(&self) -> crate::Result<()> {
        match self {
            Self::ObjectOnly => Ok(()),
            Self::Required(guard) => guard.get().ok_or(Error::Fenced)?.check(),
        }
    }

    pub(super) fn guard(&self) -> crate::Result<Option<NodeLeaseGuard>> {
        match self {
            Self::ObjectOnly => Ok(None),
            Self::Required(guard) => guard.get().cloned().map(Some).ok_or(Error::Fenced),
        }
    }

    pub(super) fn install(&self, guard: NodeLeaseGuard) -> crate::Result<()> {
        match self {
            Self::ObjectOnly => Err(Error::Control(
                "object-only Cell runtime does not accept a node lease",
            )),
            Self::Required(slot) => slot
                .set(guard)
                .map_err(|_| Error::Control("Cell runtime node lease was initialized twice")),
        }
    }
}

pub(super) enum Activation {
    Restored(Box<RestoredActivation>),
    Bootstrap(Box<BootstrapActivation>),
}

pub(super) struct RestoredActivation {
    pub(super) database: crate::cell::worker::RestoredDatabase,
    pub(super) destination: PathBuf,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) schema: u32,
    pub(super) root: cellule_ltx::RootRef,
    pub(super) reservation: CellReservation,
    pub(super) job: Option<crate::cell::worker::WorkerJobReservation>,
}

pub(super) struct BootstrapActivation {
    pub(super) replica: cellule_ltx::CellReplica,
    pub(super) destination: PathBuf,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) schema: u32,
    pub(super) initialize: Initializer,
    pub(super) reservation: CellReservation,
}

pub(super) type IdleTransferCandidates = Vec<(CellId, u64, i64, CatalogRole)>;

pub(super) enum Message {
    PublicationProgress {
        reply: oneshot::Sender<crate::Result<CellPublicationProgress>>,
    },
    Activate {
        cell: CellId,
        role: CatalogRole,
        catalog: CatalogProof,
        activation: Activation,
        publisher: Box<CellPublisher>,
        reply: oneshot::Sender<crate::Result<Arc<CellAdmission>>>,
    },
    Execute(Box<QueuedCommand>),
    Query(Box<QueuedQuery>),
    Resolve(Box<QueuedResolve>),
    Migrate(Box<QueuedMigration>),
    Lookup {
        cell: CellId,
        require_resident: bool,
        reply: oneshot::Sender<Option<LocalCell>>,
    },
    /// Lists resident Cells whose selected durable due time has passed.
    ///
    /// The scheduler uses this to tick a Cell it already owns without reading
    /// its catalog entry or control record first.
    DueResident {
        now_ms: i64,
        limit: usize,
        reply: oneshot::Sender<Vec<DueResidentCell>>,
    },
    Drain {
        cell: CellId,
        admission: Arc<CellAdmission>,
        reply: oneshot::Sender<crate::Result<()>>,
    },
    EvictIdle {
        limit: usize,
        reply: oneshot::Sender<crate::Result<usize>>,
    },
    IdleTransferCandidates {
        reply: oneshot::Sender<crate::Result<IdleTransferCandidates>>,
    },
    FleetCellsPage {
        session: SessionId,
        cursor: Option<CellInventoryCursor>,
        limit: usize,
        retained: ResourceReservation,
        reply: oneshot::Sender<crate::Result<CellInventoryPage>>,
    },
    ActiveCatalogEntries {
        reply: oneshot::Sender<crate::Result<Vec<crate::cell::catalog::CatalogEntry>>>,
    },
    ActiveCellTargets {
        reply: oneshot::Sender<crate::Result<Vec<crate::identity::CellTarget>>>,
    },
    UnreleasedCellCount {
        reply: oneshot::Sender<crate::Result<usize>>,
    },
    QuiesceCell {
        cell: CellId,
        generation: u64,
        incarnation: crate::identity::IncarnationId,
        epoch: u64,
        reply: oneshot::Sender<crate::Result<()>>,
    },
    ReleaseMaintenanceCell(maintenance::ReleaseRequest),
    ReleaseIdleCell {
        cell: CellId,
        generation: u64,
        expected: Option<(crate::identity::IncarnationId, u64)>,
        reply: DrainReply,
    },
    ObservePressure {
        sample: PressureSample,
        reply: oneshot::Sender<crate::Result<PressureState>>,
    },
    Shutdown {
        reply: oneshot::Sender<crate::Result<()>>,
    },
}

pub(super) struct QueuedCommand {
    pub(super) group: Option<CommandGroup>,
    // Pressure may admit only an original durable outcome lookup. An absent
    // identity returns this refusal without invoking the mutation handler.
    pub(super) refused_mutation: Option<Error>,
    pub(super) publication_probe: bool,
    pub(super) publication_probed: bool,
    pub(super) trace: tracing::Span,
    pub(super) telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    pub(super) queued_at: std::time::Instant,
    pub(super) response_proof: Option<(crate::node::log::DurabilitySource, std::time::Duration)>,
    pub(super) cell: CellId,
    pub(super) admission: Arc<CellAdmission>,
    pub(super) operation: QueuedOperation,
    pub(super) now_ms: i64,
    pub(super) max_result_bytes: usize,
    pub(super) handler: Option<Handler>,
    pub(super) reply: Option<oneshot::Sender<crate::Result<StoredOutcome>>>,
    pub(super) _work: WorkAdmission,
}

pub(super) struct CommandGroup {
    pub(super) members: Vec<QueuedCommand>,
    pub(super) execution: Option<GroupOutcomes>,
}

pub(super) struct GroupOutcomes {
    pub(super) outcomes: Vec<crate::Result<StoredOutcome>>,
    pub(super) base_sequence: u64,
}

#[derive(Clone, Copy)]
pub(super) enum QueuedOperation {
    Mutation {
        identity: MutationIdentity,
        operation_digest: Digest,
    },
    Effect {
        delivery: InboxDelivery,
    },
}

impl QueuedOperation {
    pub(super) fn unknown(self, source: Error) -> Error {
        match self {
            Self::Mutation {
                identity,
                operation_digest,
            } => Error::OutcomeUnknown {
                request_id: identity.request_id,
                operation_digest,
                source: Box::new(source),
            },
            Self::Effect { delivery } => Error::EffectOutcomeUnknown {
                effect_id: delivery.effect_id,
                operation_digest: delivery.operation_digest,
                source: Box::new(source),
            },
        }
    }
}

pub(super) struct QueuedQuery {
    pub(super) telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    pub(super) queued_at: std::time::Instant,
    pub(super) cell: CellId,
    pub(super) admission: Arc<CellAdmission>,
    pub(super) max_result_bytes: usize,
    pub(super) handler: Option<QueryHandler>,
    pub(super) reply: Option<oneshot::Sender<crate::Result<Vec<u8>>>>,
    pub(super) _work: WorkAdmission,
}

pub(super) struct QueuedResolve {
    pub(super) cell: CellId,
    pub(super) admission: Arc<CellAdmission>,
    pub(super) operation: ResolveOperation,
    pub(super) now_ms: i64,
    pub(super) max_result_bytes: usize,
    pub(super) reply: Option<oneshot::Sender<crate::Result<Resolution>>>,
    pub(super) _work: WorkAdmission,
}

pub(super) struct QueuedMigration {
    pub(super) cell: CellId,
    pub(super) admission: Arc<CellAdmission>,
    pub(super) successor_admission: Arc<CellAdmission>,
    pub(super) plan: MigrationPlan,
    pub(super) now_ms: i64,
    pub(super) reply: Option<oneshot::Sender<crate::Result<MigratedAdmission>>>,
    pub(super) _work: WorkAdmission,
}

pub(super) struct MigratedAdmission {
    pub(super) admission: Arc<CellAdmission>,
    pub(super) outcome: MigrationOutcome,
}

#[derive(Clone, Copy)]
pub(super) enum ResolveOperation {
    Mutation {
        identity: MutationIdentity,
        operation_digest: Digest,
    },
    Effect {
        delivery: InboxDelivery,
    },
}

pub(super) enum QueuedWork {
    Command(Box<QueuedCommand>),
    Query(Box<QueuedQuery>),
    Resolve(Box<QueuedResolve>),
    Migration(Box<QueuedMigration>),
}

pub(super) struct ActiveCell {
    pub(super) generation: u64,
    pub(super) admission: Arc<CellAdmission>,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) code: Digest,
    pub(super) schema: u32,
    pub(super) role: CatalogRole,
    pub(super) catalog: CatalogProof,
    pub(super) interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    pub(super) publisher: Option<CellPublisher>,
    pub(super) durability_submitter: CellDurabilitySubmitter,
    pub(super) publications: VecDeque<QueuedPublication>,
    // One observer per resident Cell, retaining only its original receipt.
    // Dropping/fencing the Cell cancels observation, never native publication.
    pub(super) selection_waiter: Option<tokio_util::sync::DropGuard>,
    pub(super) root_debt: Option<RootDebt>,
    pub(super) materializing: bool,
    pub(super) publishing_since: Option<std::time::Instant>,
    pub(super) publication_bytes: u64,
    pub(super) unpublished_node_logs: usize,
    pub(super) queue: VecDeque<QueuedWork>,
    pub(super) coordination: CoordinationState,
    pub(super) persisted_work: crate::primitives::maintenance::PersistedWorkInventory,
    pub(super) demand: inventory::CellDemandState,
    pub(super) resource_limits: cellule_ltx::Limits,
    pub(super) inventory_refreshing: bool,
    pub(super) inventory_revision: u64,
    pub(super) drain: Option<DrainReply>,
    // Transfer closes the old capability and installs a fresh one; failed fresh
    // inventory must leave the current owner serving through that capability.
    pub(super) transfer: Option<TransferPreflight>,
    pub(super) resident_since_ms: i64,
    pub(super) last_used_ms: i64,
    pub(super) last_work_at: std::time::Instant,
    pub(super) compaction_retry_at: std::time::Instant,
    pub(super) compaction_admission: Option<CompactionAdmission>,
    pub(super) hydration_retry_at: std::time::Instant,
    // The published head's due time and commit sequence, mirrored from the
    // authoritative control so a resident Cell can be ticked without a
    // metadata read. Both advance through the same publication that writes
    // control, so a stale reader only produces a `Stale` Tick.
    pub(super) next_due_ms: Option<i64>,
    pub(super) published_sequence: u64,
}

/// Only admission is cancellable. Once dispatched, compaction owns its native
/// work and reservations until completion, even if this guard is cancelled.
pub(super) struct CompactionAdmission {
    pub(super) cancel: tokio_util::sync::CancellationToken,
    pub(super) pending: bool,
}

impl Drop for CompactionAdmission {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(super) struct TransferPreflight {
    pub(super) reply: DrainReply,
    pub(super) maintenance: Option<maintenance::ReleaseState>,
}

pub(super) enum DrainReply {
    Maintenance(oneshot::Sender<crate::Result<MaintenanceCellRelease>>),
    Unit(oneshot::Sender<crate::Result<()>>),
    Position(oneshot::Sender<crate::Result<crate::fleet::operations::PublishedPosition>>),
}

impl DrainReply {
    /// Only call before this request transfers its reply to canonical close.
    /// A later close/publication error must remain an uncertain release.
    pub(super) fn refuse(self, blocker: DrainBlocker, error: Error) {
        let error = if matches!(&self, Self::Position(_)) {
            Error::CellReleaseRefused {
                blocker,
                source: Box::new(error),
            }
        } else {
            error
        };
        let _ = self.send(Err(error));
    }

    pub(super) fn send(self, result: crate::Result<()>) -> Result<(), crate::Result<()>> {
        self.send_released(result, None)
    }

    pub(super) fn send_released(
        self,
        result: crate::Result<()>,
        released: Option<crate::fleet::operations::PublishedPosition>,
    ) -> Result<(), crate::Result<()>> {
        match self {
            Self::Maintenance(reply) => reply
                .send(
                    result
                        .and_then(|()| released.ok_or(Error::Fenced))
                        .map(MaintenanceCellRelease::Released),
                )
                .map_err(|result| result.map(|_| ())),
            Self::Unit(reply) => reply.send(result),
            Self::Position(reply) => reply
                .send(result.and_then(|()| released.ok_or(Error::Fenced)))
                .map_err(|result| result.map(|_| ())),
        }
    }
}

pub(super) struct QueuedPublication {
    pub(super) pending: PendingCommit,
    pub(super) durability: Option<PendingDurability>,
    pub(super) retained_reservation: ResourceReservation,
    pub(super) submitted_at: std::time::Instant,
    pub(super) proof: oneshot::Sender<crate::Result<()>>,
}

#[derive(Clone)]
pub(super) struct RootDebt {
    pub(super) selected: Arc<crate::node::log_shipper::SelectedBundle>,
    pub(super) durability: PendingDurability,
    pub(super) submitted_at: std::time::Instant,
    pub(super) next_due_ms: Option<i64>,
    pub(super) node_log_bytes: u64,
    pub(super) covered_node_logs: u64,
}

pub(super) struct SelectedPublication {
    pub(super) covered: u64,
    pub(super) debt: RootDebt,
}

impl ActiveCell {
    pub(super) fn selected_due_head(&self) -> (u64, Option<i64>) {
        self.root_debt
            .as_ref()
            .map_or((self.published_sequence, self.next_due_ms), |debt| {
                (debt.selected.proof.commit_sequence(), debt.next_due_ms)
            })
    }

    pub(super) fn draining(&self) -> bool {
        self.drain.is_some()
            || self.transfer.is_some()
            || self.coordination.is_draining()
            || self.coordination.is_transfer_preparing()
    }

    pub(super) fn busy(&self) -> bool {
        self.coordination.is_busy()
    }

    pub(super) fn renewing(&self) -> bool {
        self.coordination.is_renewing()
    }

    pub(super) fn begin_task(&mut self, effect: CoordinationEffect) -> u64 {
        self.coordination.begin_effect(effect)
    }

    pub(super) fn finish_task(&mut self, effect_id: u64, effect: CoordinationEffect) -> bool {
        matches!(
            self.coordination
                .step(CoordinationInput::CompleteEffect { effect_id, effect }),
            CoordinationDecision::EffectCompleted
        )
    }

    pub(super) fn cancel_compaction_admission(&self) {
        if let Some(admission) = &self.compaction_admission
            && admission.pending
        {
            admission.cancel.cancel();
        }
    }
}

/// Retains unobserved release failures even before shutdown is requested.
#[derive(Default)]
pub(super) struct ShutdownState {
    pub(super) reply: Option<oneshot::Sender<crate::Result<()>>>,
    pub(super) draining: bool,
    pub(super) error: Option<Error>,
}

pub(super) struct LocalCell {
    pub(super) admission: Arc<CellAdmission>,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) code: Digest,
    pub(super) schema: u32,
}

/// One resident Cell whose selected durable due time has passed.
pub(super) struct DueResidentCell {
    pub(super) cell: CellId,
    pub(super) catalog: CatalogProof,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) code: Digest,
    pub(super) schema: u32,
    pub(super) admission: Arc<CellAdmission>,
    /// Commit sequence the last verified bundle or root named.
    pub(super) expected_commit_sequence: u64,
    pub(super) next_due_ms: i64,
}

/// Preparation admission carries the retry boundary from original dispatch.
pub(super) struct PublicationAdmission {
    pub(super) replica: crate::publication::PublicationPermit,
    pub(super) fleet_deadline: std::time::Instant,
}

pub(super) enum TaskResult {
    Activated {
        cell: CellId,
        generation: u64,
        role: CatalogRole,
        catalog: CatalogProof,
        publisher: Box<CellPublisher>,
        admission: Arc<CellAdmission>,
        reply: oneshot::Sender<crate::Result<Arc<CellAdmission>>>,
        result: crate::Result<(
            Arc<cellule_ltx::rusqlite::InterruptHandle>,
            Option<cellule_ltx::Hydration>,
        )>,
        inventory: crate::Result<crate::cell::worker::WorkerCellInventory>,
    },
    Hydrated {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        result: crate::Result<HydrationStep>,
    },
    InventoryRefreshed {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        inventory_revision: u64,
        result: crate::Result<crate::cell::worker::WorkerCellInventory>,
    },
    TransferPreflight {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        result: crate::Result<crate::primitives::maintenance::TransferWorkInventory>,
    },
    MaintenancePreflight {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        result: crate::Result<crate::primitives::maintenance_readiness::MaintenanceWorkInventory>,
    },
    Executed {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        command: Box<QueuedCommand>,
        result: crate::Result<CommandTaskResult>,
        fenced: bool,
    },
    Proven {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        command: Box<QueuedCommand>,
        result: crate::Result<StoredOutcome>,
        fenced: bool,
    },
    PublicationAdmitted {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        result: crate::Result<Box<PublicationAdmission>>,
    },
    BundleSelectionReady {
        cell: CellId,
        generation: u64,
        result: crate::Result<()>,
    },
    BundleSelected {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        covered: u64,
        retained_bytes: u64,
        result: crate::Result<Box<SelectedPublication>>,
    },
    Published {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        /// Bytes every commit this root covers retained.
        retained_bytes: u64,
        /// Retained bytes whose response was gated on the node log.
        node_log_bytes: u64,
        /// Commits this root covers.
        covered: u64,
        /// Covered commits whose response was gated on the node log.
        covered_node_logs: u64,
        next_due_ms: Option<i64>,
        commit_sequence: u64,
        result: crate::Result<()>,
        fenced: bool,
    },
    CompactionAdmitted {
        cell: CellId,
        generation: u64,
        result: crate::Result<Option<Box<cellule_ltx::CellReplica>>>,
    },
    Compacted {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        result: crate::Result<Option<bool>>,
    },
    Queried {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        query: Box<QueuedQuery>,
        result: crate::Result<Vec<u8>>,
        fenced: bool,
    },
    Resolved {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        resolve: Box<QueuedResolve>,
        result: crate::Result<Resolution>,
        fenced: bool,
    },
    Migrated {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        migration: Box<QueuedMigration>,
        result: crate::Result<MigrationOutcome>,
        fenced: bool,
        preserve_owner: bool,
        unpublished_bytes: u64,
    },
    Renewed {
        cell: CellId,
        generation: u64,
        effect_id: u64,
        publisher: Box<CellPublisher>,
        result: crate::Result<()>,
    },
    Deactivated {
        cell: CellId,
        generation: u64,
        reply: Option<DrainReply>,
        shutdown_drain: bool,
        result: crate::Result<()>,
        released: Option<crate::fleet::operations::PublishedPosition>,
    },
}

pub(super) enum CommandTaskResult {
    Recorded(StoredOutcome),
    AwaitPublication,
    GroupRecorded,
    Pending {
        pending: Box<PendingCommit>,
        durability: Option<PendingDurability>,
        retained_reservation: ResourceReservation,
    },
}
