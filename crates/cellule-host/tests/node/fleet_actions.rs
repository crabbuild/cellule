//! Atomic acceptance, lost replies, retained source proof and shutdown joins.

use super::*;
use cellule_host::fleet::{
    FleetActionAcceptance, FleetActionJournal, FleetAdapterFuture, FleetCellInputs,
    FleetCellProvider, FleetJournalSnapshot, FleetRecoveryInputs, FleetSnapshotRequest,
};
use cellule_runtime::cell::actor::{CellHandle, CellInventoryEntry};
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::{CellTarget, IncarnationId, NamespaceId, NodeId, TenantId};
use cellule_runtime::ltx::{CellReplica, CellStorageLayout};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

pub(super) fn clock() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

pub(super) fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([200; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    }
}

struct JournalState {
    head: FleetHead,
    registry: RegistryVersion,
    records: HashMap<Digest, (AcceptedFleetAction, Option<FleetActionOutcome>)>,
    bases: HashMap<Digest, AcquisitionBasis>,
    recovery_bases: HashMap<Digest, RecoveryBasis>,
    recovery_evidence: HashMap<Digest, RecoveryEvidence>,
    intent: NodeIntent,
}

pub(super) struct Journal {
    state: Mutex<JournalState>,
    accepts: AtomicUsize,
    publications: AtomicUsize,
    lose_publication_reply: AtomicBool,
    block_publication: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Semaphore,
    pub(super) lose_basis_reply: AtomicBool,
    pub(super) block_basis: AtomicBool,
    pub(super) basis_entered: tokio::sync::Notify,
    pub(super) basis_resume: tokio::sync::Semaphore,
    pub(super) basis_writes: AtomicUsize,
    pub(super) panic_basis: AtomicBool,
    pub(super) lose_recovery_evidence_reply: AtomicBool,
    pub(super) block_inspections: AtomicBool,
    pub(super) inspection_entered: tokio::sync::Notify,
    pub(super) inspection_resume: tokio::sync::Semaphore,
    pub(super) snapshot_calls: AtomicUsize,
    pub(super) pause_snapshot_post: AtomicBool,
    pub(super) snapshot_post_entered: tokio::sync::Notify,
    pub(super) snapshot_post_resume: tokio::sync::Semaphore,
}

impl Journal {
    pub(super) fn snapshot_request(
        &self,
        subject: cellule_host::fleet::FleetSnapshotSubject,
        nonce: u8,
    ) -> FleetSnapshotRequest {
        let state = self.state.lock().unwrap();
        let expected = FleetJournalSnapshot::new(state.head.clone(), state.registry).unwrap();
        let now = clock();
        let deadline = (now + 3_000).min(state.head.controller().unwrap().expires_at_ms);
        FleetSnapshotRequest::new(
            expected,
            Digest::from_bytes([nonce; 32]),
            state.intent.node(),
            state.intent.session(),
            subject,
            128,
            now,
            deadline,
        )
        .unwrap()
    }
    pub(super) fn advance_snapshot_registry(&self) {
        let mut state = self.state.lock().unwrap();
        state.registry = state.registry.advance(state.registry.revision()).unwrap();
    }

    fn new() -> Self {
        Self {
            state: Mutex::new(JournalState {
                head: FleetHead::new(scope(), clock()).unwrap(),
                registry: RegistryVersion::new(scope()).unwrap(),
                records: HashMap::new(),
                bases: HashMap::new(),
                recovery_bases: HashMap::new(),
                recovery_evidence: HashMap::new(),
                intent: NodeIntent::initial(
                    scope(),
                    NodeId::from_bytes([201; 16]),
                    SessionId::from_bytes([201; 16]),
                )
                .unwrap(),
            }),
            accepts: AtomicUsize::new(0),
            publications: AtomicUsize::new(0),
            lose_publication_reply: AtomicBool::new(false),
            block_publication: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Semaphore::new(0),
            lose_basis_reply: AtomicBool::new(false),
            block_basis: AtomicBool::new(false),
            basis_entered: tokio::sync::Notify::new(),
            basis_resume: tokio::sync::Semaphore::new(0),
            basis_writes: AtomicUsize::new(0),
            panic_basis: AtomicBool::new(false),
            lose_recovery_evidence_reply: AtomicBool::new(false),
            block_inspections: AtomicBool::new(false),
            inspection_entered: tokio::sync::Notify::new(),
            inspection_resume: tokio::sync::Semaphore::new(0),
            snapshot_calls: AtomicUsize::new(0),
            pause_snapshot_post: AtomicBool::new(false),
            snapshot_post_entered: tokio::sync::Notify::new(),
            snapshot_post_resume: tokio::sync::Semaphore::new(0),
        }
    }

    pub(super) fn reset_preparing(&self, spec: MoveAttemptSpec) {
        let mut state = self.state.lock().unwrap();
        assert!(state.records.is_empty());
        let now = clock();
        state.head = FleetHead::new(scope(), now)
            .unwrap()
            .claim(
                FleetProfile::default(),
                0,
                SessionId::from_bytes([206; 16]),
                now,
            )
            .unwrap();
        drop(state);
        self.transition(JournalTransition::Allocate(spec.clone()));
        self.transition(JournalTransition::Attempt {
            id: spec.id,
            event: AttemptEvent::BeginPrepare,
        });
    }

    pub(super) fn reset_maintenance(&self, node: NodeId, session: SessionId) -> FleetAction {
        let now = clock();
        let mut state = self.state.lock().unwrap();
        assert!(state.records.is_empty());
        state.head = FleetHead::new(scope(), now)
            .unwrap()
            .claim(
                FleetProfile::default(),
                0,
                SessionId::from_bytes([206; 16]),
                now,
            )
            .unwrap();
        drop(state);
        self.transition(JournalTransition::BeginMaintenance(
            MaintenanceOperation::new(
                OperationId::from_bytes([207; 16]).unwrap(),
                Digest::from_bytes([208; 32]),
                node,
                session,
                2,
                now,
                now + 60_000,
            )
            .unwrap(),
        ));
        self.maintenance_action(MaintenanceAction::Cordon)
    }

    pub(super) fn maintenance_action(&self, effect: MaintenanceAction) -> FleetAction {
        self.state
            .lock()
            .unwrap()
            .head
            .maintenance_action(effect, clock())
            .unwrap()
    }

    pub(super) fn hold_next_result(&self) {
        self.block_publication.store(true, Ordering::SeqCst);
    }

    pub(super) async fn wait_for_result_publication(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }

    pub(super) fn resume_result_publication(&self) {
        self.resume.add_permits(1);
    }

    pub(super) fn transition(&self, event: JournalTransition) {
        let mut state = self.state.lock().unwrap();
        state.head = state
            .head
            .transition(
                FleetProfile::default(),
                state.head.revision(),
                state.head.controller().unwrap().epoch,
                clock(),
                event,
            )
            .unwrap();
    }

    pub(super) fn action(&self, id: AttemptId, effect: MovementAction) -> FleetAction {
        self.state
            .lock()
            .unwrap()
            .head
            .movement_action(id, effect, clock())
            .unwrap()
    }

    pub(super) fn basis(&self, key: Digest) -> Option<AcquisitionBasis> {
        self.state.lock().unwrap().bases.get(&key).cloned()
    }

    pub(super) fn recovery_basis(&self, key: Digest) -> Option<RecoveryBasis> {
        self.state.lock().unwrap().recovery_bases.get(&key).cloned()
    }
    pub(super) fn recovery_evidence(&self, key: Digest) -> Option<RecoveryEvidence> {
        self.state
            .lock()
            .unwrap()
            .recovery_evidence
            .get(&key)
            .cloned()
    }
    pub(super) fn current_attempt(&self) -> MoveAttempt {
        self.state.lock().unwrap().head.attempts()[0].clone()
    }
    pub(super) fn accepted_count(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }
    pub(super) fn registry(&self) -> RegistryVersion {
        self.state.lock().unwrap().registry
    }

    pub(super) fn lose_next_result_reply(&self) {
        self.lose_publication_reply.store(true, Ordering::SeqCst);
    }
}

impl FleetActionJournal for Journal {
    fn authorize_snapshot<'a>(
        &'a self,
        request: &'a FleetSnapshotRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()> {
        Box::pin(async move {
            let call = self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_inspections.load(Ordering::SeqCst) {
                self.inspection_entered.notify_one();
                self.inspection_resume.acquire().await.unwrap().forget();
            }
            if call == 1 && self.pause_snapshot_post.load(Ordering::SeqCst) {
                self.snapshot_post_entered.notify_one();
                self.snapshot_post_resume.acquire().await.unwrap().forget();
            }
            let state = self.state.lock().unwrap();
            let snapshot = FleetJournalSnapshot::new(state.head.clone(), state.registry)?;
            request.authorize_against(&snapshot, &state.intent, now_ms)?;
            Ok(())
        })
    }

    fn authorize_inspection<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()> {
        Box::pin(async move {
            if self.block_inspections.load(Ordering::SeqCst) {
                self.inspection_entered.notify_one();
                self.inspection_resume.acquire().await.unwrap().forget();
            }
            let state = self.state.lock().unwrap();
            request.authorize_against(&state.head, state.registry, now_ms)?;
            Ok(())
        })
    }

    fn accept_action<'a>(
        &'a self,
        action: &'a FleetAction,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FleetActionAcceptance> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            let key = action.key()?;
            if let Some((accepted, result)) = state.records.get(&key) {
                accepted.validate_replay(action, node, session)?;
                return Ok(FleetActionAcceptance::Existing {
                    accepted: accepted.clone(),
                    result: result.clone().map(Box::new),
                });
            }
            // One critical section linearizes head validation and acceptance.
            let accepted =
                AcceptedFleetAction::new(action.clone(), &state.head, node, session, now_ms)?;
            state.records.insert(key, (accepted.clone(), None));
            self.accepts.fetch_add(1, Ordering::SeqCst);
            Ok(FleetActionAcceptance::New(accepted))
        })
    }

    fn publish_action_result<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        result: &'a FleetActionOutcome,
    ) -> FleetAdapterFuture<'a, ()> {
        Box::pin(async move {
            self.publications.fetch_add(1, Ordering::SeqCst);
            if self.block_publication.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.acquire().await.unwrap().forget();
            }
            accepted.validate_result(result)?;
            {
                let mut state = self.state.lock().unwrap();
                let (original, previous) = state.records.get_mut(&result.action_key).unwrap();
                if original != accepted {
                    return Err(Box::new(OperationError::Conflict)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if let Some(previous) = previous
                    && previous != result
                    && !matches!(previous.outcome, FleetOutcome::Unknown)
                {
                    return Err(Box::new(OperationError::Conflict)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                *previous = Some(result.clone());
            }
            if self.lose_publication_reply.swap(false, Ordering::SeqCst) {
                return Err(Box::new(std::io::Error::other(
                    "injected lost result publication reply",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(())
        })
    }

    fn load_movement_action<'a>(
        &'a self,
        scope: FleetScope,
        attempt: AttemptId,
        effect: MovementAction,
        node: NodeId,
        session: SessionId,
    ) -> FleetAdapterFuture<'a, Option<FleetActionAcceptance>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            Ok(state.records.values().find_map(|(accepted, result)| {
                let FleetActionKind::Movement {
                    action,
                    attempt: original,
                } = accepted.action().kind()
                else {
                    return None;
                };
                (accepted.action().scope() == scope
                    && original.spec().id == attempt
                    && *action == effect
                    && accepted.node() == node
                    && accepted.session() == session)
                    .then(|| FleetActionAcceptance::Existing {
                        accepted: accepted.clone(),
                        result: result.clone().map(Box::new),
                    })
            }))
        })
    }

    fn record_acquisition_basis<'a>(
        &'a self,
        basis: &'a AcquisitionBasis,
    ) -> FleetAdapterFuture<'a, AcquisitionBasis> {
        Box::pin(async move {
            if self.block_basis.swap(false, Ordering::SeqCst) {
                self.basis_entered.notify_one();
                self.basis_resume.acquire().await.unwrap().forget();
            }
            assert!(
                !self.panic_basis.swap(false, Ordering::SeqCst),
                "injected acquisition-basis panic"
            );
            let retained = {
                let mut state = self.state.lock().unwrap();
                let key = basis.accepted().action().key()?;
                if state
                    .records
                    .get(&key)
                    .is_none_or(|(original, _)| original != basis.accepted())
                {
                    return Err(Box::new(OperationError::Conflict)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if let Some(original) = state.bases.get(&key) {
                    if original.accepted() != basis.accepted()
                        || original.control() != basis.control()
                    {
                        return Err(Box::new(OperationError::Conflict)
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    original.clone()
                } else {
                    state.bases.insert(key, basis.clone());
                    self.basis_writes.fetch_add(1, Ordering::SeqCst);
                    basis.clone()
                }
            };
            if self.lose_basis_reply.swap(false, Ordering::SeqCst) {
                return Err(Box::new(std::io::Error::other(
                    "injected lost acquisition-basis reply",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(retained)
        })
    }

    fn load_acquisition_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<AcquisitionBasis>> {
        Box::pin(async move {
            let basis = self
                .state
                .lock()
                .unwrap()
                .bases
                .get(&accepted.action().key()?)
                .cloned();
            if basis
                .as_ref()
                .is_some_and(|basis| basis.accepted() != accepted)
            {
                return Err(
                    Box::new(OperationError::Conflict) as Box<dyn std::error::Error + Send + Sync>
                );
            }
            Ok(basis)
        })
    }
    fn record_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        basis: &'a RecoveryBasis,
    ) -> FleetAdapterFuture<'a, RecoveryBasis> {
        Box::pin(async move {
            if self.block_basis.swap(false, Ordering::SeqCst) {
                self.basis_entered.notify_one();
                self.basis_resume.acquire().await.unwrap().forget();
            }
            basis.validate_acceptance(accepted)?;
            let retained = {
                let mut state = self.state.lock().unwrap();
                let key = accepted.action().key()?;
                if state
                    .records
                    .get(&key)
                    .is_none_or(|(original, _)| original != accepted)
                {
                    return Err(Box::new(OperationError::Conflict)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if let Some(original) = state.recovery_bases.get(&key) {
                    original.validate_acceptance(accepted)?;
                    if original.control() != basis.control() {
                        return Err(Box::new(OperationError::Conflict)
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    original.clone()
                } else {
                    state.recovery_bases.insert(key, basis.clone());
                    self.basis_writes.fetch_add(1, Ordering::SeqCst);
                    basis.clone()
                }
            };
            if self.lose_basis_reply.swap(false, Ordering::SeqCst) {
                return Err(
                    Box::new(std::io::Error::other("injected lost recovery-basis reply"))
                        as Box<dyn std::error::Error + Send + Sync>,
                );
            }
            Ok(retained)
        })
    }
    fn load_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryBasis>> {
        Box::pin(async move {
            let basis = self
                .state
                .lock()
                .unwrap()
                .recovery_bases
                .get(&accepted.action().key()?)
                .cloned();
            if let Some(basis) = &basis {
                basis.validate_acceptance(accepted)?;
            }
            Ok(basis)
        })
    }
    fn record_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        evidence: &'a RecoveryEvidence,
    ) -> FleetAdapterFuture<'a, RecoveryEvidence> {
        Box::pin(async move {
            evidence.basis().validate_acceptance(accepted)?;
            let retained = {
                let mut state = self.state.lock().unwrap();
                let key = accepted.action().key()?;
                if state.recovery_bases.get(&key) != Some(evidence.basis()) {
                    return Err(Box::new(OperationError::Conflict)
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                if let Some(original) = state.recovery_evidence.get(&key) {
                    if original.basis() != evidence.basis()
                        || original.restored() != evidence.restored()
                    {
                        return Err(Box::new(OperationError::Conflict)
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    original.clone()
                } else {
                    state.recovery_evidence.insert(key, evidence.clone());
                    evidence.clone()
                }
            };
            if self
                .lose_recovery_evidence_reply
                .swap(false, Ordering::SeqCst)
            {
                return Err(Box::new(std::io::Error::other(
                    "injected lost recovery evidence reply",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(retained)
        })
    }
    fn load_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryEvidence>> {
        Box::pin(async move {
            let evidence = self
                .state
                .lock()
                .unwrap()
                .recovery_evidence
                .get(&accepted.action().key()?)
                .cloned();
            if let Some(evidence) = &evidence {
                evidence.basis().validate_acceptance(accepted)?;
            }
            Ok(evidence)
        })
    }
}

struct NoCells;

impl FleetCellProvider for NoCells {
    fn cell_inputs<'a>(
        &'a self,
        _spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetCellInputs> {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "source release must not resolve receiver inputs",
            )) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
    fn recovery_inputs<'a>(
        &'a self,
        _spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetRecoveryInputs> {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "source must not resolve receiver recovery",
            )) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

pub(super) struct Fixture {
    pub(super) node: Arc<CellNode>,
    pub(super) journal: Arc<Journal>,
    pub(super) action: FleetAction,
    pub(super) authority: CellAuthority,
    pub(super) handle: CellHandle,
    pub(super) lease: NodeLeaseGuard,
    lease_shutdown: CancellationToken,
    tasks: Arc<CellNodeTaskGroup>,
    pub(super) _root: tempfile::TempDir,
}

pub(super) async fn fixture() -> Fixture {
    let session = SessionId::from_bytes([201; 16]);
    let physical = NodeId::from_bytes([201; 16]);
    let journal = Arc::new(Journal::new());
    let node = Arc::new(
        CellNodeBuilder::new(application())
            .with_runtime(
                SqlWorkerPool::new(1, 8)
                    .unwrap()
                    .with_native_memory_limit(128 << 20)
                    .unwrap(),
                64 << 20,
            )
            .with_replica_host(
                ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
            )
            .with_session(session)
            .build()
            .unwrap(),
    );
    let lease_shutdown = CancellationToken::new();
    let tasks = node
        .install_task_group(CancellationToken::new(), lease_shutdown.clone())
        .unwrap();
    node.install_fleet_actions(scope(), physical, journal.clone(), Arc::new(NoCells))
        .unwrap();
    let lease = NodeLeaseGuard::new(clock(), clock() + 60_000).unwrap();
    node.install_node_lease(lease.clone()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("host-fleet-actions"),
        [3; 16],
    );
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope().application,
        NamespaceId::from_bytes([2; 16]),
        b"host-fleet-actions",
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([202; 16]);
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        ReplicaLimits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..ReplicaLimits::default()
        },
    )
    .unwrap();
    let code = node.application().registry().module_digests()[0];
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let initial = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://fleet-source.internal:8789".into(),
            },
        )
        .await
        .unwrap();
    let handle = node
        .runtime()
        .bootstrap(
            proof,
            replica,
            authority.clone(),
            initial,
            root.path().join("source.sqlite"),
            |transaction| {
                transaction.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (42)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let owner = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = node.runtime().fleet_cells_page(None, 128).await.unwrap();
            if let Some(CellInventoryEntry::Owned(owner)) = page.entries().first()
                && owner.stable_observations == 2
                && owner.cost.is_some()
            {
                return (**owner).clone();
            }
            drop(page);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let now = clock();
    let spec = MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([203; 16]).unwrap(),
            sequence: 1,
        },
        target,
        incarnation,
        source_node: physical,
        source: session,
        generation: owner.generation,
        source_epoch: owner.position.unwrap().epoch,
        destination_node: NodeId::from_bytes([204; 16]),
        destination: SessionId::from_bytes([204; 16]),
        cost: owner.cost.unwrap(),
        snapshot_digest: Digest::from_bytes([205; 32]),
        deadline_ms: now + 60_000,
    };
    let mut head = journal.state.lock().unwrap().head.clone();
    head = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([206; 16]),
            now,
        )
        .unwrap();
    for event in [
        JournalTransition::Allocate(spec.clone()),
        JournalTransition::Attempt {
            id: spec.id,
            event: AttemptEvent::BeginPrepare,
        },
        JournalTransition::Attempt {
            id: spec.id,
            event: AttemptEvent::Reserved(ReceiverReservation {
                session: spec.destination,
                expires_at_ms: spec.deadline_ms,
            }),
        },
        JournalTransition::Attempt {
            id: spec.id,
            event: AttemptEvent::BeginRelease,
        },
    ] {
        head = head
            .transition(
                FleetProfile::default(),
                head.revision(),
                head.controller().unwrap().epoch,
                now,
                event,
            )
            .unwrap();
    }
    let action = head
        .movement_action(spec.id, MovementAction::Release, now)
        .unwrap();
    journal.state.lock().unwrap().head = head;
    Fixture {
        node,
        journal,
        action,
        authority,
        handle,
        lease,
        lease_shutdown,
        tasks,
        _root: root,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_duplicate_release_has_one_acceptance_and_exact_durable_result() {
    let fixture = fixture().await;
    let before = fixture
        .authority
        .load(fixture.handle.cell_id())
        .await
        .unwrap()
        .unwrap();
    let (a, b) = tokio::join!(
        fixture
            .node
            .apply_fleet_action(fixture.action.clone(), clock()),
        fixture
            .node
            .apply_fleet_action(fixture.action.clone(), clock())
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a.committed && b.committed);
    assert_eq!(a.outcome, b.outcome);
    let FleetOutcome::Released(position) = &a.outcome.outcome else {
        panic!("not released")
    };
    assert_eq!(Some(&position.root), before.value().root.as_ref());
    assert_eq!(fixture.journal.accepts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    assert_eq!(
        fixture
            .journal
            .state
            .lock()
            .unwrap()
            .records
            .get(&fixture.action.key().unwrap())
            .unwrap()
            .1,
        Some(a.outcome.clone())
    );
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_waiter_keeps_release_and_shutdown_joins_result_publication() {
    let fixture = fixture().await;
    fixture
        .journal
        .block_publication
        .store(true, Ordering::SeqCst);
    let node = Arc::clone(&fixture.node);
    let action = fixture.action.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(action, clock()).await });
    tokio::time::timeout(Duration::from_secs(5), fixture.journal.entered.notified())
        .await
        .unwrap();
    assert_eq!(fixture.node.stats().active_cells(), 0);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let node = Arc::clone(&fixture.node);
    let shutdown = tokio::spawn(async move { node.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    fixture.journal.resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert!(matches!(
        fixture
            .journal
            .state
            .lock()
            .unwrap()
            .records
            .get(&fixture.action.key().unwrap())
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .outcome,
        FleetOutcome::Released(_)
    ));
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_publication_reply_retries_original_proof_without_releasing_again() {
    let fixture = fixture().await;
    fixture
        .journal
        .lose_publication_reply
        .store(true, Ordering::SeqCst);
    let first = fixture
        .node
        .apply_fleet_action(fixture.action.clone(), clock())
        .await
        .unwrap();
    assert!(!first.committed && first.journal_error.is_some());
    assert!(matches!(first.outcome.outcome, FleetOutcome::Released(_)));
    // Advance authority before retrying the lost source result. Its original
    // root must survive; a fresh authority read would now name another owner.
    let receiver_session = SessionId::from_bytes([204; 16]);
    let receiver = CellNodeBuilder::new(application())
        .with_runtime(
            SqlWorkerPool::new(1, 8)
                .unwrap()
                .with_native_memory_limit(128 << 20)
                .unwrap(),
            64 << 20,
        )
        .with_replica_host(ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
        .with_session(receiver_session)
        .build()
        .unwrap();
    receiver
        .install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    receiver
        .install_node_lease(NodeLeaseGuard::new(clock(), clock() + 60_000).unwrap())
        .unwrap();
    let replica = CellReplica::new(
        fixture.authority.layout().clone(),
        *fixture.handle.cell_id().as_bytes(),
        *fixture.handle.incarnation().as_bytes(),
        ReplicaLimits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..ReplicaLimits::default()
        },
    )
    .unwrap();
    let idle = fixture
        .authority
        .load(fixture.handle.cell_id())
        .await
        .unwrap()
        .unwrap();
    let activated = receiver
        .runtime()
        .acquire_idle_restored(
            fixture.handle.catalog().clone(),
            replica,
            fixture.authority.clone(),
            idle,
            fixture._root.path().join("receiver.sqlite"),
            Owner {
                session: receiver_session,
                endpoint: "https://fleet-receiver.internal:8789".into(),
            },
        )
        .await
        .unwrap();
    let now = clock();
    activated
        .execute(
            cellule_runtime::cell::executor::MutationIdentity {
                request_id: cellule_runtime::identity::RequestId::from_bytes([207; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([207; 32]),
            now,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 99", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    vec![99],
                ))
            },
        )
        .await
        .unwrap();
    let newer = fixture
        .authority
        .load(fixture.handle.cell_id())
        .await
        .unwrap()
        .unwrap();
    let FleetOutcome::Released(original) = &first.outcome.outcome else {
        panic!()
    };
    assert!(newer.value().epoch > original.epoch);
    assert!(newer.value().root.as_ref().unwrap().commit_sequence > original.root.commit_sequence);
    let final_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = fixture
                .node
                .apply_fleet_action(fixture.action.clone(), clock())
                .await
                .unwrap();
            if result.committed {
                return result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(first.outcome, final_result.outcome);
    assert_eq!(fixture.journal.accepts.load(Ordering::SeqCst), 1);
    assert!(fixture.journal.publications.load(Ordering::SeqCst) >= 2);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    fixture.node.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_drain_retains_publication_and_can_join_the_same_task_later() {
    let fixture = fixture().await;
    fixture
        .journal
        .block_publication
        .store(true, Ordering::SeqCst);
    let node = Arc::clone(&fixture.node);
    let action = fixture.action.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(action, clock()).await });
    tokio::time::timeout(Duration::from_secs(5), fixture.journal.entered.notified())
        .await
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        fixture
            .node
            .drain_until(Some(Instant::now() + Duration::from_millis(50)))
            .await
            .is_err()
    );
    assert_ne!(fixture.node.state(), NodeState::Stopped);
    assert!(
        fixture
            .journal
            .state
            .lock()
            .unwrap()
            .records
            .get(&fixture.action.key().unwrap())
            .unwrap()
            .1
            .is_none()
    );
    assert!(fixture.node.stats().retained_bytes() >= 2 * MAX_RECORD_BYTES as usize);
    fixture.journal.resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), fixture.node.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert!(matches!(
        fixture
            .journal
            .state
            .lock()
            .unwrap()
            .records
            .get(&fixture.action.key().unwrap())
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .outcome,
        FleetOutcome::Released(_)
    ));
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_deadline_cannot_cancel_runtime_drain_of_an_accepted_query() {
    let fixture = fixture().await;
    let tasks = Arc::clone(&fixture.tasks);
    let withdrawal = fixture.lease_shutdown.clone();
    let runtime = fixture.node.runtime();
    tasks
        .spawn_lease_maintenance(async move {
            withdrawal.cancelled().await;
            assert_eq!(runtime.stats().active_cells(), 0);
            Ok::<(), Error>(())
        })
        .unwrap();
    let (started, observed) = std::sync::mpsc::channel();
    let (release, resumed) = std::sync::mpsc::channel();
    let handle = fixture.handle.clone();
    let query = tokio::spawn(async move {
        handle
            .query(64, 64, move |connection| {
                started.send(()).unwrap();
                resumed.recv().unwrap();
                let value = connection
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
    });
    tokio::task::spawn_blocking(move || observed.recv().unwrap())
        .await
        .unwrap();
    assert!(
        fixture
            .node
            .drain_until(Some(Instant::now() + Duration::from_millis(50)))
            .await
            .is_err()
    );
    assert_ne!(fixture.node.state(), NodeState::Stopped);
    assert!(!query.is_finished());
    assert!(!fixture.lease_shutdown.is_cancelled());
    release.send(()).unwrap();
    assert_eq!(query.await.unwrap().unwrap(), 42_i64.to_be_bytes());
    tokio::time::timeout(Duration::from_secs(5), fixture.node.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
    assert!(fixture.lease_shutdown.is_cancelled());
    let final_control = fixture
        .authority
        .load(fixture.handle.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_control.value().state,
        cellule_runtime::control::ControlState::Idle
    );
    assert!(final_control.value().owner.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_acceptance_without_result_never_repeats_unobserved_release() {
    let fixture = fixture().await;
    let _ = fixture
        .journal
        .accept_action(
            &fixture.action,
            NodeId::from_bytes([201; 16]),
            SessionId::from_bytes([201; 16]),
            clock(),
        )
        .await
        .unwrap();
    let before = fixture
        .authority
        .load(fixture.handle.cell_id())
        .await
        .unwrap()
        .unwrap();
    let result = fixture
        .node
        .apply_fleet_action(fixture.action.clone(), clock())
        .await
        .unwrap();
    assert!(result.committed && result.execution_error.is_some());
    assert!(matches!(result.outcome.outcome, FleetOutcome::Unknown));
    assert_eq!(fixture.node.stats().active_cells(), 1);
    assert_eq!(
        fixture
            .authority
            .load(fixture.handle.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        before.value()
    );
    fixture.node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_authorization_fails_before_acceptance_or_source_effect() {
    let fixture = fixture().await;
    {
        let mut state = fixture.journal.state.lock().unwrap();
        state.head = state
            .head
            .claim(
                FleetProfile::default(),
                state.head.revision(),
                state.head.controller().unwrap().claimant,
                clock(),
            )
            .unwrap();
    }
    assert!(
        fixture
            .node
            .apply_fleet_action(fixture.action.clone(), clock())
            .await
            .is_err()
    );
    assert_eq!(fixture.journal.accepts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.node.stats().active_cells(), 1);
    assert!(fixture.journal.state.lock().unwrap().records.is_empty());
    fixture.node.shutdown().await.unwrap();
}
