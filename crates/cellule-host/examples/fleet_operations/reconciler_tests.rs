//! Public driver sequencing with a real durable journal and synthetic effects.
//! These are adapter/driver tests, not leased-node or restoration qualification.

use super::journal::SqliteJournal;
use cellule_host::fleet::*;
use cellule_runtime::cell::{actor::OwnedCellObservation, catalog::CatalogRole};
use cellule_runtime::control::RootRef;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::*;
use cellule_runtime::node::*;
use ed25519_dalek::SigningKey;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::time::{Duration, Instant};

const NOW: i64 = 1_000_000;
fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([200; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    }
}
fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}
fn session(n: u8) -> SessionId {
    SessionId::from_bytes([n + 10; 16])
}
fn target(n: u8) -> CellTarget {
    CellTarget::new(
        TenantId::from_bytes([4; 16]),
        scope().application,
        NamespaceId::from_bytes([5; 16]),
        &[n],
    )
    .unwrap()
}
fn position(epoch: u64) -> PublishedPosition {
    PublishedPosition {
        incarnation: IncarnationId::from_bytes([6; 16]),
        epoch,
        root: RootRef {
            digest: Digest::from_bytes([8; 32]),
            txid: 9,
            checksum: u64::MAX,
            commit_sequence: 9,
        },
    }
}

struct Observer {
    complete: bool,
    omit_last_cell: AtomicBool,
    capture_mutation: std::sync::Mutex<Option<Arc<SqliteJournal>>>,
    calls: AtomicUsize,
    pressured: bool,
    draining: AtomicBool,
    cell_state: std::sync::Mutex<CellState>,
}

#[derive(Clone, Copy)]
enum CellState {
    Settled,
    Busy,
    Blob,
    RoleBlocked,
    NoCost,
    NoPosition,
}
impl FleetObserver for Observer {
    fn observe<'a>(
        &'a self,
        roster: &'a FleetRoster,
        now_ms: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            let expected = roster.snapshot();
            let mutation = self.capture_mutation.lock().unwrap().take();
            if let Some(journal) = mutation {
                journal
                    .accept_enrollment(
                        &EnrollmentSpec {
                            scope: scope(),
                            request: Digest::from_bytes([229; 32]),
                            role: EnrollmentRole::Follower { log_epoch: 99 },
                            source: Some(EnrollmentEndpoint {
                                node: node(3),
                                session: session(3),
                                intent_revision: 1,
                            }),
                            target: EnrollmentEndpoint {
                                node: node(2),
                                session: session(2),
                                intent_revision: 1,
                            },
                        },
                        now_ms,
                    )
                    .await?;
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            let cell_state = *self.cell_state.lock().unwrap();
            let mut nodes = Vec::new();
            for n in 1..=3 {
                let key = SigningKey::from_bytes(&[n; 32]);
                let ad = NodeAdvertisement::sign(
                    node(n),
                    session(n),
                    format!("https://node-{n}.internal:8789"),
                    scope().fleet,
                    Digest::from_bytes([30; 32]),
                    Digest::from_bytes([31; 32]),
                    Digest::from_bytes([32; 32]),
                    &key,
                    1,
                    now_ms,
                    now_ms + 30_000,
                    vec![Digest::from_bytes([9; 32])],
                    vec![1],
                    NodeFailureDomain::default(),
                    NodeCapacity {
                        free_memory_bytes: 1 << 30,
                        free_disk_bytes: 1 << 30,
                        job_credits: 16,
                        log_protocol: 1,
                        ..NodeCapacity::default()
                    },
                )?
                .with_operational_placement(
                    NodePlacementCapacity {
                        memory_capacity_bytes: 2 << 30,
                        disk_capacity_bytes: 2 << 30,
                        active_cells: if n == 1 { 6 } else { 0 },
                        max_active_cells: 16,
                        running_jobs: 0,
                        job_capacity: 16,
                        ..NodePlacementCapacity::default()
                    },
                    NodeOperationalSample {
                        mode: if n == 1 && self.draining.load(Ordering::SeqCst) {
                            NodeMode::Draining
                        } else {
                            NodeMode::Active
                        },
                        pressure: if n == 1 && self.pressured {
                            NodePressure::Shedding
                        } else {
                            NodePressure::Normal
                        },
                        sequence: now_ms as u64,
                        observed_at_ms: now_ms,
                    },
                    &key,
                )?;
                nodes.push(ad);
            }
            let mut cells = (1..=6)
                .map(|n| {
                    let mut owned = FleetOwnedCell {
                        node: node(1),
                        session: session(1),
                        observation: OwnedCellObservation {
                            target: target(n),
                            generation: u64::from(n),
                            incarnation: position(1).incarnation,
                            code: Digest::from_bytes([9; 32]),
                            schema: 1,
                            role: CatalogRole::Sql,
                            resident_since_ms: now_ms - 120_000,
                            last_used_ms: now_ms - 120_000,
                            position: Some(position(1)),
                            cost: Some(TransferCost {
                                memory_bytes: 65536,
                                disk_bytes: 4096,
                                file_descriptors: 8,
                                job_credits: 1,
                            }),
                            maintenance_cost: Some(TransferCost {
                                memory_bytes: 65536,
                                disk_bytes: 8192,
                                file_descriptors: 8,
                                job_credits: 1,
                            }),
                            database_bytes: Some(4096),
                            sampled_at_ms: Some(now_ms),
                            stable_observations: 2,
                            work_blocker: None,
                            quiescing: false,
                            maintenance_work: None,
                            blockers: Vec::new(),
                        },
                    };
                    if !matches!(cell_state, CellState::Settled) {
                        let row = &mut owned.observation;
                        row.cost = None;
                        row.sampled_at_ms = None;
                        row.database_bytes = None;
                        row.stable_observations = 0;
                        row.blockers =
                            vec![DrainBlocker::BusyExecution, DrainBlocker::UnknownInventory];
                        row.work_blocker = Some(
                            cellule_runtime::primitives::maintenance::TransferWorkClass::Queue,
                        );
                        match cell_state {
                            CellState::Blob => row.role = CatalogRole::Blob,
                            CellState::RoleBlocked => {
                                row.blockers.push(DrainBlocker::FollowerObligation)
                            }
                            CellState::NoCost => row.maintenance_cost = None,
                            CellState::NoPosition => row.position = None,
                            CellState::Settled | CellState::Busy => {}
                        }
                    }
                    owned
                })
                .collect::<Vec<_>>();
            if self.omit_last_cell.load(Ordering::SeqCst) {
                cells.pop();
            }
            Ok(FleetObservation::new(
                scope(),
                expected.registry(),
                1,
                now_ms,
                now_ms,
                self.complete,
                nodes,
                cells,
            )?)
        })
    }
}

struct Transport {
    journal: Arc<SqliteJournal>,
    lose_release: AtomicBool,
    lose_cordon: AtomicBool,
    unknown_cordon: AtomicBool,
    unconfirmed_cordon: AtomicBool,
    block_after_acceptance: AtomicBool,
    block_before_acceptance: AtomicBool,
    blocked_before_acceptance: AtomicUsize,
    block_first_acceptance: AtomicBool,
    reject_prepare: bool,
    inspected: AtomicUsize,
    dispatched: AtomicUsize,
    released: AtomicUsize,
    maintenance_released: AtomicUsize,
    cordoned: AtomicUsize,
}
impl FleetTransport for Transport {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            if matches!(action.kind(), FleetActionKind::Maintenance { .. }) {
                return self.dispatch_cordon(action).await;
            }
            let FleetActionKind::Movement {
                action: effect,
                attempt,
            } = action.kind()
            else {
                panic!();
            };
            let spec = attempt.spec();
            let (origin, boot) = if effect.is_source_release() {
                (spec.source_node, spec.source)
            } else {
                (spec.destination_node, spec.destination)
            };
            // The real journal rejects dispatch before its phase CAS. Repeated
            // dispatch returns the same committed synthetic effect record.
            if self.block_before_acceptance.load(Ordering::SeqCst) {
                self.blocked_before_acceptance
                    .fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            let accepted = match self
                .journal
                .accept_action(action, origin, boot, action.issued_at_ms())
                .await?
            {
                FleetActionAcceptance::New(accepted) => accepted,
                FleetActionAcceptance::Existing {
                    accepted,
                    result: Some(outcome),
                } => {
                    return Ok(Arc::new(FleetActionCompletion {
                        accepted,
                        outcome: *outcome,
                        committed: true,
                        execution_error: None,
                        journal_error: None,
                    }));
                }
                FleetActionAcceptance::Existing { .. } => {
                    panic!("synthetic action must have retained outcome")
                }
            };
            self.dispatched.fetch_add(1, Ordering::SeqCst);
            if self.block_after_acceptance.load(Ordering::SeqCst)
                || self.block_first_acceptance.swap(false, Ordering::SeqCst)
            {
                // Model an accepted endpoint whose effect has no confirmed
                // result. Dropping this transport leaves its durable acceptance.
                std::future::pending::<()>().await;
            }
            let outcome = match effect {
                MovementAction::Prepare if self.reject_prepare => {
                    FleetOutcome::Rejected(DrainBlocker::ReceiverCapacity)
                }
                MovementAction::Prepare => FleetOutcome::Reserved(ReceiverReservation {
                    session: boot,
                    expires_at_ms: spec.deadline_ms,
                }),
                MovementAction::Release | MovementAction::ReleaseMaintenance => {
                    if *effect == MovementAction::ReleaseMaintenance {
                        self.maintenance_released.fetch_add(1, Ordering::SeqCst);
                    }
                    self.released.fetch_add(1, Ordering::SeqCst);
                    FleetOutcome::Released(position(spec.source_epoch))
                }
                MovementAction::Activate => {
                    let mut idle = cellule_runtime::control::Control::initial(
                        spec.target.cell_id(),
                        spec.incarnation,
                        cellule_runtime::control::Owner {
                            session: spec.source,
                            endpoint: "https://source.internal:8789".into(),
                        },
                        Digest::from_bytes([9; 32]),
                        1,
                    )?;
                    idle.state = cellule_runtime::control::ControlState::Idle;
                    idle.owner = None;
                    idle.epoch = spec.source_epoch;
                    idle.root = Some(position(spec.source_epoch).root);
                    idle.revision = 9;
                    idle.progress = 9;
                    let basis =
                        AcquisitionBasis::new(accepted.clone(), idle, action.issued_at_ms())?;
                    self.journal.record_acquisition_basis(&basis).await?;
                    FleetOutcome::Activated(ActivationEvidence {
                        node: origin,
                        session: boot,
                        position: position(spec.source_epoch + 1),
                    })
                }
                MovementAction::Cancel => FleetOutcome::ReceiverCleaned,
                _ => panic!("unsupported synthetic effect"),
            };
            let outcome = FleetActionOutcome {
                scope: scope(),
                action_key: action.key()?,
                node: origin,
                session: boot,
                observed_at_ms: action.issued_at_ms(),
                outcome,
            };
            self.journal
                .publish_action_result(&accepted, &outcome)
                .await?;
            if effect.is_source_release() && self.lose_release.swap(false, Ordering::SeqCst) {
                return Err(std::io::Error::other(
                    "injected lost release response after durable result",
                )
                .into());
            }
            Ok(Arc::new(FleetActionCompletion {
                accepted,
                outcome,
                committed: true,
                execution_error: None,
                journal_error: None,
            }))
        })
    }
    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        Box::pin(async move {
            self.journal
                .authorize_inspection(request, request.action().issued_at_ms())
                .await?;
            self.inspected.fetch_add(1, Ordering::SeqCst);
            let FleetActionKind::Movement { attempt, .. } = request.action().kind() else {
                panic!();
            };
            let effect = match attempt.phase() {
                AttemptPhase::Preparing => MovementAction::Prepare,
                AttemptPhase::Releasing => MovementAction::Release,
                AttemptPhase::MaintenanceReleasing => MovementAction::ReleaseMaintenance,
                AttemptPhase::Activating | AttemptPhase::Activated => MovementAction::Activate,
                AttemptPhase::Cancelling | AttemptPhase::CleaningReceiver => MovementAction::Cancel,
                _ => panic!("unsupported synthetic inspection"),
            };
            let outcome = match self
                .journal
                .load_movement_action(
                    scope(),
                    attempt.spec().id,
                    effect,
                    request.node(),
                    request.session(),
                )
                .await?
            {
                Some(FleetActionAcceptance::Existing {
                    result: Some(result),
                    ..
                }) => result.outcome.clone(),
                None => FleetOutcome::Unknown,
                _ => panic!("synthetic action result absent"),
            };
            let at = request.action().issued_at_ms();
            Ok(Arc::new(FleetInspectionObservation::new(
                request.clone(),
                at,
                FleetActionOutcome {
                    scope: scope(),
                    action_key: request.action().key()?,
                    node: request.node(),
                    session: request.session(),
                    observed_at_ms: at,
                    outcome,
                },
            )?))
        })
    }
}

impl Transport {
    async fn dispatch_cordon(
        &self,
        action: &FleetAction,
    ) -> std::result::Result<Arc<FleetActionCompletion>, Box<dyn std::error::Error + Send + Sync>>
    {
        let FleetActionKind::Maintenance {
            action: MaintenanceAction::Cordon,
            operation,
        } = action.kind()
        else {
            panic!("synthetic transport only implements cordon");
        };
        let accepted = match self
            .journal
            .accept_action(
                action,
                operation.node(),
                operation.session(),
                action.issued_at_ms(),
            )
            .await?
        {
            FleetActionAcceptance::New(accepted) => {
                self.dispatched.fetch_add(1, Ordering::SeqCst);
                accepted
            }
            FleetActionAcceptance::Existing {
                accepted,
                result: Some(outcome),
            } if !matches!(outcome.outcome, FleetOutcome::Unknown) => {
                return Ok(Arc::new(FleetActionCompletion {
                    accepted,
                    outcome: *outcome,
                    committed: true,
                    execution_error: None,
                    journal_error: None,
                }));
            }
            FleetActionAcceptance::Existing { accepted, .. } => accepted,
        };
        if self.block_after_acceptance.load(Ordering::SeqCst)
            || self.block_first_acceptance.swap(false, Ordering::SeqCst)
        {
            std::future::pending::<()>().await;
        }
        let unknown = self.unknown_cordon.swap(false, Ordering::SeqCst);
        if !unknown {
            self.cordoned.fetch_add(1, Ordering::SeqCst);
        }
        let outcome = FleetActionOutcome {
            scope: scope(),
            action_key: action.key()?,
            node: operation.node(),
            session: operation.session(),
            observed_at_ms: action.issued_at_ms(),
            outcome: if unknown {
                FleetOutcome::Unknown
            } else {
                FleetOutcome::Cordoned
            },
        };
        self.journal
            .publish_action_result(&accepted, &outcome)
            .await?;
        if self.lose_cordon.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "injected lost cordon response after durable result",
            )
            .into());
        }
        let unconfirmed = self.unconfirmed_cordon.swap(false, Ordering::SeqCst);
        Ok(Arc::new(FleetActionCompletion {
            accepted,
            outcome,
            committed: !unconfirmed,
            execution_error: unknown.then(|| {
                Arc::new(cellule_runtime::Error::Control(
                    "injected unresolved cordon execution",
                ))
            }),
            journal_error: unconfirmed.then(|| {
                Arc::new(cellule_runtime::Error::Facility {
                    name: "injected-cordon-publication",
                    source: Box::new(std::io::Error::other("injected unconfirmed result")),
                })
            }),
        }))
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    observer: Arc<Observer>,
    transport: Arc<Transport>,
}
impl Fixture {
    async fn new(complete: bool, pressured: bool, reject_prepare: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("fleet.sqlite"),
                scope(),
                FleetProfile::default(),
                0,
            )
            .await
            .unwrap(),
        );
        for n in 1..=3 {
            journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node(n), session(n)).unwrap(),
                )
                .await
                .unwrap();
            // Synthetic driver evidence: these tests exercise orchestration,
            // not the native boot authority qualified by scenario/startup.
            let boot = EnrollmentSpec {
                scope: scope(),
                request: Digest::from_bytes([n + 128; 32]),
                role: EnrollmentRole::Node {
                    mode: NodeMode::Active,
                },
                source: None,
                target: EnrollmentEndpoint {
                    node: node(n),
                    session: session(n),
                    intent_revision: 1,
                },
            };
            let FleetEnrollmentAcceptance::New(record) =
                journal.accept_enrollment(&boot, 0).await.unwrap()
            else {
                panic!("boot already accepted")
            };
            journal
                .publish_enrollment_result(
                    &record,
                    EnrollmentEvent::Established(Digest::from_bytes([n + 160; 32])),
                    0,
                )
                .await
                .unwrap();
        }
        let version = journal.load_snapshot(scope()).await.unwrap().registry();
        let version = journal.bootstrap_registry(version).await.unwrap();
        journal.set_scheduling(version, true).await.unwrap();
        let observer = Arc::new(Observer {
            complete,
            omit_last_cell: AtomicBool::new(false),
            capture_mutation: std::sync::Mutex::new(None),
            calls: AtomicUsize::new(0),
            pressured,
            draining: AtomicBool::new(false),
            cell_state: std::sync::Mutex::new(CellState::Settled),
        });
        let transport = Arc::new(Transport {
            journal: journal.clone(),
            lose_release: AtomicBool::new(false),
            lose_cordon: AtomicBool::new(false),
            unknown_cordon: AtomicBool::new(false),
            unconfirmed_cordon: AtomicBool::new(false),
            block_after_acceptance: AtomicBool::new(false),
            block_before_acceptance: AtomicBool::new(false),
            blocked_before_acceptance: AtomicUsize::new(0),
            block_first_acceptance: AtomicBool::new(false),
            reject_prepare,
            inspected: AtomicUsize::new(0),
            dispatched: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            maintenance_released: AtomicUsize::new(0),
            cordoned: AtomicUsize::new(0),
        });
        Self {
            _root: root,
            journal,
            observer,
            transport,
        }
    }
    fn driver(&self, claimant: u8) -> FleetReconciler {
        FleetReconciler::new(
            scope(),
            SessionId::from_bytes([claimant; 16]),
            FleetProfile::default(),
            self.journal.clone(),
            self.observer.clone(),
            self.transport.clone(),
        )
        .unwrap()
    }
    async fn step(&self, driver: &FleetReconciler, index: i64) -> FleetReconcileReport {
        driver
            .reconcile_once(
                || Ok(NOW + index * 100),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap()
    }
    async fn stop(&self) {
        let version = self
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .registry();
        self.journal.set_scheduling(version, false).await.unwrap();
    }

    async fn request_maintenance(&self, deadline_ms: i64) -> MaintenanceOperation {
        let snapshot = self.journal.load_snapshot(scope()).await.unwrap();
        let snapshot = self
            .journal
            .claim_controller(
                scope(),
                snapshot.head().revision(),
                SessionId::from_bytes([206; 16]),
                NOW,
            )
            .await
            .unwrap();
        let operation = MaintenanceOperation::new(
            OperationId::from_bytes([207; 16]).unwrap(),
            Digest::from_bytes([208; 32]),
            node(1),
            session(1),
            2,
            NOW,
            deadline_ms,
        )
        .unwrap();
        self.journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                NOW,
                &JournalTransition::BeginMaintenance(operation.clone()),
            )
            .await
            .unwrap();
        operation
    }
}

#[tokio::test]
async fn maintenance_cordon_precedes_partial_normal_pressure_evacuation() {
    let fixture = Fixture::new(false, false, false).await;
    let operation = fixture.request_maintenance(NOW + 20_000).await;
    let driver = fixture.driver(206);
    let cordoned = fixture.step(&driver, 1).await;
    assert_eq!(
        cordoned.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Cordoned
    );
    assert_eq!(cordoned.allocated, 0);
    assert_eq!(cordoned.dispatched, 1);
    assert!(cordoned.maintenance_failure.is_none());
    let evacuation = fixture.step(&driver, 2).await;
    assert_eq!(
        evacuation.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    assert_eq!(evacuation.allocated, 2);
    for attempt in evacuation.snapshot.head().attempts() {
        assert_eq!(attempt.spec().id.operation, operation.id());
        assert_eq!(attempt.spec().source_node, node(1));
        assert_ne!(attempt.spec().destination_node, node(1));
        assert_eq!(attempt.spec().deadline_ms, operation.deadline_ms());
    }
    fixture.stop().await;
    for index in 3..=6 {
        fixture.step(&driver, index).await;
    }
    let retained = fixture.step(&driver, 7).await;
    assert!(retained.snapshot.head().attempts().is_empty());
    assert_eq!(
        retained.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    assert!(
        retained
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    assert_eq!(fixture.transport.cordoned.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .transport
            .maintenance_released
            .load(Ordering::SeqCst),
        2
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn stopped_optional_scheduling_still_cordons_and_retains_maintenance() {
    let fixture = Fixture::new(false, false, false).await;
    fixture.request_maintenance(NOW + 20_000).await;
    fixture.stop().await;
    let driver = fixture.driver(206);
    let cordoned = fixture.step(&driver, 1).await;
    assert_eq!(
        cordoned.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Cordoned
    );
    let evacuation = fixture.step(&driver, 2).await;
    assert_eq!(
        evacuation.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    assert_eq!(evacuation.allocated, 0);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn busy_maintenance_plans_peak_cost_and_adopts_lost_explicit_release() {
    let fixture = Fixture::new(false, false, false).await;
    *fixture.observer.cell_state.lock().unwrap() = CellState::Busy;
    fixture.request_maintenance(NOW + 20_000).await;
    let driver = fixture.driver(206);
    let cordoned = fixture.step(&driver, 1).await;
    assert_eq!(cordoned.allocated, 0);
    let planned = fixture.step(&driver, 2).await;
    assert_eq!(planned.allocated, 2);
    assert_eq!(planned.snapshot.head().reserved_restore_bytes(), 16_384);
    let specs = planned
        .snapshot
        .head()
        .attempts()
        .iter()
        .map(|attempt| attempt.spec().clone())
        .collect::<Vec<_>>();
    assert!(specs.iter().all(|spec| spec.cost.disk_bytes == 8192));
    fixture.stop().await;
    fixture.step(&driver, 3).await;
    fixture.transport.lose_release.store(true, Ordering::SeqCst);
    let unknown = fixture.step(&driver, 4).await;
    assert_eq!(unknown.released, 1);
    assert_eq!(unknown.failures.len(), 1);
    assert!(unknown.snapshot.head().attempts().iter().any(|attempt| {
        attempt.phase() == AttemptPhase::MaintenanceReleasing
            && attempt.released().is_none()
            && unknown
                .snapshot
                .head()
                .retirement_page(&[attempt.spec().id])
                .is_err()
    }));
    assert_eq!(unknown.snapshot.head().reserved_restore_bytes(), 16_384);
    let reconstructed = fixture.driver(206);
    for index in 5..=8 {
        fixture.step(&reconstructed, index).await;
    }
    assert_eq!(
        fixture
            .transport
            .maintenance_released
            .load(Ordering::SeqCst),
        2
    );
    assert_eq!(fixture.transport.released.load(Ordering::SeqCst), 2);
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert!(snapshot.head().attempts().is_empty());
    assert_eq!(snapshot.head().reserved_restore_bytes(), 0);
    assert_eq!(
        snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    for spec in specs {
        let record = fixture
            .journal
            .load_movement_action(
                scope(),
                spec.id,
                MovementAction::ReleaseMaintenance,
                spec.source_node,
                spec.source,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(record, FleetActionAcceptance::Existing { result: Some(ref result), .. }
            if matches!(result.outcome, FleetOutcome::Released(_)))
        );
        assert!(
            fixture
                .journal
                .load_movement_action(
                    scope(),
                    spec.id,
                    MovementAction::Release,
                    spec.source_node,
                    spec.source
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn draining_advertisement_or_pressure_alone_cannot_plan_busy_work() {
    for draining in [false, true] {
        let fixture = Fixture::new(true, true, false).await;
        *fixture.observer.cell_state.lock().unwrap() = CellState::Busy;
        fixture.observer.draining.store(draining, Ordering::SeqCst);
        let report = fixture.step(&fixture.driver(206), 1).await;
        assert_eq!(report.allocated, 0);
        assert!(report.snapshot.head().attempts().is_empty());
        assert_eq!(fixture.transport.released.load(Ordering::SeqCst), 0);
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn maintenance_planning_keeps_blob_role_and_missing_evidence_blocked() {
    for state in [
        CellState::Blob,
        CellState::RoleBlocked,
        CellState::NoCost,
        CellState::NoPosition,
    ] {
        let fixture = Fixture::new(false, false, false).await;
        *fixture.observer.cell_state.lock().unwrap() = state;
        fixture.request_maintenance(NOW + 20_000).await;
        let driver = fixture.driver(206);
        fixture.step(&driver, 1).await;
        let report = fixture.step(&driver, 2).await;
        assert_eq!(report.allocated, 0);
        assert!(report.snapshot.head().attempts().is_empty());
        assert_eq!(fixture.transport.released.load(Ordering::SeqCst), 0);
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn replacement_controller_adopts_lost_cordon_reply_without_repeating_effect() {
    let fixture = Fixture::new(false, false, false).await;
    let operation = fixture.request_maintenance(NOW + 60_000).await;
    fixture.stop().await;
    fixture.transport.lose_cordon.store(true, Ordering::SeqCst);
    let lost = fixture.step(&fixture.driver(206), 1).await;
    assert!(lost.maintenance_failure.is_some());
    assert!(lost.failures.is_empty());
    assert_eq!(
        lost.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Requested
    );
    assert_eq!(
        lost.next_wake_at_ms,
        NOW + 100 + FleetProfile::default().reconcile_interval_ms
    );
    let replaced = fixture.step(&fixture.driver(209), 310).await;
    assert_eq!(replaced.snapshot.head().controller().unwrap().epoch, 2);
    assert_eq!(
        replaced.snapshot.head().maintenance().unwrap().id(),
        operation.id()
    );
    assert_eq!(
        replaced.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Cordoned
    );
    assert!(replaced.maintenance_failure.is_none());
    assert_eq!(fixture.transport.cordoned.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 1);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn timed_out_cordon_retains_acceptance_and_retries_without_false_evacuation() {
    let fixture = Fixture::new(false, false, false).await;
    fixture.request_maintenance(NOW + 20_000).await;
    fixture.stop().await;
    fixture
        .transport
        .block_first_acceptance
        .store(true, Ordering::SeqCst);
    let report = fixture
        .driver(206)
        .reconcile_once(
            || Ok(NOW + 100),
            Instant::now() + Duration::from_millis(100),
        )
        .await
        .unwrap();
    assert!(report.maintenance_failure.is_some());
    let cellule_runtime::Error::Facility { name, source } =
        report.maintenance_failure.as_ref().unwrap().as_ref()
    else {
        panic!("cordon timeout source lost");
    };
    assert_eq!(*name, "fleet-controller-deadline");
    assert!(source.is::<tokio::time::error::Elapsed>());
    assert_eq!(report.dispatched, 1);
    assert_eq!(
        report.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Requested
    );
    assert_eq!(fixture.transport.cordoned.load(Ordering::SeqCst), 0);
    let next = fixture.step(&fixture.driver(206), 2).await;
    assert_eq!(
        next.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Cordoned
    );
    assert_eq!(fixture.transport.cordoned.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 1);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn unconfirmed_or_unknown_cordon_retains_phase_and_original_error() {
    for unknown in [false, true] {
        let fixture = Fixture::new(false, false, false).await;
        fixture.request_maintenance(NOW + 20_000).await;
        fixture.stop().await;
        if unknown {
            fixture
                .transport
                .unknown_cordon
                .store(true, Ordering::SeqCst);
        } else {
            fixture
                .transport
                .unconfirmed_cordon
                .store(true, Ordering::SeqCst);
        }
        let driver = fixture.driver(206);
        let report = fixture.step(&driver, 1).await;
        assert_eq!(
            report.snapshot.head().maintenance().unwrap().phase(),
            MaintenancePhase::Requested
        );
        assert_eq!(report.allocated, 0);
        assert_eq!(
            report.next_wake_at_ms,
            NOW + 100 + FleetProfile::default().reconcile_interval_ms
        );
        let error = report.maintenance_failure.unwrap();
        if unknown {
            assert!(matches!(
                error.as_ref(),
                cellule_runtime::Error::Control("injected unresolved cordon execution")
            ));
            assert!(report.blockers.contains(&DrainBlocker::OutcomeUnknown));
        } else {
            let cellule_runtime::Error::Facility { name, source } = error.as_ref() else {
                panic!("publication source lost");
            };
            assert_eq!(*name, "injected-cordon-publication");
            assert!(source.is::<std::io::Error>());
            assert!(report.blockers.contains(&DrainBlocker::PendingPublication));
        }
        let replay = fixture.step(&fixture.driver(206), 2).await;
        assert_eq!(
            replay.snapshot.head().maintenance().unwrap().phase(),
            MaintenancePhase::Cordoned
        );
        assert!(replay.maintenance_failure.is_none());
        assert_eq!(fixture.transport.cordoned.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 1);
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn expired_maintenance_still_closes_admission_but_allocates_no_new_moves() {
    let fixture = Fixture::new(false, false, false).await;
    fixture.request_maintenance(NOW + 100).await;
    let driver = fixture.driver(206);
    let cordoned = fixture.step(&driver, 2).await;
    assert_eq!(
        cordoned.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Cordoned
    );
    assert!(cordoned.blockers.contains(&DrainBlocker::Deadline));
    let evacuation = fixture.step(&driver, 3).await;
    assert_eq!(evacuation.allocated, 0);
    assert!(evacuation.blockers.contains(&DrainBlocker::Deadline));
    assert_eq!(
        evacuation.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn public_reconciler_drives_durable_phases_fresh_activation_cleanup_and_retirement() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    let first = fixture.step(&driver, 0).await;
    assert_eq!(first.allocated, 2);
    assert_eq!(first.dispatched, 0);
    assert!(first.next_wake_at_ms < NOW + 100);
    assert_eq!(first.snapshot.head().reserved_restore_bytes(), 8192);
    fixture.stop().await;
    assert_eq!(fixture.step(&driver, 1).await.dispatched, 2); // prepare
    let released = fixture.step(&driver, 2).await;
    assert_eq!(released.dispatched, 2);
    assert_eq!(released.released, 2);
    assert_eq!(released.activated, 0);
    let activated = fixture.step(&driver, 3).await;
    assert_eq!(activated.inspected, 2);
    assert_eq!(activated.activated, 2);
    assert_eq!(activated.released, 0);
    assert!(
        activated
            .snapshot
            .head()
            .attempts()
            .iter()
            .all(|a| a.phase() == AttemptPhase::Activated)
    );
    assert_eq!(fixture.step(&driver, 4).await.dispatched, 2); // independent resource proof
    let retired = fixture.step(&fixture.driver(206), 5).await;
    assert_eq!(retired.retired, 2);
    assert_eq!(retired.inspected, 2);
    assert_eq!(retired.activated, 0);
    assert!(retired.snapshot.head().attempts().is_empty());
    let page = fixture
        .journal
        .load_progress(scope(), retired.snapshot.head().progress().unwrap().digest)
        .await
        .unwrap()
        .unwrap();
    let latest = fixture
        .journal
        .last_movement_at(&retired.snapshot)
        .await
        .unwrap()
        .unwrap();
    assert!((NOW + 300..NOW + 400).contains(&latest));
    for entry in page.entries() {
        assert_eq!(
            fixture
                .journal
                .last_moved_at(
                    &retired.snapshot,
                    entry.spec().target.cell_id(),
                    entry.spec().incarnation
                )
                .await
                .unwrap(),
            entry.completed_at_ms()
        );
    }
    assert_eq!(fixture.observer.calls.load(Ordering::SeqCst), 1);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn reconstructed_reconciler_consumes_lost_release_reply_without_repeating_effect() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    fixture.step(&driver, 1).await;
    fixture.transport.lose_release.store(true, Ordering::SeqCst);
    let report = driver
        .reconcile_once(|| Ok(NOW + 200), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0]
            .error
            .to_string()
            .contains("fleet-transport")
    );
    assert_eq!(report.released, 1); // Healthy sibling advances after the lost reply.
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(snapshot.head().attempts().len(), 2);
    assert_eq!(snapshot.head().reserved_restore_bytes(), 8192);
    let next = fixture.step(&fixture.driver(206), 3).await;
    assert_eq!(next.inspected, 2);
    assert_eq!(next.dispatched, 1);
    assert!(
        next.snapshot
            .head()
            .attempts()
            .iter()
            .all(|a| matches!(a.phase(), AttemptPhase::Released | AttemptPhase::Activated))
    );
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 5);
    assert_eq!(fixture.transport.released.load(Ordering::SeqCst), 2);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn partial_roster_blocks_optional_count_moves_but_allows_pressure_relief() {
    let partial = Fixture::new(false, false, false).await;
    let report = partial.step(&partial.driver(206), 0).await;
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    partial.journal.close().await.unwrap();
    let urgent = Fixture::new(false, true, false).await;
    assert_eq!(urgent.step(&urgent.driver(206), 0).await.allocated, 2);
    urgent.journal.close().await.unwrap();
}

#[tokio::test]
async fn definite_prepare_refusal_cancels_without_release_and_cancellation_is_not_movement() {
    let fixture = Fixture::new(true, false, true).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    let report = fixture.step(&driver, 1).await;
    assert!(
        report
            .snapshot
            .head()
            .attempts()
            .iter()
            .all(|a| a.phase() == AttemptPhase::Cancelled)
    );
    assert_eq!(report.cancelled, 2);
    assert!(report.blockers.contains(&DrainBlocker::ReceiverCapacity));
    assert!(report.next_wake_at_ms >= NOW + 100 + FleetProfile::default().reconcile_interval_ms);
    let retired = fixture.step(&driver, 2).await;
    assert_eq!(retired.retired, 2);
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .journal
            .last_movement_at(&retired.snapshot)
            .await
            .unwrap(),
        None
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn competing_controllers_are_fenced_and_stale_history_barriers_conflict() {
    let fixture = Fixture::new(true, false, false).await;
    let report = fixture.step(&fixture.driver(206), 0).await;
    assert!(
        fixture
            .driver(207)
            .reconcile_once(|| Ok(NOW + 100), Instant::now() + Duration::from_secs(3))
            .await
            .is_err()
    );
    fixture.stop().await;
    assert!(
        fixture
            .journal
            .last_movement_at(&report.snapshot)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .head()
            .attempts()
            .len(),
        2
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn racing_public_drivers_allocate_one_shared_batch() {
    let fixture = Fixture::new(true, false, false).await;
    let a = fixture.driver(206);
    let b = fixture.driver(207);
    let (a, b) = tokio::join!(
        a.reconcile_once(|| Ok(NOW), Instant::now() + Duration::from_secs(3)),
        b.reconcile_once(|| Ok(NOW), Instant::now() + Duration::from_secs(3)),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let retained = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(retained.head().attempts().len(), 2);
    assert_eq!(retained.head().reserved_restore_bytes(), 8192);
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 0);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn expired_never_dispatched_plan_cancels_and_retires_without_remote_effect() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    let cancelled = fixture.step(&fixture.driver(207), 310).await;
    assert!(
        cancelled
            .snapshot
            .head()
            .attempts()
            .iter()
            .all(|a| a.phase() == AttemptPhase::Cancelled)
    );
    assert_eq!(cancelled.snapshot.head().controller().unwrap().epoch, 2);
    assert_eq!(cancelled.dispatched, 0);
    let retired = fixture.step(&fixture.driver(207), 311).await;
    assert_eq!(retired.retired, 2);
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 0);
    fixture.journal.close().await.unwrap();
}

// Fault timing is relative to an observed protocol boundary. Freeze only this
// current-thread model clock after setup; native/process SLO clocks are unchanged.
async fn transport_deadline_at(
    driver: &FleetReconciler,
    reached: &AtomicUsize,
) -> FleetReconcileReport {
    tokio::time::pause();
    assert_eq!(reached.load(Ordering::SeqCst), 0);
    let deadline = Instant::now() + Duration::from_millis(150);
    let mut pass = Box::pin(driver.reconcile_once(|| Ok(NOW + 100), deadline));
    // The driver visits two endpoints sequentially, keeping one share for
    // planning. Freeze I/O time and expire each original partition only after
    // its selected fault boundary; one millisecond covers timer quantization.
    for (boundary, shares) in [(1, 3), (2, 2)] {
        let budget = deadline.saturating_duration_since(Instant::now()) / shares;
        let wall_deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            assert!(futures_util::poll!(pass.as_mut()).is_pending());
            if reached.load(Ordering::SeqCst) == boundary {
                break;
            }
            assert!(
                std::time::Instant::now() < wall_deadline,
                "transport fault boundary not reached"
            );
            // Keep the model runnable while SQLite's original worker completes;
            // the paused deadline cannot auto-advance ahead of the selected fault.
            tokio::task::yield_now().await;
        }
        tokio::time::advance(budget + Duration::from_millis(1)).await;
    }
    let report = pass.await.unwrap();
    tokio::time::resume();
    report
}

#[tokio::test]
async fn transport_deadline_preserves_accepted_phase_permits_and_original_timeout() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    fixture
        .transport
        .block_after_acceptance
        .store(true, Ordering::SeqCst);
    let report = transport_deadline_at(&driver, &fixture.transport.dispatched).await;
    assert_eq!(report.failures.len(), 2);
    for failure in &report.failures {
        let cellule_runtime::Error::Facility { name, source } = failure.error.as_ref() else {
            panic!("timeout source lost");
        };
        assert_eq!(*name, "fleet-controller-deadline");
        assert!(
            source
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
        );
    }
    let retained = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(retained.head().attempts().len(), 2);
    assert_eq!(retained.head().reserved_restore_bytes(), 8192);
    assert_eq!(
        retained.head().attempts()[0].phase(),
        AttemptPhase::Preparing
    );
    let attempt = &retained.head().attempts()[0];
    assert!(matches!(
        fixture
            .journal
            .load_movement_action(
                scope(),
                attempt.spec().id,
                MovementAction::Prepare,
                attempt.spec().destination_node,
                attempt.spec().destination
            )
            .await
            .unwrap(),
        Some(FleetActionAcceptance::Existing { result: None, .. })
    ));
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn transport_deadline_before_acceptance_retains_permits_without_fabricating_an_action() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    fixture
        .transport
        .block_before_acceptance
        .store(true, Ordering::SeqCst);
    let report = transport_deadline_at(&driver, &fixture.transport.blocked_before_acceptance).await;
    assert_eq!(report.failures.len(), 2);
    for failure in &report.failures {
        let cellule_runtime::Error::Facility { name, source } = failure.error.as_ref() else {
            panic!("timeout source lost");
        };
        assert_eq!(*name, "fleet-controller-deadline");
        assert!(source.is::<tokio::time::error::Elapsed>());
    }
    let retained = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(retained.head().attempts().len(), 2);
    assert_eq!(retained.head().reserved_restore_bytes(), 8192);
    for attempt in retained.head().attempts() {
        assert_eq!(attempt.phase(), AttemptPhase::Preparing);
        assert!(
            fixture
                .journal
                .load_movement_action(
                    scope(),
                    attempt.spec().id,
                    MovementAction::Prepare,
                    attempt.spec().destination_node,
                    attempt.spec().destination,
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.transport.released.load(Ordering::SeqCst), 0);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn unavailable_first_endpoint_leaves_deadline_for_healthy_sibling() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    fixture.step(&driver, 0).await;
    fixture.stop().await;
    fixture
        .transport
        .block_first_acceptance
        .store(true, Ordering::SeqCst);
    let report = driver
        .reconcile_once(
            || Ok(NOW + 100),
            Instant::now() + Duration::from_millis(300),
        )
        .await
        .unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.dispatched, 2);
    assert_eq!(report.snapshot.head().reserved_restore_bytes(), 8192);
    assert_eq!(
        report.snapshot.head().attempts()[0].phase(),
        AttemptPhase::Preparing
    );
    assert_eq!(
        report.snapshot.head().attempts()[1].phase(),
        AttemptPhase::Reserved
    );
    assert_eq!(
        report.failures[0].attempt,
        report.snapshot.head().attempts()[0].spec().id
    );
    assert!(report.blockers.contains(&DrainBlocker::OutcomeUnknown));
    assert_eq!(
        report.next_wake_at_ms,
        NOW + 100 + FleetProfile::default().reconcile_interval_ms
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn regressing_application_clock_fails_before_controller_claim() {
    let fixture = Fixture::new(true, false, false).await;
    let reads = AtomicUsize::new(0);
    let error = fixture
        .driver(206)
        .reconcile_once(
            || Ok(NOW - reads.fetch_add(1, Ordering::SeqCst) as i64),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        cellule_runtime::Error::Control("fleet controller clock regressed")
    ));
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert!(snapshot.head().controller().is_none());
    assert!(snapshot.head().attempts().is_empty());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn pressure_relief_uses_remaining_shared_permit_without_forgetting_existing_receive() {
    let fixture = Fixture::new(false, true, false).await;
    let driver = fixture.driver(206);
    let initial = fixture.step(&driver, 0).await;
    let id = initial.snapshot.head().attempts()[0].spec().id;
    let mut snapshot = initial.snapshot;
    for event in [AttemptEvent::BeginCancel, AttemptEvent::Cancelled] {
        snapshot = fixture
            .journal
            .compare_exchange(
                &snapshot,
                1,
                NOW + 50,
                &JournalTransition::Attempt { id, event },
            )
            .await
            .unwrap();
    }
    let progress = snapshot.head().retirement_page(&[id]).unwrap();
    fixture
        .journal
        .compare_exchange(
            &snapshot,
            1,
            NOW + 50,
            &JournalTransition::Retire { progress },
        )
        .await
        .unwrap();
    let next = fixture.step(&driver, 1).await;
    assert_eq!(next.dispatched, 1); // finish preparation of the prior receive
    assert_eq!(next.allocated, 1); // only the remaining shared slot
    assert_eq!(next.snapshot.head().attempts().len(), 2);
    assert_eq!(next.snapshot.head().reserved_restore_bytes(), 8192);
    let cells = next
        .snapshot
        .head()
        .attempts()
        .iter()
        .map(|a| a.spec().target.cell_id())
        .collect::<Vec<_>>();
    assert_ne!(cells[0], cells[1]);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn unknown_without_acceptance_requires_atomic_absence_cas_before_retry() {
    let fixture = Fixture::new(true, false, false).await;
    let driver = fixture.driver(206);
    let initial = fixture.step(&driver, 0).await;
    let id = initial.snapshot.head().attempts()[0].spec().id;
    fixture.stop().await;
    let mut snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    for event in [AttemptEvent::BeginPrepare, AttemptEvent::OutcomeUnknown] {
        snapshot = fixture
            .journal
            .compare_exchange(
                &snapshot,
                1,
                NOW + 50,
                &JournalTransition::Attempt { id, event },
            )
            .await
            .unwrap();
    }
    let report = fixture.step(&driver, 1).await;
    assert_eq!(report.inspected, 1);
    assert_eq!(report.dispatched, 2); // absence CAS rearms the first; the other is Planned
    assert_eq!(report.retired, 0);
    let unknown = report
        .snapshot
        .head()
        .attempts()
        .iter()
        .find(|attempt| attempt.spec().id == id)
        .unwrap();
    assert_eq!(unknown.phase(), AttemptPhase::Reserved);
    assert_eq!(unknown.blocker(), None);
    assert_eq!(report.snapshot.head().reserved_restore_bytes(), 8192);
    assert!(
        fixture
            .journal
            .load_movement_action(
                scope(),
                id,
                MovementAction::Prepare,
                unknown.spec().destination_node,
                unknown.spec().destination
            )
            .await
            .unwrap()
            .is_some()
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn count_balancing_rejects_unknown_live_boot_despite_adapter_completeness() {
    let fixture = Fixture::new(true, false, false).await;
    let key = EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([131; 32]),
        role: EnrollmentRole::Node {
            mode: NodeMode::Active,
        },
        source: None,
        target: EnrollmentEndpoint {
            node: node(3),
            session: session(3),
            intent_revision: 1,
        },
    }
    .key()
    .unwrap();
    let record = fixture
        .journal
        .load_enrollment(scope(), key)
        .await
        .unwrap()
        .unwrap();
    fixture
        .journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Retired(Digest::from_bytes([230; 32])),
            1,
        )
        .await
        .unwrap();
    let report = fixture.step(&fixture.driver(206), 0).await;
    assert_eq!(fixture.observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    assert!(report.snapshot.head().attempts().is_empty());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn missing_failed_boot_blocks_count_balancing_without_expiring_its_obligation() {
    let fixture = Fixture::new(true, false, false).await;
    fixture
        .journal
        .register_initial_intent(&NodeIntent::initial(scope(), node(4), session(4)).unwrap())
        .await
        .unwrap();
    let request = EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([232; 32]),
        role: EnrollmentRole::Node {
            mode: NodeMode::Active,
        },
        source: None,
        target: EnrollmentEndpoint {
            node: node(4),
            session: session(4),
            intent_revision: 1,
        },
    };
    let FleetEnrollmentAcceptance::New(record) = fixture
        .journal
        .accept_enrollment(&request, 0)
        .await
        .unwrap()
    else {
        panic!("not a new boot")
    };
    fixture
        .journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Established(Digest::from_bytes([231; 32])),
            0,
        )
        .await
        .unwrap();
    let report = fixture.step(&fixture.driver(206), 0).await;
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    let retained = fixture
        .journal
        .load_enrollment(scope(), request.key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.status(), EnrollmentStatus::Established);
    assert_eq!(retained.updated_at_ms(), 0);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn enrollment_during_capture_invalidates_planning_before_allocation() {
    let fixture = Fixture::new(true, false, false).await;
    *fixture.observer.capture_mutation.lock().unwrap() = Some(fixture.journal.clone());
    let result = fixture
        .driver(206)
        .reconcile_once(|| Ok(NOW), Instant::now() + Duration::from_secs(3))
        .await;
    assert!(
        matches!(result, Err(cellule_runtime::Error::FleetOperation(error)) if matches!(*error, OperationError::Conflict))
    );
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert!(current.head().attempts().is_empty());
    assert_eq!(fixture.transport.dispatched.load(Ordering::SeqCst), 0);
    let roster = FleetRoster::collect(
        fixture.journal.as_ref(),
        &current,
        Instant::now() + Duration::from_secs(3),
    )
    .await
    .unwrap();
    let enrolled = roster
        .enrollments()
        .iter()
        .find(|record| record.spec().request == Digest::from_bytes([229; 32]))
        .unwrap();
    assert_eq!(enrolled.status(), EnrollmentStatus::Pending);
    assert!(enrolled.unresolved());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn complete_flag_cannot_hide_an_owned_or_transitioning_cell_from_signed_counts() {
    let fixture = Fixture::new(true, false, false).await;
    fixture
        .observer
        .omit_last_cell
        .store(true, Ordering::SeqCst);
    let report = fixture.step(&fixture.driver(206), 0).await;
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    assert!(report.snapshot.head().attempts().is_empty());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn unknown_enrollment_disables_counts_and_keeps_pressure_relief_available() {
    for pressured in [false, true] {
        let fixture = Fixture::new(true, pressured, false).await;
        let request = EnrollmentSpec {
            scope: scope(),
            request: Digest::from_bytes([235; 32]),
            role: EnrollmentRole::Follower { log_epoch: 99 },
            source: Some(EnrollmentEndpoint {
                node: node(3),
                session: session(3),
                intent_revision: 1,
            }),
            target: EnrollmentEndpoint {
                node: node(2),
                session: session(2),
                intent_revision: 1,
            },
        };
        fixture
            .journal
            .accept_enrollment(&request, 0)
            .await
            .unwrap();
        let report = fixture.step(&fixture.driver(206), 0).await;
        assert_eq!(report.allocated, if pressured { 2 } else { 0 });
        assert!(
            report
                .blockers
                .contains(&DrainBlocker::IncompleteObservation)
        );
        let record = fixture
            .journal
            .load_enrollment(scope(), request.key().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.status(), EnrollmentStatus::Pending);
        assert_eq!(record.updated_at_ms(), 0);
        fixture.journal.close().await.unwrap();
    }
}
