//! Native closing owns canonical withdrawal and the original durable boot row.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetAdapterFuture, FleetCellInputs, FleetCellProvider,
    FleetObservation, FleetObserver, FleetRecoveryInputs, FleetRoster, FleetTransport,
};

struct NoCells;

impl FleetCellProvider for NoCells {
    fn cell_inputs<'a>(
        &'a self,
        _: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetCellInputs> {
        Box::pin(async { Err(invalid("Finalize fixture has no movable Cells")) })
    }

    fn recovery_inputs<'a>(
        &'a self,
        _: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetRecoveryInputs> {
        Box::pin(async { Err(invalid("Finalize fixture has no recovery work")) })
    }
}

struct UnusedObserver;

impl FleetObserver for UnusedObserver {
    fn observe<'a>(
        &'a self,
        _: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async { Err(invalid("closing test does not observe Cells")) })
    }
}

struct NodeTransport {
    node: Arc<CellNode>,
    actions: std::sync::Mutex<Vec<FleetAction>>,
}

impl FleetTransport for NodeTransport {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            self.actions.lock().unwrap().push(action.clone());
            self.node
                .apply_fleet_action(action.clone(), clock()?)
                .await
                .map_err(|error| Box::new(error) as JournalError)
        })
    }

    fn inspect<'a>(
        &'a self,
        _: &'a cellule_runtime::fleet::operations::FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<cellule_runtime::fleet::operations::FleetInspectionObservation>>
    {
        Box::pin(async { Err(invalid("closing test does not inspect Cells")) })
    }
}

async fn prepare(fixture: &Fixture) -> EnrollmentRecord {
    let original = enroll(
        &fixture.journal,
        &fixture.directory,
        &spec(&fixture.intent).unwrap(),
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), original.spec().key().unwrap())
        .await
        .unwrap();
    let observed = fixture
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    fixture
        .node
        .install_fleet_boot_withdrawal(
            fixture.directory.clone(),
            observed,
            original.clone(),
            fixture.journal.clone(),
        )
        .unwrap();
    fixture
        .node
        .install_fleet_actions(
            scope(),
            node_id(0),
            fixture.journal.clone(),
            Arc::new(NoCells),
        )
        .unwrap();
    original
}

async fn retired(fixture: &Fixture, original: &EnrollmentRecord) -> EnrollmentRecord {
    let record = fixture
        .journal
        .load_enrollment(scope(), original.spec().key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.spec(), original.spec());
    assert_eq!(record.accepted_at_ms(), original.accepted_at_ms());
    assert_eq!(
        record.established_evidence(),
        original.established_evidence()
    );
    assert_eq!(record.status(), EnrollmentStatus::Retired);
    assert!(record.settlement_evidence().is_some());
    assert!(fixture.directory.is_retired(session(0)).await.unwrap());
    assert!(fixture.directory.is_withdrawn(session(0)).await.unwrap());
    record
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_finalize_joins_the_node_and_publishes_only_after_exact_withdrawal() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture.node.start().unwrap();

    let registry = fixture
        .journal
        .load_snapshot(scope())
        .await
        .unwrap()
        .registry();
    let registry = fixture.journal.bootstrap_registry(registry).await.unwrap();
    fixture
        .journal
        .set_scheduling(registry, true)
        .await
        .unwrap();

    let snapshot = maintenance(&fixture).await;
    let now = clock().unwrap();
    let cordon = snapshot
        .head()
        .maintenance_action(MaintenanceAction::Cordon, now)
        .unwrap();
    let cordoned = fixture.node.apply_fleet_action(cordon, now).await.unwrap();
    assert!(cordoned.committed);
    assert_eq!(cordoned.outcome.outcome, FleetOutcome::Cordoned);

    let snapshot = fixture
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        )
        .await
        .unwrap();
    let snapshot = fixture
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
        )
        .await
        .unwrap();
    let operation = snapshot.head().maintenance().unwrap();
    crate::scenario::commit_test_role_settlement(&fixture.journal, clock().unwrap())
        .await
        .unwrap();
    let snapshot = fixture
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(DrainEvidence {
                node: operation.node(),
                session: operation.session(),
                remaining_cells: 0,
                unresolved_attempts: 0,
                relocated: true,
                readers_settled: true,
                followers_settled: true,
                facilities_closed: false,
                stopped: false,
                withdrawn: false,
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Closing
    );
    let registry = fixture
        .journal
        .load_snapshot(scope())
        .await
        .unwrap()
        .registry();
    fixture
        .journal
        .set_scheduling(registry, false)
        .await
        .unwrap();

    let transport = Arc::new(NodeTransport {
        node: Arc::clone(&fixture.node),
        actions: std::sync::Mutex::new(Vec::new()),
    });
    let reconciler = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
        FleetProfile::default(),
        fixture.journal.clone(),
        Arc::new(UnusedObserver),
        Arc::clone(&transport) as Arc<dyn FleetTransport>,
    )
    .unwrap();
    let report = reconciler
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(report.dispatched, 1);
    assert_eq!(
        report.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Completed
    );
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    retired(&fixture, &original).await;

    let finalize = transport.actions.lock().unwrap()[0].clone();
    let replay = fixture
        .node
        .apply_fleet_action(finalize, clock().unwrap())
        .await
        .unwrap();
    assert!(replay.committed);
    assert!(replay.execution_error.is_none());
    let FleetOutcome::Stopped(evidence) = replay.outcome.outcome else {
        panic!("Finalize replay did not prove terminal shutdown: {replay:?}")
    };
    assert!(evidence.facilities_closed && evidence.stopped && evidence.withdrawn);
    close(fixture).await;
}

#[tokio::test]
async fn native_shutdown_withdraws_and_retires_original_boot_before_stopped() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture.node.start().unwrap();
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    let terminal = retired(&fixture, &original).await;
    fixture.node.shutdown().await.unwrap();
    assert_eq!(retired(&fixture, &original).await, terminal);
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sole_waiter_cancellation_keeps_withdrawal_and_durable_retirement_owned() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered_drain = entered.clone();
    let gate_drain = gate.clone();
    fixture
        .node
        .install_facility(
            cellule_host::CellNodeFacility::new("withdrawal-order", move || {
                let entered = entered_drain.clone();
                let gate = gate_drain.clone();
                async move {
                    entered.notify_one();
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            })
            .unwrap(),
        )
        .unwrap();
    fixture.node.start().unwrap();
    let node = fixture.node.clone();
    let caller = tokio::spawn(async move { node.shutdown().await });
    entered.notified().await;
    assert_eq!(fixture.node.state(), NodeState::Draining);
    assert!(!fixture.directory.is_retired(session(0)).await.unwrap());
    assert_eq!(
        fixture
            .journal
            .load_enrollment(scope(), original.spec().key().unwrap())
            .await
            .unwrap()
            .unwrap(),
        original
    );
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    gate.add_permits(2);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.node.state() != NodeState::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    retired(&fixture, &original).await;
    close(fixture).await;
}

#[tokio::test]
async fn ambiguous_retirement_reply_keeps_draining_and_replays_original_evidence() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture.node.start().unwrap();
    let (committed, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let node = fixture.node.clone();
    let caller = tokio::spawn(async move { node.shutdown().await });
    committed.await.unwrap();
    let terminal = retired(&fixture, &original).await;
    assert_eq!(fixture.node.state(), NodeState::Draining);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    resume.send(()).unwrap();
    let error = caller.await.unwrap().unwrap_err();
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(fixture.node.state(), NodeState::Draining);
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert_eq!(retired(&fixture, &original).await, terminal);
    close(fixture).await;
}

#[tokio::test]
async fn retirement_reply_deadline_retains_original_commit_and_resumes_closing() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture.node.start().unwrap();
    let (committed, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let node = fixture.node.clone();
    let caller = tokio::spawn(async move {
        node.drain_until(Some((Instant::now() + Duration::from_secs(1)).into_std()))
            .await
    });
    committed.await.unwrap();
    let terminal = retired(&fixture, &original).await;
    assert_eq!(fixture.node.state(), NodeState::Draining);
    let error = caller.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        Error::Facility {
            name: "fleet-boot-withdrawal-deadline",
            ..
        }
    ));
    assert_eq!(fixture.node.state(), NodeState::Draining);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    assert!(
        resume.send(()).is_err(),
        "expired publication waiter still runs"
    );
    fixture.node.shutdown().await.unwrap();
    assert_eq!(retired(&fixture, &original).await, terminal);
    close(fixture).await;
}

#[tokio::test]
async fn native_withdrawal_reconciles_the_original_token_after_a_late_heartbeat() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    let previous = fixture
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    // Republishing the original signed lease is an idempotent refresh. Issue
    // an actual later heartbeat so the original withdrawal token is stale.
    while clock().unwrap() <= previous.advertisement().issued_at_ms() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let next = advertisement(0, &fixture.node, &fixture.intent)
        .await
        .unwrap();
    let newer = fixture
        .directory
        .refresh(&previous, next, clock().unwrap())
        .await
        .unwrap();
    assert!(newer.advertisement().generation() > previous.advertisement().generation());
    fixture.node.start().unwrap();
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    retired(&fixture, &original).await;
    close(fixture).await;
}

#[tokio::test]
async fn missing_canonical_boot_cannot_be_retired_or_report_stopped() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture.node.start().unwrap();
    fixture
        .layout
        .store()
        .delete(&fixture.layout.node_path(session(0).as_bytes()))
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            fixture.node.shutdown().await,
            Err(Error::Control("fleet boot withdrawal lacks its tombstone"))
        ));
        assert_eq!(fixture.node.state(), NodeState::Draining);
        assert_eq!(fixture.node.stats().active_cells(), 0);
        assert_eq!(fixture.node.stats().retained_bytes(), 0);
        assert!(!fixture.directory.is_retired(session(0)).await.unwrap());
        assert_eq!(
            fixture
                .journal
                .load_enrollment(scope(), original.spec().key().unwrap())
                .await
                .unwrap()
                .unwrap(),
            original
        );
    }
    // The failed attempt still joined native resources. The original journal
    // obligation stays Established; deleting storage cannot invent retirement.
    assert_eq!(
        fixture.node.drain_observation().unwrap().unwrap().phase,
        cellule_host::NodeDrainPhase::Joined
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_facility_keeps_original_boot_live_after_runtime_join() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    fixture
        .node
        .install_facility(
            cellule_host::CellNodeFacility::new("required-role-owner", || async {
                Err(
                    Box::new(std::io::Error::other("original role closure failed"))
                        as Box<dyn std::error::Error + Send + Sync>,
                )
            })
            .unwrap(),
        )
        .unwrap();
    fixture.node.start().unwrap();
    for _ in 0..2 {
        let error = fixture.node.shutdown().await.unwrap_err();
        assert!(matches!(
            error,
            Error::Facility {
                name: "required-role-owner",
                ..
            }
        ));
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(fixture.node.state(), NodeState::Draining);
        assert_eq!(fixture.node.stats().active_cells(), 0);
        assert_eq!(fixture.node.stats().retained_bytes(), 0);
        assert!(!fixture.directory.is_retired(session(0)).await.unwrap());
        assert!(
            fixture
                .directory
                .load(session(0), clock().unwrap())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            fixture
                .journal
                .load_enrollment(scope(), original.spec().key().unwrap())
                .await
                .unwrap()
                .unwrap(),
            original
        );
    }
    assert_eq!(
        fixture.node.drain_observation().unwrap().unwrap().phase,
        cellule_host::NodeDrainPhase::Joined
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn withdrawal_binding_is_exact_and_cannot_be_replaced_after_startup() {
    let fixture = fixture().await;
    let original = prepare(&fixture).await;
    let observed = fixture
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture.node.install_fleet_boot_withdrawal(
            fixture.directory.clone(),
            observed.clone(),
            original.clone(),
            fixture.journal.clone()
        ),
        Err(Error::Control("CellNode boot withdrawal already installed"))
    ));
    let changed = EnrollmentRecord::pending(
        EnrollmentSpec {
            request: Digest::from_bytes([77; 32]),
            ..original.spec().clone()
        },
        None,
        None,
        &fixture.intent,
        clock().unwrap(),
    )
    .unwrap()
    .establish(original.established_evidence().unwrap(), clock().unwrap())
    .unwrap();
    assert!(matches!(
        fixture.node.install_fleet_boot_withdrawal(
            fixture.directory.clone(),
            observed.clone(),
            changed,
            fixture.journal.clone()
        ),
        Err(Error::Fenced)
    ));
    fixture.node.start().unwrap();
    assert!(matches!(
        fixture.node.install_fleet_boot_withdrawal(
            fixture.directory.clone(),
            observed,
            original.clone(),
            fixture.journal.clone()
        ),
        Err(Error::CellDraining)
    ));
    fixture.node.shutdown().await.unwrap();
    retired(&fixture, &original).await;
    close(fixture).await;
}
