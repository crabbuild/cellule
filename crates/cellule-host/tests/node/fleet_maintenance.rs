//! Exact boot-bound cordon through the public node-owned action boundary.

use super::fleet_actions::{Fixture, clock, fixture};
use super::*;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::NodeId;
use cellule_runtime::node::NodeMode;

async fn release_action(fixture: &Fixture) -> FleetAction {
    let FleetActionKind::Movement { attempt, .. } = fixture.action.kind() else {
        panic!("fixture lacks source identity")
    };
    let spec = attempt.spec().clone();
    fixture.journal.reset_preparing(spec.clone());
    fixture.journal.transition(JournalTransition::Attempt {
        id: spec.id,
        event: AttemptEvent::Reserved(ReceiverReservation {
            session: spec.destination,
            expires_at_ms: spec.deadline_ms,
        }),
    });
    fixture
        .journal
        .transition(JournalTransition::BeginMaintenance(
            MaintenanceOperation::new(
                spec.id.operation,
                Digest::from_bytes([221; 32]),
                spec.source_node,
                spec.source,
                2,
                clock(),
                spec.deadline_ms,
            )
            .unwrap(),
        ));
    let cordon = fixture
        .journal
        .maintenance_action(MaintenanceAction::Cordon);
    let result = fixture
        .node
        .apply_fleet_action(cordon, clock())
        .await
        .unwrap();
    assert!(result.committed && result.execution_error.is_none());
    assert_eq!(result.outcome.outcome, FleetOutcome::Cordoned);
    fixture
        .journal
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned));
    fixture.journal.transition(JournalTransition::Maintenance(
        MaintenanceEvent::BeginEvacuation,
    ));
    fixture.journal.transition(JournalTransition::Attempt {
        id: spec.id,
        event: AttemptEvent::BeginMaintenanceRelease,
    });
    fixture
        .journal
        .action(spec.id, MovementAction::ReleaseMaintenance)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_maintenance_release_preserves_sql_receipt_after_waiter_or_publication_loss() {
    use cellule_runtime::cell::actor::CellInventoryEntry;
    use cellule_runtime::cell::executor::{HandlerOutcome, MutationIdentity, Resolution};
    use cellule_runtime::control::Owner;
    use cellule_runtime::identity::RequestId;
    use cellule_runtime::ltx::CellReplica;

    for (drop_waiter, lose_result) in [(false, false), (true, false), (false, true)] {
        let fixture = fixture().await;
        let action = release_action(&fixture).await;
        if lose_result {
            fixture.journal.lose_next_result_reply();
        }
        let now = clock();
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes([222; 16]),
            issued_at_ms: now,
            expires_at_ms: now + 60_000,
        };
        let digest = Digest::from_bytes([223; 32]);
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let (resume, paused) = std::sync::mpsc::channel();
        let handle = fixture.handle.clone();
        let command = tokio::spawn(async move {
            handle
                .execute(identity, digest, now, 64, 64, move |transaction| {
                    signal.notify_one();
                    paused.recv().unwrap();
                    transaction.execute("UPDATE counter SET value = 77", [])?;
                    Ok(HandlerOutcome::Success(vec![77]))
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        let node = fixture.node.clone();
        let input = action.clone();
        let waiter = tokio::spawn(async move { node.apply_fleet_action(input, clock()).await });
        let quiesced = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let page = fixture
                    .node
                    .runtime()
                    .fleet_cells_page(None, 128)
                    .await
                    .unwrap();
                if page.entries().iter().any(|entry| {
                    matches!(entry,
                    CellInventoryEntry::Owned(owner) if owner.quiescing)
                }) {
                    break;
                }
                drop(page);
                tokio::task::yield_now().await;
            }
        })
        .await;
        let foreground = tokio::time::timeout(
            Duration::from_millis(500),
            fixture.handle.query(64, 64, |_| {
                panic!("new foreground work crossed maintenance closure")
            }),
        )
        .await;
        if drop_waiter {
            waiter.abort();
        }
        // Release and join accepted SQL before asserting any preflight evidence.
        resume.send(()).unwrap();
        let receipt = command.await.unwrap().unwrap();
        assert!(quiesced.is_ok());
        assert!(matches!(foreground, Ok(Err(Error::CellDraining))));
        if drop_waiter {
            assert!(waiter.await.unwrap_err().is_cancelled());
        } else {
            let first = waiter.await.unwrap().unwrap();
            assert_eq!(first.committed, !lose_result);
            assert_eq!(first.journal_error.is_some(), lose_result);
            assert!(first.execution_error.is_none());
        }
        let completed = fixture
            .node
            .apply_fleet_action(action.clone(), clock())
            .await
            .unwrap();
        assert!(completed.committed && completed.execution_error.is_none());
        let FleetOutcome::Released(position) = &completed.outcome.outcome else {
            panic!("maintenance did not release: {completed:?}")
        };
        let idle = fixture
            .authority
            .load(fixture.handle.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            idle.value().state,
            cellule_runtime::control::ControlState::Idle
        );
        assert_eq!(idle.value().root.as_ref(), Some(&position.root));
        assert_eq!(position.root.commit_sequence, receipt.commit_sequence());
        assert_eq!(fixture.journal.accepted_count(), 2); // Cordon and explicit source release.

        let session = SessionId::from_bytes([204; 16]);
        let receiver = CellNodeBuilder::new(application())
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
        let destination = receiver
            .runtime()
            .acquire_idle_restored(
                fixture.handle.catalog().clone(),
                replica,
                fixture.authority.clone(),
                idle,
                fixture._root.path().join("receiver.sqlite"),
                Owner {
                    session,
                    endpoint: "https://fleet-receiver.internal:8789".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            destination
                .resolve(identity, digest, clock(), 64)
                .await
                .unwrap(),
            Resolution::Committed(receipt)
        );
        assert_eq!(
            destination
                .query(64, 64, |connection| {
                    let value = connection
                        .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
                    Ok(value.to_be_bytes().to_vec())
                })
                .await
                .unwrap(),
            77_i64.to_be_bytes()
        );
        // Retained release is historical proof even after authority moves on.
        let replay = fixture
            .node
            .apply_fleet_action(action, clock())
            .await
            .unwrap();
        assert_eq!(replay.outcome, completed.outcome);
        assert_eq!(fixture.journal.accepted_count(), 2);
        receiver.shutdown().await.unwrap();
        assert_eq!(receiver.stats().active_cells(), 0);
        assert_eq!(receiver.stats().retained_bytes(), 0);
        close(fixture).await;
    }
}

fn cordon(fixture: &Fixture) -> FleetAction {
    fixture.journal.reset_maintenance(
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([201; 16]),
    )
}

async fn check_existing_owner(fixture: &Fixture) {
    let value = fixture
        .handle
        .query(64, 64, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(value, 42_i64.to_be_bytes());
    assert_eq!(fixture.node.stats().active_cells(), 1);
}

async fn close(fixture: Fixture) {
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.stats().active_cells(), 0);
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
    assert_eq!(fixture.node.state(), NodeState::Stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_cordon_closes_shared_role_gate_keeps_owner_and_is_idempotent() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    let gate = fixture.node.runtime().node_admission();
    assert_eq!(gate.mode().unwrap(), NodeMode::Active);
    let (a, b) = tokio::join!(
        fixture.node.apply_fleet_action(action.clone(), clock()),
        fixture.node.apply_fleet_action(action.clone(), clock()),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a.committed && b.committed);
    assert_eq!(a.outcome, b.outcome);
    assert_eq!(a.outcome.outcome, FleetOutcome::Cordoned);
    assert_eq!(fixture.journal.accepted_count(), 1);
    assert_eq!(gate.mode().unwrap(), NodeMode::Draining);
    assert!(matches!(gate.check_new_role(), Err(Error::CellDraining)));
    // Even another lifecycle cordon cannot clear terminal maintenance intent.
    gate.cordon().unwrap();
    assert_eq!(gate.mode().unwrap(), NodeMode::Draining);
    assert!(fixture.node.is_ready());
    check_existing_owner(&fixture).await;
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_publication_reply_replays_original_cordon_and_keeps_gate_closed() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    fixture.journal.lose_next_result_reply();
    let original = fixture
        .node
        .apply_fleet_action(action.clone(), clock())
        .await
        .unwrap();
    assert!(!original.committed);
    assert!(original.journal_error.is_some());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    let replay = fixture
        .node
        .apply_fleet_action(action, clock())
        .await
        .unwrap();
    assert!(replay.committed);
    assert_eq!(replay.accepted, original.accepted);
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(fixture.journal.accepted_count(), 1);
    check_existing_owner(&fixture).await;
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_cordon_waiter_does_not_cancel_node_owned_publication() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    fixture.journal.hold_next_result();
    let node = fixture.node.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(action, clock()).await });
    fixture.journal.wait_for_result_publication().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    fixture.journal.resume_result_publication();
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_boot_stale_intent_and_unsupported_roles_do_not_accept_effects() {
    let fixture = fixture().await;
    let wrong = fixture.journal.reset_maintenance(
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([202; 16]),
    );
    assert!(
        fixture
            .node
            .apply_fleet_action(wrong, clock())
            .await
            .is_err()
    );
    assert_eq!(fixture.journal.accepted_count(), 0);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    let stale = cordon(&fixture);
    fixture.journal.transition(JournalTransition::Maintenance(
        MaintenanceEvent::ExtendDeadline(clock() + 120_000),
    ));
    assert!(
        fixture
            .node
            .apply_fleet_action(stale, clock())
            .await
            .is_err()
    );
    fixture
        .journal
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned));
    fixture.journal.transition(JournalTransition::Maintenance(
        MaintenanceEvent::BeginEvacuation,
    ));
    let roles = fixture
        .journal
        .maintenance_action(MaintenanceAction::SettleRoles);
    assert!(
        fixture
            .node
            .apply_fleet_action(roles, clock())
            .await
            .is_err()
    );
    let inspection = FleetInspectionRequest::new(
        fixture
            .journal
            .maintenance_action(MaintenanceAction::Inspect),
        fixture.journal.registry(),
        Digest::from_bytes([211; 32]),
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([201; 16]),
        clock() + 30_000,
    )
    .unwrap();
    assert!(fixture.node.inspect_fleet_action(inspection).await.is_err());
    assert_eq!(fixture.journal.accepted_count(), 0);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    check_existing_owner(&fixture).await;
    close(fixture).await;
}
