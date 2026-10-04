use super::*;
use fixture::Fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_routes_activation_and_cleanup_after_receiver_boot_closure() {
    route_idle_receiver(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_activation_without_claim_continues_after_receiver_boot_closure() {
    route_idle_receiver(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_receiver_recovering_claim_requires_canonical_recovery() {
    refuse_claimed_receiver(ControlState::Recovering).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_receiver_serving_claim_requires_canonical_recovery() {
    refuse_claimed_receiver(ControlState::Serving).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_recovering_receiver_recovers_through_canonical_takeover() {
    route_receiver(false, Some(ControlState::Recovering)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_serving_receiver_recovers_through_canonical_takeover() {
    route_receiver(false, Some(ControlState::Serving)).await;
}

async fn refuse_claimed_receiver(state: ControlState) {
    let fixture = Fixture::released().await;
    fixture.claim_without_actor(state).await;
    let record = &fixture.records[&fixture.spec.target.cell_id()];
    let claimed = record
        .authority
        .load(fixture.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.value().state, state);
    assert_eq!(claimed.value().owner.as_ref().unwrap().session, session(1));
    let observer = fixture.close_receiver().await;
    let driver = fixture.driver(
        SessionId::from_bytes([206; 16]),
        observer,
        fixture.fleet.clone(),
    );
    for _ in 0..3 {
        let report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(report.activated, 0);
        assert_eq!(report.retired, 0);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].attempt, fixture.spec.id);
        let attempt = &report.snapshot.head().attempts()[0];
        assert_eq!(attempt.phase(), AttemptPhase::Activating);
        assert_eq!(attempt.released(), Some(&fixture.released));
        assert!(!attempt.receiver_resources_settled());
        assert_eq!(
            report.snapshot.head().reserved_restore_bytes(),
            fixture.spec.cost.disk_bytes
        );
        assert_eq!(fixture.nodes[2].stats().active_cells(), 0);
        assert_eq!(fixture.nodes[2].stats().local_disk_reserved_bytes(), 0);
        assert_eq!(
            record
                .authority
                .load(fixture.spec.target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .value(),
            claimed.value()
        );
    }
    let actions = fixture
        .journal
        .load_movement_actions(scope(), &fixture.released_attempt, MovementAction::Activate)
        .await
        .unwrap();
    let (accepted, result) = actions
        .into_iter()
        .find_map(|acceptance| match acceptance {
            FleetActionAcceptance::Existing { accepted, result }
                if accepted.action().receiver_endpoint() == Some((node_id(2), session(2))) =>
            {
                Some((accepted, result))
            }
            _ => None,
        })
        .expect("checked routed refusal missing");
    assert!(matches!(result.unwrap().outcome, FleetOutcome::Unknown));
    assert!(
        fixture
            .journal
            .load_acquisition_basis(&accepted)
            .await
            .unwrap()
            .is_none()
    );

    // Receiver-credit cleanup is not permission to erase an unresolved
    // ownership claim, even after the original process has joined shutdown.
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Attempt {
                id: fixture.spec.id,
                event: AttemptEvent::BeginCancel,
            },
        )
        .await
        .unwrap();
    let report = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let attempt = &report.snapshot.head().attempts()[0];
    assert_eq!(attempt.phase(), AttemptPhase::CleaningReceiver);
    assert!(!attempt.receiver_resources_settled());
    assert_eq!(report.retired, 0);
    assert_eq!(
        report.snapshot.head().reserved_restore_bytes(),
        fixture.spec.cost.disk_bytes
    );
    fixture.shutdown().await;
}

async fn route_idle_receiver(accepted_before_shutdown: bool) {
    route_receiver(accepted_before_shutdown, None).await;
}

async fn route_receiver(accepted_before_shutdown: bool, failed_state: Option<ControlState>) {
    let fixture = Fixture::released_with_recovery(failed_state.is_some()).await;
    if let Some(state) = failed_state {
        fixture.claim_without_actor(state).await;
    }
    if accepted_before_shutdown {
        fixture.accept_original_activation().await;
    }
    let observer = fixture.close_receiver().await;
    let journal = &fixture.journal;
    let nodes = &fixture.nodes;
    let fleet = &fixture.fleet;
    let profile = fixture.profile;
    let spec = &fixture.spec;
    let released_attempt = &fixture.released_attempt;
    let released = &fixture.released;
    let acknowledged = &fixture.acknowledged;
    let controller = SessionId::from_bytes([206; 16]);
    let new_controller_observer = Arc::new(ClosedBootObserver {
        fleet: fleet.clone(),
        request: observer.request.clone(),
        processes: observer.processes.clone(),
    });
    let loss_transport = Arc::new(LoseRoutedActivationReply {
        inner: fleet.clone(),
        lost: std::sync::atomic::AtomicBool::new(false),
    });
    let driver = FleetReconciler::new(
        scope(),
        controller,
        profile,
        journal.clone(),
        observer,
        loss_transport.clone(),
    )
    .unwrap();
    let mut report = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    for _ in 0..8 {
        if loss_transport
            .lost
            .load(std::sync::atomic::Ordering::Acquire)
        {
            break;
        }
        report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
    assert!(
        loss_transport
            .lost
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        report
            .failures
            .iter()
            .any(|failure| failure.attempt == spec.id)
    );
    let accepted_activation = journal
        .load_movement_actions(scope(), released_attempt, MovementAction::Activate)
        .await
        .unwrap()
        .into_iter()
        .find_map(|acceptance| match acceptance {
            FleetActionAcceptance::New(accepted)
            | FleetActionAcceptance::Existing { accepted, .. }
                if accepted.action().receiver_endpoint() == Some((node_id(2), session(2))) =>
            {
                Some(accepted)
            }
            _ => None,
        })
        .expect("routed Activate acceptance missing after reply loss");
    assert_eq!(
        accepted_activation.action().receiver_endpoint(),
        Some((node_id(2), session(2)))
    );
    let duplicate = fleet
        .dispatch(
            accepted_activation.action(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(duplicate.committed);
    assert!(matches!(
        &duplicate.outcome.outcome,
        FleetOutcome::Activated(_)
    ));
    assert_eq!(nodes[2].stats().active_cells(), 1);
    if failed_state.is_some() {
        assert!(
            journal
                .load_acquisition_basis(&accepted_activation)
                .await
                .unwrap()
                .is_none()
        );
        let evidence = journal
            .load_receiver_recovery_evidence(&accepted_activation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(evidence.basis().control().state, failed_state.unwrap());
        assert_eq!(
            evidence.basis().control().owner.as_ref().unwrap().session,
            session(1)
        );
        assert_eq!(evidence.basis().control().epoch, released.epoch + 1);
        assert_eq!(evidence.restored().epoch, released.epoch + 2);
        assert_eq!(evidence.restored().root.as_ref(), Some(&released.root));
        assert_eq!(
            journal
                .record_receiver_recovery_evidence(&evidence)
                .await
                .unwrap(),
            evidence
        );
        assert_eq!(
            journal
                .record_receiver_recovery_basis(evidence.basis())
                .await
                .unwrap(),
            *evidence.basis()
        );
    }

    // Let the original lease expire. The new claimant must adopt the exact
    // route and committed result already retained by the application journal.
    tokio::time::sleep(Duration::from_millis(
        u64::try_from(profile.controller_lease_ms).unwrap() + 50,
    ))
    .await;
    let reopened_journal = fixture.reopen_journal().await;
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([207; 16]),
        profile,
        reopened_journal.clone(),
        new_controller_observer,
        fleet.clone(),
    )
    .unwrap();
    let mut activated = None;
    for _ in 0..12 {
        if report.snapshot.head().attempts().is_empty() {
            break;
        }
        report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        activated = activated.or_else(|| {
            report
                .snapshot
                .head()
                .attempts()
                .first()?
                .activated()
                .cloned()
        });
    }
    assert!(report.snapshot.head().attempts().is_empty());
    assert_eq!(report.retired, 1);
    assert_eq!(report.snapshot.head().reserved_restore_bytes(), 0);
    let activated = activated.expect("routed replacement activation was not observed");
    assert_eq!(
        (activated.node, activated.session),
        (node_id(2), session(2))
    );
    assert_eq!(activated.position.root, released.root);
    assert_eq!(
        activated.position.epoch,
        released.epoch + 1 + u64::from(failed_state.is_some())
    );
    if failed_state.is_some() {
        let original = journal
            .load_receiver_recovery_evidence(&accepted_activation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened_journal
                .load_receiver_recovery_evidence(&accepted_activation)
                .await
                .unwrap(),
            Some(original)
        );
    }

    for effect in [MovementAction::Activate, MovementAction::Cancel] {
        let actions = journal
            .load_movement_actions(scope(), released_attempt, effect)
            .await
            .unwrap();
        let accepted = actions.into_iter().find_map(|acceptance| match acceptance {
            FleetActionAcceptance::New(accepted)
            | FleetActionAcceptance::Existing { accepted, .. }
                if accepted.action().receiver_endpoint() == Some((node_id(2), session(2))) =>
            {
                Some(accepted)
            }
            _ => None,
        });
        let accepted = accepted.expect("routed receiver action acceptance missing");
        assert_eq!(
            accepted.action().receiver_endpoint(),
            Some((node_id(2), session(2)))
        );
        let route = accepted.action().receiver_route().unwrap();
        assert_eq!(
            route.latest_handoff().unwrap().previous(),
            (node_id(1), session(1))
        );
    }

    let record = &fixture.records[&spec.target.cell_id()];
    let serving = record
        .authority
        .load(spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let successor = nodes[2]
        .runtime()
        .local_handle(record.catalog.clone(), &serving)
        .await
        .unwrap()
        .unwrap();
    let original = &acknowledged[&spec.target.cell_id()];
    assert_eq!(
        successor
            .resolve(original.identity, original.digest, clock().unwrap(), 64)
            .await
            .unwrap(),
        Resolution::Committed(original.outcome.clone())
    );
    let bytes = successor
        .query(64, 64, |connection| {
            let value: i64 =
                connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(bytes, original.value.to_be_bytes());

    fixture.shutdown().await;
    reopened_journal.close().await.unwrap();
}
