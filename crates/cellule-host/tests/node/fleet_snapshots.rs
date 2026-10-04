//! Native page reads share the original fleet bank and exact journal barrier.
use super::fleet_actions::{clock, fixture};
use super::*;
use cellule_host::fleet::{
    FleetActionWorkKind, FleetActionWorkState, FleetSnapshotNativePage, FleetSnapshotSubject,
};
use cellule_runtime::fleet::operations::MAX_RECORD_BYTES;

async fn until(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_snapshot_waiter_retains_original_job_and_native_page_until_join() {
    let test = fixture().await;
    let cellule_runtime::fleet::operations::FleetActionKind::Movement { attempt, .. } =
        test.action.kind()
    else {
        panic!("movement fixture required");
    };
    let cell = attempt.spec().target.cell_id();
    let original = test.authority.load(cell).await.unwrap().unwrap();
    let request = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Cells(None), 220);
    test.journal.block_inspections.store(true, Ordering::SeqCst);
    let node = test.node.clone();
    let issued = request.clone();
    let waiter = tokio::spawn(async move { node.fleet_snapshot(issued).await });
    test.journal.inspection_entered.notified().await;
    waiter.abort();
    assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), 1);
    {
        let work = test.node.fleet_action_work().unwrap().unwrap();
        assert_eq!(work.entries().len(), 1);
        assert_eq!(work.entries()[0].key(), request.key().unwrap());
        assert_eq!(work.entries()[0].kind(), FleetActionWorkKind::Snapshot);
        assert_eq!(work.entries()[0].state(), FleetActionWorkState::Running);
        let charged = test.node.stats().retained_bytes();
        let shared = work.clone();
        assert_eq!(test.node.stats().retained_bytes(), charged);
        assert_eq!(shared.digest(), work.digest());
    }
    let retained = test.node.stats().retained_bytes();
    let node = test.node.clone();
    let replay = request.clone();
    let mut retry = Box::pin(node.fleet_snapshot(replay));
    // Register this waiter on the still-blocked original job before releasing
    // it. A yield does not guarantee that a spawned retry has reached submit;
    // a late retry can correctly recapture after the original job was joined.
    assert!(futures_util::poll!(retry.as_mut()).is_pending());
    assert_eq!(test.node.stats().retained_bytes(), retained);
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), 1);
    test.journal
        .block_inspections
        .store(false, Ordering::SeqCst);
    test.journal.inspection_resume.add_permits(1);
    let result = retry.await.unwrap();
    result.validate(&request, clock()).unwrap();
    assert_eq!(result.request(), &request);
    assert!(result.action_work().entries().is_empty());
    assert_eq!(
        result.action_work().excluded_capture(),
        Some(request.key().unwrap())
    );
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), 2);
    assert!(!result.bindings().managed_startup);
    let FleetSnapshotNativePage::Cells(page) = result.page() else {
        panic!("original actor page required");
    };
    assert_eq!(page.session(), request.session());
    assert_eq!(page.owned_cells(), 1);
    assert_eq!(page.transitioning_cells(), 0);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(page.entries()[0].cell(), cell);
    let unchanged = test.authority.load(cell).await.unwrap().unwrap();
    assert_eq!(unchanged.value(), original.value());
    assert!(test.node.stats().retained_bytes() >= 3 * MAX_RECORD_BYTES as usize + (1 << 20));
    drop(result);
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_registry_after_native_capture_fails_without_empty_or_restamped_evidence() {
    let test = fixture().await;
    let request = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Cells(None), 221);
    test.journal
        .pause_snapshot_post
        .store(true, Ordering::SeqCst);
    let node = test.node.clone();
    let issued = request.clone();
    let capture = tokio::spawn(async move { node.fleet_snapshot(issued).await });
    test.journal.snapshot_post_entered.notified().await;
    assert!(!capture.is_finished());
    assert!(test.node.stats().retained_bytes() >= 3 * MAX_RECORD_BYTES as usize + (1 << 20));
    test.journal.advance_snapshot_registry();
    test.journal.snapshot_post_resume.add_permits(1);
    let error = capture.await.unwrap().err().unwrap();
    assert!(matches!(
        error.as_ref(),
        Error::Facility {
            name: "fleet-action-journal",
            ..
        }
    ));
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), 2);
    assert!(test.node.stats().retained_bytes() < 1 << 20);
    assert!(test.node.fleet_snapshot(request).await.is_err());
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_snapshot_reads_use_the_same_bound_as_effects_and_drain_retains_their_joins() {
    let test = fixture().await;
    test.journal.block_inspections.store(true, Ordering::SeqCst);
    let mut callers = Vec::new();
    for nonce in [222, 223] {
        let node = test.node.clone();
        let request = test
            .journal
            .snapshot_request(FleetSnapshotSubject::Host, nonce);
        callers.push(tokio::spawn(
            async move { node.fleet_snapshot(request).await },
        ));
    }
    until(|| test.journal.snapshot_calls.load(Ordering::SeqCst) == 2).await;
    {
        let work = test.node.fleet_action_work().unwrap().unwrap();
        assert_eq!(work.entries().len(), 2);
        assert!(
            work.entries()
                .iter()
                .all(|job| job.kind() == FleetActionWorkKind::Snapshot)
        );
    }
    let error = test
        .node
        .apply_fleet_action(test.action.clone(), clock())
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error.as_ref(),
        Error::Capacity("fleet action receipt bound")
    ));
    for caller in callers {
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    }
    let node = test.node.clone();
    let drain = tokio::spawn(async move { node.shutdown().await });
    until(|| test.node.state() == NodeState::Draining).await;
    assert!(!drain.is_finished());
    test.journal
        .block_inspections
        .store(false, Ordering::SeqCst);
    test.journal.inspection_resume.add_permits(2);
    tokio::time::timeout(Duration::from_secs(3), drain)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), 4);
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

fn inspection(
    test: &super::fleet_actions::Fixture,
    nonce: u8,
) -> cellule_runtime::fleet::operations::FleetInspectionRequest {
    use cellule_runtime::fleet::operations::{
        FleetActionKind, FleetInspectionRequest, MovementAction,
    };
    let FleetActionKind::Movement { attempt, .. } = test.action.kind() else {
        panic!("movement fixture");
    };
    let snapshot = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Host, nonce);
    FleetInspectionRequest::new(
        test.journal
            .action(attempt.spec().id, MovementAction::Inspect),
        test.journal.registry(),
        Digest::from_bytes([nonce; 32]),
        snapshot.node(),
        snapshot.session(),
        snapshot.deadline_ms(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_inspection_survives_lost_waiter_and_changes_empty_work_revision() {
    let test = fixture().await;
    let original = inspection(&test, 230);
    test.journal.block_inspections.store(true, Ordering::SeqCst);
    let node = test.node.clone();
    let issued = original.clone();
    let waiter = tokio::spawn(async move { node.inspect_fleet_action(issued).await });
    test.journal.inspection_entered.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    test.journal
        .block_inspections
        .store(false, Ordering::SeqCst);
    let request = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Host, 231);
    let response = test.node.fleet_snapshot(request.clone()).await.unwrap();
    response.validate(&request, clock()).unwrap();
    let work = response.action_work();
    assert_eq!(work.entries().len(), 1);
    assert_eq!(work.entries()[0].key(), original.key().unwrap());
    assert_eq!(work.entries()[0].kind(), FleetActionWorkKind::Inspection);
    assert_eq!(work.entries()[0].state(), FleetActionWorkState::Running);
    assert!(!work.entries()[0].response_received());
    let original_revision = work.work_revision();
    test.journal.inspection_resume.add_permits(1);
    // Reading metadata does not reap the returned original job.
    until(|| {
        let observed = test.node.fleet_action_work().unwrap().unwrap();
        observed
            .entries()
            .iter()
            .any(|job| job.key() == original.key().unwrap() && job.response_received())
    })
    .await;
    let next = test
        .node
        .fleet_snapshot(
            test.journal
                .snapshot_request(FleetSnapshotSubject::Host, 232),
        )
        .await
        .unwrap();
    assert!(next.action_work().entries().is_empty());
    assert_eq!(next.action_work().work_revision(), original_revision + 1);
    assert_eq!(work.entries()[0].state(), FleetActionWorkState::Running);
    let fresh = test
        .node
        .fleet_snapshot(
            test.journal
                .snapshot_request(FleetSnapshotSubject::Host, 233),
        )
        .await
        .unwrap();
    assert_eq!(next.action_work().digest(), fresh.action_work().digest());
    drop(response);
    drop(next);
    drop(fresh);
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_job_during_native_capture_refuses_original_page_without_restamping() {
    let test = fixture().await;
    test.journal
        .pause_snapshot_post
        .store(true, Ordering::SeqCst);
    let node = test.node.clone();
    let request = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Host, 234);
    let capture = tokio::spawn(async move { node.fleet_snapshot(request).await });
    test.journal.snapshot_post_entered.notified().await;
    test.journal.block_inspections.store(true, Ordering::SeqCst);
    let node = test.node.clone();
    let request = inspection(&test, 235);
    let sibling = tokio::spawn(async move { node.inspect_fleet_action(request).await });
    test.journal.inspection_entered.notified().await;
    test.journal.snapshot_post_resume.add_permits(1);
    let error = capture.await.unwrap().err().unwrap();
    assert!(matches!(
        error.as_ref(),
        Error::Node("native snapshot accepted fleet work changed")
    ));
    sibling.abort();
    assert!(sibling.await.unwrap_err().is_cancelled());
    test.journal
        .block_inspections
        .store(false, Ordering::SeqCst);
    test.journal.inspection_resume.add_permits(1);
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test]
async fn unbound_native_owner_and_foreign_endpoint_never_supply_empty_role_coverage() {
    let test = fixture().await;
    let request = test
        .journal
        .snapshot_request(FleetSnapshotSubject::Readers(None), 224);
    let result = test.node.fleet_snapshot(request.clone()).await.unwrap();
    result.validate(&request, clock()).unwrap();
    assert!(matches!(result.page(), FleetSnapshotNativePage::Unbound));
    assert!(!result.bindings().readers);
    let foreign = cellule_host::fleet::FleetSnapshotRequest::new(
        request.expected().clone(),
        request.nonce(),
        request.node(),
        SessionId::from_bytes([225; 16]),
        request.subject().clone(),
        request.limit(),
        request.issued_at_ms(),
        request.deadline_ms(),
    )
    .unwrap();
    let calls = test.journal.snapshot_calls.load(Ordering::SeqCst);
    assert!(matches!(
        test.node
            .fleet_snapshot(foreign)
            .await
            .err()
            .unwrap()
            .as_ref(),
        Error::FleetOperation(source) if matches!(source.as_ref(), cellule_runtime::fleet::operations::OperationError::Fenced)
    ));
    assert_eq!(test.journal.snapshot_calls.load(Ordering::SeqCst), calls);
    drop(result);
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}
