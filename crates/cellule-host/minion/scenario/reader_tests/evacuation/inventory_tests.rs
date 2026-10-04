//! Aggregate capture follows real native views and original producer retirement.
use super::*;
use cellule_host::fleet::{
    FleetNodeInventory, FleetNodeInventoryScan, FleetRoster, FleetSnapshotRequest,
};

async fn collect(fixture: &Fixture, index: usize) -> (FleetRoster, FleetNodeInventory) {
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(
        fixture.journal.as_ref(),
        &snapshot,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut scan = FleetNodeInventoryScan::new(&roster, node_id(index), session(index)).unwrap();
    let mut sequence = 0u64;
    while let Some(subject) = scan.next_subject().unwrap() {
        sequence += 1;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.reader-inventory-test.v1\0");
        hash.update(&sequence.to_be_bytes());
        hash.update(&clock().unwrap().to_be_bytes());
        let now = clock().unwrap();
        let request = FleetSnapshotRequest::new(
            snapshot.clone(),
            Digest::from_bytes(*hash.finalize().as_bytes()),
            node_id(index),
            session(index),
            subject,
            1,
            now,
            (now + 5_000).min(snapshot.head().controller().unwrap().expires_at_ms),
        )
        .unwrap();
        let response = fixture.nodes[index]
            .fleet_snapshot(request.clone())
            .await
            .unwrap();
        scan.accept(&request, &response, clock().unwrap()).unwrap();
    }
    let inventory = scan.finish().unwrap();
    (roster, inventory)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_reader_capture_retains_original_inputs_and_joined_retirement() {
    let fixture = Fixture::new().await;
    let (roster, inventory) = collect(&fixture, 1).await;
    assert_eq!(inventory.mode(), cellule_runtime::node::NodeMode::Draining);
    assert!(inventory.bindings().readers);
    assert!(inventory.cells().is_empty());
    assert_eq!(inventory.readers_closed(), Some(false));
    let jobs = inventory.reader_jobs().unwrap();
    assert_eq!(jobs.running(), 0);
    assert_eq!(jobs.unobserved(), 0);
    assert_eq!(jobs.joining(), 0);
    assert_eq!(inventory.readers().len(), 1);
    assert_eq!(inventory.reader_enrollments().len(), 1);
    let original = &inventory.reader_enrollments()[0];
    assert_eq!(original.spec, *fixture.original.spec());
    assert_eq!(
        original.accepted.as_ref().unwrap().accepted_at_ms(),
        fixture.original.accepted_at_ms()
    );
    assert!(original.published);
    assert!(original.opening_started && original.opening_joined);
    assert!(!inventory.readers()[0].locally_joined());
    inventory.validate_enrollments(&roster).unwrap();
    fixture.read_original(17).await;
    fixture.spare().await;
    fixture.evacuate().await.unwrap();
    let (after_roster, after) = collect(&fixture, 1).await;
    assert!(after.readers().is_empty());
    assert!(after.reader_enrollments().is_empty());
    let retired = after_roster
        .enrollments()
        .iter()
        .find(|row| row.spec() == fixture.original.spec())
        .unwrap();
    assert_eq!(retired.status(), EnrollmentStatus::Retired);
    assert!(retired.settlement_evidence().is_some());
    after.validate_enrollments(&after_roster).unwrap();
    assert!(inventory.validate_enrollments(&after_roster).is_err());
    drop(inventory);
    drop(after);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_retirement_capture_preserves_the_original_source_error() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    resume.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    let original = fixture.managers[1]
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let (roster, inventory) = collect(&fixture, 1).await;
    inventory.validate_enrollments(&roster).unwrap();
    assert_eq!(inventory.readers().len(), 1);
    assert!(inventory.readers()[0].locally_joined());
    let observed = &inventory.reader_enrollments()[0];
    assert!(!observed.published);
    assert_eq!(observed.spec, original.spec);
    assert_eq!(observed.accepted, original.accepted);
    assert_eq!(observed.event, original.event);
    assert!(Arc::ptr_eq(
        observed.journal_error.as_ref().unwrap(),
        original.journal_error.as_ref().unwrap()
    ));
    drop(inventory);
    fixture.evacuate().await.unwrap();
    fixture.finish().await;
}
