//! Real lost member acceptance, original producer joining and durable exclusion.
use super::*;
use crate::scenario::reader_tests::nonexecution::{Proofs, retain};
use cellule_host::fleet::{
    FleetEnrollmentNonexecutionRequest, FleetMaintenanceEnrollments, FleetMaintenanceNonexecution,
    FleetObservation, FleetRoster,
};
use cellule_runtime::fleet::operations::{
    JournalTransition, MaintenanceEvent, MaintenanceOperation, OperationId,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_confirms_actual_follower_exclusion_after_lost_acceptance_and_join()
 {
    let fixture = Fixture::new().await;
    let (accepted, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    fixture.install();
    captured(accepted).await;
    let pending = fixture.rows().await[0].clone();
    assert_eq!(pending.status(), EnrollmentStatus::Pending);
    assert!(
        !fixture
            .node
            .follower_enrollment_completion(1)
            .unwrap()
            .unwrap()
            .native_started
    );
    let now = clock().unwrap();
    let mut snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .bootstrap_registry(snapshot.registry())
        .await
        .unwrap();
    snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    snapshot = fixture
        .journal
        .claim_controller(scope(), snapshot.head().revision(), session(9), now)
        .await
        .unwrap();
    for transition in [
        JournalTransition::BeginMaintenance(
            MaintenanceOperation::new(
                OperationId::from_bytes([214; 16]).unwrap(),
                Digest::from_bytes([215; 32]),
                node_id(0),
                session(0),
                2,
                now,
                now + 60_000,
            )
            .unwrap(),
        ),
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    ] {
        snapshot = fixture
            .journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                clock().unwrap(),
                &transition,
            )
            .await
            .unwrap();
    }
    resume.send(()).unwrap();
    // The existing single owner joins the failed acceptance and excludes every
    // original member. No native epoch, follower RPC or durability handle starts.
    fixture.node.shutdown().await.unwrap();
    let terminal = fixture.rows().await;
    assert_eq!(terminal.len(), 2);
    assert!(
        terminal
            .iter()
            .all(|row| row.status() == EnrollmentStatus::Refused)
    );
    assert!(fixture.node.runtime().node_durability().is_none());
    assert!(fixture.transport.requests.lock().unwrap().is_empty());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    for row in &terminal {
        assert!(
            matches!(fixture.journal.accept_enrollment(row.spec(), clock().unwrap()).await.unwrap(),
            cellule_host::fleet::FleetEnrollmentAcceptance::Existing(existing) if existing == *row)
        );
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(fixture.journal.as_ref(), &snapshot, deadline)
        .await
        .unwrap();
    let original =
        FleetMaintenanceEnrollments::collect(fixture.journal.as_ref(), &roster, deadline, clock)
            .await
            .unwrap();
    assert_eq!(original.entries().count(), 1);
    assert_eq!(original.entries().next().unwrap(), &pending);
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, pending.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("joined-follower-proof");
    retain(&path, &request);
    let checks = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &original,
        &roster,
        &Proofs::new(path),
        deadline,
        clock,
    )
    .await
    .unwrap();
    let observation = FleetObservation::new(
        scope(),
        snapshot.registry(),
        snapshot.registry().revision(),
        original.interval().0,
        clock().unwrap(),
        false,
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
    .with_maintenance_enrollments(original)
    .unwrap()
    .with_maintenance_nonexecution(checks)
    .unwrap()
    .check_maintenance_policies(&roster, clock().unwrap())
    .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert!(coverage.is_complete());
    assert_eq!(coverage.progress().required, 1);
    assert_eq!(coverage.progress().nonexecution, 1);
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    fixture.finish().await;
}
