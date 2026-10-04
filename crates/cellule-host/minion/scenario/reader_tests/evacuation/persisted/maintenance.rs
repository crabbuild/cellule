//! Automatic full-set lookup uses real ready readers and immutable journal history.
use super::*;
use cellule_host::fleet::{FleetMaintenanceEnrollments, FleetRoster};
use std::sync::atomic::Ordering;

async fn originals(
    fixture: &Fixture,
    journal: &SqliteJournal,
) -> (FleetRoster, FleetMaintenanceEnrollments) {
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(journal, &snapshot, deadline())
        .await
        .unwrap();
    let original = FleetMaintenanceEnrollments::collect(journal, &roster, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(original.entries().next(), Some(&fixture.original));
    (roster, original)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_maintenance_policy_lookup_discovers_latest_history_after_restart() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let client = fixture.client().await;
    let (roster, original) = originals(&fixture, &client).await;
    let before = clock().unwrap();
    let checks = fixture
        .verifier()
        .collect_maintenance(&client, &original, &roster, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].record(), publication.record().unwrap());
    assert_eq!(checks[0].snapshot(), roster.snapshot());
    assert_eq!(checks[0].roster_digest(), roster.digest().unwrap());
    assert!(checks[0].interval().0 >= before);
    assert!(!checks[0].replacements().is_empty());
    assert_eq!(
        client.load_snapshot(scope()).await.unwrap(),
        *roster.snapshot()
    );
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_maintenance_policy_lookup_preserves_missing_history_without_native_probes() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let (roster, original) = originals(&fixture, fixture.journal.as_ref()).await;
    let probes = fixture.transport.probes.load(Ordering::Acquire);
    let checks = fixture
        .verifier()
        .collect_maintenance(
            fixture.journal.as_ref(),
            &original,
            &roster,
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert!(checks.is_empty());
    let observation = cellule_host::fleet::FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        original.interval().0,
        clock().unwrap(),
        false,
        super::observation::advertisements(&fixture.directory).await,
        Vec::new(),
    )
    .unwrap()
    .with_role_evacuations(checks, Vec::new())
    .unwrap()
    .with_maintenance_enrollments(original)
    .unwrap()
    .check_maintenance_policies(&roster, clock().unwrap())
    .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert!(!coverage.is_complete());
    assert_eq!(coverage.progress().required, 1);
    assert_eq!(coverage.progress().missing_policy, 1);
    assert_eq!(fixture.transport.probes.load(Ordering::Acquire), probes);
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        *roster.snapshot()
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_maintenance_policy_lookup_refuses_an_earlier_head_and_invalid_capture_clock() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    fixture.publish(&capture).await;
    let (old, original) = originals(&fixture, fixture.journal.as_ref()).await;
    let next = fixture
        .journal
        .claim_controller(
            scope(),
            old.snapshot().head().revision(),
            SessionId::from_bytes([206; 16]),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next.registry(), old.snapshot().registry());
    let roster = FleetRoster::collect(fixture.journal.as_ref(), &next, deadline())
        .await
        .unwrap();
    let verifier = fixture.verifier();
    assert!(matches!(
        verifier
            .collect_maintenance(
                fixture.journal.as_ref(),
                &original,
                &roster,
                deadline(),
                clock
            )
            .await,
        Err(Error::Fenced)
    ));
    let (_, original) = originals(&fixture, fixture.journal.as_ref()).await;
    let probes = fixture.transport.probes.load(Ordering::Acquire);
    for delta in [-1, 30_001] {
        let start = clock().unwrap();
        let mut calls = 0;
        let invalid = || {
            calls += 1;
            Ok(if calls == 1 { start } else { start + delta })
        };
        assert!(matches!(
            verifier
                .collect_maintenance(
                    fixture.journal.as_ref(),
                    &original,
                    &roster,
                    deadline(),
                    invalid
                )
                .await,
            Err(Error::Deadline)
        ));
    }
    assert!(matches!(
        verifier
            .collect_maintenance(
                fixture.journal.as_ref(),
                &original,
                &roster,
                Instant::now(),
                clock
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(fixture.transport.probes.load(Ordering::Acquire), probes);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_maintenance_policy_lookup_refuses_controller_change_during_native_probe() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let (roster, original) = originals(&fixture, fixture.journal.as_ref()).await;
    let verifier = fixture.verifier();
    let (entered, resume) = fixture.transport.pause_probe(1);
    let mut work = Box::pin(verifier.collect_maintenance(
        fixture.journal.as_ref(),
        &original,
        &roster,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}", result.is_ok()),result=entered=>result.unwrap()}
    let next = fixture
        .journal
        .claim_controller(
            scope(),
            roster.snapshot().head().revision(),
            SessionId::from_bytes([206; 16]),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next.registry(), roster.snapshot().registry());
    resume.send(()).unwrap();
    assert!(work.await.is_err());
    assert_eq!(
        fixture
            .stored(publication.record().unwrap().digest().unwrap())
            .await,
        *publication.record().unwrap()
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_maintenance_policy_lookup_keeps_new_source_acceptance_as_a_policy_gap() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let mut spec = fixture.original.spec().clone();
    spec.request = Digest::from_bytes([253; 32]);
    let mut source = spec.target;
    source.intent_revision = 2;
    spec.source = Some(source);
    spec.target.node = node_id(2);
    spec.target.session = session(2);
    spec.target.intent_revision = 1;
    let accepted = fixture
        .journal
        .accept_enrollment(&spec, clock().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        accepted,
        cellule_host::fleet::FleetEnrollmentAcceptance::New(_)
    ));
    let (roster, original) = originals(&fixture, fixture.journal.as_ref()).await;
    let checks = fixture
        .verifier()
        .collect_maintenance(
            fixture.journal.as_ref(),
            &original,
            &roster,
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].record(), publication.record().unwrap());
    let observation = cellule_host::fleet::FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        original.interval().0,
        clock().unwrap(),
        false,
        super::observation::advertisements(&fixture.directory).await,
        Vec::new(),
    )
    .unwrap()
    .with_role_evacuations(checks, Vec::new())
    .unwrap()
    .with_maintenance_enrollments(original)
    .unwrap()
    .check_maintenance_policies(&roster, clock().unwrap())
    .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert!(!coverage.is_complete());
    assert_eq!(coverage.progress().required, 2);
    assert_eq!(coverage.progress().checked, 1);
    assert_eq!(coverage.progress().pending, 1);
    let pending = coverage
        .obligations()
        .iter()
        .find(|row| row.current().spec() == &spec)
        .unwrap();
    assert!(pending.original().is_none());
    // No native effect was started for this accepted request. Publish the exact
    // exclusion before the native boots and SQLite owner are joined.
    fixture
        .journal
        .refuse_unexecuted_enrollment(&spec, Digest::from_bytes([254; 32]), clock().unwrap())
        .await
        .unwrap();
    drop(capture);
    fixture.finish().await;
}
