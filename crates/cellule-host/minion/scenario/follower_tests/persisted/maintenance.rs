//! Automatic full-set lookup of actual native rotated ensembles.
use super::*;
use cellule_host::fleet::{FleetMaintenanceEnrollments, FleetRoster};

async fn originals(journal: &SqliteJournal) -> (FleetRoster, FleetMaintenanceEnrollments) {
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(journal, &snapshot, deadline())
        .await
        .unwrap();
    let original = FleetMaintenanceEnrollments::collect(journal, &roster, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(original.original().operation().node(), node_id(1));
    (roster, original)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_maintenance_policy_lookup_discovers_latest_ensemble_after_restart() {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let client = client(&fixture).await;
    let (roster, original) = originals(&client).await;
    let before = clock().unwrap();
    let checks = verifier(&fixture)
        .collect_maintenance(&client, &original, &roster, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].record(), publication.record().unwrap());
    assert_eq!(checks[0].snapshot(), roster.snapshot());
    assert_eq!(checks[0].roster_digest(), roster.digest().unwrap());
    assert!(checks[0].interval().0 >= before);
    assert_eq!(checks[0].native().len(), 3);
    assert_eq!(
        client.load_snapshot(scope()).await.unwrap(),
        *roster.snapshot()
    );
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_maintenance_policy_lookup_preserves_missing_history() {
    let (fixture, capture, _) = setup().await;
    let (roster, original) = originals(fixture.native.journal.as_ref()).await;
    let checks = verifier(&fixture)
        .collect_maintenance(
            fixture.native.journal.as_ref(),
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
        super::observation::advertisements(&fixture).await,
        Vec::new(),
    )
    .unwrap()
    .with_role_evacuations(Vec::new(), checks)
    .unwrap()
    .with_maintenance_enrollments(original)
    .unwrap()
    .check_maintenance_policies(&roster, clock().unwrap())
    .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert!(!coverage.is_complete());
    assert_eq!(coverage.progress().required, 1);
    assert_eq!(coverage.progress().missing_policy, 1);
    assert_eq!(
        fixture.native.journal.load_snapshot(scope()).await.unwrap(),
        *roster.snapshot()
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_maintenance_policy_lookup_refuses_an_earlier_head_and_invalid_capture_clock() {
    let (fixture, capture, policy) = setup().await;
    publish(&fixture, &capture, policy).await;
    let (old, original) = originals(fixture.native.journal.as_ref()).await;
    let next = fixture
        .native
        .journal
        .claim_controller(
            scope(),
            old.snapshot().head().revision(),
            session(9),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next.registry(), old.snapshot().registry());
    let roster = FleetRoster::collect(fixture.native.journal.as_ref(), &next, deadline())
        .await
        .unwrap();
    let verifier = verifier(&fixture);
    assert!(matches!(
        verifier
            .collect_maintenance(
                fixture.native.journal.as_ref(),
                &original,
                &roster,
                deadline(),
                clock
            )
            .await,
        Err(Error::Fenced)
    ));
    let (_, original) = originals(fixture.native.journal.as_ref()).await;
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
                    fixture.native.journal.as_ref(),
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
                fixture.native.journal.as_ref(),
                &original,
                &roster,
                Instant::now(),
                clock
            )
            .await,
        Err(Error::Deadline)
    ));
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_maintenance_policy_lookup_refuses_policy_change_during_native_capture() {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let (roster, original) = originals(fixture.native.journal.as_ref()).await;
    let transport = Arc::new(Snapshots::new(&fixture));
    let verifier =
        FleetFollowerEvacuationVerifier::new(fixture.native.directory.clone(), transport.clone());
    let (entered, resume) = transport.pause_next();
    let mut work = Box::pin(verifier.collect_maintenance(
        fixture.native.journal.as_ref(),
        &original,
        &roster,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}", result.is_ok()),result=entered=>result.unwrap()}
    fixture
        .native
        .journal
        .set_follower_replacement_policy(
            roster.snapshot(),
            FollowerReplacementPolicy::new(scope(), policy.revision() + 1, 1).unwrap(),
            clock().unwrap(),
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(work.await.is_err());
    assert_eq!(
        stored(&fixture, publication.record().unwrap().digest().unwrap()).await,
        *publication.record().unwrap()
    );
    drop(capture);
    fixture.finish().await;
}
