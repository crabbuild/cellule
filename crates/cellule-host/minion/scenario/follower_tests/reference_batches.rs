//! Shared foreign traversal keeps the complete roster and global recheck barrier.
use super::*;
use cellule_host::fleet::FleetFollowerReferences;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_reference_windows_match_individual_complete_captures() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let members = [node_id(2), node_id(0), node_id(1)];
    let mut batch = FleetFollowerReferences::collect_all(
        &fixture.native.directory,
        &roster,
        &members,
        1,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(batch.len(), members.len());
    for (references, member) in batch.iter().zip(members) {
        assert_eq!(references.member(), member);
        let single = FleetFollowerReferences::collect(
            &fixture.native.directory,
            &roster,
            member,
            1,
            deadline(),
            clock,
        )
        .await
        .unwrap();
        assert_eq!(references.entries(), single.entries());
        references.validate_enrollments(&roster).unwrap();
    }
    assert!(batch[1].entries().is_empty());
    assert_eq!(batch[0].entries().len(), 1);
    assert_eq!(batch[2].entries().len(), 1);
    FleetFollowerReferences::recheck_all(
        &mut batch,
        &fixture.native.directory,
        &roster,
        1,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    for members in [
        vec![],
        vec![node_id(1), node_id(1)],
        vec![NodeId::from_bytes([250; 16])],
    ] {
        assert!(
            FleetFollowerReferences::collect_all(
                &fixture.native.directory,
                &roster,
                &members,
                1,
                deadline(),
                clock
            )
            .await
            .is_err()
        );
    }
    drop(batch);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_shared_recheck_invalidates_every_original_without_restamping() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = coverage::captures(&fixture, &roster).await;
    coverage::native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    coverage::foreign_rechecks(&fixture, &roster, &mut foreign).await;
    assert!(coverage::check(&roster, &native, &foreign).is_ok());
    let original = foreign
        .iter()
        .map(|references| (references.interval(), references.entries().to_vec()))
        .collect::<Vec<_>>();
    assert!(
        FleetFollowerReferences::recheck_all(
            &mut foreign,
            &fixture.native.directory,
            &roster,
            1,
            Instant::now(),
            clock
        )
        .await
        .is_err()
    );
    for (references, (interval, entries)) in foreign.iter().zip(&original) {
        assert_eq!(&references.interval(), interval);
        assert_eq!(references.entries(), entries);
    }
    assert!(coverage::check(&roster, &native, &foreign).is_err());
    for index in 0..foreign.len() {
        foreign[index]
            .recheck(&fixture.native.directory, &roster, 1, deadline(), clock)
            .await
            .unwrap();
        assert_eq!(
            coverage::check(&roster, &native, &foreign).is_ok(),
            index + 1 == foreign.len(),
            "every member needs a new confirmation after the failed batch"
        );
    }
    assert!(coverage::check(&roster, &native, &foreign).is_ok());
    // Exact rows include liveness even when topology/cursor bytes are unchanged.
    // This negative logical-clock capture grants no lease or takeover authority.
    let owner = fixture
        .native
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    let expired_at = owner.advertisement().expires_at_ms() + 1;
    let retried = foreign
        .iter()
        .map(FleetFollowerReferences::interval)
        .collect::<Vec<_>>();
    let result = FleetFollowerReferences::recheck_all(
        &mut foreign,
        &fixture.native.directory,
        &roster,
        1,
        deadline(),
        || Ok(expired_at),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Node("authoritative follower inventory changed"))
    ));
    for ((references, (interval, entries)), retried) in foreign.iter().zip(&original).zip(retried) {
        // The successful retry may extend the interval; this failed recheck
        // must retain that retry's end instead of stamping the future clock.
        assert!(references.interval().1 < expired_at);
        assert_eq!(references.interval(), retried);
        assert_eq!(references.interval().0, interval.0);
        assert_eq!(references.entries(), entries);
    }
    assert!(coverage::check(&roster, &native, &foreign).is_err());
    coverage::foreign_rechecks(&fixture, &roster, &mut foreign).await;
    roster
        .confirm(fixture.native.journal.as_ref(), deadline())
        .await
        .unwrap();
    assert!(coverage::check(&roster, &native, &foreign).is_ok());
    drop((native, foreign));
    fixture.finish().await;
}
