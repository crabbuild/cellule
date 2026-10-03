//! Complete cross-node graph, including enrolled lanes before their first append.
use super::*;
use cellule_host::fleet::{
    FleetFollowerReferences, FleetNodeInventory, FleetRoleCoverage, FleetRoster,
};

pub(super) async fn captures(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
) -> (Vec<FleetNodeInventory>, Vec<FleetFollowerReferences>, u64) {
    let mut sequence = 0;
    let mut native = Vec::new();
    let mut foreign = Vec::new();
    for index in 0..fixture.nodes.len() {
        native.push(aggregate::collect(fixture, roster, index, &mut sequence).await);
        foreign.push(aggregate::references(fixture, roster, index).await);
    }
    (native, foreign, sequence)
}

pub(super) async fn native_rechecks(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
    native: &mut [FleetNodeInventory],
    sequence: &mut u64,
) {
    for (index, inventory) in native.iter_mut().enumerate() {
        let mut recheck = inventory.recheck();
        while let Some(subject) = recheck.next_subject().unwrap() {
            let page = aggregate::page(fixture, roster, index, subject, sequence).await;
            recheck
                .accept(page.request(), &page, clock().unwrap())
                .unwrap();
        }
        recheck.finish().unwrap();
    }
}

pub(super) async fn foreign_rechecks(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
    foreign: &mut [FleetFollowerReferences],
) {
    for references in foreign {
        references
            .recheck(
                &fixture.native.directory,
                roster,
                1,
                Instant::now() + Duration::from_secs(5),
                clock,
            )
            .await
            .unwrap();
    }
}

pub(super) fn check(
    roster: &FleetRoster,
    native: &[FleetNodeInventory],
    foreign: &[FleetFollowerReferences],
) -> cellule_runtime::Result<FleetRoleCoverage> {
    FleetRoleCoverage::check(
        roster,
        &native.iter().collect::<Vec<_>>(),
        &foreign.iter().collect::<Vec<_>>(),
        clock().unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_role_coverage_requires_global_rechecks_and_is_order_independent() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &roster).await;
    assert!(check(&roster, &native, &foreign).is_err());
    tokio::time::sleep(Duration::from_millis(2)).await;
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    assert!(check(&roster, &native, &foreign).is_err());
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    let coverage = check(&roster, &native, &foreign).unwrap();
    roster
        .confirm(
            fixture.native.journal.as_ref(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(coverage.native_boots(), 3);
    assert_eq!(coverage.physical_nodes(), 3);
    assert_eq!(coverage.pending_enrollments(), 0);
    assert_eq!(coverage.snapshot(), roster.snapshot());
    assert_eq!(coverage.roster_digest(), roster.digest().unwrap());
    native.reverse();
    foreign.reverse();
    assert_eq!(
        check(&roster, &native, &foreign).unwrap().digest(),
        coverage.digest()
    );
    assert!(check(&roster, &native[..2], &foreign).is_err());
    assert!(check(&roster, &native, &foreign[..2]).is_err());
    assert!(
        FleetRoleCoverage::check(
            &roster,
            &[&native[0], &native[0], &native[2]],
            &foreign.iter().collect::<Vec<_>>(),
            clock().unwrap()
        )
        .is_err()
    );
    assert!(
        FleetRoleCoverage::check(
            &roster,
            &native.iter().collect::<Vec<_>>(),
            &[&foreign[0], &foreign[0], &foreign[2]],
            clock().unwrap()
        )
        .is_err()
    );
    drop((native, foreign, coverage));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_global_native_recheck_cannot_reuse_an_earlier_confirmation() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &roster).await;
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    assert!(check(&roster, &native, &foreign).is_ok());
    let original = native[1].interval();
    drop(native[1].recheck());
    assert_eq!(native[1].interval(), original);
    assert!(check(&roster, &native, &foreign).is_err());
    tokio::time::sleep(Duration::from_millis(2)).await;
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    // Foreign checks must follow this latest complete native round.
    assert!(check(&roster, &native, &foreign).is_err());
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    assert!(check(&roster, &native, &foreign).is_ok());
    drop((native, foreign));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_foreign_recheck_preserves_rows_and_invalidates_coverage() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &roster).await;
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    assert!(check(&roster, &native, &foreign).is_ok());
    let interval = foreign[1].interval();
    let rows = foreign[1].entries().to_vec();
    assert!(
        foreign[1]
            .recheck(&fixture.native.directory, &roster, 1, Instant::now(), clock)
            .await
            .is_err()
    );
    assert_eq!(foreign[1].interval(), interval);
    assert_eq!(foreign[1].entries(), rows);
    assert!(check(&roster, &native, &foreign).is_err());
    drop((native, foreign));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_role_coverage_matches_enrolled_empty_lanes_to_original_cross_node_producer() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    evacuation::maintenance(&fixture).await;
    evacuation::spare(&fixture).await;
    evacuation::rotated(&fixture).await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &roster).await;
    for index in [2, 3] {
        assert!(
            !native[index]
                .follower_lanes()
                .iter()
                .any(|lane| lane.epoch == 2)
        );
        assert!(matches!(
            native[index].validate_enrollments(&roster),
            Err(Error::Control(
                "Established follower has no persisted native lane"
            ))
        ));
        assert_eq!(foreign[index].entries().len(), 1);
        assert_eq!(foreign[index].entries()[0].log.epoch(), 2);
    }
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    let coverage = check(&roster, &native, &foreign).unwrap();
    roster
        .confirm(
            fixture.native.journal.as_ref(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(coverage.native_boots(), 4);
    assert_eq!(coverage.pending_enrollments(), 0);
    assert!(
        native[1]
            .follower_lanes()
            .iter()
            .all(|lane| lane.state == cellule_runtime::follower::FollowerLaneState::Retired)
    );
    // No missing local lane became absence: both actual Established epoch-two
    // responsibilities and the configured source binding remain visible.
    assert_eq!(native[0].node_log(), Some((session(0), node_id(0), 2)));
    assert_eq!(native[0].follower_enrollments()[0].members.len(), 2);
    assert!(
        FleetRoleCoverage::check(
            &roster,
            &[&native[1], &native[2], &native[3]],
            &foreign.iter().collect::<Vec<_>>(),
            clock().unwrap()
        )
        .is_err()
    );
    drop((native, foreign, coverage));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn earlier_foreign_checks_cannot_cover_a_later_global_native_round() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &roster).await;
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    assert!(check(&roster, &native, &foreign).is_err());
    foreign_rechecks(&fixture, &roster, &mut foreign).await;
    assert!(check(&roster, &native, &foreign).is_ok());
    drop((native, foreign));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_full_roster_cannot_reuse_original_role_captures_or_confirmation() {
    let fixture = ManagedFixture::new().await;
    let original = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = captures(&fixture, &original).await;
    native_rechecks(&fixture, &original, &mut native, &mut sequence).await;
    foreign_rechecks(&fixture, &original, &mut foreign).await;
    let coverage = check(&original, &native, &foreign).unwrap();
    evacuation::maintenance(&fixture).await;
    let changed = aggregate::roster(&fixture).await;
    assert_ne!(changed.digest().unwrap(), original.digest().unwrap());
    assert!(check(&changed, &native, &foreign).is_err());
    assert!(
        original
            .confirm(
                fixture.native.journal.as_ref(),
                Instant::now() + Duration::from_secs(5)
            )
            .await
            .is_err()
    );
    assert_eq!(coverage.snapshot(), original.snapshot());
    assert!(
        FleetRoleCoverage::check(
            &original,
            &native.iter().collect::<Vec<_>>(),
            &foreign.iter().collect::<Vec<_>>(),
            clock().unwrap() + 30_001
        )
        .is_err()
    );
    drop((native, foreign, coverage));
    fixture.finish().await;
}
