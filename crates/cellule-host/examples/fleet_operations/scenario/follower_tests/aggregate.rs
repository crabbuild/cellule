//! Full native capture with managed follower producers and persisted tails.
use super::*;
use cellule_host::fleet::{
    FleetFollowerReferences, FleetNodeInventory, FleetNodeInventoryScan, FleetRoster,
    FleetSnapshotRequest, FleetSnapshotSubject,
};
use cellule_runtime::follower::FollowerLaneState;

pub(super) async fn roster(fixture: &ManagedFixture) -> FleetRoster {
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    FleetRoster::collect(
        fixture.native.journal.as_ref(),
        &snapshot,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap()
}

pub(super) async fn page(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
    index: usize,
    subject: FleetSnapshotSubject,
    sequence: &mut u64,
) -> Arc<cellule_host::fleet::FleetNodeSnapshot> {
    *sequence += 1;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.managed-follower-inventory-test.v1\0");
    hash.update(&sequence.to_be_bytes());
    hash.update(session(index).as_bytes());
    let now = clock().unwrap();
    let request = FleetSnapshotRequest::new(
        roster.snapshot().clone(),
        Digest::from_bytes(*hash.finalize().as_bytes()),
        node_id(index),
        session(index),
        subject,
        1,
        now,
        (now + 5_000).min(roster.snapshot().head().controller().unwrap().expires_at_ms),
    )
    .unwrap();
    fixture.nodes[index].fleet_snapshot(request).await.unwrap()
}

pub(super) async fn collect(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
    index: usize,
    sequence: &mut u64,
) -> FleetNodeInventory {
    let mut scan = FleetNodeInventoryScan::new(roster, node_id(index), session(index)).unwrap();
    while let Some(subject) = scan.next_subject().unwrap() {
        let response = page(fixture, roster, index, subject, sequence).await;
        scan.accept(response.request(), &response, clock().unwrap())
            .unwrap();
    }
    scan.finish().unwrap()
}

pub(super) async fn references(
    fixture: &ManagedFixture,
    roster: &FleetRoster,
    index: usize,
) -> FleetFollowerReferences {
    FleetFollowerReferences::collect(
        &fixture.native.directory,
        roster,
        node_id(index),
        1,
        Instant::now() + Duration::from_secs(5),
        clock,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_aggregate_matches_original_producer_and_foreign_persisted_tails() {
    let fixture = ManagedFixture::new().await;
    let roster = roster(&fixture).await;
    let mut sequence = 0;
    let mut inventories = Vec::new();
    let mut authorities = Vec::new();
    for index in 0..3 {
        let inventory = collect(&fixture, &roster, index, &mut sequence).await;
        inventory.validate_enrollments(&roster).unwrap();
        let logs = references(&fixture, &roster, index).await;
        logs.validate_enrollments(&roster).unwrap();
        if index == 0 {
            assert_eq!(inventory.cells().len(), 1);
            assert!(inventory.bindings().follower_producer);
            assert!(inventory.bindings().durability_supervisor);
            assert_eq!(inventory.node_log(), Some((session(0), node_id(0), 1)));
            assert_eq!(inventory.follower_enrollments().len(), 1);
            let original = fixture
                .native
                .node
                .follower_enrollment_completion(1)
                .unwrap()
                .unwrap();
            let observed = &inventory.follower_enrollments()[0];
            assert_eq!(
                observed.attempt,
                original.attempt.evidence_digest().unwrap()
            );
            assert!(observed.delivered && observed.native_started);
            for (member, original) in observed.members.iter().zip(&original.members) {
                assert_eq!(member.spec, original.spec);
                assert_eq!(member.accepted, original.accepted);
                assert_eq!(member.event, original.event);
                assert!(member.published);
            }
            assert!(logs.entries().is_empty());
        } else {
            // Zero writers does not remove the other owner's durable obligation.
            assert!(inventory.cells().is_empty());
            assert_eq!(inventory.follower_store_state(), Some((1, 0)));
            assert_eq!(inventory.follower_lanes().len(), 1);
            let lane = &inventory.follower_lanes()[0];
            assert_eq!(
                (lane.leader, lane.epoch, lane.state),
                (session(0), 1, FollowerLaneState::Open)
            );
            assert_eq!(logs.entries().len(), 1);
            let authority = &logs.entries()[0];
            assert_eq!(authority.leader, lane.leader);
            assert_eq!(authority.log.epoch(), lane.epoch);
            assert!(authority.log.active());
            assert!(authority.log.members().contains(&node_id(index)));
        }
        inventories.push(inventory);
        authorities.push(logs);
    }
    for authority in &mut authorities {
        authority
            .recheck(
                &fixture.native.directory,
                &roster,
                1,
                Instant::now() + Duration::from_secs(5),
                clock,
            )
            .await
            .unwrap();
    }
    for (index, inventory) in inventories.iter_mut().enumerate() {
        let mut recheck = inventory.recheck();
        while let Some(subject) = recheck.next_subject().unwrap() {
            let response = page(&fixture, &roster, index, subject, &mut sequence).await;
            recheck
                .accept(response.request(), &response, clock().unwrap())
                .unwrap();
        }
        recheck.finish().unwrap();
    }
    roster
        .confirm(
            fixture.native.journal.as_ref(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    let value = fixture
        .handle
        .query(64, 64, |connection| {
            let value: i64 =
                connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(value, 29i64.to_be_bytes());
    drop(inventories);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_rotation_failure_preserves_original_error_and_foreign_obligation() {
    let fixture = ManagedFixture::new().await;
    fixture.handle.drain().await.unwrap();
    fixture
        .native
        .transport
        .lose_retire
        .store(true, Ordering::Release);
    let (entered, resume) = fixture.native.transport.pause_retirement_retry();
    let request = fixture.native.node.request_node_log_rotation(1).unwrap();
    captured(entered).await;
    let roster = roster(&fixture).await;
    let mut sequence = 0;
    let source = collect(&fixture, &roster, 0, &mut sequence).await;
    source.validate_enrollments(&roster).unwrap();
    let original = fixture
        .native
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    let observed = &source.follower_enrollments()[0];
    assert!(Arc::ptr_eq(
        observed.execution_error.as_ref().unwrap(),
        original.execution_error.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        observed.retirement.as_ref().unwrap(),
        original.retirement.as_ref().unwrap()
    ));
    assert!(!observed.native_closed);
    assert!(observed.retirement.as_ref().unwrap().confirmed().is_err());
    let receiver = collect(&fixture, &roster, 1, &mut sequence).await;
    receiver.validate_enrollments(&roster).unwrap();
    assert_eq!(
        receiver.follower_lanes()[0].state,
        FollowerLaneState::Retired
    );
    let mut logs = references(&fixture, &roster, 1).await;
    logs.validate_enrollments(&roster).unwrap();
    assert_eq!(logs.entries().len(), 1);
    assert_eq!(fixture.native.authority.attempts.load(Ordering::Acquire), 0);
    let interval = logs.interval();
    fixture
        .native
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    resume.send(()).unwrap();
    until(|| request.observe().unwrap().phase() == cellule_host::NodeLogRotationPhase::Recruiting)
        .await;
    assert!(request.observe().unwrap().retirement().is_some());
    // This fixed fixture has no spare ensemble. Retirement does not invent a
    // replacement or erase the retained canonical binding while recruitment waits.
    assert_eq!(
        fixture
            .native
            .node
            .runtime()
            .node_durability()
            .unwrap()
            .1
            .identity()
            .unwrap(),
        (session(0), node_id(0), 1)
    );
    assert!(
        logs.recheck(
            &fixture.native.directory,
            &roster,
            1,
            Instant::now() + Duration::from_secs(5),
            clock
        )
        .await
        .is_err()
    );
    assert_eq!(logs.interval(), interval);
    assert_eq!(logs.entries().len(), 1);
    let after_roster = self::roster(&fixture).await;
    let after = references(&fixture, &after_roster, 1).await;
    assert!(after.entries().is_empty());
    let inventory = collect(&fixture, &after_roster, 1, &mut sequence).await;
    inventory.validate_enrollments(&after_roster).unwrap();
    assert_eq!(inventory.follower_store_state(), Some((0, 0)));
    assert_eq!(
        inventory.follower_lanes()[0].state,
        FollowerLaneState::Retired
    );
    drop(request);
    drop(source);
    drop(receiver);
    drop(inventory);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authority_recheck_rejects_real_object_coverage_even_with_unchanged_topology() {
    let fixture = ManagedFixture::new().await;
    let roster = roster(&fixture).await;
    let mut logs = references(&fixture, &roster, 1).await;
    let before = fixture
        .native
        .directory
        .follower_logs_page(node_id(1), None, 1, clock().unwrap())
        .await
        .unwrap();
    let original = logs.entries()[0].log.tiered_through();
    let now = clock().unwrap();
    fixture
        .handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([2; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([2; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute_batch("UPDATE counter SET value = 41")?;
                Ok(HandlerOutcome::Success(41i64.to_be_bytes().to_vec()))
            },
        )
        .await
        .unwrap();
    // Canonical drain publishes the accepted mutation and closes its SQLite
    // owner. No invented coverage watermark is written by this test.
    fixture.handle.drain().await.unwrap();
    let after = fixture
        .native
        .directory
        .follower_logs_page(node_id(1), None, 1, clock().unwrap())
        .await
        .unwrap();
    assert_eq!(before.topology(), after.topology());
    assert!(after.entries()[0].log.tiered_through() > original);
    let interval = logs.interval();
    assert!(matches!(
        logs.recheck(
            &fixture.native.directory,
            &roster,
            1,
            Instant::now() + Duration::from_secs(5),
            clock
        )
        .await,
        Err(Error::Node("authoritative follower inventory changed"))
    ));
    assert_eq!(logs.interval(), interval);
    assert_eq!(logs.entries()[0].log.tiered_through(), original);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authority_scan_rejects_deadlines_regressed_clocks_and_changed_liveness() {
    let fixture = ManagedFixture::new().await;
    let roster = roster(&fixture).await;
    for limit in [0, 129] {
        assert!(
            FleetFollowerReferences::collect(
                &fixture.native.directory,
                &roster,
                node_id(1),
                limit,
                Instant::now() + Duration::from_secs(3),
                clock
            )
            .await
            .is_err()
        );
    }
    assert!(matches!(
        FleetFollowerReferences::collect(
            &fixture.native.directory,
            &roster,
            node_id(1),
            1,
            Instant::now(),
            || panic!("expired scan called the clock")
        )
        .await,
        Err(Error::Deadline)
    ));
    let now = clock().unwrap();
    let mut calls = 0;
    assert!(
        FleetFollowerReferences::collect(
            &fixture.native.directory,
            &roster,
            node_id(1),
            1,
            Instant::now() + Duration::from_secs(3),
            || {
                calls += 1;
                Ok(now - i64::from(calls > 1))
            }
        )
        .await
        .is_err()
    );
    let owner = fixture
        .native
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    let expiry = owner.advertisement().expires_at_ms();
    // Controlled directory time exercises expiry without fencing or restamping
    // the native processes, which are still joined at their actual clock.
    let mut logs = FleetFollowerReferences::collect(
        &fixture.native.directory,
        &roster,
        node_id(1),
        1,
        Instant::now() + Duration::from_secs(3),
        || Ok(expiry - 1),
    )
    .await
    .unwrap();
    assert_eq!(
        logs.entries()[0].leader_state,
        cellule_runtime::node::LogLeaderState::Live
    );
    let before = fixture
        .native
        .directory
        .follower_logs_page(node_id(1), None, 1, expiry - 1)
        .await
        .unwrap();
    let after = fixture
        .native
        .directory
        .follower_logs_page(node_id(1), None, 1, expiry + 1)
        .await
        .unwrap();
    assert_eq!(before.topology(), after.topology());
    assert_eq!(
        after.entries()[0].leader_state,
        cellule_runtime::node::LogLeaderState::Expired
    );
    assert!(matches!(
        logs.recheck(
            &fixture.native.directory,
            &roster,
            1,
            Instant::now() + Duration::from_secs(3),
            || Ok(expiry + 1)
        )
        .await,
        Err(Error::Node("authoritative follower inventory changed"))
    ));
    fixture.finish().await;
}
