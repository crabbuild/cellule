use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_retains_full_ensembles_after_independent_reconstruction() {
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    assert_eq!(result.confirmed().unwrap().native().len(), 3);
    assert_eq!(record.retired(), capture.retired_members());
    assert_eq!(
        record.covered_through(),
        capture.rotation().retirement().barrier().covered_through()
    );
    assert_eq!(
        record.original_key(),
        capture.original().spec().key().unwrap()
    );
    assert_eq!(record.interval(), capture.interval());
    assert_eq!(record.replacements().len(), 2);
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        snapshot.registry().revision(),
        capture.snapshot().registry().revision() + 1
    );
    let replay = publish(&fixture, &capture, policy).await;
    assert_eq!(replay.record().unwrap(), record);
    replay.confirmed().unwrap();
    assert_eq!(
        fixture.native.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    let client = client(&fixture).await;
    verifier(&fixture)
        .recheck(&client, record, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(
        client
            .load_follower_evacuation(scope(), record.digest().unwrap())
            .await
            .unwrap()
            .unwrap(),
        *record
    );
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_refreshes_new_epoch_after_original_local_receipt_eviction() {
    let (fixture, capture, policy) = setup_epochs(3).await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    assert!(
        !result
            .confirmed()
            .unwrap()
            .authority()
            .log()
            .unwrap()
            .active()
    );
    let request = fixture.native.node.request_node_log_rotation(2).unwrap();
    until(|| request.observe().unwrap().phase() == cellule_host::NodeLogRotationPhase::Completed)
        .await;
    assert!(
        fixture
            .native
            .node
            .node_log_rotation_request(1)
            .unwrap()
            .is_none()
    );
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    let refreshed = verifier(&fixture)
        .refresh(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(refreshed.record().replacement_epoch(), 3);
    let updated = FleetFollowerEvacuationPublication::publish_refreshed(
        &refreshed,
        fixture.native.journal.as_ref(),
        &verifier(&fixture),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    updated.confirmed().unwrap();
    assert_eq!(updated.record().unwrap().retired(), record.retired());
    assert_eq!(
        updated.record().unwrap().covered_through(),
        record.covered_through()
    );
    assert_eq!(
        updated.record().unwrap().original_digest(),
        record.original_digest()
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_refreshes_current_policy_without_repeating_retirement() {
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let old = result.record().unwrap();
    result.confirmed().unwrap();
    let attempts = fixture.native.authority.attempts.load(Ordering::Acquire);
    let original = capture.retired().clone();
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .set_follower_replacement_policy(
            &snapshot,
            FollowerReplacementPolicy::new(scope(), 2, 1).unwrap(),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), old, deadline(), clock)
            .await
            .is_err()
    );
    let refreshed = verifier(&fixture)
        .refresh(fixture.native.journal.as_ref(), old, deadline(), clock)
        .await
        .unwrap();
    let updated = FleetFollowerEvacuationPublication::publish_refreshed(
        &refreshed,
        fixture.native.journal.as_ref(),
        &verifier(&fixture),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    updated.confirmed().unwrap();
    let new = updated.record().unwrap();
    assert_eq!(new.retired(), old.retired());
    assert_eq!(new.original_digest(), old.original_digest());
    assert_eq!(new.policy().revision(), 2);
    assert_ne!(new.digest().unwrap(), old.digest().unwrap());
    assert_eq!(latest(&fixture, old).await, *new);
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        fixture
            .native
            .journal
            .persist_follower_evacuation(capture.snapshot(), old, clock().unwrap())
            .await
            .unwrap(),
        *old
    );
    assert_eq!(
        fixture.native.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    assert_eq!(latest(&fixture, old).await, *new);
    assert!(
        publish(&fixture, &capture, policy)
            .await
            .confirmed()
            .is_err()
    );
    assert_eq!(stored(&fixture, old.digest().unwrap()).await, *old);
    assert_eq!(capture.retired(), &original);
    assert_eq!(
        fixture.native.authority.attempts.load(Ordering::Acquire),
        attempts
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_withdrawn_receiver_cannot_revalidate() {
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    fixture.nodes[3].shutdown().await.unwrap();
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert!(
        verifier(&fixture)
            .refresh(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert_eq!(stored(&fixture, record.digest().unwrap()).await, *record);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_closing_and_deadline_refresh_preserve_original_rows() {
    use cellule_runtime::fleet::operations::{DrainEvidence, JournalTransition, MaintenanceEvent};
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    crate::scenario::commit_test_role_settlement(&fixture.native.journal, clock().unwrap())
        .await
        .unwrap();
    let operation = snapshot.head().maintenance().unwrap();
    let after = fixture
        .native
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(DrainEvidence {
                node: operation.node(),
                session: operation.session(),
                remaining_cells: 0,
                unresolved_attempts: 0,
                relocated: true,
                readers_settled: true,
                followers_settled: true,
                facilities_closed: false,
                stopped: false,
                withdrawn: false,
            })),
        )
        .await
        .unwrap();
    verifier(&fixture)
        .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let after = fixture
        .native
        .journal
        .compare_exchange(
            &after,
            after.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::ExtendDeadline(
                record.operation().deadline_ms() + 10_000,
            )),
        )
        .await
        .unwrap();
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    let refreshed = verifier(&fixture)
        .refresh(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(
        refreshed.record().operation(),
        after.head().maintenance().unwrap()
    );
    let updated = FleetFollowerEvacuationPublication::publish_refreshed(
        &refreshed,
        fixture.native.journal.as_ref(),
        &verifier(&fixture),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    updated.confirmed().unwrap();
    assert_eq!(updated.record().unwrap().retired(), record.retired());
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_requires_current_barrier_stored_history_and_fresh_clock() {
    let (fixture, capture, policy) = setup().await;
    let record = capture.durable_record(policy).unwrap();
    assert!(
        verifier(&fixture)
            .refresh(fixture.native.journal.as_ref(), &record, deadline(), clock)
            .await
            .is_err()
    );
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .set_follower_replacement_policy(
            &snapshot,
            FollowerReplacementPolicy::new(scope(), 2, 2).unwrap(),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        FleetFollowerEvacuationPublication::publish(
            &capture,
            policy,
            fixture.native.journal.as_ref(),
            &verifier(&fixture),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    assert!(
        fixture
            .native
            .journal
            .load_follower_evacuation(scope(), record.digest().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let (candidate_policy, snapshot) = {
        let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
        (
            fixture
                .native
                .journal
                .follower_replacement_policy(&snapshot)
                .await
                .unwrap()
                .unwrap(),
            snapshot,
        )
    };
    let fresh = fixture
        .native
        .node
        .follower_evacuation(
            capture.original(),
            snapshot.head().maintenance().unwrap(),
            2,
            deadline(),
        )
        .await
        .unwrap();
    let result = publish(&fixture, &fresh, candidate_policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    let now = clock().unwrap();
    let mut n = 0;
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), || {
                n += 1;
                Ok(now - n)
            })
            .await
            .is_err()
    );
    assert!(
        verifier(&fixture)
            .recheck(
                fixture.native.journal.as_ref(),
                record,
                Instant::now(),
                clock
            )
            .await
            .is_err()
    );
    drop((fresh, capture));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_closed_original_leader_requires_recovery_instead_of_refresh() {
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    fixture.native.node.shutdown().await.unwrap();
    assert!(
        verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert!(
        verifier(&fixture)
            .refresh(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert_eq!(stored(&fixture, record.digest().unwrap()).await, *record);
    drop(capture);
    fixture.finish().await;
}
