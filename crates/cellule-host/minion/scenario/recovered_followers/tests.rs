use super::*;

#[tokio::test]
async fn recovered_publication_cancelled_waiter_joins_backend_work_then_recaptures_original_records()
 {
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let capture = fixture.capture().await;
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! {
        result = &mut work => panic!("publication completed before held reply: {}", result.is_ok()),
        result = entered => result.unwrap(),
    }
    drop(work);
    assert!(resume.send(()).is_err());
    // The reference adapter joins every accepted blocking transaction before
    // closing its SQLite handle, even though its caller no longer awaits it.
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let committed = roster
        .enrollments()
        .iter()
        .filter(|row| {
            matches!(row.spec().role, EnrollmentRole::Follower { .. })
                && row.status() == EnrollmentStatus::Retired
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!committed.is_empty());
    let replay = FleetRecoveredFollowerRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &fixture.sealed,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let repeated = replay
        .publish(&journal, &fixture.directory, session(1), deadline(), || {
            Ok(CHECK + 10)
        })
        .await
        .unwrap();
    let closure = repeated.confirmed().unwrap();
    for original in committed {
        assert_eq!(
            closure
                .members()
                .iter()
                .find(|row| row.spec() == original.spec())
                .unwrap(),
            &original
        );
    }
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovered_publication_final_barrier_rejects_a_delayed_new_original_epoch_request() {
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let capture = fixture.capture().await;
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! {
        result = &mut work => panic!("publication completed before held reply: {}", result.is_ok()),
        result = entered => result.unwrap(),
    }
    let mut delayed = fixture.originals[0].spec().clone();
    delayed.request = Digest::from_bytes([98; 32]);
    fixture
        .journal
        .accept_enrollment(&delayed, CHECK)
        .await
        .unwrap();
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    assert!(
        result
            .members()
            .iter()
            .all(|member| member.result().is_ok())
    );
    assert!(result.confirmed().is_err());
    assert!(matches!(
        result.closure_error().as_deref(),
        Some(Error::Control(
            "recovered follower enrollment set is incomplete"
        ))
    ));
    let current = fixture
        .journal
        .load_enrollment(scope(), delayed.key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status(), EnrollmentStatus::Pending);
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovered_publication_settles_every_original_request_and_replays_after_native_collection()
{
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let capture = fixture.capture().await;
    assert_eq!(capture.leader_node(), node_id(0));
    assert_eq!(capture.members(), fixture.originals);
    let published = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    let closure = published.confirmed().unwrap();
    assert_eq!(closure.leader_node(), node_id(0));
    assert_eq!(closure.retired().session(), session(0));
    assert_eq!(closure.members().len(), 2);
    let records = closure.members().to_vec();
    for (original, retired) in fixture.originals.iter().zip(&records) {
        assert_eq!(retired.spec(), original.spec());
        assert_eq!(retired.accepted_at_ms(), original.accepted_at_ms());
        assert_eq!(
            retired.established_evidence(),
            original.established_evidence()
        );
        assert_eq!(retired.status(), EnrollmentStatus::Retired);
        assert_eq!(retired.updated_at_ms(), CHECK);
    }
    assert!(
        !fixture
            .directory
            .log_epoch_referenced(session(0), 4)
            .await
            .unwrap()
    );
    for store in &fixture.stores {
        let lanes = store.retired_lanes(i64::MAX, 2).await.unwrap();
        assert_eq!(lanes.len(), 1);
        assert!(
            store
                .remove_retired(lanes[0], lanes[0].retired_at_ms())
                .await
                .unwrap()
        );
        assert_eq!(store.retained_bytes(), 0);
    }
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    assert!(
        roster
            .required_boots()
            .iter()
            .any(|boot| boot.node == node_id(0) && boot.session == session(0))
    );
    let replay = FleetRecoveredFollowerRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &fixture.sealed,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let repeated = replay
        .publish(&journal, &fixture.directory, session(1), deadline(), || {
            Ok(CHECK + 10)
        })
        .await
        .unwrap();
    let repeated = repeated.confirmed().unwrap();
    assert_eq!(repeated.members(), records);
    assert_eq!(repeated.digest(), closure.digest());
    assert_eq!(repeated.snapshot(), closure.snapshot());
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovered_publication_joins_healthy_siblings_and_retains_lost_reply_across_reconstruction()
{
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let capture = fixture.capture().await;
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! {
        result = &mut work => panic!("publication completed before held reply: {}", result.is_ok()),
        result = entered => result.unwrap(),
    }
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    assert_eq!(result.members().len(), 2);
    let failed = result
        .members()
        .iter()
        .position(|member| member.result().is_err())
        .unwrap();
    assert_eq!(
        result
            .members()
            .iter()
            .filter(|member| member.result().is_err())
            .count(),
        1
    );
    let original_error = result.members()[failed].result().unwrap_err();
    let source = std::error::Error::source(original_error.as_ref()).unwrap();
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Other
    );
    assert!(
        source
            .to_string()
            .contains("injected lost enrollment reply after durable commit")
    );
    assert!(result.members()[1 - failed].result().is_ok());
    assert!(result.confirmed().is_err());
    assert!(Arc::ptr_eq(
        &original_error,
        &result.members()[failed].result().unwrap_err()
    ));
    assert!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                session(1),
                deadline(),
                || Ok(CHECK + 1)
            )
            .await
            .is_err()
    );
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let replay = FleetRecoveredFollowerRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &fixture.sealed,
        session(1),
        deadline(),
        || Ok(CHECK + 2),
    )
    .await
    .unwrap();
    let repeated = replay
        .publish(&journal, &fixture.directory, session(1), deadline(), || {
            Ok(CHECK + 2)
        })
        .await
        .unwrap();
    let closure = repeated.confirmed().unwrap();
    assert!(
        closure
            .members()
            .iter()
            .all(|row| row.updated_at_ms() == CHECK)
    );
    assert!(Arc::ptr_eq(
        &original_error,
        &result.members()[failed].result().unwrap_err()
    ));
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovered_publication_refuses_unretired_authority_stale_rosters_and_expired_claimants() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    assert!(
        FleetRecoveredFollowerRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &roster,
            &fixture.sealed,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 0);
    fixture.retire().await;
    let capture = fixture.capture().await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .register_initial_intent(
            &NodeIntent::initial(
                scope(),
                NodeId::from_bytes([99; 16]),
                SessionId::from_bytes([99; 16]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        fixture
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .registry(),
        before.registry()
    );
    assert!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await
            .is_err()
    );
    let roster = fixture.roster().await;
    for original in &fixture.originals {
        assert_eq!(
            roster
                .enrollments()
                .iter()
                .find(|row| row.spec() == original.spec())
                .unwrap(),
            original
        );
    }
    assert!(
        FleetRecoveredFollowerRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &roster,
            &fixture.sealed,
            session(1),
            deadline(),
            || Ok(NOW + 19_000)
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn recovered_publication_refuses_duplicate_original_member_requests_before_effects() {
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let mut duplicate = fixture.originals[0].spec().clone();
    duplicate.request = Digest::from_bytes([99; 32]);
    fixture
        .journal
        .accept_enrollment(&duplicate, CHECK)
        .await
        .unwrap();
    assert!(
        FleetRecoveredFollowerRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &fixture.sealed,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    let roster = fixture.roster().await;
    assert_eq!(
        roster
            .enrollments()
            .iter()
            .filter(|row| matches!(row.spec().role, EnrollmentRole::Follower { .. }))
            .count(),
        3
    );
    assert!(
        !roster
            .enrollments()
            .iter()
            .any(|row| row.status() == EnrollmentStatus::Retired)
    );
}

#[tokio::test]
async fn recovered_publication_clock_regression_retains_committed_rows_without_closure() {
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let capture = fixture.capture().await;
    let mut calls = 0;
    let result = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            session(1),
            deadline(),
            || {
                calls += 1;
                Ok(if calls == 1 { CHECK + 2 } else { CHECK + 1 })
            },
        )
        .await
        .unwrap();
    assert!(
        result
            .members()
            .iter()
            .all(|member| member.result().is_ok())
    );
    assert!(result.confirmed().is_err());
    assert!(matches!(
        result.closure_error().as_deref(),
        Some(Error::Deadline)
    ));
    assert!(
        fixture
            .roster()
            .await
            .enrollments()
            .iter()
            .filter(|row| matches!(row.spec().role, EnrollmentRole::Follower { .. }))
            .all(
                |row| row.status() == EnrollmentStatus::Retired && row.updated_at_ms() == CHECK + 2
            )
    );
}
