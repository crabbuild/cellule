use super::*;

#[tokio::test]
async fn failed_boot_closure_requires_joined_original_process_and_replays_after_reconstruction() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    let mut process = Process::start(fixture.process_path());
    let processes = Processes::new(fixture.process_path());
    let result = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await;
    assert!(matches!(result, Err(Error::Facility { .. })));
    assert!(process.child.try_wait().unwrap().is_none());
    assert_eq!(fixture.failed_boot().await, original);
    process.stop_and_retain(capture.request());
    let publication = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    let closure = publication.confirmed().unwrap();
    assert_eq!(closure.boot().spec(), original.spec());
    assert_eq!(closure.boot().accepted_at_ms(), original.accepted_at_ms());
    assert_eq!(
        closure.boot().established_evidence(),
        original.established_evidence()
    );
    assert_eq!(closure.canonical().node(), node_id(0));
    assert_eq!(closure.canonical().session(), session(0));
    assert_eq!(
        closure.canonical().log().unwrap().phase(),
        cellule_runtime::node::log_state::NodeLogPhase::Retired
    );
    assert!(
        !fixture
            .roster()
            .await
            .required_boots()
            .iter()
            .any(|boot| boot.session == session(0))
    );
    // This remains a recovery tombstone, not a clean local drain/withdrawal.
    assert!(!fixture.directory.is_withdrawn(session(0)).await.unwrap());
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let processes = Processes::new(fixture.process_path());
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let replay = FleetFailedBootRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &original,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    assert_eq!(replay.request().digest(), capture.request().digest());
    let repeated = replay
        .publish(
            &journal,
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(repeated.confirmed().unwrap().boot(), closure.boot());
    assert_eq!(repeated.confirmed().unwrap().digest(), closure.digest());
    assert_eq!(repeated.confirmed().unwrap().snapshot(), closure.snapshot());
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_refuses_unretired_logs_unresolved_roles_and_duplicate_boots() {
    let fixture = Fixture::new().await;
    let original = fixture.failed_boot().await;
    // Even Sealed, inactive recovery is not terminal leader-log retirement.
    assert!(
        fixture
            .directory
            .closed_session(node_id(0), session(0), session(1), CHECK)
            .await
            .is_err()
    );
    fixture.retire().await;
    assert!(
        FleetFailedBootRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &original,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    fixture
        .capture()
        .await
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap()
        .confirmed()
        .unwrap();
    let mut duplicate = original.spec().clone();
    duplicate.request = Digest::from_bytes([218; 32]);
    fixture
        .journal
        .accept_enrollment(&duplicate, CHECK)
        .await
        .unwrap();
    assert!(
        FleetFailedBootRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &original,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    assert_eq!(
        fixture.failed_boot().await.status(),
        EnrollmentStatus::Established
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_retains_lost_retirement_reply_and_original_times() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    let mut process = Process::start(fixture.process_path());
    process.stop_and_retain(capture.request());
    let processes = Processes::new(fixture.process_path());
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = entered => result.unwrap() }
    let committed = fixture.failed_boot().await;
    assert_eq!(committed.status(), EnrollmentStatus::Retired);
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    assert!(result.confirmed().is_err());
    let error = result.record().unwrap_err();
    let Error::Facility { source, .. } = error.as_ref() else {
        panic!("original source absent")
    };
    assert!(source.downcast_ref::<std::io::Error>().is_some());
    assert!(Arc::ptr_eq(&error, &result.record().unwrap_err()));
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let replay = FleetFailedBootRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &original,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let processes = Processes::new(fixture.process_path());
    let replay = replay
        .publish(
            &journal,
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(replay.confirmed().unwrap().boot(), &committed);
    assert!(Arc::ptr_eq(&error, &result.record().unwrap_err()));
    journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_retains_publication_when_final_process_confirmation_fails_or_changes()
{
    for changed in [false, true] {
        let fixture = Fixture::new().await;
        fixture.settle_followers().await;
        let original = fixture.failed_boot().await;
        let capture = fixture.failed_capture(&original).await;
        let mut process = Process::start(fixture.process_path());
        process.stop_and_retain(capture.request());
        let processes = Processes::new(fixture.process_path());
        if changed {
            *processes.final_witness.lock().unwrap() = Some(Digest::from_bytes([217; 32]));
        } else {
            *processes.final_fault.lock().unwrap() = Some(std::io::ErrorKind::ConnectionReset);
        }
        let result = capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await
            .unwrap();
        assert!(result.confirmed().is_err());
        let committed = result.record().unwrap().clone();
        assert_eq!(committed.status(), EnrollmentStatus::Retired);
        let error = result.closure_error().unwrap();
        assert!(Arc::ptr_eq(&error, &result.closure_error().unwrap()));
        if !changed {
            let Error::Facility { source, .. } = error.as_ref() else {
                panic!("original process failure absent")
            };
            assert_eq!(
                source.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::ConnectionReset
            );
        }
        let replay = fixture.failed_capture(&original).await;
        let replay = replay
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                || Ok(CHECK + 1),
            )
            .await
            .unwrap();
        assert_eq!(replay.confirmed().unwrap().boot(), &committed);
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn failed_boot_closure_final_roster_rejects_delayed_original_session_responsibility() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    let mut process = Process::start(fixture.process_path());
    process.stop_and_retain(capture.request());
    let processes = Processes::new(fixture.process_path());
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = entered => result.unwrap() }
    let mut delayed = fixture.originals[0].spec().clone();
    delayed.request = Digest::from_bytes([216; 32]);
    let FleetEnrollmentAcceptance::New(pending) = fixture
        .journal
        .accept_enrollment(&delayed, CHECK)
        .await
        .unwrap()
    else {
        panic!("new late responsibility expected")
    };
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    assert!(result.record().is_ok());
    assert!(result.confirmed().is_err());
    assert!(
        fixture
            .roster()
            .await
            .required_boots()
            .iter()
            .any(|boot| boot.session == session(0))
    );
    assert!(
        FleetFailedBootRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &original,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    // The unit case never dispatched this delayed native CAS: join and publish
    // that exact nonexecution exclusion through the existing atomic path.
    fixture
        .journal
        .refuse_unexecuted_enrollment(pending.spec(), Digest::from_bytes([215; 32]), CHECK + 1)
        .await
        .unwrap();
    let replay = fixture.failed_capture(&original).await;
    replay
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK + 1),
        )
        .await
        .unwrap()
        .confirmed()
        .unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_refuses_stale_snapshot_foreign_process_evidence_and_clock_regression()
{
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    assert!(
        FleetFailedBootProcessEvidence::new(capture.request(), Digest::from_bytes([0; 32]))
            .is_err()
    );
    let other = fixture
        .roster()
        .await
        .enrollments()
        .iter()
        .find(|row| {
            matches!(row.spec().role, EnrollmentRole::Node { .. })
                && row.spec().target.session == session(2)
        })
        .unwrap()
        .clone();
    let observed = fixture
        .directory
        .load(session(2), CHECK)
        .await
        .unwrap()
        .unwrap();
    fixture.directory.withdraw(&observed, CHECK).await.unwrap();
    let other = FleetFailedBootRetirement::capture(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        &other,
        session(1),
        deadline(),
        || Ok(CHECK),
    )
    .await
    .unwrap();
    // Inject a response naming another actual canonical boot request. A public
    // evidence constructor cannot bypass the receiver's original digest check.
    let foreign = ForeignEvidence(
        FleetFailedBootProcessEvidence::new(other.request(), Digest::from_bytes([212; 32]))
            .unwrap(),
    );
    assert!(matches!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &foreign,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.failed_boot().await, original);
    let mut process = Process::start(fixture.process_path());
    process.stop_and_retain(capture.request());
    let processes = Processes::new(fixture.process_path());
    let bytes = std::fs::read(fixture.process_path()).unwrap();
    let mut foreign = bytes.clone();
    foreign[0] ^= 1;
    std::fs::write(fixture.process_path(), foreign).unwrap();
    assert!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.failed_boot().await, original);
    std::fs::write(fixture.process_path(), bytes).unwrap();
    let mut calls = 0;
    let clock = || {
        calls += 1;
        Ok(if calls == 1 { CHECK } else { CHECK - 1 })
    };
    assert!(matches!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                clock
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(fixture.failed_boot().await, original);
    fixture
        .journal
        .register_initial_intent(
            &NodeIntent::initial(
                scope(),
                NodeId::from_bytes([214; 16]),
                SessionId::from_bytes([213; 16]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.failed_boot().await, original);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_cancelled_publication_joins_backend_before_original_replay() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    let mut process = Process::start(fixture.process_path());
    process.stop_and_retain(capture.request());
    let processes = Processes::new(fixture.process_path());
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = entered => result.unwrap() }
    drop(work);
    assert!(resume.send(()).is_err());
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let committed = journal
        .load_enrollment(scope(), original.spec().key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.status(), EnrollmentStatus::Retired);
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let capture = FleetFailedBootRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &original,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let processes = Processes::new(fixture.process_path());
    let replay = capture
        .publish(
            &journal,
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(replay.confirmed().unwrap().boot(), &committed);
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_boot_closure_replay_preserves_new_boot_foreign_roles_on_the_same_physical_node() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capture = fixture.failed_capture(&original).await;
    let mut process = Process::start(fixture.process_path());
    process.stop_and_retain(capture.request());
    let processes = Processes::new(fixture.process_path());
    let publication = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    let closure = publication.confirmed().unwrap();
    let old = NodeIntent::initial(scope(), node_id(0), session(0)).unwrap();
    let next = fixture
        .journal
        .rebind_active_intent(&old, SessionId::from_bytes([50; 16]), 2)
        .await
        .unwrap();
    let ad = NodeAdvertisement::sign(
        next.node(),
        next.session(),
        "https://replacement.invalid".into(),
        scope().fleet,
        Digest::from_bytes([30; 32]),
        Digest::from_bytes([31; 32]),
        super::super::super::application::compile()
            .unwrap()
            .registry()
            .release_digest(),
        &SigningKey::from_bytes(&[50; 32]),
        1,
        CHECK,
        CHECK + 10_000,
        super::super::super::application::compile()
            .unwrap()
            .registry()
            .module_digests(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1 << 20,
            free_disk_bytes: 1 << 20,
            follower_free_bytes: 1 << 20,
            job_credits: 4,
            log_protocol: 1,
            ..Default::default()
        },
    )
    .unwrap();
    startup::enroll(
        fixture.journal.as_ref(),
        &fixture.directory,
        &startup::spec(&next).unwrap(),
        ad,
        CHECK,
    )
    .await
    .unwrap();
    let leader = fixture
        .directory
        .load(session(1), CHECK)
        .await
        .unwrap()
        .unwrap();
    let prepared = fixture
        .directory
        .prepare_log_enrollment(&leader, 8, 1, 3, CHECK)
        .await
        .unwrap()
        .unwrap();
    assert!(
        prepared
            .followers()
            .iter()
            .any(|ad| ad.node() == node_id(0) && ad.session() == next.session())
    );
    let attempt = fixture
        .directory
        .prepare_log_enrollment_attempt(&prepared, CHECK)
        .await
        .unwrap();
    let mut accepted = Vec::new();
    for (index, member) in prepared.followers().iter().enumerate() {
        let spec = EnrollmentSpec {
            scope: scope(),
            request: Digest::from_bytes([index as u8 + 200; 32]),
            source: Some(EnrollmentEndpoint {
                node: node_id(1),
                session: session(1),
                intent_revision: 1,
            }),
            target: EnrollmentEndpoint {
                node: member.node(),
                session: member.session(),
                intent_revision: if member.node() == next.node() { 2 } else { 1 },
            },
            role: EnrollmentRole::Follower { log_epoch: 8 },
        };
        let FleetEnrollmentAcceptance::New(row) = fixture
            .journal
            .accept_enrollment(&spec, CHECK)
            .await
            .unwrap()
        else {
            panic!("new replacement enrollment expected")
        };
        accepted.push(row);
    }
    fixture
        .directory
        .commit_log_enrollment(&attempt, CHECK)
        .await
        .unwrap();
    for row in accepted {
        fixture
            .journal
            .publish_enrollment_result(
                &row,
                EnrollmentEvent::Established(Digest::from_bytes([198; 32])),
                CHECK,
            )
            .await
            .unwrap();
    }
    // The original process evidence stays immutable. The different boot's
    // current foreign responsibility is retained, never retired by old replay.
    let replay = fixture.failed_capture(&original).await;
    assert_eq!(replay.request().digest(), capture.request().digest());
    let replay = replay
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK + 1),
        )
        .await
        .unwrap();
    assert_eq!(replay.confirmed().unwrap().boot(), closure.boot());
    assert_eq!(replay.confirmed().unwrap().digest(), closure.digest());
    let roster = fixture.roster().await;
    assert!(
        roster
            .required_boots()
            .iter()
            .any(|boot| boot.session == next.session())
    );
    assert!(
        !roster
            .required_boots()
            .iter()
            .any(|boot| boot.session == session(0))
    );
    assert!(
        roster
            .enrollments()
            .iter()
            .any(|row| row.spec().target.session == next.session()
                && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch: 8 })
                && row.status() == EnrollmentStatus::Established)
    );
    fixture.journal.close().await.unwrap();
}
