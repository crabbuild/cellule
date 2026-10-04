use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_requires_joined_original_native_lifetime_before_boot_closure() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    let processes = Processes::new(fixture.process_path());
    assert!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(0),
                deadline(),
                clock
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.row(0).await, fixture.readers[0]);
    assert!(
        !fixture.views[0]
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    assert!(
        FleetFailedBootRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &fixture.boot,
            session(0),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    fixture.join_and_retain(capture.request()).await;
    let request = capture.request().digest();
    for index in [0, 1] {
        let capture = fixture.capture(index).await;
        assert_eq!(capture.request().digest(), request);
        let publication = capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(0),
                deadline(),
                clock,
            )
            .await
            .unwrap();
        let closure = publication.confirmed().unwrap();
        let retired = closure.reader();
        assert_eq!(retired.spec(), fixture.readers[index].spec());
        assert_eq!(
            retired.accepted_at_ms(),
            fixture.readers[index].accepted_at_ms()
        );
        assert_eq!(
            retired.established_evidence(),
            fixture.readers[index].established_evidence()
        );
        assert_eq!(retired.status(), EnrollmentStatus::Retired);
        assert_eq!(closure.process().request_digest(), request);
        let replay = fixture.capture(index).await;
        let replay = replay
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(0),
                deadline(),
                clock,
            )
            .await
            .unwrap();
        assert_eq!(replay.confirmed().unwrap().reader(), retired);
        assert_eq!(replay.confirmed().unwrap().digest(), closure.digest());
        if index == 0 {
            assert!(
                FleetFailedBootRetirement::capture(
                    fixture.journal.as_ref(),
                    &fixture.directory,
                    &fixture.roster().await,
                    &fixture.boot,
                    session(0),
                    deadline(),
                    clock
                )
                .await
                .is_err()
            );
        }
    }
    let boot = FleetFailedBootRetirement::capture(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        &fixture.boot,
        session(0),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(boot.request().digest(), request);
    boot.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(0),
        deadline(),
        clock,
    )
    .await
    .unwrap()
    .confirmed()
    .unwrap();
    assert!(
        !fixture
            .roster()
            .await
            .required_boots()
            .iter()
            .any(|boot| boot.session == session(1))
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_adopts_lost_reply_after_independent_adapter_reconstruction() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(1).await;
    fixture.join_and_retain(capture.request()).await;
    let processes = Processes::new(fixture.process_path());
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(0),
        deadline(),
        clock,
    ));
    tokio::select! { result=&mut work=>panic!("unexpected completion {}",result.is_ok()), result=entered=>result.unwrap() }
    let committed = fixture.row(1).await;
    resume.send(()).unwrap();
    let publication = work.await.unwrap();
    assert!(publication.confirmed().is_err());
    let error = publication.record().unwrap_err();
    let Error::Facility { source, .. } = error.as_ref() else {
        panic!("original publication source absent")
    };
    assert!(source.downcast_ref::<std::io::Error>().is_some());
    assert!(Arc::ptr_eq(&error, &publication.record().unwrap_err()));
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let replay = FleetFailedReaderRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &fixture.boot,
        &fixture.readers[1],
        session(0),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let replay = replay
        .publish(
            &journal,
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(0),
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert_eq!(replay.confirmed().unwrap().reader(), &committed);
    assert!(committed.established_evidence().is_none());
    journal.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_cancelled_waiter_is_joined_by_original_backend_owner() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    fixture.join_and_retain(capture.request()).await;
    let processes = Processes::new(fixture.process_path());
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(0),
        deadline(),
        clock,
    ));
    tokio::select! { result=&mut work=>panic!("unexpected completion {}",result.is_ok()), result=entered=>result.unwrap() }
    let committed = fixture.row(0).await;
    drop(work);
    let _ = resume.send(());
    fixture.journal.close().await.unwrap();
    let journal = fixture.reconstruct().await;
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&journal, &snapshot, deadline())
        .await
        .unwrap();
    let capture = FleetFailedReaderRetirement::capture(
        &journal,
        &fixture.directory,
        &roster,
        &fixture.boot,
        &fixture.readers[0],
        session(0),
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let result = capture
        .publish(
            &journal,
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(0),
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert_eq!(result.confirmed().unwrap().reader(), &committed);
    journal.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_retains_original_establishment_completed_after_acceptance() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let initial = fixture.capture(1).await;
    fixture.join_and_retain(initial.request()).await;
    let established = fixture
        .journal
        .publish_enrollment_result(
            &fixture.readers[1],
            EnrollmentEvent::Established(Digest::from_bytes([72; 32])),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        initial
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(0),
                deadline(),
                clock
            )
            .await
            .is_err()
    );
    let recaptured = fixture.capture(1).await;
    assert_eq!(recaptured.request().digest(), initial.request().digest());
    let result = recaptured
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(0),
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert_eq!(
        result.confirmed().unwrap().reader().established_evidence(),
        established.established_evidence()
    );
    assert_eq!(
        result.confirmed().unwrap().reader().accepted_at_ms(),
        fixture.readers[1].accepted_at_ms()
    );
    fixture.finish().await;
}
