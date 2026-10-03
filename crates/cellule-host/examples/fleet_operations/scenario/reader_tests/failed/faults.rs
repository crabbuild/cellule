use super::*;

struct Foreign(FleetFailedBootProcessEvidence);
impl FleetFailedBootProcesses for Foreign {
    fn confirm_stopped<'a>(
        &'a self,
        _: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_refuses_live_receiver_and_failed_source_as_receiver_proof() {
    let fixture = Fixture::new().await;
    assert!(
        FleetFailedReaderRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &fixture.boot,
            &fixture.readers[0],
            session(0),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    let source = fixture
        .roster()
        .await
        .enrollments()
        .iter()
        .find(|row| {
            matches!(row.spec().role, EnrollmentRole::Node { .. })
                && row.spec().target.session == session(0)
        })
        .unwrap()
        .clone();
    let original = fixture
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    fixture
        .directory
        .withdraw(&original, clock().unwrap())
        .await
        .unwrap();
    assert!(
        FleetFailedReaderRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            &source,
            &fixture.readers[0],
            session(1),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    // Source advertisement failure has not joined the original receiver views.
    assert!(
        !fixture.views[0]
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    assert_eq!(fixture.row(0).await, fixture.readers[0]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_refuses_foreign_process_payload_history_and_stale_barrier() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    fixture.join_and_retain(capture.request()).await;
    // A response bound to another independently enrolled receiver lifetime
    // cannot certify this original request, even with a well-shaped witness.
    let other = Fixture::new().await;
    other.fence().await;
    let other_capture = other.capture(0).await;
    assert_ne!(other_capture.request().digest(), capture.request().digest());
    let foreign = Foreign(
        FleetFailedBootProcessEvidence::new(other_capture.request(), Digest::from_bytes([211; 32]))
            .unwrap(),
    );
    assert!(matches!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &foreign,
                session(0),
                deadline(),
                clock
            )
            .await,
        Err(Error::Fenced)
    ));
    other.finish().await;
    let roster = fixture.roster().await;
    let mut spec = fixture.readers[0].spec().clone();
    let EnrollmentRole::Reader { position, .. } = &mut spec.role else {
        panic!("reader expected")
    };
    position.root.commit_sequence += 1;
    let source = roster
        .intents()
        .iter()
        .find(|intent| intent.node() == node_id(0))
        .unwrap();
    let target = roster
        .intents()
        .iter()
        .find(|intent| intent.node() == node_id(1))
        .unwrap();
    let changed = EnrollmentRecord::pending(spec, Some(source), target, clock().unwrap()).unwrap();
    assert!(
        FleetFailedReaderRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &roster,
            &fixture.boot,
            &changed,
            session(0),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    let changed = EnrollmentRecord::pending(
        fixture.readers[0].spec().clone(),
        Some(source),
        target,
        fixture.readers[0].accepted_at_ms() + 1,
    )
    .unwrap();
    assert!(
        FleetFailedReaderRetirement::capture(
            fixture.journal.as_ref(),
            &fixture.directory,
            &roster,
            &fixture.boot,
            &changed,
            session(0),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    fixture
        .journal
        .publish_enrollment_result(
            &fixture.readers[1],
            EnrollmentEvent::Established(Digest::from_bytes([214; 32])),
            clock().unwrap(),
        )
        .await
        .unwrap();
    let processes = Processes::new(fixture.process_path());
    assert!(matches!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(0),
                deadline(),
                clock
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(processes.reads.load(Ordering::Acquire), 0);
    assert_eq!(fixture.row(0).await, fixture.readers[0]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_keeps_committed_row_when_final_process_read_fails_or_changes() {
    for changed in [false, true] {
        let fixture = Fixture::new().await;
        fixture.fence().await;
        let capture = fixture.capture(0).await;
        fixture.join_and_retain(capture.request()).await;
        let processes = Processes::new(fixture.process_path());
        if changed {
            *processes.final_witness.lock().unwrap() = Some(Digest::from_bytes([212; 32]));
        } else {
            *processes.final_fault.lock().unwrap() = true;
        }
        let result = capture
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
        assert!(result.confirmed().is_err());
        let committed = result.record().unwrap();
        assert_eq!(committed.status(), EnrollmentStatus::Retired);
        let error = result.closure_error().unwrap();
        assert!(Arc::ptr_eq(&error, &result.closure_error().unwrap()));
        if !changed {
            let Error::Facility { source, .. } = error.as_ref() else {
                panic!("original failure absent")
            };
            assert_eq!(
                source.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::ConnectionReset
            );
        }
        let replay = fixture.capture(0).await;
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
        assert_eq!(replay.confirmed().unwrap().reader(), committed);
        fixture.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_refuses_regressing_clock_and_retirement_with_different_witness() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    fixture.join_and_retain(capture.request()).await;
    let processes = Processes::new(fixture.process_path());
    let mut calls = 0;
    let now = clock().unwrap();
    assert!(matches!(
        capture
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(0),
                deadline(),
                || {
                    calls += 1;
                    Ok(if calls == 1 { now } else { now - 1 })
                }
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(fixture.row(0).await, fixture.readers[0]);
    let result = capture
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
    let committed = result.confirmed().unwrap().reader().clone();
    let replay = fixture.capture(0).await;
    let foreign = Foreign(
        FleetFailedBootProcessEvidence::new(replay.request(), Digest::from_bytes([213; 32]))
            .unwrap(),
    );
    assert!(
        replay
            .publish(
                fixture.journal.as_ref(),
                &fixture.directory,
                &foreign,
                session(0),
                deadline(),
                clock
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.row(0).await, committed);
    fixture.finish().await;
}
