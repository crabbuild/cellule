use super::*;
use tokio::sync::oneshot;

struct PausedProcesses {
    processes: Processes,
    pause: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}
impl FleetFailedBootProcesses for PausedProcesses {
    fn confirm_stopped<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async move {
            let evidence = self.processes.confirm_stopped(request).await?;
            let pause = self.pause.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                resume.await?;
            }
            Ok(evidence)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_rechecks_complete_barrier_after_suspended_process_provider() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    fixture.join_and_retain(capture.request()).await;
    let (entered, waiting) = oneshot::channel();
    let (resume, paused) = oneshot::channel();
    let processes = PausedProcesses {
        processes: Processes::new(fixture.process_path()),
        pause: Mutex::new(Some((entered, paused))),
    };
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(0),
        deadline(),
        clock,
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = waiting => result.unwrap() }
    fixture
        .journal
        .publish_enrollment_result(
            &fixture.readers[1],
            EnrollmentEvent::Established(Digest::from_bytes([215; 32])),
            clock().unwrap(),
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(matches!(work.await, Err(Error::Fenced)));
    assert_eq!(fixture.row(0).await, fixture.readers[0]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reader_closure_retains_committed_row_without_restamping_expired_collection() {
    let fixture = Fixture::new().await;
    fixture.fence().await;
    let capture = fixture.capture(0).await;
    fixture.join_and_retain(capture.request()).await;
    let now = clock().unwrap();
    let expired = capture.interval().0 + 30_001;
    let processes = Processes::new(fixture.process_path());
    let mut calls = 0;
    let result = capture
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(0),
            deadline(),
            || {
                calls += 1;
                Ok(if calls == 4 { expired } else { now })
            },
        )
        .await
        .unwrap();
    assert_eq!(calls, 4);
    let committed = result.record().unwrap();
    assert_eq!(committed.status(), EnrollmentStatus::Retired);
    assert!(matches!(
        result.closure_error().unwrap().as_ref(),
        Error::Deadline
    ));
    let replay = fixture.capture(0).await;
    let result = replay
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
    assert_eq!(result.confirmed().unwrap().reader(), committed);
    fixture.finish().await;
}
