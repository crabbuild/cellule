//! Original child lifetime evidence through recovery stages. This uses no Cell
//! suffix or external jobs and does not qualify an OS-crashed CellNode provider.
use super::*;
use tokio::sync::oneshot;

async fn fenced_request(fixture: &Fixture) -> FleetFailedBootProcessRequest {
    FleetFailedBootProcessRequest::capture_fenced(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        &fixture.failed_boot().await,
        session(1),
        deadline(),
        || Ok(CHECK),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn original_process_can_join_before_recovery_and_retire_with_the_same_identity() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    let original_interval = request.interval();
    let original = fixture.failed_boot().await;
    assert!(request.canonical().is_none());
    assert_eq!(fenced_request(&fixture).await.digest(), request.digest());
    assert!(
        FleetFailedBootRetirement::capture_retained(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            request,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .is_err()
    );
    fixture.settle_followers().await;
    let recaptured = fenced_request(&fixture).await;
    assert_eq!(recaptured.digest(), request.digest());
    assert_eq!(recaptured.fence(), request.fence());
    let legacy = fixture.failed_capture(&original).await;
    assert!(legacy.request().canonical().is_some());
    assert_ne!(legacy.request().digest(), request.digest());
    let processes = Processes::new(fixture.process_path());
    let capture = FleetFailedBootRetirement::capture_retained(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        request,
        session(1),
        deadline(),
        || Ok(CHECK),
    )
    .await
    .unwrap();
    assert_eq!(capture.request(), request);
    assert_eq!(capture.request().interval(), original_interval);
    assert_ne!(capture.snapshot(), request.snapshot());
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
    assert_eq!(closure.process().request_digest(), request.digest());
    assert_eq!(closure.canonical().fence(), *request.fence());
    assert_eq!(
        closure.canonical().log().unwrap().phase(),
        cellule_runtime::node::log_state::NodeLogPhase::Retired
    );
    let retired = closure.boot().clone();
    let closure_digest = closure.digest();
    fixture.journal.close().await.unwrap();
    let independent = fixture.reconstruct().await;
    let snapshot = independent.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&independent, &snapshot, deadline())
        .await
        .unwrap();
    let recaptured = FleetFailedBootProcessRequest::capture_fenced(
        &independent,
        &fixture.directory,
        &roster,
        &original,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    assert_eq!(recaptured.digest(), request.digest());
    let confirmation = recaptured
        .confirm(
            &independent,
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(confirmation.process(), closure.process());
    let replay = FleetFailedBootRetirement::capture_retained(
        &independent,
        &fixture.directory,
        &roster,
        &recaptured,
        session(1),
        deadline(),
        || Ok(CHECK + 10),
    )
    .await
    .unwrap();
    let repeated = replay
        .publish(
            &independent,
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK + 10),
        )
        .await
        .unwrap();
    assert_eq!(repeated.confirmed().unwrap().boot(), &retired);
    assert_eq!(repeated.confirmed().unwrap().digest(), closure_digest);
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    independent.close().await.unwrap();
}

#[tokio::test]
async fn a_permanent_fence_and_sealed_log_do_not_prove_original_process_termination() {
    let fixture = Fixture::new().await;
    let request = fenced_request(&fixture).await;
    let process = Process::start(fixture.process_path());
    let mut child = process;
    let result = request
        .confirm(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await;
    assert!(matches!(result, Err(Error::Facility { .. })));
    assert!(child.child.try_wait().unwrap().is_none());
    assert_eq!(fixture.failed_boot().await, *request.boot());
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 0);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn fenced_process_confirmation_preserves_provider_error_and_rejects_changed_witness() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    let processes = Processes::new(fixture.process_path());
    *processes.final_fault.lock().unwrap() = Some(std::io::ErrorKind::ConnectionReset);
    let error = request
        .confirm(
            fixture.journal.as_ref(),
            &fixture.directory,
            &processes,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .err()
        .unwrap();
    let Error::Facility { source, .. } = error else {
        panic!("provider source error required")
    };
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::ConnectionReset
    );
    let processes = Processes::new(fixture.process_path());
    *processes.final_witness.lock().unwrap() = Some(Digest::from_bytes([201; 32]));
    assert!(matches!(
        request
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &processes,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await,
        Err(Error::Control("original failed process evidence changed"))
    ));
    assert_eq!(fixture.failed_boot().await, *request.boot());
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 0);
    fixture.journal.close().await.unwrap();
}

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

#[tokio::test]
async fn fenced_process_confirmation_rechecks_registry_after_a_suspended_provider() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    let (entered, waiting) = oneshot::channel();
    let (resume, paused) = oneshot::channel();
    let processes = PausedProcesses {
        processes: Processes::new(fixture.process_path()),
        pause: Mutex::new(Some((entered, paused))),
    };
    let mut work = Box::pin(request.confirm(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = waiting => result.unwrap() }
    fixture
        .journal
        .publish_enrollment_result(
            &fixture.originals[1],
            EnrollmentEvent::Established(Digest::from_bytes([212; 32])),
            CHECK,
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(matches!(work.await, Err(Error::FleetOperation(source))
        if matches!(source.as_ref(), cellule_runtime::fleet::operations::OperationError::Conflict)));
    assert_eq!(processes.processes.reads.load(Ordering::Acquire), 1);
    assert_eq!(fixture.failed_boot().await, *request.boot());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn fenced_process_identity_survives_native_log_progress_during_confirmation() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    let (entered, waiting) = oneshot::channel();
    let (resume, paused) = oneshot::channel();
    let processes = PausedProcesses {
        processes: Processes::new(fixture.process_path()),
        pause: Mutex::new(Some((entered, paused))),
    };
    let original = fixture.roster().await;
    let mut work = Box::pin(request.confirm(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = waiting => result.unwrap() }
    fixture.retire().await;
    resume.send(()).unwrap();
    let confirmed = work.await.unwrap();
    assert_eq!(confirmed.snapshot(), original.snapshot());
    assert_eq!(confirmed.fence(), request.fence());
    assert_eq!(confirmed.process().request_digest(), request.digest());
    // Native log closure cannot settle the original Pending/Established journal
    // rows; a process confirmation cannot bypass that separate boot barrier.
    assert!(
        FleetFailedBootRetirement::capture_retained(
            fixture.journal.as_ref(),
            &fixture.directory,
            &fixture.roster().await,
            request,
            session(1),
            deadline(),
            || Ok(CHECK)
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.failed_boot().await, *request.boot());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn fenced_process_confirmation_rejects_foreign_evidence_and_regressing_clocks() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    fixture.settle_followers().await;
    let terminal = fixture.failed_capture(request.boot()).await;
    let foreign = ForeignEvidence(
        FleetFailedBootProcessEvidence::new(terminal.request(), Digest::from_bytes([213; 32]))
            .unwrap(),
    );
    assert!(matches!(
        request
            .confirm(
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
    let mut times = [CHECK, CHECK + 1, CHECK].into_iter();
    assert!(matches!(
        request
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(1),
                deadline(),
                || Ok(times.next().unwrap_or(CHECK))
            )
            .await,
        Err(Error::Deadline)
    ));
    assert!(matches!(
        request
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &Processes::new(fixture.process_path()),
                session(1),
                deadline(),
                || Ok(request.interval().1 - 1)
            )
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(fixture.failed_boot().await, *request.boot());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn retained_fenced_boot_retirement_adopts_lost_reply_with_unchanged_process_evidence() {
    let fixture = Fixture::with_process_observation(true).await;
    let request = fixture.process_request.as_ref().unwrap();
    fixture.settle_followers().await;
    let capture = FleetFailedBootRetirement::capture_retained(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        request,
        session(1),
        deadline(),
        || Ok(CHECK),
    )
    .await
    .unwrap();
    let (entered, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let processes = Processes::new(fixture.process_path());
    let mut work = Box::pin(capture.publish(
        fixture.journal.as_ref(),
        &fixture.directory,
        &processes,
        session(1),
        deadline(),
        || Ok(CHECK),
    ));
    tokio::select! { result = &mut work => panic!("unexpected completion {}", result.is_ok()), result = entered => result.unwrap() }
    assert_eq!(
        fixture.failed_boot().await.status(),
        EnrollmentStatus::Retired
    );
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    assert!(result.record().is_err());
    assert_eq!(
        fixture.failed_boot().await.status(),
        EnrollmentStatus::Retired
    );
    let replay = FleetFailedBootRetirement::capture_retained(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        request,
        session(1),
        deadline(),
        || Ok(CHECK + 1),
    )
    .await
    .unwrap();
    let result = replay
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK + 1),
        )
        .await
        .unwrap();
    assert_eq!(
        result.confirmed().unwrap().process().request_digest(),
        request.digest()
    );
    assert_eq!(
        result.confirmed().unwrap().boot(),
        &fixture.failed_boot().await
    );
    assert_eq!(replay.request().interval(), request.interval());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn retired_process_confirmation_refuses_a_stable_changed_witness() {
    let fixture = confirmation::settled(NOW, true).await;
    let request = fixture.process_request.as_ref().unwrap();
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let retired = fixture.failed_boot().await;
    let original = request
        .confirm(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    let changed =
        FleetFailedBootProcessEvidence::new(request, Digest::from_bytes([234; 32])).unwrap();
    assert_ne!(&changed, original.process());
    assert!(matches!(
        request
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &ForeignEvidence(changed),
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.failed_boot().await, retired);
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn retired_process_confirmation_refuses_switching_the_original_request_basis() {
    let fixture = confirmation::settled(NOW, true).await;
    let fenced = fixture.process_request.as_ref().unwrap();
    let original = Processes::new(fixture.process_path())
        .confirm_stopped(fenced)
        .await
        .unwrap();
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let retired = fixture.failed_boot().await;
    let terminal = FleetFailedBootProcessRequest::capture(
        fixture.journal.as_ref(),
        &fixture.directory,
        &fixture.roster().await,
        &retired,
        session(1),
        deadline(),
        || Ok(CHECK),
    )
    .await
    .unwrap();
    assert_ne!(terminal.digest(), fenced.digest());
    // The shape-valid alternate provider binds the same lifetime witness to a
    // different request. Neither constructor nor two equal reads can change the
    // immutable binding already published in the original Retired boot row.
    let alternate = FleetFailedBootProcessEvidence::new(&terminal, original.witness()).unwrap();
    assert!(matches!(
        terminal
            .confirm(
                fixture.journal.as_ref(),
                &fixture.directory,
                &ForeignEvidence(alternate),
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.failed_boot().await, retired);
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn retired_process_confirmation_preserves_the_original_terminal_v1_binding() {
    let fixture = Fixture::new().await;
    fixture.settle_followers().await;
    let original = fixture.failed_boot().await;
    let capsule = fixture.failed_capture(&original).await;
    assert!(capsule.request().canonical().is_some());
    let mut child = Process::start(fixture.process_path());
    child.stop_and_retain(capsule.request());
    let publication = capsule
        .publish(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    let closure = publication.confirmed().unwrap();
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let confirmed = capsule
        .request()
        .confirm(
            fixture.journal.as_ref(),
            &fixture.directory,
            &Processes::new(fixture.process_path()),
            session(1),
            deadline(),
            || Ok(CHECK + 1),
        )
        .await
        .unwrap();
    assert_eq!(confirmed.process(), closure.process());
    assert_eq!(confirmed.snapshot(), &before);
    assert_eq!(fixture.failed_boot().await, *closure.boot());
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(fixture.transport.retirements.load(Ordering::Acquire), 2);
    fixture.journal.close().await.unwrap();
}
