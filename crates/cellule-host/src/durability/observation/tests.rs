use super::*;

fn runtime() -> CellRuntime {
    CellRuntime::new(
        SqlWorkerPool::new(1, 2).unwrap(),
        1 << 20,
        SessionId::from_bytes([7; 16]),
    )
    .unwrap()
}

fn capture(
    progress: &SupervisorProgress,
    requests: &requests::RotationRequests,
) -> NodeDurabilitySupervisorObservation {
    progress
        .capture(
            ApplicationId::from_bytes([3; 16]),
            SessionId::from_bytes([7; 16]),
            17,
            false,
            requests,
        )
        .unwrap()
}

#[tokio::test]
async fn returned_error_is_original_and_requires_join_and_request_stop() {
    let runtime = runtime();
    let before = runtime.stats().retained_bytes();
    let progress = Arc::new(SupervisorProgress::new(&runtime).unwrap());
    assert_eq!(runtime.stats().retained_bytes(), before + 4 * 1024);
    assert!(std::mem::size_of::<NodeDurabilitySupervisorObservation>() < 4 * 1024);
    let requests = requests::RotationRequests::new(ApplicationId::from_bytes([3; 16]));
    assert_eq!(
        capture(&progress, &requests).state,
        NodeDurabilitySupervisorState::NotStarted
    );
    let source: SharedFailure = Arc::new(std::io::Error::other("original supervisor failure"));
    let returned = progress.clone();
    let original = source.clone();
    let task = progress
        .start(Box::pin(async move {
            let result = Err(original);
            returned.returned(&result).unwrap();
            result
        }))
        .unwrap();
    let result = task.await.unwrap();
    let observed = capture(&progress, &requests);
    assert_eq!(observed.state, NodeDurabilitySupervisorState::Returned);
    assert!(!observed.rotations.unwrap().stopped);
    assert!(Arc::ptr_eq(
        observed.supervisor_error.as_ref().unwrap(),
        &source
    ));
    requests.stop(Some(source.clone())).unwrap();
    assert!(progress.joined(&result, None).unwrap().is_none());
    for _ in 0..2 {
        let observed = capture(&progress, &requests);
        assert_eq!(observed.state, NodeDurabilitySupervisorState::Joined);
        assert!(observed.rotations.unwrap().stopped);
        assert!(Arc::ptr_eq(
            observed.supervisor_error.as_ref().unwrap(),
            &source
        ));
    }
    drop(progress);
    assert_eq!(runtime.stats().retained_bytes(), before);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn panicked_task_is_unobserved_until_its_original_join() {
    let runtime = runtime();
    let progress = SupervisorProgress::new(&runtime).unwrap();
    let requests = requests::RotationRequests::new(ApplicationId::from_bytes([3; 16]));
    let task = progress
        .start(Box::pin(async { panic!("original supervisor panic") }))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let observed = capture(&progress, &requests);
    assert_eq!(
        observed.state,
        NodeDurabilitySupervisorState::FinishedUnobserved
    );
    assert!(observed.supervisor_error.is_none());
    let source: SharedFailure = Arc::new(task.await.unwrap_err());
    requests.stop(Some(source.clone())).unwrap();
    progress.joined(&Err(source.clone()), None).unwrap();
    let observed = capture(&progress, &requests);
    assert_eq!(observed.state, NodeDurabilitySupervisorState::Joined);
    assert!(Arc::ptr_eq(
        observed.supervisor_error.as_ref().unwrap(),
        &source
    ));
    assert!(
        observed
            .supervisor_error
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic()
    );
    drop(progress);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn bookkeeping_failure_stays_separate_and_preserves_first_error() {
    let runtime = runtime();
    let progress = SupervisorProgress::new(&runtime).unwrap();
    let requests = requests::RotationRequests::new(ApplicationId::from_bytes([3; 16]));
    let source: SharedFailure = Arc::new(std::io::Error::other("native error"));
    let original = Arc::new(Error::Control("original stop error"));
    let first = progress
        .joined(&Err(source.clone()), Some(original.clone()))
        .unwrap()
        .unwrap();
    let retry = progress
        .joined(
            &Err(source.clone()),
            Some(Arc::new(Error::Control("retry stop error"))),
        )
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&first, &original));
    assert!(Arc::ptr_eq(&retry, &original));
    let observed = capture(&progress, &requests);
    assert_eq!(
        observed.state,
        NodeDurabilitySupervisorState::JoinedUnsettled
    );
    assert!(Arc::ptr_eq(
        observed.supervisor_error.as_ref().unwrap(),
        &source
    ));
    assert!(Arc::ptr_eq(
        observed.requests_error.as_ref().unwrap(),
        &original
    ));
    requests.stop(Some(source.clone())).unwrap();
    progress.joined(&Err(source), None).unwrap();
    let observed = capture(&progress, &requests);
    assert_eq!(observed.state, NodeDurabilitySupervisorState::Joined);
    assert!(observed.rotations.unwrap().stopped);
    assert!(Arc::ptr_eq(
        observed.requests_error.as_ref().unwrap(),
        &original
    ));
    drop(progress);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_admission_and_poison_fail_without_fake_absence() {
    let runtime = runtime();
    let retained = runtime
        .try_reserve_node_bytes(runtime.stats().retained_capacity_bytes())
        .unwrap();
    assert!(matches!(
        SupervisorProgress::new(&runtime),
        Err(Error::Capacity(_))
    ));
    drop(retained);
    let progress = SupervisorProgress::new(&runtime).unwrap();
    let requests = requests::RotationRequests::new(ApplicationId::from_bytes([3; 16]));
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = progress.status.lock().unwrap();
        panic!("poison supervisor metadata");
    }));
    assert!(poisoned.is_err());
    assert!(matches!(
        progress.capture(
            ApplicationId::from_bytes([3; 16]),
            SessionId::from_bytes([7; 16]),
            17,
            false,
            &requests
        ),
        Err(Error::Control(
            "node-log supervisor observation lock poisoned"
        ))
    ));
    drop(progress);
    assert_eq!(runtime.stats().retained_bytes(), 0);
    runtime.shutdown().await.unwrap();
}
