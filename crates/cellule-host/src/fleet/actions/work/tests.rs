use super::*;

#[tokio::test]
async fn original_response_does_not_join_task_and_failures_keep_source_identity() {
    let (sender, response) = watch::channel(None);
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let owned = hold.clone();
    let task = tokio::sync::Mutex::new(ActionJoin {
        task: Some(tokio::spawn(async move {
            owned.acquire().await.unwrap().forget();
        })),
        failure: None,
    });
    let key = Digest::from_bytes([1; 32]);
    let observe = || observe_parts(key, FleetActionWorkKind::Effect, &response, &task).unwrap();
    assert_eq!(observe().state(), FleetActionWorkState::Running);
    let source = Arc::new(Error::Control("original response failure"));
    sender.send(Some(Err(source.clone()))).unwrap();
    let returned = observe();
    assert_eq!(returned.state(), FleetActionWorkState::Returned);
    assert!(Arc::ptr_eq(returned.response_error().unwrap(), &source));
    let mut lane = task.lock().await;
    assert_eq!(observe().state(), FleetActionWorkState::Joining);
    hold.add_permits(1);
    lane.join().await.unwrap();
    let join_source = Arc::new(Error::Control("original join failure"));
    lane.failure = Some(join_source.clone());
    drop(lane);
    let joined = observe();
    assert_eq!(joined.state(), FleetActionWorkState::Joined);
    assert!(joined.response_received());
    assert!(Arc::ptr_eq(joined.task_error().unwrap(), &join_source));
    assert_eq!(returned.state(), FleetActionWorkState::Returned);
}

#[tokio::test]
async fn finished_without_response_never_becomes_successful_joined_work() {
    let (_sender, response) = watch::channel(None);
    let original = tokio::spawn(async {});
    while !original.is_finished() {
        tokio::task::yield_now().await;
    }
    let task = tokio::sync::Mutex::new(ActionJoin {
        task: Some(original),
        failure: None,
    });
    let read = || {
        observe_parts(
            Digest::from_bytes([0; 32]),
            FleetActionWorkKind::Inspection,
            &response,
            &task,
        )
        .unwrap()
    };
    assert_eq!(read().state(), FleetActionWorkState::FinishedUnobserved);
    task.lock().await.join().await.unwrap();
    assert_eq!(read().state(), FleetActionWorkState::Joined);
    assert!(!read().response_received());
}

fn empty_work() -> WorkSnapshot {
    WorkSnapshot {
        scope: FleetScope {
            fleet: Digest::from_bytes([1; 32]),
            application: cellule_runtime::identity::ApplicationId::from_bytes([1; 16]),
        },
        node: NodeId::from_bytes([2; 16]),
        session: SessionId::from_bytes([3; 16]),
        observed_at_ms: 100,
        excluded_capture: None,
        admission_closed: false,
        work_revision: 0,
        capture_revision: 0,
        entries: Vec::new(),
        failure: None,
        digest: Digest::from_bytes([0; 32]),
        _memory: None,
    }
}

#[test]
fn frozen_digest_binds_turnover_and_errors_but_fresh_read_identity_converges() {
    let mut work = empty_work();
    let original = work_digest(&work);
    work.observed_at_ms = 200;
    work.excluded_capture = Some(Digest::from_bytes([9; 32]));
    work.capture_revision = 2;
    assert_eq!(original, work_digest(&work));
    work.work_revision = 2;
    assert_ne!(original, work_digest(&work));
    work.work_revision = 0;
    work.failure = Some(Arc::new(Error::Control("retained bank failure")));
    assert_ne!(original, work_digest(&work));
    work.failure = None;
    work.admission_closed = true;
    assert_ne!(original, work_digest(&work));
}

#[test]
fn capture_revision_exhaustion_refuses_without_mutating_owner() {
    let bank = ActionBank {
        capture_revision: u64::MAX,
        ..ActionBank::default()
    };
    assert!(bank.next_capture_revision().is_err());
    assert_eq!(bank.capture_revision, u64::MAX);
}
