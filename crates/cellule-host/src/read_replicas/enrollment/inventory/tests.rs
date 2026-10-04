use super::*;
use tokio::sync::watch;

#[test]
fn variable_row_budget_stops_before_copy_and_cannot_skip_an_oversized_first_row() {
    let mut bytes = PAGE_BYTES - 128;
    assert!(admit_entry(&mut bytes, 64, true).unwrap());
    assert_eq!(bytes, PAGE_BYTES - 64);
    assert!(!admit_entry(&mut bytes, 65, false).unwrap());
    assert_eq!(bytes, PAGE_BYTES - 64);
    assert!(admit_entry(&mut bytes, 64, false).unwrap());
    assert_eq!(bytes, PAGE_BYTES);
    assert!(!admit_entry(&mut bytes, 1, false).unwrap());
    assert!(matches!(
        admit_entry(&mut bytes, 1, true),
        Err(Error::Capacity(_))
    ));
    assert_eq!(bytes, PAGE_BYTES);
    bytes = usize::MAX;
    assert!(matches!(
        admit_entry(&mut bytes, 1, false),
        Err(Error::Capacity(_))
    ));
    assert_eq!(bytes, usize::MAX);
}

#[test]
fn cursor_rejects_width_and_zero_identity() {
    for bytes in [&[][..], &[1; 63][..], &[1; 65][..], &[0; 64][..]] {
        assert!(ReaderEnrollmentInventoryCursor::from_bytes(bytes).is_err());
    }
    for range in [0..32, 32..64] {
        let mut bytes = [1; 64];
        bytes[range].fill(0);
        assert!(ReaderEnrollmentInventoryCursor::from_bytes(&bytes).is_err());
    }
    let bytes = [3; 64];
    assert_eq!(
        ReaderEnrollmentInventoryCursor::from_bytes(&bytes)
            .unwrap()
            .to_bytes(),
        bytes
    );
}

#[tokio::test]
async fn live_job_and_joining_owner_remain_unknown() {
    let (sender, response) = watch::channel(None);
    let task = tokio::spawn(async move {
        let _sender = sender;
        std::future::pending::<()>().await;
    });
    let job = Arc::new(Mutex::new(ReaderJob {
        task: Some(task),
        failure: None,
        response,
    }));
    let running = observe_jobs(vec![job.clone()], false, None);
    assert_eq!(
        (
            running.retained(),
            running.running(),
            running.unobserved(),
            running.joining()
        ),
        (1, 1, 0, 0)
    );
    let mut owner = job.lock().await;
    let joining = observe_jobs(vec![job.clone()], true, None);
    assert_eq!(
        (
            joining.retained(),
            joining.running(),
            joining.unobserved(),
            joining.joining()
        ),
        (1, 0, 0, 1)
    );
    assert!(joining.draining());
    owner.task.as_ref().unwrap().abort();
    assert!(owner.join().await.is_err());
}

#[tokio::test]
async fn handle_completion_without_original_response_is_unobserved() {
    let (sender, response) = watch::channel(None);
    let task = tokio::spawn(async move {
        drop(sender);
        panic!("original reader owner failed without a protocol response");
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let job = Arc::new(Mutex::new(ReaderJob {
        task: Some(task),
        failure: None,
        response,
    }));
    let observation = observe_jobs(vec![job.clone()], false, None);
    assert_eq!(
        (
            observation.retained(),
            observation.running(),
            observation.unobserved(),
            observation.joining()
        ),
        (1, 0, 1, 0)
    );
    let original = {
        let mut owner = job.lock().await;
        assert!(owner.join().await.is_err());
        let original = owner.failure.clone().unwrap();
        assert!(matches!(original.as_ref(), Error::Facility { source, .. }
            if source.downcast_ref::<tokio::task::JoinError>().unwrap().is_panic()));
        original
    };
    let joined = observe_jobs(vec![job], false, None);
    assert_eq!(joined.unobserved(), 1);
    assert!(Arc::ptr_eq(joined.task_failure().unwrap(), &original));
}

#[tokio::test]
async fn original_success_response_distinguishes_returned_protocol_from_unknown_task() {
    let receipt = Receipt {
        cell: CellId::from_bytes([1; 32]),
        incarnation: IncarnationId::from_bytes([2; 16]),
        commit_sequence: 3,
    };
    let (_, response) = watch::channel(Some(Ok(receipt)));
    let job = Arc::new(Mutex::new(ReaderJob {
        task: None,
        failure: None,
        response,
    }));
    let observation = observe_jobs(vec![job], false, None);
    assert_eq!(
        (
            observation.retained(),
            observation.running(),
            observation.unobserved(),
            observation.joining()
        ),
        (1, 0, 0, 0)
    );
    assert!(observation.protocol_failure().is_none());
    assert!(observation.task_failure().is_none());
}

#[tokio::test]
async fn protocol_and_task_errors_preserve_original_arcs_separately() {
    let protocol = Arc::new(Error::Node("original reader protocol failure"));
    let task_error = Arc::new(Error::Node("original reader task failure"));
    let (_, response) = watch::channel(Some(Err(protocol.clone())));
    let job = Arc::new(Mutex::new(ReaderJob {
        task: None,
        failure: Some(task_error.clone()),
        response,
    }));
    let observation = observe_jobs(vec![job], false, None);
    assert_eq!(
        (
            observation.retained(),
            observation.running(),
            observation.unobserved(),
            observation.joining()
        ),
        (1, 0, 0, 0)
    );
    assert!(Arc::ptr_eq(
        observation.protocol_failure().unwrap(),
        &protocol
    ));
    assert!(Arc::ptr_eq(
        observation.task_failure().unwrap(),
        &task_error
    ));
    let bank_failure = Arc::new(Error::Node("original bank failure"));
    let observation = observe_jobs(vec![], true, Some(bank_failure.clone()));
    assert!(Arc::ptr_eq(
        observation.task_failure().unwrap(),
        &bank_failure
    ));
    assert!(observation.draining());
}
