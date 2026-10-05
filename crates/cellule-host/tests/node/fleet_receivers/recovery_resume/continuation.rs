use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_failed_source_evidence_reply_resumes_with_unchanged_durable_evidence() {
    let (movement, action, idle, receipt) = failed(0, 77, false).await;
    let evidence = movement
        .source
        .journal
        .recovery_evidence(action.key().unwrap())
        .unwrap();
    let basis = movement
        .source
        .journal
        .recovery_basis(action.key().unwrap())
        .unwrap();
    assert_eq!(evidence.basis(), &basis);
    assert!(
        movement
            .receiver
            .inspect_fleet_action(movement.inspection(249))
            .await
            .is_err()
    );
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        &idle
    );
    // The helper compares the returned evidence to the original canonical
    // materialization; capture time must also retain this committed value.
    let completion = apply(&movement.receiver, action.clone()).await;
    assert!(
        completion.committed && completion.execution_error.is_none(),
        "{completion:?}"
    );
    let FleetOutcome::Recovered(result) = &completion.outcome.outcome else {
        panic!("{completion:?}");
    };
    assert_eq!(result.recovery, evidence);
    finish(movement, action, &idle, 77, true, receipt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_failed_source_repair_waiter_leaves_the_original_work_owned() {
    let (movement, action, idle, receipt) = failed(1, 42, false).await;
    let basis = movement
        .source
        .journal
        .recovery_basis(action.key().unwrap())
        .unwrap();
    movement
        .source
        .journal
        .block_recovery_evidence
        .store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let issued = action.clone();
    let waiter = tokio::spawn(async move { apply(&node, issued).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        movement.source.journal.recovery_evidence_entered.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        &idle
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    let inspection = movement.inspect(250).await;
    assert!(matches!(
        inspection.outcome().outcome,
        FleetOutcome::Unknown
    ));
    assert!(
        movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .is_none()
    );
    movement
        .source
        .journal
        .recovery_evidence_resume
        .add_permits(1);
    let completion = apply(&movement.receiver, action.clone()).await;
    assert!(
        completion.committed && completion.execution_error.is_none(),
        "{completion:?}"
    );
    let FleetOutcome::Recovered(result) = &completion.outcome.outcome else {
        panic!("{completion:?}");
    };
    assert_eq!(result.recovery.basis(), &basis);
    assert_eq!(result.serving.position.epoch, idle.epoch + 1);
    finish(movement, action, &idle, 42, true, receipt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_evidence_repair_resumes_idle_without_replacing_original_basis() {
    let (movement, action, idle, receipt) = failed(1, 77, false).await;
    // Fresh inspection cannot reconstruct metadata or start native acquisition.
    let observation = movement.inspect(247).await;
    assert!(matches!(
        observation.outcome().outcome,
        FleetOutcome::Unknown
    ));
    assert!(
        movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .is_none()
    );
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        &idle
    );
    finish(movement, action, &idle, 77, false, receipt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_failed_source_evidence_write_failure_cannot_start_another_claim() {
    let (movement, action, idle, receipt) = failed(2, 42, false).await;
    let failure = apply(&movement.receiver, action.clone()).await;
    assert!(failure.committed && matches!(failure.outcome.outcome, FleetOutcome::Unknown));
    let Error::Facility { source, .. } = failure.execution_error.as_ref().unwrap().as_ref() else {
        panic!("{failure:?}");
    };
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().to_string(),
        "injected recovery evidence write failure"
    );
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        &idle
    );
    assert!(
        movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .is_none()
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    finish(movement, action, &idle, 42, false, receipt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_evidence_repair_accepts_an_ordinary_acquisition_winner() {
    let (movement, action, idle, receipt) = failed(1, 42, false).await;
    let observed = movement
        .inputs
        .authority
        .load(idle.cell)
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .acquire_idle_restored(
            movement.inputs.catalog.clone(),
            movement.inputs.replica.clone(),
            movement.inputs.authority.clone(),
            observed,
            movement.inputs.destination.clone(),
            movement.inputs.owner.clone(),
        )
        .await
        .unwrap();
    assert_eq!(counter(&handle).await, 42);
    finish(movement, action, &idle, 42, true, receipt).await;
}
