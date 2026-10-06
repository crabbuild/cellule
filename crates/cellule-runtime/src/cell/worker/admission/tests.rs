use super::*;

fn query(pool: &SqlWorkerPool) -> impl Future<Output = Result<Vec<u8>>> + '_ {
    pool.query(
        CellId::from_bytes([0; 32]),
        64,
        SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
        Box::new(|_| Ok(vec![1])),
        None,
    )
}

#[tokio::test]
async fn pending_descriptor_survives_cancellation_without_consuming_another_native_slot() {
    let pool = SqlWorkerPool::new(2, 2).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let held = pool
        .reserve_job(CellId::from_bytes([0; 32]), SqlJobKind::Query)
        .await
        .unwrap();
    let mut pending = Box::pin(query(&pool));
    assert!(futures_util::poll!(&mut pending).is_pending());
    // The control reply proves that this descriptor reached native admission
    // and remains parked behind the held slot before abandoning its receiver.
    assert!(matches!(
        pool.state(CellId::from_bytes([0; 32])).await,
        Err(Error::CellNotActive)
    ));
    let used = pool.resource_ledger().snapshot().unwrap().used;
    assert_eq!(used.worker_jobs(), 1);
    assert!(used.retained_bytes() >= QueuedJob::retained_bytes(false));
    drop(pending);
    assert_eq!(pool.resource_ledger().snapshot().unwrap().used, used);
    let idle = pool.reserve_snapshot_job().await.unwrap();
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .worker_jobs(),
        2
    );
    drop(idle);
    drop(held);
    // Control can preempt pending SQL; the terminal pool drain reconciles the
    // descriptor even when its submitter no longer waits for a reply.
    assert!(matches!(
        pool.state(CellId::from_bytes([0; 32])).await,
        Err(Error::CellNotActive)
    ));
    pool.shutdown().await.unwrap();
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
}

#[tokio::test]
async fn a_waiting_snapshot_is_not_overtaken_by_a_later_native_descriptor() {
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let held = pool
        .reserve_job(CellId::from_bytes([0; 32]), SqlJobKind::Query)
        .await
        .unwrap();
    let snapshot = pool.reserve_snapshot_job();
    tokio::pin!(snapshot);
    assert!(futures_util::poll!(&mut snapshot).is_pending());
    let pending = query(&pool);
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    drop(held);
    let snapshot = snapshot.await.unwrap();
    assert!(futures_util::poll!(&mut pending).is_pending());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .worker_jobs(),
        1
    );
    drop(snapshot);
    assert!(matches!(pending.await, Err(Error::CellNotActive)));
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_wakes_parked_native_admission_and_releases_queued_bytes() {
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let held = pool.reserve_snapshot_job().await.unwrap();
    let pending = query(&pool);
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    let shutdown = pool.shutdown();
    tokio::pin!(shutdown);
    assert!(futures_util::poll!(&mut shutdown).is_pending());
    drop(held);
    shutdown.await.unwrap();
    assert!(matches!(pending.await, Err(Error::RuntimeClosed)));
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
}

#[tokio::test]
async fn native_admission_preserves_capacity_errors_in_the_original_result_channel() {
    let resources = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(4096));
    let slots = Arc::new(ExecutionSlots::new(1, resources.clone()));
    let (sender, receiver) = mpsc::channel(1);
    let worker_slots = slots.clone();
    let worker = std::thread::spawn(move || run::run_worker(receiver, worker_slots, 0));
    let (reply, result) = oneshot::channel();
    sender
        .send(WorkerCommand::Queued {
            command: Box::new(WorkerCommand::Query {
                cell: CellId::from_bytes([0; 32]),
                max_result_bytes: 64,
                deadline: SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
                handler: Box::new(|_| Ok(vec![1])),
                timing: None,
                reply,
            }),
            reservation: slots.queued(0, SqlJobKind::Query).await.unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        result.await.unwrap(),
        Err(Error::Capacity("resource ledger"))
    ));
    assert_eq!(resources.snapshot().unwrap().used, ResourceCost::zero());
    assert_eq!(slots.permits[0].available_permits(), 1);
    drop(sender);
    worker.join().unwrap();
}

#[tokio::test]
async fn full_native_queue_keeps_its_bound_and_services_control_while_a_snapshot_holds_the_slot() {
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let held = pool.reserve_snapshot_job().await.unwrap();
    let mut pending = Vec::new();
    for _ in 0..WORKER_QUEUE {
        let mut future = Box::pin(query(&pool));
        assert!(futures_util::poll!(&mut future).is_pending());
        pending.push(future);
        // Reset this test task's cooperative budget so every future reaches
        // queue admission rather than stopping at an unrelated Tokio yield.
        tokio::task::yield_now().await;
    }
    let expected = WORKER_QUEUE * QueuedJob::retained_bytes(false);
    let used = pool.resource_ledger().snapshot().unwrap().used;
    assert_eq!(used.retained_bytes(), expected);
    assert_eq!(used.worker_jobs(), 1);
    let mut overflow = Box::pin(query(&pool));
    assert!(futures_util::poll!(&mut overflow).is_pending());
    assert_eq!(pool.resource_ledger().snapshot().unwrap().used, used);
    drop(overflow);
    let control = tokio::time::timeout(
        Duration::from_secs(5),
        pool.state(CellId::from_bytes([0; 32])),
    )
    .await;
    drop(held);
    for result in futures_util::future::join_all(pending).await {
        assert!(matches!(result, Err(Error::CellNotActive)));
    }
    pool.shutdown().await.unwrap();
    assert!(matches!(control.unwrap(), Err(Error::CellNotActive)));
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
}
