use super::*;

#[tokio::test(flavor = "current_thread")]
async fn prepaid_activation_progresses_while_a_queued_query_waits_for_its_slot() {
    let first = sparse_activation(1, Store::new(Arc::new(InMemory::new())), 0).await;
    let incoming = sparse_activation(2, Store::new(Arc::new(InMemory::new())), 0).await;
    let pool = SqlWorkerPool::new(1, 2).unwrap();
    pool.configure_retained_capacity(1 << 20).unwrap();
    pool.activate_restored(
        first.cell,
        RestoredDatabase::Paged(Box::new(first.database)),
        first.destination,
        first.incarnation,
        1,
        first.root,
        pool.reserve_activation().unwrap(),
        None,
    )
    .await
    .unwrap();
    let prepaid = pool.try_reserve_job(incoming.cell).unwrap();
    let query = pool.query(
        first.cell,
        64,
        SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
        Box::new(|_| Ok(vec![1])),
        None,
    );
    tokio::pin!(query);
    assert!(futures_util::poll!(&mut query).is_pending());
    // This FIFO control reply proves the worker has reached the query's slot
    // wait. Incoming activation already owns that slot and must run there.
    tokio::time::timeout(Duration::from_secs(5), pool.state(first.cell))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        pool.activate_restored(
            incoming.cell,
            RestoredDatabase::Paged(Box::new(incoming.database)),
            incoming.destination,
            incoming.incarnation,
            1,
            incoming.root,
            pool.reserve_activation().unwrap(),
            Some(prepaid),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(query.await.unwrap(), [1]);
    pool.deactivate(first.cell).await.unwrap();
    pool.deactivate(incoming.cell).await.unwrap();
    pool.shutdown().await.unwrap();
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn queued_native_work_progresses_without_rescheduling_its_async_submitter() {
    let fixture = sparse_activation(1, Store::new(Arc::new(InMemory::new())), 0).await;
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(1 << 20).unwrap();
    pool.activate_restored(
        fixture.cell,
        RestoredDatabase::Paged(Box::new(fixture.database)),
        fixture.destination,
        fixture.incarnation,
        1,
        fixture.root,
        pool.reserve_activation().unwrap(),
        None,
    )
    .await
    .unwrap();
    let (entered, started) = oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let first = pool.query(
        fixture.cell,
        64,
        SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
        Box::new(move |_| {
            entered.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(10)).unwrap();
            Ok(vec![1])
        }),
        None,
    );
    tokio::pin!(first);
    assert!(futures_util::poll!(&mut first).is_pending());
    started.await.unwrap();
    let (executed, observed) = std::sync::mpsc::channel();
    let second = pool.query(
        fixture.cell,
        64,
        SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
        Box::new(move |_| {
            executed.send(()).unwrap();
            Ok(vec![2])
        }),
        None,
    );
    tokio::pin!(second);
    assert!(futures_util::poll!(&mut second).is_pending());
    release.send(()).unwrap();
    // Deliberately occupy the only Tokio thread. The native worker must take
    // an already queued successor without asking this submitter to run again.
    let native_progress = observed.recv_timeout(Duration::from_secs(5));
    assert_eq!(first.await.unwrap(), [1]);
    assert_eq!(second.await.unwrap(), [2]);
    pool.deactivate(fixture.cell).await.unwrap();
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
    pool.shutdown().await.unwrap();
    assert!(
        native_progress.is_ok(),
        "native work waited for its async submitter"
    );
}

#[tokio::test]
async fn a_fence_is_processed_while_snapshot_admission_blocks_a_native_query() {
    let fixture = sparse_activation(1, Store::new(Arc::new(InMemory::new())), 0).await;
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    pool.activate_restored(
        fixture.cell,
        RestoredDatabase::Paged(Box::new(fixture.database)),
        fixture.destination,
        fixture.incarnation,
        1,
        fixture.root,
        pool.reserve_activation().unwrap(),
        None,
    )
    .await
    .unwrap();
    let held = pool.reserve_snapshot_job().await.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let executed = called.clone();
    let query = pool.query(
        fixture.cell,
        64,
        SqlDeadline::new(Instant::now() + Duration::from_secs(10)),
        Box::new(move |_| {
            executed.store(true, Ordering::Release);
            Ok(vec![1])
        }),
        None,
    );
    tokio::pin!(query);
    assert!(futures_util::poll!(&mut query).is_pending());
    // A fence must not wait for an unrelated immutable reader to release its
    // slot, or queued SQL could overtake revocation after that reader returns.
    let fenced = tokio::time::timeout(Duration::from_secs(5), pool.fence(fixture.cell)).await;
    drop(held);
    let result = query.await;
    pool.discard(fixture.cell).await.unwrap();
    pool.shutdown().await.unwrap();
    assert!(fenced.unwrap().is_ok());
    assert!(matches!(result, Err(Error::Fenced)));
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
}
