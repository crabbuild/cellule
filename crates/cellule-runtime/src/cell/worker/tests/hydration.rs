use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_hydration_releases_fetch_bytes_without_installing_pages() {
    let backend = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let armed = Arc::new(AtomicBool::new(false));
    let started = Arc::new(tokio::sync::Notify::new());
    let watching = armed.clone();
    let notify = started.clone();
    let store = Store::new(backend.clone()).with_read_request_observer(Arc::new(move |_| {
        if watching.load(Ordering::Acquire) {
            notify.notify_one();
        }
    }));
    let cold = sparse_activation(4, store, 4 << 20).await;
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(1 << 20).unwrap();
    pool.activate_restored(
        cold.cell,
        RestoredDatabase::Paged(Box::new(cold.database)),
        cold.destination,
        cold.incarnation,
        1,
        cold.root,
        pool.reserve_activation().unwrap(),
        None,
    )
    .await
    .unwrap();
    let before = pool.hydration(cold.cell).await.unwrap();
    backend.config_mut(|config| config.wait_get_per_call = Duration::from_secs(3));
    armed.store(true, Ordering::Release);
    let hydration = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.hydrate(cold.cell, 64, Instant::now() + Duration::from_secs(10))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let used = pool.resource_ledger().snapshot().unwrap().used;
    assert!(used.retained_bytes() > 0);
    assert_eq!(
        used.worker_jobs(),
        0,
        "fetch must release SQL worker admission"
    );
    hydration.abort();
    assert!(hydration.await.unwrap_err().is_cancelled());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    assert_eq!(pool.hydration(cold.cell).await.unwrap(), before);
    let deadline = pool
        .hydrate(cold.cell, 64, Instant::now() + Duration::from_millis(20))
        .await
        .unwrap();
    assert!(matches!(deadline, HydrationStep::Deferred(_)));
    assert_eq!(pool.hydration(cold.cell).await.unwrap(), before);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    let foreground_bytes = pool
        .resource_ledger()
        .try_reserve(ResourceCost::zero().with_retained_bytes(1 << 20))
        .unwrap();
    let pressure = pool
        .hydrate(cold.cell, 64, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(pressure, HydrationStep::Deferred(_)));
    assert_eq!(pool.hydration(cold.cell).await.unwrap(), before);
    drop(foreground_bytes);
    armed.store(false, Ordering::Release);
    backend.config_mut(|config| config.wait_get_per_call = Duration::ZERO);
    let HydrationStep::Progress(Some(after)) = pool
        .hydrate(cold.cell, 64, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap()
    else {
        panic!("hydration unexpectedly deferred")
    };
    assert!(after.resolved > before.unwrap().resolved);
    pool.deactivate(cold.cell).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_worker_deadline_preserves_sparse_cell_for_retry() {
    let fixture = sparse_activation(7, Store::new(Arc::new(InMemory::new())), 64 * 1024).await;
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
    let before = pool.hydration(fixture.cell).await.unwrap().unwrap();
    let deadline = Instant::now();
    assert!(matches!(
        pool.hydrate(fixture.cell, 64, deadline).await,
        Ok(HydrationStep::Deferred(_))
    ));
    let after = pool.hydration(fixture.cell).await.unwrap().unwrap();
    assert_eq!(after.resolved, before.resolved);
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed = entered.clone();
    assert!(matches!(
        pool.query(
            fixture.cell,
            64,
            SqlDeadline::new(Instant::now()),
            Box::new(move |_| {
                executed.store(true, Ordering::SeqCst);
                Ok(Vec::new())
            })
        )
        .await,
        Err(Error::Deadline)
    ));
    assert!(!entered.load(Ordering::SeqCst));
    let HydrationStep::Progress(Some(progress)) = pool
        .hydrate(fixture.cell, 64, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
    else {
        panic!("hydration unexpectedly deferred")
    };
    assert!(progress.resolved > before.resolved);
    pool.deactivate(fixture.cell).await.unwrap();
    pool.shutdown().await.unwrap();
}
