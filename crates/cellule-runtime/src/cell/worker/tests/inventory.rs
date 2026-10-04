use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn fleet_probe_measures_logical_sparse_size_and_preserves_errors_and_job_accounting() {
    let fixture = sparse_activation(21, Store::new(Arc::new(InMemory::new())), 262_144).await;
    let expected =
        u64::from(fixture.database.page_count()) * u64::from(fixture.database.page_size());
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(16 << 20).unwrap();
    let cell = fixture.cell;
    pool.activate_restored(
        cell,
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
    let sample = pool
        .fleet_inventory(
            cell,
            CatalogRole::Application,
            10,
            SqlDeadline::new(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(sample.database_bytes, expected);
    assert!(sample.database_bytes >= 262_144);
    assert_eq!(sample.commit_sequence, 0);
    assert_eq!(sample.observed_at_ms, 10);
    assert!(sample.transfer_work.is_settled());
    assert!(sample.persisted_work.is_empty());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .worker_jobs(),
        0
    );
    assert!(matches!(
        pool.fleet_inventory(
            cell,
            CatalogRole::Application,
            11,
            SqlDeadline::new(Instant::now() - Duration::from_secs(1))
        )
        .await,
        Err(Error::Deadline)
    ));
    let schema_error = pool
        .fleet_inventory(
            cell,
            CatalogRole::Queue,
            12,
            SqlDeadline::new(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap_err();
    assert!(matches!(schema_error, Error::Sqlite(_)));
    assert!(std::error::Error::source(&schema_error).is_some());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .worker_jobs(),
        0
    );
    pool.fence(cell).await.unwrap();
    assert!(matches!(
        pool.fleet_inventory(
            cell,
            CatalogRole::Application,
            13,
            SqlDeadline::new(Instant::now() + Duration::from_secs(5))
        )
        .await,
        Err(Error::Fenced)
    ));
    pool.discard(cell).await.unwrap();
    pool.shutdown().await.unwrap();
}
