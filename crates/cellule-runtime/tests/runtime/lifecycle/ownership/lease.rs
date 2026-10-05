//! Node-lease fencing for a live Cell runtime.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_leased_idle_cells_do_not_publish_per_cell_renewals() {
    let session = SessionId::from_bytes([47; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(2, 4).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let fixtures: Vec<_> = (0..4_u8).map(|index| fixture_for(&[47, index])).collect();
    let mut handles = Vec::new();
    let mut before = Vec::new();
    for fixture in &fixtures {
        handles.push(bootstrap_on(&runtime, fixture, session).await);
        before.push(
            CellAuthority::new(fixture.layout.clone())
                .load(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    tokio::time::sleep(std::time::Duration::from_millis(3_400)).await;
    let mut after = Vec::new();
    for (fixture, handle) in fixtures.iter().zip(&handles) {
        assert_eq!(
            handle.query(1, 1, |_| Ok(Vec::new())).await.unwrap(),
            Vec::<u8>::new()
        );
        after.push(
            CellAuthority::new(fixture.layout.clone())
                .load(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(runtime.stats().active_cells(), 4);
    // A quiet control still permits an ordinary exact-root publication.
    let write = handles[0]
        .execute(
            mutation_identity_window(47, 10, 10_000),
            Digest::from_bytes([47; 32]),
            20,
            1,
            16,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(vec![1]))
            },
        )
        .await
        .unwrap();
    lease.fence();
    let mut fenced = Vec::new();
    for handle in &handles {
        fenced.push(handle.query(1, 1, |_| Ok(Vec::new())).await);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let refused = handles[0]
        .execute(
            mutation_identity_window(48, 10, 10_000),
            Digest::from_bytes([48; 32]),
            21,
            1,
            16,
            move |_| {
                called.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerOutcome::Success(vec![2]))
            },
        )
        .await;
    let shutdown = runtime.shutdown().await;

    // Compare after closing the runtime, so a failed regression cannot strand
    // its accepted jobs or native handles.
    for (before, after) in before.iter().zip(&after) {
        assert_eq!(after.value(), before.value(), "idle Cell wrote a renewal");
    }
    assert_eq!(write.commit_sequence(), 1);
    assert!(
        fenced
            .iter()
            .all(|result| matches!(result, Err(cellule_runtime::Error::Fenced)))
    );
    assert!(refused.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(shutdown, Err(cellule_runtime::Error::Fenced)));
    assert_eq!(runtime.stats().active_cells(), 0);
}

#[tokio::test]
async fn unleased_idle_cell_keeps_its_control_renewal() {
    let fixture = fixture_for(b"unleased-idle-renewal");
    let session = SessionId::from_bytes([49; 16]);
    let runtime =
        CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 2 * 1024 * 1024, session).unwrap();
    let _handle = bootstrap_on(&runtime, &fixture, session).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(3_400)).await;
    let after = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    runtime.shutdown().await.unwrap();
    assert!(after.value().progress > before.value().progress);
    assert_eq!(after.value().root, before.value().root);
    assert_eq!(after.value().owner, before.value().owner);
    assert_eq!(after.value().epoch, before.value().epoch);
}

#[tokio::test]
async fn lifecycle_metadata_uses_shared_credit_without_opening_fenced_native_work() {
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        1_024,
        SessionId::from_bytes([46; 16]),
        ReplicaHost::default(),
    )
    .unwrap();
    assert!(matches!(
        runtime.try_reserve_node_metadata_bytes(0),
        Err(cellule_runtime::Error::Capacity("node retained bytes"))
    ));
    let metadata = runtime.try_reserve_node_metadata_bytes(1_024).unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 1_024);
    assert!(matches!(
        runtime.try_reserve_node_metadata_bytes(1),
        Err(cellule_runtime::Error::Capacity("node retained bytes"))
    ));
    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::Fenced)
    ));
    drop(metadata);
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let native = runtime.try_reserve_node_bytes(1_023).unwrap();
    let metadata = runtime.try_reserve_node_metadata_bytes(1).unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 1_024);
    assert!(matches!(
        runtime.try_reserve_node_metadata_bytes(1),
        Err(cellule_runtime::Error::Capacity("node retained bytes"))
    ));
    drop(native);
    lease.fence();
    let fenced_metadata = runtime.try_reserve_node_metadata_bytes(1_023).unwrap();
    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::Fenced)
    ));
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        runtime.try_reserve_node_metadata_bytes(1),
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
    assert_eq!(runtime.stats().retained_bytes(), 1_024);
    drop((metadata, fenced_metadata));
    assert_eq!(runtime.stats().retained_bytes(), 0);
}

#[tokio::test]
async fn fleet_runtime_stays_fenced_until_one_live_node_lease_is_installed() {
    let session = SessionId::from_bytes([41; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        1_024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();

    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::Fenced)
    ));
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    assert!(runtime.install_node_lease(lease.clone()).is_err());
    drop(runtime.try_reserve_node_bytes(1).unwrap());

    lease.fence();
    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::Fenced)
    ));
    runtime.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread")]
async fn node_lease_expiry_hides_an_inflight_committed_command() {
    let fixture = fixture_for(b"node-lease-output-gate");
    let session = SessionId::from_bytes([42; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();

    let command = tokio::spawn(async move {
        handle
            .execute(
                mutation_identity_window(43, 10, 10_000),
                Digest::from_bytes([44; 32]),
                20,
                1,
                16,
                move |transaction| {
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    entered_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                    Ok(HandlerOutcome::Success(b"hidden".to_vec()))
                },
            )
            .await
    });
    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    lease.fence();
    resume_tx.send(()).unwrap();

    match command.await.unwrap() {
        Err(cellule_runtime::Error::OutcomeUnknown { source, .. }) => {
            assert!(matches!(*source, cellule_runtime::Error::Fenced));
        }
        other => panic!("unexpected command result: {other:?}"),
    }
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().root.as_ref().unwrap().commit_sequence, 0);
    assert!(matches!(
        runtime.shutdown().await,
        Err(cellule_runtime::Error::Fenced)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn node_lease_expiry_hides_an_inflight_query_result() {
    let fixture = fixture_for(b"node-lease-query-output-gate");
    let session = SessionId::from_bytes([45; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();

    let query = tokio::spawn(async move {
        handle
            .query(1, 16, move |_connection| {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(b"stale-result".to_vec())
            })
            .await
    });
    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    lease.fence();
    resume_tx.send(()).unwrap();

    assert!(matches!(
        query.await.unwrap(),
        Err(cellule_runtime::Error::Fenced)
    ));
    assert!(matches!(
        runtime.shutdown().await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert_eq!(runtime.stats().active_cells(), 0);
}
#[tokio::test]
async fn activation_rejects_control_owned_by_another_node_session() {
    let fixture = fixture();
    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(
        fixture.layout.clone(),
        fixture.target.tenant(),
    );
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &fixture.target,
                CatalogRole::Application,
                Digest::from_bytes([5; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([2; 16]),
            Owner {
                session: SessionId::from_bytes([4; 16]),
                endpoint: "https://node.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        SessionId::from_bytes([9; 16]),
    )
    .unwrap();
    assert!(matches!(
        runtime
            .bootstrap(
                proof,
                fixture.replica,
                authority,
                observed,
                fixture.database,
                |_| Ok(()),
            )
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
}
