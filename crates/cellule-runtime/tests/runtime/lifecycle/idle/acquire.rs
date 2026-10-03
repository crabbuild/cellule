//! Idle receiver failure and stop-acquiring behaviour.

use super::*;

#[tokio::test]
async fn acquisition_metadata_failure_blocks_admission_and_releases_resources() {
    acquisition_metadata_fault(1).await;
}

#[tokio::test]
async fn lost_acquisition_metadata_reply_adopts_the_original_before_serving() {
    acquisition_metadata_fault(2).await;
}

async fn acquisition_metadata_fault(fault: usize) {
    let backend = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"acquisition-metadata-fault",
        Limits::default(),
        Store::new(backend.clone()),
    );
    let first = SessionId::from_bytes([112; 16]);
    let original = CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 16 << 20, first).unwrap();
    let handle = bootstrap_on(&original, &fixture, first).await;
    let catalog = handle.catalog().clone();
    handle.drain().await.unwrap();
    original.shutdown().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let input = idle.value().clone();
    let next = SessionId::from_bytes([113; 16]);
    let receiver = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        16 << 20,
        next,
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )
    .unwrap();
    backend.acquisition_fault.store(fault, Ordering::Release);
    let result = receiver
        .acquire_idle_restored(
            catalog.clone(),
            fixture.replica.clone(),
            authority.clone(),
            idle,
            fixture._directory.path().join("metadata-receiver.sqlite"),
            Owner {
                session: next,
                endpoint: "https://metadata-receiver.internal:8081".into(),
            },
        )
        .await;
    assert_eq!(backend.acquisition_fault.load(Ordering::Acquire), 0);
    let current = authority.load(input.cell).await.unwrap().unwrap();
    let retained = authority
        .acquisition_record(input.cell, input.incarnation, input.epoch + 1)
        .await
        .unwrap();
    if fault == 1 {
        assert!(matches!(result, Err(cellule_runtime::Error::Storage(_))));
        assert_eq!(current.value().state, ControlState::Idle);
        assert_eq!(current.value().root, input.root);
        assert!(current.value().owner.is_none());
        assert!(retained.is_none());
        assert!(
            receiver
                .local_handle(catalog, &current)
                .await
                .unwrap()
                .is_none()
        );
    } else {
        let handle = result.unwrap();
        assert_eq!(current.value().state, ControlState::Serving);
        let retained = retained.unwrap();
        assert_eq!(retained.input(), &input);
        assert_eq!(retained.materialized().root, input.root);
        assert_eq!(retained.materialized().state, ControlState::Recovering);
        assert_eq!(
            handle
                .query(64, 64, |connection| {
                    Ok(connection
                        .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                        .to_be_bytes()
                        .to_vec())
                })
                .await
                .unwrap(),
            0_i64.to_be_bytes(),
        );
        handle.drain().await.unwrap();
        assert_eq!(
            authority
                .acquisition_record(input.cell, input.incarnation, input.epoch + 1)
                .await
                .unwrap(),
            Some(retained),
        );
    }
    receiver.shutdown().await.unwrap();
    let stats = receiver.stats();
    assert_eq!(stats.active_cells(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_idle_receiver_does_not_leave_authority_owned() {
    let fixture = fixture_for(b"receiver-activation-failure");
    let session = SessionId::from_bytes([112; 16]);
    let first_runtime =
        CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024, session).unwrap();
    let handle = bootstrap_on(&first_runtime, &fixture, session).await;
    handle.drain().await.unwrap();
    first_runtime.shutdown().await.unwrap();

    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(
        fixture.layout.clone(),
        fixture.target.tenant(),
    );
    let proof = catalog
        .lookup(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let expected_root = idle.value().root.clone();
    let original_input = idle.value().clone();
    let successor = SessionId::from_bytes([113; 16]);
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        16 * 1024 * 1024,
        successor,
    )
    .unwrap();
    let missing_parent = fixture
        ._directory
        .path()
        .join("receiver-parent-does-not-exist")
        .join("receiver.sqlite");
    assert!(
        runtime
            .acquire_idle_restored(
                proof,
                fixture.replica.clone(),
                authority.clone(),
                idle,
                missing_parent,
                Owner {
                    session: successor,
                    endpoint: "https://receiver-failure.internal:8081".into(),
                },
            )
            .await
            .is_err()
    );

    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().state, ControlState::Idle);
    assert!(current.value().owner.is_none());
    assert_eq!(current.value().root, expected_root);
    // Retained input survives a failed restore; it cannot certify serving.
    let acquisition = authority
        .acquisition_record(
            original_input.cell,
            original_input.incarnation,
            current.value().epoch,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(acquisition.input(), &original_input);
    assert_eq!(acquisition.materialized().state, ControlState::Recovering);
    assert_eq!(
        acquisition.materialized().owner.as_ref().unwrap().session,
        successor
    );

    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn stop_acquiring_keeps_existing_cell_serving() {
    let first = fixture_for(b"scale-down-serving");
    let second = fixture_for(b"scale-down-new");
    let session = SessionId::from_bytes([96; 16]);
    let runtime =
        CellRuntime::new(SqlWorkerPool::new(1, 2).unwrap(), 8 * 1024 * 1024, session).unwrap();
    let handle = bootstrap_on(&runtime, &first, session).await;
    runtime.stop_acquiring().unwrap();
    assert!(!runtime.is_acquiring());
    assert_eq!(
        handle
            .query(64, 64, |connection| {
                let value = connection
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        0_i64.to_be_bytes()
    );
    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(
        second.layout.clone(),
        second.target.tenant(),
    );
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &second.target,
                CatalogRole::Application,
                Digest::from_bytes([5; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(second.layout.clone());
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([2; 16]),
            Owner {
                session,
                endpoint: "https://node.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let result = runtime
        .bootstrap(
            proof,
            second.replica.clone(),
            authority,
            observed,
            second._directory.path().join("blocked.sqlite"),
            |_| Ok(()),
        )
        .await;
    assert!(matches!(result, Err(cellule_runtime::Error::CellDraining)));
    assert_eq!(runtime.unreleased_cell_count().await.unwrap(), 1);
    handle.drain().await.unwrap();
    assert_eq!(runtime.unreleased_cell_count().await.unwrap(), 0);
    runtime.shutdown().await.unwrap();
}
