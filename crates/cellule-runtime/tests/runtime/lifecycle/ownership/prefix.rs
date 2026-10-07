//! Complete origin verification and native derivation across owner movement.
use super::*;
use object_store::ObjectStoreExt;
use std::time::Duration;

async fn increment(handle: &cellule_runtime::cell::actor::CellHandle, byte: u8) {
    let clock = now_ms();
    handle
        .execute(
            mutation_identity_window(byte, clock, clock + 60_000),
            Digest::from_bytes([byte; 32]),
            clock,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_prefix_survives_compaction_movement_and_origin_loss_of_old_roots() {
    let backend = Arc::new(InMemory::new());
    let fixture = fixture_with_limits_and_store(
        b"root-prefix-movement",
        Limits::default(),
        Store::new(backend.clone()),
    );
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let catalog = handle.catalog().clone();
    let authority = CellAuthority::new(fixture.layout.clone());
    increment(&handle, 1).await;
    let first = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    for byte in 2..=20 {
        increment(&handle, byte).await;
    }
    let original = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    handle.drain().await.unwrap();
    source.shutdown().await.unwrap();
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let next = SessionId::from_bytes([81; 16]);
    let receiver = CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 64 << 20, next).unwrap();
    let handle = receiver
        .acquire_idle_restored(
            catalog,
            fixture.replica.clone(),
            authority.clone(),
            idle,
            fixture._directory.path().join("prefix-receiver.sqlite"),
            Owner {
                session: next,
                endpoint: "https://prefix-receiver.internal".into(),
            },
        )
        .await
        .unwrap();
    for byte in 21..=40 {
        increment(&handle, byte).await;
    }
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = current.value().ltx_root().unwrap();
    let independent = CellAuthority::new(fixture.layout.clone());
    let proof = independent
        .verify_root_prefix(original, root, &fixture.replica, 128)
        .await
        .unwrap();
    assert_eq!(proof.root(), root);
    assert_eq!(proof.prefix(), original);
    assert!(proof.dependency_count() > 0);
    let lineage = independent.root_lineage(root).await.unwrap().unwrap();
    assert!(!lineage.predecessors().is_empty());
    // Old root documents are not selected dependencies after compaction. Their
    // historical verified links remain; no test fabricates a preparation link.
    let old_path = fixture.layout.incarnation_object_path(
        &first.cell,
        &first.incarnation,
        &first.digest,
        CellObjectKind::Root,
    );
    backend.delete(&old_path).await.unwrap();
    assert!(fixture.replica.open_root(&first).await.is_err());
    independent
        .verify_root_prefix(first, root, &fixture.replica, 128)
        .await
        .unwrap();
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
        40_i64.to_be_bytes()
    );
    handle.drain().await.unwrap();
    receiver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cached_root_metadata_cannot_hide_a_missing_or_corrupt_current_dependency() {
    for corrupt in [false, true] {
        let backend = Arc::new(InMemory::new());
        let fixture = fixture_with_limits_and_store(
            b"root-prefix-origin-loss",
            Limits::default(),
            Store::new(backend.clone()),
        );
        let (runtime, handle, _) = activate_runtime(&fixture, 64 << 20).await;
        let authority = CellAuthority::new(fixture.layout.clone());
        let prefix = authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap();
        increment(&handle, 1).await;
        let root = authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap();
        let objects = fixture.replica.reachable_objects(&root).await.unwrap();
        let proof = authority
            .verify_root_prefix(prefix, root, &fixture.replica, 8)
            .await
            .unwrap();
        assert_eq!(proof.dependency_count(), objects.len());
        handle.drain().await.unwrap();
        runtime.shutdown().await.unwrap();
        let object = objects
            .iter()
            .find(|object| matches!(object.kind, CellObjectKind::Ltx | CellObjectKind::Packed))
            .unwrap();
        let path = fixture.layout.incarnation_object_path(
            &root.cell,
            &root.incarnation,
            &object.digest,
            object.kind,
        );
        if corrupt {
            backend
                .put(&path, Bytes::from_static(b"corrupt-current-ltx").into())
                .await
                .unwrap();
        } else {
            backend.delete(&path).await.unwrap();
        }
        assert!(matches!(
            authority
                .verify_root_prefix(prefix, root, &fixture.replica, 8)
                .await,
            Err(cellule_runtime::Error::Ltx(_))
        ));
    }
}

#[tokio::test]
async fn unrelated_or_missing_lineage_and_invalid_limits_cannot_certify_a_prefix() {
    let backend = Arc::new(InMemory::new());
    let fixture = fixture_with_limits_and_store(
        b"root-prefix-missing",
        Limits::default(),
        Store::new(backend.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let prefix = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    increment(&handle, 1).await;
    let root = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    let mut unrelated = prefix;
    unrelated.digest = [99; 32];
    assert!(matches!(
        authority
            .verify_root_prefix(unrelated, root, &fixture.replica, 8)
            .await,
        Err(cellule_runtime::Error::RootPrefixUnproven { .. })
    ));
    assert!(matches!(
        authority
            .verify_root_prefix(prefix, root, &fixture.replica, 0)
            .await,
        Err(cellule_runtime::Error::Capacity(_))
    ));
    let mut foreign = prefix;
    foreign.incarnation = [99; 16];
    assert!(matches!(
        authority
            .verify_root_prefix(foreign, root, &fixture.replica, 8)
            .await,
        Err(cellule_runtime::Error::Control(_))
    ));
    backend
        .delete(
            &fixture
                .layout
                .root_lineage_path(&root.cell, &root.incarnation, &root.digest),
        )
        .await
        .unwrap();
    assert!(
        matches!(authority.verify_root_prefix(prefix, root, &fixture.replica, 8).await,
        Err(cellule_runtime::Error::RootLineageIncomplete { root: missing }) if missing == root)
    );
    // Exact identity still requires actual current graph availability.
    authority
        .verify_root_prefix(root, root, &fixture.replica, 1)
        .await
        .unwrap();
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_prefix_verification_admits_memory_before_io_and_releases_every_exit() {
    let backend = Arc::new(InMemory::new());
    let fixture = fixture_with_limits_and_store(
        b"root-prefix-budget",
        Limits::default(),
        Store::new(backend.clone()),
    );
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let catalog = handle.catalog().clone();
    let authority = CellAuthority::new(fixture.layout.clone());
    let root = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        64 << 20,
        SessionId::from_bytes([82; 16]),
        ReplicaHost::default().with_io_slots(slots.clone()),
    )
    .unwrap();
    // Block the shared origin facility, then poll the same owned proof future.
    // Cancellation drops its memory token and all native admission; it does not
    // fabricate completion or leave another task running outside the caller.
    let io = slots.acquire().await.unwrap();
    let mut proof = Box::pin(runtime.verify_root_prefix(
        &catalog,
        &authority,
        fixture.replica.clone(),
        root,
        root,
        8,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut proof)
            .await
            .is_err()
    );
    assert!(runtime.stats().retained_bytes() > 16 << 20);
    drop(proof);
    assert_eq!(runtime.stats().retained_bytes(), 0);
    drop(io);
    runtime
        .verify_root_prefix(&catalog, &authority, fixture.replica.clone(), root, root, 8)
        .await
        .unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    // A missing origin root cannot hide refusal before the first origin read.
    let path = fixture.layout.incarnation_object_path(
        &root.cell,
        &root.incarnation,
        &root.digest,
        CellObjectKind::Root,
    );
    backend.delete(&path).await.unwrap();
    let held = runtime.try_reserve_node_bytes(64 << 20).unwrap();
    assert!(matches!(
        runtime
            .verify_root_prefix(&catalog, &authority, fixture.replica.clone(), root, root, 8,)
            .await,
        Err(cellule_runtime::Error::Capacity("node retained bytes"))
    ));
    assert_eq!(runtime.stats().retained_bytes(), 64 << 20);
    drop(held);
    assert!(matches!(
        runtime
            .verify_root_prefix(&catalog, &authority, fixture.replica.clone(), root, root, 8,)
            .await,
        Err(cellule_runtime::Error::Ltx(_))
    ));
    assert_eq!(runtime.stats().retained_bytes(), 0);
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        runtime
            .verify_root_prefix(&catalog, &authority, fixture.replica.clone(), root, root, 8,)
            .await,
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
    handle.drain().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_serving_observation_requires_exact_later_admitted_owner() {
    let fixture = fixture_for(b"native-serving-boundary");
    let (runtime, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let catalog = handle.catalog().clone();
    let authority = CellAuthority::new(fixture.layout.clone());
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let first = runtime
        .observe_serving(&catalog, &authority, current.value().incarnation, 0)
        .await
        .unwrap();
    let second = runtime
        .observe_serving(&catalog, &authority, current.value().incarnation, 0)
        .await
        .unwrap();
    assert!(first.same_writer(&second));
    assert_eq!(first.owner(), current.value().owner.as_ref().unwrap());
    assert_eq!(&first.native().target, &fixture.target);
    assert!(matches!(
        runtime
            .observe_serving(
                &catalog,
                &authority,
                current.value().incarnation,
                current.value().epoch
            )
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert!(matches!(
        runtime
            .observe_serving(&catalog, &authority, IncarnationId::from_bytes([99; 16]), 0)
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    increment(&handle, 88).await;
    let advanced = runtime
        .observe_serving(&catalog, &authority, current.value().incarnation, 0)
        .await
        .unwrap();
    assert!(!first.same_writer(&advanced));
    assert!(advanced.position().root.commit_sequence > first.position().root.commit_sequence);
    handle.drain().await.unwrap();
    assert!(
        runtime
            .observe_serving(&catalog, &authority, current.value().incarnation, 0)
            .await
            .is_err()
    );
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        runtime
            .observe_serving(&catalog, &authority, current.value().incarnation, 0)
            .await,
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
}

#[tokio::test]
async fn a_different_runtime_cannot_report_a_native_serving_writer() {
    let fixture = fixture_for(b"native-serving-other-runtime");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let other = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        64 << 20,
        SessionId::from_bytes([99; 16]),
    )
    .unwrap();
    assert!(matches!(
        other
            .observe_serving(handle.catalog(), &authority, current.value().incarnation, 0)
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    other.shutdown().await.unwrap();
    handle.drain().await.unwrap();
    source.shutdown().await.unwrap();
}
