//! Public maintenance admission while accepted SQLite work is still running.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_quiescence_keeps_accepted_work_and_original_resolution() {
    let fixture = fixture();
    let (runtime, handle, _) = activate_runtime(&fixture, 16 << 20).await;
    let owner = inventory::stable_owner(&runtime).await;
    let identity = mutation_identity_window(91, 10, 10_000);
    let digest = Digest::from_bytes([92; 32]);
    let started = Arc::new(Notify::new());
    let (release_tx, release_rx) = mpsc::channel();
    let executing = {
        let handle = handle.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            handle
                .execute(identity, digest, 20, 1024, 1024, move |transaction| {
                    started.notify_one();
                    release_rx.recv().unwrap();
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(
                        b"accepted-before-quiescence".to_vec(),
                    ))
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        runtime.quiesce_cell_at(
            fixture.target.cell_id(),
            SessionId::from_bytes([4; 16]),
            owner.generation,
            owner.incarnation,
            owner.position.unwrap().epoch,
        ),
    )
    .await;
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        handle.query(1, 1, |_| panic!("foreground query admitted")),
    )
    .await;
    // Join the accepted effect before asserting, including on a failed barrier.
    release_tx.send(()).unwrap();
    let outcome = executing.await.unwrap().unwrap();
    closed.unwrap().unwrap();
    assert!(matches!(
        refused.unwrap(),
        Err(cellule_runtime::Error::CellDraining)
    ));
    assert!(
        matches!(outcome, StoredOutcome::Success { ref result, commit_sequence: 1 } if result == b"accepted-before-quiescence")
    );
    assert_eq!(
        handle.resolve(identity, digest, 21, 1024).await.unwrap(),
        Resolution::Committed(outcome)
    );
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_maintenance_inventory_cannot_refuse_the_new_request_and_accepted_sql_moves() {
    use cellule_runtime::cell::actor::MaintenanceCellRelease;
    use cellule_runtime::fleet::operations::DrainBlocker;
    let fixture = fixture();
    let (runtime, handle, _) = activate_runtime(&fixture, 16 << 20).await;
    let owner = inventory::stable_owner(&runtime).await;
    let identity = mutation_identity_window(93, 10, 10_000);
    let digest = Digest::from_bytes([94; 32]);
    let started = Arc::new(Notify::new());
    let (release_tx, release_rx) = mpsc::channel();
    let executing = {
        let handle = handle.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            handle
                .execute(identity, digest, 20, 1024, 1024, move |transaction| {
                    started.notify_one();
                    release_rx.recv().unwrap();
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(b"accepted".to_vec()))
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let source = SessionId::from_bytes([4; 16]);
    let epoch = owner.position.unwrap().epoch;
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runtime.release_maintenance_cell_at(
            fixture.target.cell_id(),
            source,
            owner.generation,
            owner.incarnation,
            epoch,
            tokio::time::Instant::now() + std::time::Duration::from_millis(150),
        ),
    )
    .await;
    let second = {
        let runtime = runtime.clone();
        let cell = fixture.target.cell_id();
        tokio::spawn(async move {
            runtime
                .release_maintenance_cell_at(
                    cell,
                    source,
                    owner.generation,
                    owner.incarnation,
                    epoch,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
        })
    };
    // Let the replacement request reach the actor while the original read is
    // still queued behind SQL. Join SQL on every path before assertions.
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();
    let outcome = executing.await.unwrap().unwrap();
    assert!(matches!(
        first.unwrap().unwrap(),
        MaintenanceCellRelease::Refused {
            blocker: DrainBlocker::Deadline,
            error: None
        }
    ));
    let MaintenanceCellRelease::Released(position) = second.await.unwrap().unwrap() else {
        panic!("new request was refused by stale inventory");
    };
    assert_eq!(position.root.commit_sequence, 1);
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value().root.as_ref(), Some(&position.root));
    let receiver = SessionId::from_bytes([95; 16]);
    let destination =
        CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20, receiver).unwrap();
    let restored = destination
        .acquire_idle_restored(
            handle.catalog().clone(),
            fixture.replica.clone(),
            authority,
            idle,
            fixture
                ._directory
                .path()
                .join("maintenance-receiver.sqlite"),
            Owner {
                session: receiver,
                endpoint: "https://receiver.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        restored
            .query(64, 64, |connection| Ok(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_be_bytes()
                .to_vec()))
            .await
            .unwrap(),
        1_i64.to_be_bytes()
    );
    assert_eq!(
        restored.resolve(identity, digest, 21, 1024).await.unwrap(),
        Resolution::Committed(outcome)
    );
    assert!(
        handle
            .query(1, 1, |_| panic!("released SQL owner ran"))
            .await
            .is_err()
    );
    restored.drain().await.unwrap();
    destination.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(destination.stats().retained_bytes(), 0);
}
