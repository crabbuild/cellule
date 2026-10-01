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
