//! Prepared Blob dispatch shares the original store admission and native owner.
use super::*;
use cellule_runtime::primitives::blob::BlobCommand;

async fn part(
    fixture: &Fixture,
    id: u8,
) -> cellule_runtime::client::PreparedCommand<BlobCommand<TestBlob>> {
    let now = now_ms();
    let prepared = fixture
        .blobs
        .prepare_mutation(
            mutation_identity_window(id, now, now + 60_000),
            BlobMutation::PutPart {
                key: b"key".to_vec(),
                upload_id: [105; 16],
                part_number: 1,
                payload: b"prepared-part".to_vec(),
            },
        )
        .await
        .unwrap();
    idle(&fixture.artifacts).await;
    prepared
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_original_store_refuses_prepared_clones_and_restored_dispatch() {
    let fixture = Fixture::new().await;
    fixture.begin().await;
    fixture.provider.gate.kind.store(1, Ordering::Release);
    fixture.provider.gate.release();
    let prepared = part(&fixture, 110).await;
    let evidence = prepared.evidence().clone();
    let body = prepared.input_bytes().to_vec();
    let header = prepared.snapshot().to_bytes().unwrap();
    let snapshot = cellule_runtime::client::PreparedCommandSnapshot::from_bytes(&header).unwrap();
    assert_eq!(snapshot.to_bytes().unwrap(), header);
    let restored = fixture
        .client
        .restore_command::<BlobCommand<TestBlob>>(snapshot.clone(), body.clone())
        .unwrap();
    assert_eq!(restored.evidence(), &evidence);
    assert_eq!(restored.input_bytes(), body);
    let clone = prepared.clone();
    let closed = joined(&fixture.artifacts).await;
    // Import still preserves evidence after closure without dispatching or
    // changing the original identity, digest, body or durable snapshot bytes.
    let after_close = fixture
        .client
        .restore_command::<BlobCommand<TestBlob>>(snapshot, body)
        .unwrap();
    let mut refusals = Vec::new();
    for command in [prepared, clone, restored, after_close] {
        refusals.push(matches!(
            command.execute().await,
            Err(InvocationError::NotStarted(
                cellule_runtime::Error::CellDraining
            ))
        ));
    }
    let resolution = fixture.client.resolve(&evidence).await.unwrap();
    let final_observation = fixture.artifacts.lifecycle_observation().unwrap();
    fixture.finish().await;
    assert!(closed.locally_joined() && final_observation.locally_joined());
    assert!(refusals.into_iter().all(|refused| refused));
    assert_eq!(resolution, cellule_runtime::Resolution::Absent);
    assert!(final_observation.first_failure().is_none());
    assert_eq!(fixture.provider.gate.entered.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_prepared_blob_dispatch_survives_caller_loss_and_store_closure() {
    let fixture = Fixture::new().await;
    fixture.begin().await;
    let prepared = part(&fixture, 111).await;
    let evidence = prepared.evidence().clone();
    let replay = prepared.clone();
    let started = Arc::new(Notify::new());
    let (release, receive) = std::sync::mpsc::channel();
    let held = {
        let handle = fixture.handle.clone();
        let started = started.clone();
        tokio::spawn(async move {
            handle
                .query(1, 1, move |_| {
                    started.notify_one();
                    receive.recv().unwrap();
                    Ok(vec![1])
                })
                .await
        })
    };
    started.notified().await;
    let caller = tokio::spawn(async move { prepared.execute().await });
    let accepted = tokio::time::timeout(Duration::from_millis(500), async {
        while fixture
            .artifacts
            .lifecycle_observation()
            .unwrap()
            .accepted_jobs()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    fixture.artifacts.close();
    caller.abort();
    let caller_cancelled = caller.await.is_err_and(|error| error.is_cancelled());
    let pending = fixture
        .artifacts
        .lifecycle_observation()
        .unwrap()
        .accepted_jobs();
    let (polled, pending_join) = tokio::sync::oneshot::channel();
    let close = {
        let store = fixture.artifacts.clone();
        tokio::spawn(async move {
            let join = store.close_and_join();
            tokio::pin!(join);
            let mut polled = Some(polled);
            std::future::poll_fn(|context| {
                let progress = std::future::Future::poll(join.as_mut(), context);
                if progress.is_pending()
                    && let Some(polled) = polled.take()
                {
                    let _ = polled.send(());
                }
                progress
            })
            .await
        })
    };
    let close_polled = tokio::time::timeout(Duration::from_millis(500), pending_join)
        .await
        .is_ok_and(|result| result.is_ok());
    close.abort();
    let close_cancelled = close.await.is_err_and(|error| error.is_cancelled());
    // Always release and join the real SQL worker before ownership assertions.
    release.send(()).unwrap();
    held.await.unwrap().unwrap();
    let observation = joined(&fixture.artifacts).await;
    let resolution = fixture.client.resolve(&evidence).await.unwrap();
    let refused = matches!(
        replay.execute().await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::CellDraining
        ))
    );
    fixture.finish().await;
    assert!(accepted && caller_cancelled && close_polled && close_cancelled && refused);
    assert_eq!(pending, 1);
    assert!(observation.locally_joined() && observation.first_failure().is_none());
    assert!(
        matches!(resolution, cellule_runtime::Resolution::Committed(outcome)
        if outcome.commit_sequence() == 2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_prepared_blob_replay_preserves_exact_result_without_uploading_again() {
    let fixture = Fixture::new().await;
    fixture.begin().await;
    fixture.provider.gate.kind.store(1, Ordering::Release);
    fixture.provider.gate.release();
    let prepared = part(&fixture, 112).await;
    let evidence = prepared.evidence().clone();
    let replay = fixture
        .client
        .restore_command::<BlobCommand<TestBlob>>(
            prepared.snapshot(),
            prepared.input_bytes().to_vec(),
        )
        .unwrap();
    let committed = prepared.execute().await.unwrap();
    assert!(matches!(
        fixture.client.resolve(&evidence).await.unwrap(),
        cellule_runtime::Resolution::Committed(_)
    ));
    let duplicate = replay.execute().await.unwrap();
    assert_eq!(duplicate.receipt, committed.receipt);
    assert_eq!(duplicate.output, committed.output);
    assert_eq!(committed.receipt.commit_sequence, 2);
    assert_eq!(fixture.provider.gate.entered.load(Ordering::Acquire), 1);
    assert!(joined(&fixture.artifacts).await.first_failure().is_none());
    fixture.finish().await;
}
