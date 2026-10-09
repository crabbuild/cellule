//! Root coverage is selected only after shared preparation admission.

use super::*;
use cellule_store::test_support::CountingObjectStore;

#[tokio::test(flavor = "multi_thread")]
async fn publication_admission_batches_commits_that_arrive_while_waiting() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let fixture = fixture_with_limits_and_store(
        b"coalesced-admission",
        Limits::default(),
        Store::new(counted.clone()),
    );
    let session = SessionId::from_bytes([71; 16]);
    let leader = NodeId::from_bytes([72; 16]);
    let follower = NodeId::from_bytes([73; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default().with_dirty_slots(dirty.clone()),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let gate = DurabilityGate::new(session, leader, 1, [follower]).unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(TestNodeTransport::default());
    let shipper =
        NodeLogShipper::new(gate.clone(), Arc::clone(&transport), Limits::default()).unwrap();
    let authority = Arc::new(TestNodeAuthority::default());
    let node_authority: Arc<dyn NodeLogAuthority> = authority.clone();
    runtime
        .install_node_durability(
            fixture.target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                node_authority,
                transport,
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let baseline_control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let baseline = baseline_control.value().revision;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let occupied = dirty.clone().acquire_owned().await.unwrap();

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handle.execute(
            mutation_identity_window(80, 10, 10_000),
            Digest::from_bytes([90; 32]),
            20,
            1_024,
            1_024,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.commit_sequence(), 1);
    // No root work can be dispatched yet. All commits still acknowledge on
    // follower proof and remain retained in the actor's bounded publication queue.
    for sequence in 1_u8..4 {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle.execute(
                mutation_identity_window(80 + sequence, 10, 10_000),
                Digest::from_bytes([90 + sequence; 32]),
                20 + i64::from(sequence),
                1_024,
                1_024,
                |transaction| {
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outcome.commit_sequence(), u64::from(sequence) + 1);
    }

    responses.wait_for_responses(4).await;
    let progress = runtime.publication_progress().await.unwrap();
    assert_eq!(progress.pending_publications, 4);
    assert!(progress.retained_capture_bytes > 0);
    assert!(progress.oldest_unpublished.is_some());
    assert!(
        responses
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|source| *source == CommandResponseSource::Fleet)
    );
    drop(occupied);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.drain())
        .await
        .unwrap()
        .unwrap();
    let progress = runtime.publication_progress().await.unwrap();
    assert_eq!(progress.pending_publications, 0);
    assert_eq!(progress.retained_capture_bytes, 0);
    assert_eq!(progress.oldest_unpublished, None);
    runtime.shutdown().await.unwrap();

    let released = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released.value().root.as_ref().unwrap().commit_sequence, 4);
    assert_eq!(released.value().state, ControlState::Idle);
    {
        let timings = responses.1.lock().unwrap();
        assert_eq!(
            timings.len(),
            1,
            "admission wait split one queued range into extra roots: {timings:?}"
        );
        assert_eq!(timings[0].commit_sequence, 4);
        assert!(timings[0].succeeded);
    }
    // One root CAS and one release CAS; bootstrap preceded this baseline.
    assert_eq!(released.value().revision - baseline, 2);
    assert_eq!(dirty.available_permits(), 1);
    let root = released.value().ltx_root().unwrap();
    let restored = fixture._directory.path().join("admission-restored.sqlite");
    fixture
        .replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        4
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_publication_admission_preserves_source_and_reports_no_root_coverage() {
    let fixture = fixture_for(b"failed-root-admission");
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let session = SessionId::from_bytes([74; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default().with_dirty_slots(dirty.clone()),
    )
    .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let occupied = dirty.clone().acquire_owned().await.unwrap();
    let (entered, observed) = tokio::sync::oneshot::channel();
    let command = tokio::spawn(async move {
        handle
            .execute(
                mutation_identity_window(88, 10, 10_000),
                Digest::from_bytes([98; 32]),
                20,
                1_024,
                1_024,
                move |transaction| {
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    let _ = entered.send(());
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    dirty.close();
    drop(occupied);
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), command)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut original = false;
    while let Some(error) = cause {
        original |= error.is::<tokio::sync::AcquireError>();
        cause = error.source();
    }
    assert!(original, "admission source was lost: {error:?}");
    let _shutdown = runtime.shutdown().await;
    assert_eq!(runtime.stats().active_cells(), 0);
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().ltx_root().unwrap().commit_sequence, 0);
    let timings = responses.1.lock().unwrap();
    assert_eq!(timings.len(), 1);
    assert!(!timings[0].succeeded);
    assert_eq!(timings[0].commit_sequence, 1);
}
