//! Follower and fleet proofs, object publication, and binding epochs.

use super::*;
use crate::runtime::fault_fs::FaultFileSystem;

#[tokio::test(flavor = "multi_thread")]
async fn accepted_control_cas_with_lost_response_releases_once() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let fixture = fixture_with_limits_and_store(
        b"lost-control-cas-response",
        Limits::default(),
        Store::new(object_store),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let request = mutation_identity_window(136, 10, 10_000);
    let digest = Digest::from_bytes([137; 32]);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    store.lose_next_update_response();

    let first = handle
        .execute(request, digest, 20, 1_024, 1_024, move |transaction| {
            assert_eq!(
                transaction.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))?,
                1
            );
            observed.fetch_add(1, Ordering::SeqCst);
            transaction.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(b"published".to_vec()))
        })
        .await
        .unwrap();
    assert!(matches!(
        first,
        StoredOutcome::Success { ref result, commit_sequence: 1 } if result == b"published"
    ));
    assert!(store.lost_update_response_consumed());
    let authority = CellAuthority::new(fixture.layout.clone());
    let control = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = control.value().ltx_root().unwrap();
    eprintln!(
        "fault_seed=136 schedule=accepted_control_cas_response_lost request={request:?} operation={digest:?} committed_sequence={} selected_follower_tickets=[] observed_control={:?}",
        first.commit_sequence(),
        control.value(),
    );
    assert_eq!(root.commit_sequence, 1);
    assert_eq!(root.position.txid, 2);

    let duplicate_calls = calls.clone();
    let replay = handle
        .execute(request, digest, 21, 1_024, 1_024, move |_| {
            duplicate_calls.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerOutcome::Success(b"duplicate".to_vec()))
        })
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Object,
            CommandResponseSource::Recorded
        ]
    );
    {
        let publication_timings = responses.1.lock().unwrap();
        assert_eq!(publication_timings.len(), 1);
        assert_eq!(publication_timings[0].commit_sequence, 1);
        assert!(publication_timings[0].succeeded);
    }

    let restored = fixture._directory.path().join("lost-cas-restored.sqlite");
    let verified = fixture.replica.open_root(&root).await.unwrap();
    assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
    let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id = ?1",
                [request.request_id.as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap(),
        b"published"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn capture_failure_after_sql_commit_fences_until_authoritative_recovery() {
    let fixture = fixture_for(b"capture-after-sql-commit");
    let filesystem = Arc::new(FaultFileSystem::new());
    let session = SessionId::from_bytes([133; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default().with_filesystem(filesystem.clone()),
    )
    .unwrap();
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let before = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = before.value().ltx_root().unwrap();
    let request = mutation_identity_window(134, 10, 10_000);
    let digest = Digest::from_bytes([135; 32]);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    filesystem.fail_next_capture();
    let first = handle
        .execute(request, digest, 20, 1_024, 1_024, move |transaction| {
            assert_eq!(
                transaction.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))?,
                1
            );
            observed.fetch_add(1, Ordering::SeqCst);
            transaction.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(b"unpublished".to_vec()))
        })
        .await;
    let observed_control = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(&fixture.database).unwrap();
    let committed_sequence: i64 = connection
        .query_row(
            "SELECT commit_sequence FROM sys_requests WHERE request_id = ?1",
            [request.request_id.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    eprintln!(
        "fault_seed=133 schedule=after_sql_commit_before_capture request={request:?} operation={digest:?} committed_sequence={committed_sequence} selected_follower_tickets=[] observed_control={:?} result={first:?}",
        observed_control.value(),
    );
    assert_eq!(committed_sequence, 1);
    drop(connection);
    assert!(
        matches!(
            first,
            Err(cellule_runtime::Error::OutcomeUnknown {
                request_id,
                operation_digest,
                ..
            }) if request_id == request.request_id && operation_digest == digest
        ),
        "{first:?}"
    );
    assert!(filesystem.capture_failure_consumed());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed_control.value().ltx_root(), Some(root));
    assert_eq!(
        handle.resolve(request, digest, 21, 1_024).await.unwrap(),
        Resolution::Unknown
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(responses.0.lock().unwrap().as_slice(), &[]);
    assert!(responses.1.lock().unwrap().is_empty());

    let restored = fixture._directory.path().join("capture-restored.sqlite");
    let verified = fixture.replica.open_root(&root).await.unwrap();
    assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
    let mut recovered = cellule_runtime::cell::executor::CellExecutor::new(
        cellule_ltx::Db::open(&restored, Limits::default()).unwrap(),
        fixture.target.cell_id(),
        IncarnationId::from_bytes([2; 16]),
        1,
    );
    assert_eq!(
        recovered.resolve(request, digest, 22, 1_024).unwrap(),
        Resolution::Absent
    );
    recovered.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn object_root_publication_fences_actor_when_local_pruning_fails() {
    let fixture = fixture_for(b"prune-after-root-cas");
    let filesystem = Arc::new(FaultFileSystem::new());
    let host = ReplicaHost::default().with_filesystem(filesystem.clone());
    let session = SessionId::from_bytes([130; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        host,
    )
    .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let request = mutation_identity_window(131, 10, 10_000);
    let digest = Digest::from_bytes([132; 32]);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    filesystem.fail_next_prune();
    let first = handle
        .execute(request, digest, 20, 1_024, 1_024, move |transaction| {
            observed.fetch_add(1, Ordering::SeqCst);
            transaction.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(b"committed".to_vec()))
        })
        .await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let control = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    eprintln!(
        "fault_seed=130 schedule=after_root_cas_before_prune request={request:?} operation={digest:?} committed_sequence=1 selected_follower_tickets=[] observed_control={:?} result={first:?}",
        control.value(),
    );
    assert!(
        matches!(
            first,
            Err(cellule_runtime::Error::OutcomeUnknown {
                request_id,
                operation_digest,
                ..
            }) if request_id == request.request_id && operation_digest == digest
        ),
        "{first:?}"
    );
    assert!(filesystem.prune_failure_consumed());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, 1);
    let duplicate_calls = calls.clone();
    assert!(matches!(
        handle
            .execute(request, digest, 21, 1_024, 1_024, move |_| {
                duplicate_calls.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerOutcome::Success(b"duplicate".to_vec()))
            })
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    runtime.shutdown().await.unwrap();

    let restored = fixture._directory.path().join("prune-restored.sqlite");
    let verified = fixture.replica.open_root(&root).await.unwrap();
    assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
    let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id = ?1",
                [request.request_id.as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap(),
        b"committed"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_replaces_node_durability_binding_for_a_new_epoch() {
    let fixture = fixture_for(b"node-durability-rotation");
    let session = SessionId::from_bytes([60; 16]);
    let leader = NodeId::from_bytes([61; 16]);
    let follower = NodeId::from_bytes([62; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(TestNodeTransport(None));
    let make_binding = |session, leader, log_epoch| {
        let gate = DurabilityGate::new(session, leader, log_epoch, [follower]).unwrap();
        let shipper =
            NodeLogShipper::new(gate.clone(), Arc::clone(&transport), Limits::default()).unwrap();
        let authority: Arc<dyn NodeLogAuthority> = Arc::new(TestNodeAuthority::default());
        Arc::new(NodeDurability::new(
            gate,
            shipper,
            authority,
            Arc::clone(&transport),
            lease.clone(),
        ))
    };
    let make_durability = |epoch| make_binding(session, leader, epoch);
    let foreign_boot = make_binding(SessionId::from_bytes([63; 16]), leader, 1);
    assert!(matches!(
        runtime.install_node_durability(fixture.target.application(), foreign_boot.clone()),
        Err(cellule_runtime::Error::Control(
            "Cell runtime node durability boot differs"
        ))
    ));
    assert!(runtime.node_durability().is_none());
    foreign_boot.shutdown().await.unwrap();
    let first = make_durability(1);
    runtime
        .install_node_durability(fixture.target.application(), Arc::clone(&first))
        .unwrap();
    let second = make_durability(2);

    let previous = runtime
        .replace_node_durability(fixture.target.application(), &first, Arc::clone(&second))
        .unwrap();

    assert!(Arc::ptr_eq(&previous, &first));
    let (application, current) = runtime.node_durability().unwrap();
    assert_eq!(application, fixture.target.application());
    assert!(Arc::ptr_eq(&current, &second));
    let rejected = make_durability(3);
    assert!(matches!(
        runtime.replace_node_durability(
            fixture.target.application(),
            &first,
            Arc::clone(&rejected)
        ),
        Err(cellule_runtime::Error::Control(
            "Cell runtime node durability binding changed"
        ))
    ));
    assert!(Arc::ptr_eq(&runtime.node_durability().unwrap().1, &second));
    rejected.shutdown().await.unwrap();
    for (candidate, refusal) in [
        (
            make_binding(SessionId::from_bytes([63; 16]), leader, 3),
            "Cell runtime node durability identity changed",
        ),
        (
            make_binding(session, NodeId::from_bytes([64; 16]), 3),
            "Cell runtime node durability identity changed",
        ),
        (
            make_durability(2),
            "Cell runtime node durability epoch did not advance",
        ),
    ] {
        assert!(
            matches!(runtime.replace_node_durability(fixture.target.application(), &second, candidate.clone()), Err(cellule_runtime::Error::Control(message)) if message == refusal)
        );
        assert!(Arc::ptr_eq(&runtime.node_durability().unwrap().1, &second));
        candidate.shutdown().await.unwrap();
    }
    runtime.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread")]
async fn command_flows_through_follower_proof_object_coverage_and_clean_close() {
    let fixture = fixture_for(b"node-durability-command");
    let session = SessionId::from_bytes([46; 16]);
    let leader = NodeId::from_bytes([47; 16]);
    let follower = NodeId::from_bytes([48; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
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
    let durability = Arc::new(NodeDurability::new(
        gate,
        shipper,
        node_authority,
        transport,
        lease,
    ));
    runtime
        .install_node_durability(fixture.target.application(), durability)
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;

    let outcome = handle
        .execute(
            mutation_identity_window(49, 10, 10_000),
            Digest::from_bytes([50; 32]),
            20,
            1_024,
            1_024,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"durable".to_vec()))
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        StoredOutcome::Success {
            ref result,
            commit_sequence: 1
        } if result == b"durable"
    ));
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(*authority.coverage.lock().unwrap(), vec![(1, 1)]);
    assert_eq!(*authority.closes.lock().unwrap(), vec![1]);
}
#[tokio::test(flavor = "multi_thread")]
async fn follower_proofs_advance_logical_head_and_bound_the_object_backlog() {
    let pausing = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = pausing.clone();
    let fixture = fixture_with_limits_and_store(
        b"dual-head-command",
        Limits::default(),
        Store::new(object_store),
    );
    let session = SessionId::from_bytes([51; 16]);
    let leader = NodeId::from_bytes([52; 16]);
    let follower = NodeId::from_bytes([53; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
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
    pausing.arm();

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handle.execute(
            mutation_identity_window(54, 10, 10_000),
            Digest::from_bytes([55; 32]),
            20,
            1_024,
            1_024,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"one".to_vec()))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.commit_sequence(), 1);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pausing.wait_until_blocked(),
    )
    .await
    .unwrap();

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handle.execute(
            mutation_identity_window(56, 10, 10_000),
            Digest::from_bytes([57; 32]),
            21,
            1_024,
            1_024,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"two".to_vec()))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(second.commit_sequence(), 2);
    let value = handle
        .query(64, 64, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(i64::from_be_bytes(value.try_into().unwrap()), 2);

    for sequence in 3_u8..=64 {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle.execute(
                mutation_identity_window(sequence.saturating_add(54), 10, 10_000),
                Digest::from_bytes([sequence.saturating_add(55); 32]),
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
        assert_eq!(outcome.commit_sequence(), u64::from(sequence));
    }
    let sixty_fifth = handle.execute(
        mutation_identity_window(119, 10, 10_000),
        Digest::from_bytes([120; 32]),
        85,
        1_024,
        1_024,
        |transaction| {
            transaction.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(Vec::new()))
        },
    );
    tokio::pin!(sixty_fifth);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut sixty_fifth)
            .await
            .is_err()
    );
    let blocked = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blocked.value().root.as_ref().unwrap().commit_sequence, 0);
    assert!(runtime.stats().unpublished_node_log_bytes() > 0);

    pausing.release();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), &mut sixty_fifth)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.commit_sequence(), 65);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.drain())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(runtime.stats().unpublished_node_log_bytes(), 0);
    runtime.shutdown().await.unwrap();
    let released = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released.value().root.as_ref().unwrap().commit_sequence, 65);
    assert_eq!(*authority.activations.lock().unwrap(), vec![1]);
    assert!(
        authority
            .coverage
            .lock()
            .unwrap()
            .last()
            .is_some_and(|(epoch, covered)| *epoch == 1 && *covered >= 65)
    );
}
use cellule_store::test_support::CountingObjectStore;

/// A coalesced root that cannot publish must leave every covered commit
/// recoverable rather than half-released.
///
/// The follower proofs already acknowledged the covered commands, so the failure
/// may not invent a second outcome: it must keep the authority root where it
/// was and let the node log carry the commits until recovery.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_coalesced_root_keeps_every_covered_commit_recoverable() {
    let pausing = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = pausing.clone();
    let fixture = fixture_with_limits_and_store(
        b"coalesced-failure",
        Limits::default(),
        Store::new(object_store),
    );
    let session = SessionId::from_bytes([81; 16]);
    let leader = NodeId::from_bytes(*session.as_bytes());
    let follower_session = SessionId::from_bytes([83; 16]);
    let follower = NodeId::from_bytes([83; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let gate = DurabilityGate::new(session, leader, 1, [follower]).unwrap();
    let follower_directory = tempfile::tempdir().unwrap();
    let follower_store = cellule_runtime::FollowerStore::open(
        follower_directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(
        cellule_runtime::node::log_transport::LocalFollowerTransport::new(
            follower,
            follower_store.clone(),
        ),
    );
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
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
                transport.clone(),
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    // Let the first root upload, then hold its CAS while commits 2..4 queue.
    pausing.arm_next_update();

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handle.execute(
            mutation_identity_window(100, 10, 10_000),
            Digest::from_bytes([101; 32]),
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
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pausing.wait_until_blocked(),
    )
    .await
    .unwrap();

    for sequence in 1_u8..4 {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle.execute(
                mutation_identity_window(100 + sequence, 10, 10_000),
                Digest::from_bytes([101 + sequence; 32]),
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

    // The paused first CAS succeeds; subsequent writes for the coalesced 2..4 range fail.
    pausing.fail_puts();
    pausing.release();
    // The publication path retries a storage failure until its fleet grace
    // deadline, so draining must wait that budget out before reporting.
    let drained = tokio::time::timeout(std::time::Duration::from_secs(20), handle.drain()).await;
    assert!(
        matches!(drained, Ok(Ok(()))),
        "accepted work must finish before the failure is reported: {drained:?}"
    );
    // Unpublished commits remain, so the runtime refuses to close cleanly and
    // recovery owns them instead of reporting a clean shutdown.
    let shutdown = runtime.shutdown().await;
    assert!(
        matches!(
            shutdown,
            Err(cellule_runtime::Error::PendingPublication) | Err(cellule_runtime::Error::Fenced)
        ),
        "a failed coalesced root must leave recovery work behind: {shutdown:?}"
    );

    // The covered commits stay uncovered in the object store: the authority
    // root is still the one that reached it, and the node log carries the rest.
    let released = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let sequence = released
        .value()
        .root
        .as_ref()
        .map_or(0, |root| root.commit_sequence);
    assert_eq!(
        sequence, 1,
        "the first root must publish before the coalesced range fails"
    );
    assert!(
        responses
            .1
            .lock()
            .unwrap()
            .iter()
            .any(|timing| { timing.commit_sequence == 4 && !timing.succeeded }),
        "the failing publication must cover the queued 2..4 range"
    );
    assert!(follower_store.retained_bytes() > 0);
    // Object coverage never advances for the covered commits: their recoverable
    // proof is the follower log that already released them, and the runtime
    // reports the remainder as recovery work instead of a clean close.
    assert!(
        !authority
            .coverage
            .lock()
            .unwrap()
            .iter()
            .any(|(_, covered)| *covered >= 4),
        "a failed coalesced root must not claim object coverage"
    );
    // Recover from the actual fsynced follower suffix and prove all four recorded
    // outcomes survive without executing any handler again.
    pausing.allow_puts();
    let successor = SessionId::from_bytes([84; 16]);
    let fenced = fence_log_session(&fixture.layout, session, successor, follower_session, 1).await;
    let recovery = cellule_runtime::node::log_recovery::NodeLogRecovery::from_fenced(
        transport,
        &fenced,
        Limits::default(),
    )
    .unwrap();
    let manifests = cellule_runtime::recovery::manifest::RecoveryManifestStore::new(
        fixture.layout.clone(),
        Limits::default(),
    );
    let coordinator =
        cellule_runtime::node::log_recovery::RecoveryCoordinator::new(recovery, manifests.clone());
    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(
        fixture.layout.clone(),
        fixture.target.tenant(),
    );
    let control = CellAuthority::new(fixture.layout.clone());
    let inventory =
        cellule_runtime::node::log_recovery::recoverable_cells(&catalog, &control, session, 10)
            .await
            .unwrap();
    let directory = cellule_runtime::node::NodeDirectory::new(
        fixture.layout.clone(),
        Digest::from_bytes([90; 32]),
        Digest::from_bytes([91; 32]),
        Digest::from_bytes([92; 32]),
    );
    let completed = coordinator
        .recover_and_seal(&directory, fenced, inventory, 10_002)
        .await
        .unwrap();
    let attached = completed.controls.into_iter().next().unwrap();
    let successor_runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        successor,
    )
    .unwrap();
    let restored = successor_runtime
        .takeover_restored(
            catalog
                .lookup(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap(),
            fixture.replica.clone(),
            control,
            attached,
            completed.takeover,
            manifests,
            fixture
                ._directory
                .path()
                .join("coalesced-failure-successor.sqlite"),
            Owner {
                session: successor,
                endpoint: "https://successor.internal".into(),
            },
        )
        .await
        .unwrap();
    for sequence in 0_u8..4 {
        let outcome = restored
            .execute(
                mutation_identity_window(100 + sequence, 10, 10_000),
                Digest::from_bytes([101 + sequence; 32]),
                30,
                1_024,
                1_024,
                |_| panic!("a recovered acknowledged command must not execute again"),
            )
            .await
            .unwrap();
        assert_eq!(outcome.commit_sequence(), u64::from(sequence) + 1);
    }
    let value = restored
        .query(64, 64, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(i64::from_be_bytes(value.try_into().unwrap()), 4);
    restored.drain().await.unwrap();
    successor_runtime.shutdown().await.unwrap();
}

/// One published root must cover every commit that queued behind it.
///
/// While every object write is paused, four commands acknowledge on their
/// follower proofs; the first root is already in flight, so the rest queue
/// behind it. Coalescing must publish fewer roots than commits, and the last
/// root must still name the newest commit.
#[tokio::test(flavor = "multi_thread")]
async fn one_published_root_covers_every_queued_commit() {
    let pausing = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let counted = Arc::new(CountingObjectStore::new(pausing.clone()));
    let fixture = fixture_with_limits_and_store(
        b"coalesced-command",
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
        ReplicaHost::default(),
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
    let baseline_puts = counted.put_requests();
    pausing.arm();

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
    // The first root is now unpublished, so the remaining commits queue behind
    // it instead of each publishing their own root.
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pausing.wait_until_blocked(),
    )
    .await
    .unwrap();

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

    pausing.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.drain())
        .await
        .unwrap()
        .unwrap();
    runtime.shutdown().await.unwrap();

    let released = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released.value().root.as_ref().unwrap().commit_sequence, 4);
    // Control advances once per published root, so four commits must need fewer
    // transitions than commits: the first root is already in flight, and the
    // remaining three coalesce behind it.
    let transitions = released.value().revision.saturating_sub(baseline);
    assert!(
        transitions < 4,
        "four commits must coalesce into fewer roots, control advanced {transitions} times"
    );
    // Publishing one root per commit would upload at least the segment body,
    // index, directory node, segment page, and root document for each of the
    // four commits; a coalesced range shares the last three.
    let uploads = counted.put_requests().saturating_sub(baseline_puts);
    assert!(
        uploads < 24,
        "coalescing must not upload one full object set per commit, uploaded {uploads}"
    );
}

struct FaultFollowerTransport {
    inner: cellule_runtime::node::log_transport::LocalFollowerTransport,
    after_object_failure: Option<Arc<PausingStore>>,
    release: tokio::sync::Semaphore,
    tickets: Mutex<
        Vec<(
            NodeId,
            cellule_ltx::NodeFrameScope,
            cellule_ltx::NodeFrameScope,
        )>,
    >,
    receipts: Mutex<Vec<FollowerReceipt>>,
}

impl NodeLogTransport for FaultFollowerTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            let first = cellule_ltx::inspect_node_frame(
                request.frames.first().unwrap().clone(),
                Limits::default(),
            )?;
            let last = cellule_ltx::inspect_node_frame(
                request.frames.last().unwrap().clone(),
                Limits::default(),
            )?;
            self.tickets
                .lock()
                .unwrap()
                .push((member, first.scope(), last.scope()));
            if let Some(store) = &self.after_object_failure {
                store.wait_until_failed().await;
                self.release
                    .acquire()
                    .await
                    .map_err(|_| cellule_runtime::Error::RuntimeClosed)?
                    .forget();
            }
            let receipt = self.inner.append(member, request).await?;
            self.receipts.lock().unwrap().push(receipt);
            Ok(receipt)
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.inner.seal(member, request)
    }

    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        self.inner.tail(member, request)
    }

    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.inner.retire(member, request)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_fsync_can_acknowledge_before_object_root_cas() {
    exercise_fleet_ack_drain(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_ack_drain_reconciles_a_lost_root_cas_response() {
    exercise_fleet_ack_drain(true).await;
}

async fn exercise_fleet_ack_drain(lose_response: bool) {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let fixture = fixture_with_limits_and_store(
        b"follower-before-object-cas",
        Limits::default(),
        Store::new(object_store),
    );
    let session = SessionId::from_bytes([141; 16]);
    let follower = NodeId::from_bytes([142; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let gate = DurabilityGate::new(
        session,
        NodeId::from_bytes(*session.as_bytes()),
        1,
        [follower],
    )
    .unwrap();
    let follower_directory = tempfile::TempDir::new().unwrap();
    let follower_store = cellule_runtime::FollowerStore::open(
        follower_directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport = Arc::new(FaultFollowerTransport {
        inner: cellule_runtime::node::log_transport::LocalFollowerTransport::new(
            follower,
            follower_store.clone(),
        ),
        after_object_failure: None,
        release: tokio::sync::Semaphore::new(0),
        tickets: Mutex::new(Vec::new()),
        receipts: Mutex::new(Vec::new()),
    });
    let node_transport: Arc<dyn NodeLogTransport> = transport.clone();
    let shipper =
        NodeLogShipper::new(gate.clone(), Arc::clone(&node_transport), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            fixture.target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(TestNodeAuthority::default()),
                node_transport,
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let authority = CellAuthority::new(fixture.layout.clone());
    let initial_root = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(initial_root.commit_sequence, 0);
    // Finish activation's actor turn before subscribing to command publication.
    runtime.active_catalog_entries().await.unwrap();
    let mut publications = runtime.subscribe_publications();
    let request = mutation_identity_window(143, 10, 10_000);
    let digest = Digest::from_bytes([144; 32]);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    store.arm_next_update();
    let command_handle = handle.clone();
    let command = tokio::spawn(async move {
        command_handle
            .execute(request, digest, 20, 1_024, 1_024, move |transaction| {
                observed.fetch_add(1, Ordering::SeqCst);
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"fleet-before-cas".to_vec()))
            })
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.wait_until_blocked(),
    )
    .await
    .expect("seed=141: object control CAS did not pause");
    let pending = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.value().ltx_root(), Some(initial_root));
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), command)
        .await
        .expect("seed=141: follower fsync should prove the command while CAS is paused")
        .unwrap()
        .unwrap();
    assert_eq!(outcome.commit_sequence(), 1);
    assert!(
        matches!(
            publications.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "fleet-only acknowledgement emitted an object-publication hint"
    );
    let (selected_member, first_frame, last_frame) = {
        let tickets = transport.tickets.lock().unwrap();
        assert_eq!(tickets.len(), 1);
        tickets[0]
    };
    assert_eq!(selected_member, follower);
    assert_eq!(first_frame.leader_session, *session.as_bytes());
    assert_eq!(first_frame.log_epoch, 1);
    assert_eq!(first_frame.node_sequence, 1);
    assert_eq!(last_frame.node_sequence, 1);
    assert_eq!(first_frame.commit_sequence, 1);
    assert_eq!(
        transport.receipts.lock().unwrap().as_slice(),
        &[FollowerReceipt {
            base_sequence: 1,
            durable_through: 1,
        }]
    );
    assert!(follower_store.retained_bytes() > 0);
    eprintln!(
        "fault_seed=141 schedule=follower_fsync_before_object_cas request={request:?} committed_sequence={} selected_follower_ticket=({selected_member:?},epoch={},{}..={}) follower_receipts={:?} observed_control={:?}",
        outcome.commit_sequence(),
        first_frame.log_epoch,
        first_frame.node_sequence,
        last_frame.node_sequence,
        transport.receipts.lock().unwrap().as_slice(),
        pending.value(),
    );
    assert_eq!(
        authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root(),
        Some(initial_root)
    );
    let duplicate_calls = calls.clone();
    assert_eq!(
        handle
            .execute(request, digest, 21, 1_024, 1_024, move |_| {
                duplicate_calls.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerOutcome::Success(b"duplicate".to_vec()))
            })
            .await
            .unwrap(),
        outcome
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let drain = handle.drain();
    tokio::pin!(drain);
    let early = tokio::time::timeout(std::time::Duration::from_millis(50), &mut drain).await;
    if lose_response {
        store.lose_next_update_response();
    }
    // Release the provider before asserting so a failed gate does not strand SQL work.
    store.release();
    assert!(
        early.is_err(),
        "fleet-only acknowledgement must not complete drain"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut drain)
        .await
        .unwrap()
        .unwrap();
    if lose_response {
        assert!(store.lost_update_response_consumed());
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let control = authority
                .load(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if control
                .value()
                .ltx_root()
                .is_some_and(|root| root.commit_sequence == 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let published_root = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(published_root.position.txid, 2);
    assert_ne!(published_root.digest, initial_root.digest);
    let notified = tokio::time::timeout(std::time::Duration::from_secs(2), publications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notified.cell(), fixture.target.cell_id());
    runtime.shutdown().await.unwrap();
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Fleet,
            CommandResponseSource::Recorded
        ]
    );
    {
        let publication_timings = responses.1.lock().unwrap();
        assert_eq!(publication_timings.len(), 1);
        assert_eq!(publication_timings[0].commit_sequence, 1);
        assert!(publication_timings[0].succeeded);
    }

    let verified = fixture.replica.open_root(&published_root).await.unwrap();
    let recovered = fixture
        ._directory
        .path()
        .join("follower-before-cas-recovered.sqlite");
    assert_eq!(verified.root(), published_root);
    assert_eq!(
        verified.restore(&recovered).await.unwrap(),
        published_root.position
    );
    let connection = cellule_ltx::rusqlite::Connection::open(recovered).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id = ?1",
                [request.request_id.as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap(),
        b"fleet-before-cas"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fleet_proof_retains_owner_when_object_publication_fails_first() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let fixture = fixture_with_limits_and_store(
        b"fleet-object-failure",
        Limits::default(),
        Store::new(object_store),
    );
    let session = SessionId::from_bytes([121; 16]);
    let leader = NodeId::from_bytes([122; 16]);
    let follower = NodeId::from_bytes([123; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let gate = DurabilityGate::new(session, leader, 1, [follower]).unwrap();
    let follower_directory = tempfile::TempDir::new().unwrap();
    let follower_store = cellule_runtime::FollowerStore::open(
        follower_directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport = Arc::new(FaultFollowerTransport {
        inner: cellule_runtime::node::log_transport::LocalFollowerTransport::new(
            follower,
            follower_store,
        ),
        after_object_failure: Some(store.clone()),
        release: tokio::sync::Semaphore::new(0),
        tickets: Mutex::new(Vec::new()),
        receipts: Mutex::new(Vec::new()),
    });
    let node_transport: Arc<dyn NodeLogTransport> = transport.clone();
    let shipper =
        NodeLogShipper::new(gate.clone(), Arc::clone(&node_transport), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            fixture.target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(TestNodeAuthority::default()),
                node_transport,
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    store.fail_puts();

    let command = handle.execute(
        mutation_identity_window(124, 10, 10_000),
        Digest::from_bytes([125; 32]),
        20,
        1_024,
        1_024,
        |transaction| {
            transaction.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(b"fleet-proof".to_vec()))
        },
    );
    tokio::pin!(command);
    tokio::select! {
        () = store.wait_until_failed() => {}
        result = &mut command => panic!("command answered before object failure: {result:?}"),
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut command)
            .await
            .is_err()
    );
    transport.release.add_permits(1);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), &mut command)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.commit_sequence(), 1);
    let (selected_member, first_frame, last_frame) = {
        let tickets = transport.tickets.lock().unwrap();
        assert_eq!(tickets.len(), 1);
        tickets[0]
    };
    assert_eq!(selected_member, follower);
    assert_eq!(first_frame.leader_session, *session.as_bytes());
    assert_eq!(first_frame.log_epoch, 1);
    assert_eq!(first_frame.node_sequence, 1);
    assert_eq!(last_frame.node_sequence, 1);
    assert_eq!(first_frame.commit_sequence, 1);
    assert_eq!(
        transport.receipts.lock().unwrap().as_slice(),
        &[FollowerReceipt {
            base_sequence: 1,
            durable_through: 1,
        }]
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), store.wait_until_failed())
        .await
        .unwrap();

    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    eprintln!(
        "fault_seed=121 schedule=object_failure_before_follower_fsync request={:?} committed_sequence={} selected_follower_ticket=({selected_member:?},epoch={},{}..={}) follower_receipts={:?} observed_control={:?}",
        mutation_identity_window(124, 10, 10_000),
        outcome.commit_sequence(),
        first_frame.log_epoch,
        first_frame.node_sequence,
        last_frame.node_sequence,
        transport.receipts.lock().unwrap().as_slice(),
        control.value(),
    );
    assert_eq!(control.value().root.as_ref().unwrap().commit_sequence, 0);
    let value = handle
        .query(64, 64, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(i64::from_be_bytes(value.try_into().unwrap()), 1);
    assert_eq!(runtime.stats().active_cells(), 1);
    let pending = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.value().state, ControlState::Serving);
    assert_eq!(pending.value().owner.as_ref().unwrap().session, session);
    assert_eq!(pending.value().root.as_ref().unwrap().commit_sequence, 0);
    assert!(runtime.stats().unpublished_node_log_bytes() > 0);
    store.allow_puts();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let control = CellAuthority::new(fixture.layout.clone())
                .load(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if control.value().root.as_ref().unwrap().commit_sequence == 1
                && runtime.stats().unpublished_node_log_bytes() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn post_commit_publication_failure_returns_resolvable_unknown_outcome() {
    let fixture = fixture();
    let handle = activate(&fixture, 16 * 1024 * 1024).await;
    delete_control_root(&fixture).await;
    let request = mutation_identity_window(14, 10, 10_000);
    let digest = Digest::from_bytes([15; 32]);
    assert!(matches!(
        handle
            .execute(request, digest, 20, 1_024, 1_024, |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"not-yet-published".to_vec()))
            })
            .await,
        Err(cellule_runtime::Error::OutcomeUnknown {
            request_id,
            operation_digest,
            ..
        }) if request_id == request.request_id && operation_digest == digest
    ));
    assert!(matches!(
        handle
            .execute(request, digest, 21, 1_024, 1_024, |_| {
                Ok(HandlerOutcome::Success(Vec::new()))
            })
            .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert_eq!(
        handle.resolve(request, digest, 21, 1_024).await.unwrap(),
        Resolution::Unknown
    );
}
