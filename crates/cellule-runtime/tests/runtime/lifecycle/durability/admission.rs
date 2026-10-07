//! Node durability byte admission and reservation bounds.

use super::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn disk_refusal_with_a_follower_proven_head_preserves_the_owner() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"disk-refusal-with-follower-head",
        Limits::default(),
        Store::new(store.clone()),
    );
    let leader = SessionId::from_bytes([141; 16]);
    let member = NodeId::from_bytes([142; 16]);
    let disk = DiskBudget::new(1 << 30);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        leader,
        ReplicaHost::default().with_local_disk_budget(disk.clone()),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let follower = cellule_runtime::FollowerStore::open(
        directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(
        cellule_runtime::node::log_transport::LocalFollowerTransport::new(member, follower),
    );
    let gate =
        DurabilityGate::new(leader, NodeId::from_bytes(*leader.as_bytes()), 1, [member]).unwrap();
    let shipper = NodeLogShipper::new(gate.clone(), transport.clone(), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            fixture.target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(TestNodeAuthority::default()),
                transport,
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, leader).await;
    store.arm_next_update();
    let first = handle
        .execute(
            mutation_identity_window(141, 10, 10_000),
            Digest::from_bytes([141; 32]),
            20,
            1_024,
            1_024,
            |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(vec![1]))
            },
        )
        .await
        .unwrap();
    assert_eq!(first.commit_sequence(), 1);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();

    // The first commit is on a real fsynced follower, but its object-root CAS
    // is still paused. Refuse the next reservation before its SQL callback.
    let pressure = disk.try_reserve(disk.available() - 1).unwrap();
    let invoked = Arc::new(AtomicUsize::new(0));
    let denied_calls = invoked.clone();
    let identity = mutation_identity_window(143, 10, 10_000);
    let digest = Digest::from_bytes([143; 32]);
    let refused = handle
        .execute(identity, digest, 21, 1_024, 1_024, move |tx| {
            denied_calls.fetch_add(1, Ordering::SeqCst);
            tx.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(vec![2]))
        })
        .await;
    drop(pressure);
    // Always release the injected pause before assertions or shutdown, so a
    // failing regression cannot strand the accepted first publication.
    store.release();
    let query = read_counter(handle.clone()).await;
    let retry_calls = invoked.clone();
    let retried = handle
        .execute(identity, digest, 22, 1_024, 1_024, move |tx| {
            retry_calls.fetch_add(1, Ordering::SeqCst);
            tx.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(vec![2]))
        })
        .await;
    let shutdown = runtime.shutdown().await;

    assert!(
        matches!(
            refused,
            Err(cellule_runtime::Error::Capacity("publication backlog"))
        ),
        "{refused:?}"
    );
    assert_eq!(query.unwrap(), 1_i64.to_be_bytes());
    assert_eq!(retried.unwrap().commit_sequence(), 2);
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
    shutdown.unwrap();
    assert_eq!(disk.used(), 0);
}

struct ReplyHandoffGate {
    skip: AtomicUsize,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl CellTelemetry for ReplyHandoffGate {
    fn command_response(
        &self,
        source: CommandResponseSource,
        _elapsed: Duration,
        _confirmation: Duration,
    ) {
        assert_eq!(source, CommandResponseSource::Object);
        let mut remaining = self.skip.load(Ordering::Acquire);
        while remaining > 0 {
            match self.skip.compare_exchange_weak(
                remaining,
                remaining - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => remaining = observed,
            }
        }
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            // Inject preemption after delivery, before the actor drops its
            // completion. Production telemetry callbacks must remain nonblocking.
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_reply_releases_request_slot_before_followup_admission() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"durable-reply-admission-handoff",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    runtime
        .install_telemetry(Arc::new(ReplyHandoffGate {
            skip: AtomicUsize::new(0),
            entered: Mutex::new(Some(entered)),
            resume: Mutex::new(resume_rx),
        }))
        .unwrap();
    store.arm_next_update();
    let first_handle = handle.clone();
    let mut first = Box::pin(async move {
        first_handle
            .execute(
                mutation_identity_window(1, 10, 10_000),
                Digest::from_bytes([1; 32]),
                20,
                1_024,
                1_024,
                |tx| {
                    tx.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(vec![1]))
                },
            )
            .await
    });
    assert!(futures_util::poll!(&mut first).is_pending());
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let mut queued = Vec::new();
    for _ in 0..63 {
        let handle = handle.clone();
        let mut query = Box::pin(async move {
            handle
                .query(16, 16, |db| {
                    let value: i64 =
                        db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                    Ok(value.to_be_bytes().to_vec())
                })
                .await
        });
        assert!(futures_util::poll!(&mut query).is_pending());
        queued.push(query);
    }
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    assert!(matches!(
        handle.query(16, 16, |_| Ok(Vec::new())).await,
        Err(cellule_runtime::Error::Capacity("Cell mailbox requests"))
    ));
    assert!(futures_util::poll!(&mut first).is_pending());
    store.release();
    let outcome = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.commit_sequence(), 1);
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    // A caller has received the durable mutation. Its next read must be able
    // to occupy that completed request's slot even if the actor is preempted.
    let mut followup = Box::pin(handle.query(16, 16, |db| {
        let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
        Ok(value.to_be_bytes().to_vec())
    }));
    let admitted = futures_util::poll!(&mut followup);
    resume.send(()).unwrap();
    let observed = match admitted {
        std::task::Poll::Ready(result) => result,
        std::task::Poll::Pending => followup.await,
    };
    for query in queued {
        assert_eq!(query.await.unwrap(), 1_i64.to_be_bytes());
    }
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(
        observed.unwrap(),
        1_i64.to_be_bytes(),
        "a completed mutation must release its request slot before delivery"
    );
}

async fn read_counter(
    handle: cellule_runtime::cell::actor::CellHandle,
) -> cellule_runtime::Result<Vec<u8>> {
    handle
        .query(16, 16, |db| {
            let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_group_releases_all_request_slots_before_its_first_reply() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"durable-group-admission-handoff",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    runtime
        .install_telemetry(Arc::new(ReplyHandoffGate {
            skip: AtomicUsize::new(1),
            entered: Mutex::new(Some(entered)),
            resume: Mutex::new(resume_rx),
        }))
        .unwrap();
    store.arm_next_update();
    let first_handle = handle.clone();
    let mut first = Box::pin(async move {
        first_handle
            .execute(
                mutation_identity_window(1, 10, 10_000),
                Digest::from_bytes([1; 32]),
                20,
                1_024,
                1_024,
                |tx| {
                    tx.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(vec![1]))
                },
            )
            .await
    });
    assert!(futures_util::poll!(&mut first).is_pending());
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let (sql_entered, sql_entered_rx) = tokio::sync::oneshot::channel();
    let (sql_resume, sql_resume_rx) = mpsc::channel();
    let mut sql_gate = Some((sql_entered, sql_resume_rx));
    let mut commands = Vec::new();
    for id in 2_u8..=5 {
        let handle = handle.clone();
        let gate = if id == 2 { sql_gate.take() } else { None };
        let mut command = Box::pin(async move {
            handle
                .execute(
                    mutation_identity_window(id, 10, 10_000),
                    Digest::from_bytes([id; 32]),
                    20,
                    1_024,
                    1_024,
                    move |tx| {
                        if let Some((entered, resume)) = gate {
                            entered.send(()).unwrap();
                            resume.recv_timeout(Duration::from_secs(5)).unwrap();
                        }
                        tx.execute("UPDATE counter SET value = value + 1", [])?;
                        Ok(HandlerOutcome::Success(vec![id]))
                    },
                )
                .await
        });
        assert!(futures_util::poll!(&mut command).is_pending());
        commands.push(command);
    }
    let mut queries = Vec::new();
    for _ in 0..59 {
        let mut query = Box::pin(read_counter(handle.clone()));
        assert!(futures_util::poll!(&mut query).is_pending());
        queries.push(query);
    }
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    assert_eq!(first.await.unwrap().commit_sequence(), 1);
    tokio::time::timeout(Duration::from_secs(5), sql_entered_rx)
        .await
        .unwrap()
        .unwrap();
    // The first command has finished, leaving one slot before the group commits.
    let mut last_query = Box::pin(read_counter(handle.clone()));
    assert!(futures_util::poll!(&mut last_query).is_pending());
    queries.push(last_query);
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.blocked.store(false, Ordering::Release);
    store.released.store(false, Ordering::Release);
    store.arm_next_update();
    sql_resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    for command in &mut commands {
        assert!(futures_util::poll!(command).is_pending());
    }
    assert!(matches!(
        read_counter(handle.clone()).await,
        Err(cellule_runtime::Error::Capacity("Cell mailbox requests"))
    ));
    store.release();
    let first_member = commands.remove(0).await.unwrap();
    assert_eq!(first_member.commit_sequence(), 2);
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let mut followups = Vec::new();
    for _ in 0..4 {
        let mut query = Box::pin(read_counter(handle.clone()));
        let admitted = futures_util::poll!(&mut query);
        followups.push((query, admitted));
    }
    // All completion data remains charged while its terminal request slots
    // become available; releasing slots must not discharge retained bytes.
    assert!(runtime.stats().retained_bytes() >= 4 * 2_048);
    resume.send(()).unwrap();
    let mut observed = Vec::new();
    for (query, admitted) in followups {
        observed.push(match admitted {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => query.await,
        });
    }
    for (offset, command) in commands.into_iter().enumerate() {
        assert_eq!(
            command.await.unwrap().commit_sequence(),
            (offset + 3) as u64
        );
    }
    for query in queries {
        assert_eq!(query.await.unwrap(), 5_i64.to_be_bytes());
    }
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    for result in observed {
        assert_eq!(result.unwrap(), 5_i64.to_be_bytes());
    }
}

#[tokio::test]
async fn node_byte_reservation_rejects_overcommit_and_releases_capacity() {
    let session = SessionId::from_bytes([40; 16]);
    let local_disk = DiskBudget::new(4_096);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        1_024,
        session,
        ReplicaHost::default().with_local_disk_budget(local_disk.clone()),
    )
    .unwrap();
    let disk = local_disk.try_reserve(512).unwrap();
    let held = runtime.try_reserve_node_bytes(1_024).unwrap();

    let full = runtime.stats();
    assert_eq!(full.active_cells(), 0);
    assert_eq!(full.active_cell_capacity(), 1);
    assert_eq!(full.file_descriptors(), 0);
    assert_eq!(
        full.file_descriptor_capacity(),
        ACTIVE_CELL_FILE_DESCRIPTORS
            + cellule_runtime::fleet::resource::PUBLICATION_FILE_DESCRIPTORS
    );
    assert_eq!(full.retained_bytes(), 1_024);
    assert_eq!(full.retained_capacity_bytes(), 1_024);
    assert_eq!(full.local_disk_reserved_bytes(), 512);
    assert_eq!(full.local_disk_capacity_bytes(), 4_096);

    assert!(matches!(
        runtime.try_reserve_node_bytes(1),
        Err(cellule_runtime::Error::Capacity("node retained bytes"))
    ));
    drop(held);
    drop(disk);
    let empty = runtime.stats();
    assert_eq!(empty.file_descriptors(), 0);
    assert_eq!(
        empty.file_descriptor_capacity(),
        ACTIVE_CELL_FILE_DESCRIPTORS
            + cellule_runtime::fleet::resource::PUBLICATION_FILE_DESCRIPTORS
    );
    assert_eq!(empty.retained_bytes(), 0);
    assert_eq!(empty.local_disk_reserved_bytes(), 0);
    let released = runtime.try_reserve_node_bytes(1_024).unwrap();
    drop(released);

    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn node_byte_admission_rejects_before_sql_execution() {
    let fixture = fixture();
    let handle = activate(&fixture, 1024 * 1024).await;
    assert!(matches!(
        handle
            .execute(
                mutation_identity_window(12, 10, 10_000),
                Digest::from_bytes([13; 32]),
                20,
                1_025,
                1024 * 1024,
                |_| Ok(HandlerOutcome::Success(Vec::new())),
            )
            .await,
        Err(cellule_runtime::Error::Capacity(_))
    ));
    handle.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn precommit_disk_refusal_rolls_back_and_reuses_the_same_owner_and_identity() {
    let fixture = fixture();
    let session = SessionId::from_bytes([146; 16]);
    let disk = DiskBudget::new(1 << 30);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default().with_local_disk_budget(disk.clone()),
    )
    .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let pressure = Arc::new(Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let identity = mutation_identity_window(146, 10, 10000);
    let digest = Digest::from_bytes([146; 32]);
    let callback_pressure = pressure.clone();
    let callback_disk = disk.clone();
    let callback_calls = calls.clone();
    let refused = handle
        .execute(identity, digest, 20, 1024, 1024, move |tx| {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            tx.execute("UPDATE counter SET value = value + 1", [])?;
            // Remove the remaining headroom after SQL has run, before COMMIT.
            // The managed database must prove rollback rather than fence a safe
            // owner or claim the callback never ran.
            *callback_pressure.lock().unwrap() = Some(
                callback_disk
                    .try_reserve(callback_disk.available())
                    .unwrap(),
            );
            Ok(HandlerOutcome::Success(vec![1]))
        })
        .await;
    drop(pressure.lock().unwrap().take());
    let after_refusal = read_counter(handle.clone()).await;
    let retry_calls = calls.clone();
    let retried = handle
        .execute(identity, digest, 21, 1024, 1024, move |tx| {
            retry_calls.fetch_add(1, Ordering::SeqCst);
            tx.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(vec![1]))
        })
        .await;
    let after_retry = read_counter(handle.clone()).await;
    let shutdown = runtime.shutdown().await;
    assert!(
        matches!(
            refused,
            Err(cellule_runtime::Error::Capacity("local disk bytes"))
        ),
        "{refused:?}"
    );
    assert_eq!(after_refusal.unwrap(), 0_i64.to_be_bytes());
    assert_eq!(retried.unwrap().commit_sequence(), 1);
    assert_eq!(after_retry.unwrap(), 1_i64.to_be_bytes());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    shutdown.unwrap();
    assert_eq!(disk.used(), 0);
}
