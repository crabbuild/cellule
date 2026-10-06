//! Queued native mutations share publication without exposing unproved state.

use super::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn queued_native_mutations_share_one_capture_and_exact_root_proof() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"queued-native-group",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first_handle = handle.clone();
    let first = tokio::spawn(async move {
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
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    let mut group_gate = Some((entered, resume_rx));
    let mut queued = Vec::new();
    for id in 2_u8..=5 {
        let handle = handle.clone();
        let gate = if id == 2 { group_gate.take() } else { None };
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
        queued.push(command);
    }
    // This round trip follows the command messages on the same actor ingress
    // channel, proving they are queued before the first publication resumes.
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    assert!(!first.is_finished(), "no reply before the first root CAS");
    store.release();
    let mut outcomes = vec![first.await.unwrap().unwrap()];
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    // The first publication has finished and the next worker is paused before
    // committing. Reset this one-shot fixture to pause the group's own CAS.
    store.blocked.store(false, Ordering::Release);
    store.released.store(false, Ordering::Release);
    store.arm_next_update();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    for command in &mut queued {
        assert!(
            futures_util::poll!(command).is_pending(),
            "no group reply before its root CAS"
        );
    }
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().ltx_root().unwrap().commit_sequence, 1);
    let mut query = Box::pin(handle.query(1_024, 1_024, |db| {
        let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
        Ok(value.to_be_bytes().to_vec())
    }));
    assert!(futures_util::poll!(&mut query).is_pending());
    let mut resolve = Box::pin(handle.resolve(
        mutation_identity_window(2, 10, 10_000),
        Digest::from_bytes([2; 32]),
        21,
        1_024,
    ));
    assert!(futures_util::poll!(&mut resolve).is_pending());
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[CommandResponseSource::Object]
    );
    store.lose_next_update_response();
    store.release();
    for command in queued {
        outcomes.push(
            tokio::time::timeout(Duration::from_secs(5), command)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(query.await.unwrap(), 5_i64.to_be_bytes());
    assert!(store.lost_update_response_consumed());
    assert!(
        matches!(resolve.await.unwrap(), Resolution::Committed(ref outcome) if outcome == &outcomes[1])
    );
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome.commit_sequence(), (index + 1) as u64);
        assert_eq!(outcome.result(), &[(index + 1) as u8]);
        let replay = handle
            .execute(
                mutation_identity_window((index + 1) as u8, 10, 10_000),
                Digest::from_bytes([(index + 1) as u8; 32]),
                21,
                1_024,
                1_024,
                |_| panic!("an exact retry must not execute its native callback"),
            )
            .await
            .unwrap();
        assert_eq!(&replay, outcome);
    }
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, 5);
    let restored = fixture._directory.path().join("group-restored.sqlite");
    fixture
        .replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        5
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM sys_requests", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        5
    );
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Object,
            CommandResponseSource::Object,
            CommandResponseSource::Object,
            CommandResponseSource::Object,
            CommandResponseSource::Object,
            CommandResponseSource::Recorded,
            CommandResponseSource::Recorded,
            CommandResponseSource::Recorded,
            CommandResponseSource::Recorded,
            CommandResponseSource::Recorded,
        ]
    );
    let publications = responses.1.lock().unwrap();
    assert_eq!(
        publications.len(),
        2,
        "queued mutations must amortize publication"
    );
    assert_eq!(
        publications
            .iter()
            .map(|timing| timing.commit_sequence)
            .collect::<Vec<_>>(),
        vec![1, 5]
    );
    assert_eq!(
        root.position.txid, 3,
        "one SQLite commit/capture for the queued group"
    );
}

fn enqueue<F>(
    handle: &cellule_runtime::cell::actor::CellHandle,
    id: u8,
    digest: u8,
    handler: F,
) -> futures_util::future::BoxFuture<'static, cellule_runtime::Result<StoredOutcome>>
where
    F: for<'connection> FnOnce(
            &cellule_ltx::rusqlite::Transaction<'connection>,
        ) -> cellule_runtime::Result<HandlerOutcome>
        + Send
        + 'static,
{
    let handle = handle.clone();
    let mut future: futures_util::future::BoxFuture<'static, _> = Box::pin(async move {
        handle
            .execute(
                mutation_identity_window(id, 10, 10_000),
                Digest::from_bytes([digest; 32]),
                20,
                1_024,
                1_024,
                handler,
            )
            .await
    });
    let waker = futures_util::task::noop_waker();
    let mut context = std::task::Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut context).is_pending());
    future
}

fn increment(
    tx: &cellule_ltx::rusqlite::Transaction<'_>,
) -> cellule_runtime::Result<HandlerOutcome> {
    tx.execute("UPDATE counter SET value = value + 1", [])?;
    Ok(HandlerOutcome::Success(b"incremented".to_vec()))
}

#[tokio::test(flavor = "multi_thread")]
async fn grouped_errors_and_rejections_preserve_each_command_savepoint() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-savepoints",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let queued = vec![
        enqueue(&handle, 2, 2, increment),
        enqueue(&handle, 2, 2, |_| {
            panic!("same-group exact replay must not run")
        }),
        enqueue(&handle, 2, 9, |_| {
            panic!("same-group conflict must not run")
        }),
        enqueue(&handle, 3, 3, |tx| {
            tx.execute("UPDATE counter SET value = value + 100", [])?;
            Ok(HandlerOutcome::Rejected(b"refused".to_vec()))
        }),
        enqueue(&handle, 4, 4, |tx| {
            tx.execute("UPDATE counter SET value = value + 100", [])?;
            Err(cellule_runtime::Error::Command("application refusal"))
        }),
        enqueue(&handle, 5, 5, increment),
        enqueue(&handle, 6, 6, |tx| {
            tx.execute("UPDATE counter SET value = value + 100", [])?;
            Ok(HandlerOutcome::Success(vec![0; 1_025]))
        }),
        enqueue(&handle, 7, 7, increment),
    ];
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    assert_eq!(first.await.unwrap().commit_sequence(), 1);
    let mut results = Vec::new();
    for future in queued {
        results.push(
            tokio::time::timeout(Duration::from_secs(5), future)
                .await
                .unwrap(),
        );
    }
    assert_eq!(results[0].as_ref().unwrap().commit_sequence(), 2);
    assert_eq!(results[0].as_ref().unwrap(), results[1].as_ref().unwrap());
    assert!(matches!(
        results[2],
        Err(cellule_runtime::Error::RequestConflict)
    ));
    assert!(
        matches!(&results[3], Ok(StoredOutcome::Rejected { commit_sequence: 3, result }) if result == b"refused")
    );
    assert!(matches!(
        results[4],
        Err(cellule_runtime::Error::Command("application refusal"))
    ));
    assert_eq!(results[5].as_ref().unwrap().commit_sequence(), 4);
    assert!(matches!(
        results[6],
        Err(cellule_runtime::Error::Command(
            "handler result exceeds command limit"
        ))
    ));
    assert_eq!(results[7].as_ref().unwrap().commit_sequence(), 5);
    assert_eq!(
        handle
            .query(1, 8, |db| {
                let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        4_i64.to_be_bytes()
    );
    for id in [4, 6] {
        assert_eq!(
            handle
                .resolve(
                    mutation_identity_window(id, 10, 10_000),
                    Digest::from_bytes([id; 32]),
                    21,
                    1_024
                )
                .await
                .unwrap(),
            Resolution::Absent
        );
    }
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(
        responses
            .1
            .lock()
            .unwrap()
            .iter()
            .map(|timing| timing.commit_sequence)
            .collect::<Vec<_>>(),
        // The head publishes alone, then the whole queued backlog fills one
        // group. The group's commit sequence is its last member's, so the two
        // publications record 1 and 5 whatever the group ceiling is; the
        // per-member outcomes above are what prove savepoint isolation.
        vec![1, 5]
    );
    assert!(
        responses
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|source| *source == CommandResponseSource::Object)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_of_durable_replays_and_conflicts_needs_no_new_capture() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-recorded",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let queued: Vec<_> = [1, 2, 1, 2]
        .into_iter()
        .map(|digest| {
            enqueue(&handle, 1, digest, |_| {
                panic!("recorded identity must not run")
            })
        })
        .collect();
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    let original = first.await.unwrap();
    for (index, future) in queued.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap();
        if index % 2 == 0 {
            assert_eq!(result.unwrap(), original);
        } else {
            assert!(matches!(
                result,
                Err(cellule_runtime::Error::RequestConflict)
            ));
        }
    }
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(responses.1.lock().unwrap().len(), 1);
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Object,
            CommandResponseSource::Recorded,
            CommandResponseSource::Recorded
        ]
    );
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().ltx_root().unwrap().position.txid, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_read_and_resolve_stop_grouping_and_observe_their_fifo_position() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-read-barrier",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let second = enqueue(&handle, 2, 2, increment);
    let third = enqueue(&handle, 3, 3, increment);
    let mut query = Box::pin(handle.query(1, 8, |db| {
        let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
        Ok(value.to_be_bytes().to_vec())
    }));
    assert!(futures_util::poll!(&mut query).is_pending());
    let mut resolve = Box::pin(handle.resolve(
        mutation_identity_window(4, 10, 10_000),
        Digest::from_bytes([4; 32]),
        21,
        1_024,
    ));
    assert!(futures_util::poll!(&mut resolve).is_pending());
    let fourth = enqueue(&handle, 4, 4, increment);
    let fifth = enqueue(&handle, 5, 5, increment);
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    for (index, future) in [first, second, third, fourth, fifth]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), future)
                .await
                .unwrap()
                .unwrap()
                .commit_sequence(),
            (index + 1) as u64
        );
    }
    assert_eq!(query.await.unwrap(), 3_i64.to_be_bytes());
    assert_eq!(resolve.await.unwrap(), Resolution::Absent);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(
        responses
            .1
            .lock()
            .unwrap()
            .iter()
            .map(|timing| timing.commit_sequence)
            .collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn group_capture_failure_preserves_each_unknown_identity_and_original_source() {
    use crate::runtime::fault_fs::FaultFileSystem;
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-capture-failure",
        Limits::default(),
        Store::new(store.clone()),
    );
    let filesystem = Arc::new(FaultFileSystem::new());
    let session = SessionId::from_bytes([140; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        16 * 1024 * 1024,
        session,
        ReplicaHost::default().with_filesystem(filesystem.clone()),
    )
    .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let queued: Vec<_> = (2..=5)
        .map(|id| enqueue(&handle, id, id, increment))
        .collect();
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    // The first command has captured and is already publishing. The fault
    // therefore hits the queued group's capture after its SQLite commit.
    filesystem.fail_next_capture();
    store.release();
    first.await.unwrap();
    let mut shared = Vec::new();
    for (index, future) in queued.into_iter().enumerate() {
        let error = tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap()
            .unwrap_err();
        let cellule_runtime::Error::OutcomeUnknown {
            request_id,
            operation_digest,
            source,
        } = error
        else {
            panic!("group must remain unresolved: {error:?}");
        };
        let id = (index + 2) as u8;
        assert_eq!(
            request_id,
            mutation_identity_window(id, 10, 10_000).request_id
        );
        assert_eq!(operation_digest, Digest::from_bytes([id; 32]));
        let cellule_runtime::Error::Shared(original) = *source else {
            panic!("group must retain its original source");
        };
        let mut cause: &(dyn std::error::Error + 'static) = original.as_ref();
        let mut found = false;
        loop {
            if let Some(io) = cause.downcast_ref::<std::io::Error>() {
                assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
                found = true;
            }
            match cause.source() {
                Some(next) => cause = next,
                None => break,
            }
        }
        assert!(found, "capture's original I/O failure must be inspectable");
        shared.push(original);
    }
    assert!(shared.iter().all(|error| Arc::ptr_eq(error, &shared[0])));
    assert!(filesystem.capture_failure_consumed());
    assert!(matches!(
        handle.query(1, 1, |_| Ok(Vec::new())).await,
        Err(cellule_runtime::Error::Fenced)
    ));
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(runtime.stats().worker_jobs(), 0);
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[CommandResponseSource::Object]
    );
    let root = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(root.commit_sequence, 1);
    let restored = fixture
        ._directory
        .path()
        .join("group-failure-restored.sqlite");
    fixture
        .replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM sys_requests", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_whole_sqlite_rollback_aborts_the_group_without_losing_the_original_error() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-whole-rollback",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let second = enqueue(&handle, 2, 2, increment);
    let third = enqueue(&handle, 3, 3, |tx| {
        tx.execute_batch("CREATE TABLE unique_keys(key INTEGER PRIMARY KEY); INSERT INTO unique_keys VALUES (1); INSERT OR ROLLBACK INTO unique_keys VALUES (1)")?;
        Ok(HandlerOutcome::Success(Vec::new()))
    });
    let fourth = enqueue(&handle, 4, 4, |_| {
        panic!("a whole rollback must stop the group")
    });
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    first.await.unwrap();
    for future in [second, third, fourth] {
        let error = future.await.unwrap_err();
        assert!(
            matches!(error, cellule_runtime::Error::Shared(ref original) if matches!(original.as_ref(), cellule_runtime::Error::Sqlite(_))),
            "{error:?}"
        );
    }
    assert_eq!(
        handle
            .query(1, 8, |db| {
                let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        1_i64.to_be_bytes()
    );
    let next = enqueue(&handle, 5, 5, increment).await.unwrap();
    assert_eq!(next.commit_sequence(), 2);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn group_deadline_retains_all_admissions_until_dispatched_sql_exits() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-deadline",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    let second = enqueue(&handle, 2, 2, move |tx| {
        entered.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(15)).unwrap();
        increment(tx)
    });
    let rest: Vec<_> = (3..=5)
        .map(|id| {
            enqueue(&handle, id, id, |_| {
                panic!("deadline must stop the remaining callbacks")
            })
        })
        .collect();
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    first.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    for future in std::iter::once(second).chain(rest) {
        let error = tokio::time::timeout(Duration::from_secs(7), future)
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, cellule_runtime::Error::OutcomeUnknown { source, .. } if matches!(*source, cellule_runtime::Error::Shared(ref original) if matches!(original.as_ref(), cellule_runtime::Error::Deadline)))
        );
    }
    assert!(
        runtime.stats().retained_bytes() >= 4 * 2_048,
        "accepted members stay charged while native SQL is still running"
    );
    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    assert!(!shutdown.is_finished());
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(runtime.stats().worker_jobs(), 0);
    let root = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(root.commit_sequence, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn binding_a_node_log_during_a_group_keeps_its_members_object_gated() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-binding-race",
        Limits::default(),
        Store::new(store.clone()),
    );
    let session = SessionId::from_bytes([145; 16]);
    let leader = NodeId::from_bytes([146; 16]);
    let follower = NodeId::from_bytes([147; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        16 * 1024 * 1024,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    let second = enqueue(&handle, 2, 2, move |tx| {
        entered.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        increment(tx)
    });
    let third = enqueue(&handle, 3, 3, increment);
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.release();
    first.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
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
                gate.clone(),
                shipper,
                node_authority,
                transport,
                lease,
            )),
        )
        .unwrap();
    resume.send(()).unwrap();
    assert_eq!(second.await.unwrap().commit_sequence(), 2);
    assert_eq!(third.await.unwrap().commit_sequence(), 3);
    // Sending the final reply wakes its waiter before response telemetry runs.
    // Wait for observation without draining admission for the next log-backed command.
    responses.wait_for_responses(3).await;
    assert_eq!(
        gate.issued_through(),
        0,
        "a final-only group log record would create a recovery gap"
    );
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Object,
            CommandResponseSource::Object,
            CommandResponseSource::Object
        ]
    );
    assert_eq!(
        enqueue(&handle, 4, 4, increment)
            .await
            .unwrap()
            .commit_sequence(),
        4
    );
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(gate.issued_through(), 1);
    assert_eq!(*authority.coverage.lock().unwrap(), vec![(1, 1)]);
    let root = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(root.commit_sequence, 4);
    assert_eq!(root.position.txid, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_group_publication_keeps_known_results_and_preserves_the_storage_cause() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"group-publication-failure",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (runtime, handle, _) = activate_runtime(&fixture, 16 * 1024 * 1024).await;
    let responses = Arc::new(RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    store.arm_next_update();
    let first = enqueue(&handle, 1, 1, increment);
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let second = enqueue(&handle, 2, 2, increment);
    let failed = enqueue(&handle, 3, 3, |_| {
        Err(cellule_runtime::Error::Command("application refusal"))
    });
    let fourth = enqueue(&handle, 4, 4, increment);
    let recorded = enqueue(&handle, 1, 1, |_| panic!("durable replay must not run"));
    assert_eq!(runtime.active_catalog_entries().await.unwrap().len(), 1);
    store.fail_puts();
    store.release();
    let original = first.await.unwrap();
    for (id, future) in [(2, second), (4, fourth)] {
        let error = tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap()
            .unwrap_err();
        let cellule_runtime::Error::OutcomeUnknown {
            request_id,
            operation_digest,
            source,
        } = error
        else {
            panic!("new group result must remain unresolved");
        };
        assert_eq!(
            request_id,
            mutation_identity_window(id, 10, 10_000).request_id
        );
        assert_eq!(operation_digest, Digest::from_bytes([id; 32]));
        let mut cause: &(dyn std::error::Error + 'static) = source.as_ref();
        let mut found = false;
        loop {
            if let Some(storage) = cause.downcast_ref::<cellule_store::StorageError>() {
                assert!(
                    matches!(storage, cellule_store::StorageError::Forbidden { path } if !path.is_empty()),
                    "{storage:?}"
                );
                found = true;
            }
            match cause.source() {
                Some(next) => cause = next,
                None => break,
            }
        }
        assert!(
            found,
            "publication's original storage failure must reach every new member"
        );
    }
    assert!(matches!(
        failed.await,
        Err(cellule_runtime::Error::Command("application refusal"))
    ));
    assert_eq!(recorded.await.unwrap(), original);
    store.allow_puts();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(runtime.stats().worker_jobs(), 0);
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Object,
            CommandResponseSource::Recorded
        ]
    );
    let root = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(root.commit_sequence, 1);
}
