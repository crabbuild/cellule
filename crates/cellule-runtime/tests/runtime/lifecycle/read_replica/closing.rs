//! Joined reader closure across retained clones and cancelled waiters.

use super::*;
use std::time::Duration;

struct ClosingFixture {
    fixture: Fixture,
    source: CellRuntime,
    handle: CellHandle,
    runtime: CellRuntime,
    reader: CellReadReplica,
    original: std::path::PathBuf,
}

async fn opened(fixture: Fixture) -> ClosingFixture {
    let owner = SessionId::from_bytes([44; 16]);
    let source = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1).unwrap(),
        8 << 20,
        owner,
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
    )
    .unwrap();
    let handle = bootstrap_on(&source, &fixture, owner).await;
    let registry = compiled_reader_registry();
    let directory = owner_directory(&fixture, owner, &registry).await;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 512).unwrap(),
        8 << 20,
        SessionId::from_bytes([45; 16]),
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
    )
    .unwrap();
    let original = fixture._directory.path().join("joined-reader.sqlite");
    let reader = CellReadReplica::open(
        runtime.clone(),
        registry,
        CellAuthority::new(fixture.layout.clone()),
        directory,
        fixture.replica.clone(),
        fixture.target.clone(),
        &original,
    )
    .await
    .unwrap();
    ClosingFixture {
        fixture,
        source,
        handle,
        runtime,
        reader,
        original,
    }
}

fn empty(runtime: &CellRuntime) {
    let stats = runtime.stats();
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
    assert_eq!(stats.io_slots(), 0);
}

async fn finish(fixture: ClosingFixture) {
    fixture.reader.close_and_join().await;
    fixture.runtime.shutdown().await.unwrap();
    fixture.handle.drain().await.unwrap();
    fixture.source.shutdown().await.unwrap();
}

#[tokio::test]
async fn joined_close_detaches_native_snapshots_from_retained_peer_clones() {
    let fixture = opened(fixture_for(b"reader-close-retained-clones")).await;
    let peer = fixture.reader.clone();
    let receipt = fixture.reader.receipt().await;
    assert!(fixture.original.exists());
    let (first, duplicate) = tokio::join!(fixture.reader.close_and_join(), peer.close_and_join(),);
    assert_eq!((first, duplicate), (receipt, receipt));
    assert_eq!(peer.receipt().await, receipt);
    assert!(!fixture.original.exists());
    empty(&fixture.runtime);
    assert!(matches!(
        peer.readiness().await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert!(matches!(
        peer.query::<ReadCounter>(None, 0).await,
        Err(cellule_runtime::Error::Fenced)
    ));
    let replacement = fixture
        .fixture
        ._directory
        .path()
        .join("closed-refresh.sqlite");
    assert!(matches!(
        peer.refresh(&replacement).await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert!(!replacement.exists());
    assert_eq!(peer.close_and_join().await, receipt);
    empty(&fixture.runtime);
    // The external capability remains alive through complete node drain.
    finish(fixture).await;
    assert_eq!(peer.receipt().await, receipt);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_close_keeps_old_native_query_owned_after_query_or_close_waiter_cancellation() {
    for (drop_query, drop_close) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = opened(fixture_for(b"reader-close-owned-query")).await;
        let pause = QueryPause::new();
        let token = pause.id;
        let reader = fixture.reader.clone();
        let query = tokio::spawn(async move { reader.query::<ReadCounter>(None, token).await });
        pause.entered().await;
        fixture
            .handle
            .execute(
                crate::support::fixtures::mutation_identity(46),
                Digest::from_bytes([47; 32]),
                now_ms(),
                64,
                64,
                |tx| {
                    tx.execute("UPDATE counter SET value = 1", [])?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            )
            .await
            .unwrap();
        let replacement = fixture.fixture._directory.path().join("new-reader.sqlite");
        let receipt = fixture.reader.refresh(&replacement).await.unwrap();
        let peer = fixture.reader.clone();
        let mut closing = Box::pin(fixture.reader.close_and_join());
        let first_pending = futures_util::poll!(closing.as_mut()).is_pending();
        let old_retained = fixture.original.exists();
        let new_detached = !replacement.exists();
        if drop_close {
            drop(closing);
            closing = Box::pin(peer.close_and_join());
        }
        if drop_query {
            query.abort();
        }
        let mut sibling = Box::pin(peer.close_and_join());
        let second_pending = futures_util::poll!(sibling.as_mut()).is_pending();
        let retained = fixture.runtime.stats();
        // Always unblock accepted native SQL before checking fixture assertions.
        pause.release();
        if drop_query {
            assert!(query.await.unwrap_err().is_cancelled());
        } else {
            assert!(matches!(
                query.await.unwrap(),
                Err(cellule_runtime::Error::Fenced)
            ));
        }
        assert_eq!(closing.await, receipt);
        assert_eq!(sibling.await, receipt);
        assert!(first_pending && second_pending && old_retained && new_detached);
        assert_eq!(retained.worker_jobs(), 1);
        assert!(retained.resident_bytes() > 0);
        assert!(!fixture.original.exists() && !replacement.exists());
        empty(&fixture.runtime);
        finish(fixture).await;
        assert_eq!(peer.receipt().await, receipt);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_close_owns_cancelled_refresh_native_open_until_its_uninstalled_view_is_released() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let runtime_slot = Arc::new(Mutex::new(None::<CellRuntime>));
    let slot = runtime_slot.clone();
    let armed = Arc::new(AtomicBool::new(false));
    let once = armed.clone();
    let pausing = store.clone();
    let fixture = opened(fixture_with_limits_and_store(
        b"reader-close-cancelled-native-open",
        Limits::default(),
        Store::new(store.clone()).with_read_request_observer(Arc::new(move |kind| {
            // Async root preparation has no SQL job. Arm only the actual VFS
            // page fault inside the accepted native snapshot-open job.
            if kind == cellule_store::StorageReadKind::Range
                && slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|runtime| runtime.stats().worker_jobs() == 1)
                && !once.swap(true, Ordering::AcqRel)
            {
                pausing.arm_gets();
            }
        })),
    ))
    .await;
    *runtime_slot.lock().unwrap() = Some(fixture.runtime.clone());
    let original_receipt = fixture.reader.receipt().await;
    fixture
        .handle
        .execute(
            crate::support::fixtures::mutation_identity(46),
            Digest::from_bytes([47; 32]),
            now_ms(),
            64,
            64,
            |tx| {
                // Change SQLite's schema page, ensuring open faults a new page
                // rather than reusing the unchanged first page from the old view.
                tx.execute_batch(
                    "CREATE TABLE extra(value INTEGER); UPDATE counter SET value = 1",
                )?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    let destination = fixture
        .fixture
        ._directory
        .path()
        .join("cancelled-refresh.sqlite");
    let reader = fixture.reader.clone();
    let path = destination.clone();
    let refresh = tokio::spawn(async move { reader.refresh(&path).await });
    let entered =
        tokio::time::timeout(Duration::from_secs(3), store.wait_until_get_blocked()).await;
    if entered.is_err() {
        store.release_gets();
        let _ = refresh.await;
        finish(fixture).await;
        panic!("native snapshot open did not reach its page fault");
    }
    refresh.abort();
    assert!(refresh.await.unwrap_err().is_cancelled());
    let mut closing = Box::pin(fixture.reader.close_and_join());
    let pending = futures_util::poll!(closing.as_mut()).is_pending();
    let before = fixture.runtime.stats();
    store.release_gets();
    assert_eq!(closing.await, original_receipt);
    assert!(armed.load(Ordering::Acquire) && pending);
    assert_eq!(before.worker_jobs(), 1);
    assert!(before.resident_bytes() > 0);
    assert!(!destination.exists() && !fixture.original.exists());
    empty(&fixture.runtime);
    runtime_slot.lock().unwrap().take();
    finish(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joined_close_waits_for_accepted_authority_read_and_fences_its_late_readiness_reply() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = opened(fixture_with_limits_and_store(
        b"reader-close-stalled-observation",
        Limits::default(),
        Store::new(store.clone()),
    ))
    .await;
    let receipt = fixture.reader.receipt().await;
    store.arm_gets();
    let reader = fixture.reader.clone();
    let readiness = tokio::spawn(async move { reader.readiness().await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.wait_until_get_blocked(),
    )
    .await
    .unwrap();
    let mut closing = Box::pin(fixture.reader.close_and_join());
    let pending = futures_util::poll!(closing.as_mut()).is_pending();
    let retained = fixture.original.exists();
    store.release_gets();
    assert!(matches!(
        readiness.await.unwrap(),
        Err(cellule_runtime::Error::Fenced)
    ));
    assert_eq!(closing.await, receipt);
    assert!(pending && retained);
    empty(&fixture.runtime);
    finish(fixture).await;
}
