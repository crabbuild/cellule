//! Node durability byte admission and reservation bounds.

use super::*;
use std::time::Duration;

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
        if self
            .skip
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return;
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
