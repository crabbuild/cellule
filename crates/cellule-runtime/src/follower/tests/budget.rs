//! Follower byte budgets, admission, and grace-aged lane collection.

use super::*;

#[tokio::test]
async fn multiple_rotations_in_one_batch_preserve_warm_and_cold_tail_locations() {
    let limits = cellule_ltx::Limits {
        max_capture_bytes: 256 << 20,
        ..Default::default()
    };
    let source = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    db.transaction(|tx| {
        tx.execute_batch("CREATE TABLE data(value); INSERT INTO data VALUES(randomblob(8388608))")
    })
    .unwrap();
    let capture = db.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let inputs = (1..=17)
        .map(|sequence| frame(sequence, segment, limits))
        .collect::<Vec<_>>();
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(512 << 20);
    let store = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    store
        .append(leader, 2, vec![inputs[0].clone()], 0)
        .await
        .unwrap();
    store
        .append(leader, 2, inputs[1..].to_vec(), 0)
        .await
        .unwrap();
    let chunks = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks");
    assert!(inputs[0].len() as u64 > ROTATE_BYTES);
    assert_eq!(
        std::fs::read_dir(chunks)
            .unwrap()
            .filter(
                |entry| parse_chunk_name(entry.as_ref().unwrap().file_name().to_str().unwrap())
                    .is_some()
            )
            .count(),
        inputs.len() - 1
    );
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    store.seal(leader, 2).await.unwrap();
    for (index, expected) in inputs.iter().enumerate() {
        let page = store
            .read_tail_page(leader, 2, (index + 1) as u64)
            .await
            .unwrap();
        assert_eq!(page.frames, vec![expected.clone()]);
    }
    drop(store);
    assert_eq!(disk.used(), 0);
    let cold = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    for (index, expected) in inputs.iter().enumerate() {
        let page = cold
            .read_tail_page(leader, 2, (index + 1) as u64)
            .await
            .unwrap();
        assert_eq!(page.frames, vec![expected.clone()]);
    }
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    db.close().unwrap();
}

#[tokio::test]
async fn rotation_and_closed_chunk_collection_settle_exact_physical_bytes() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    db.transaction(|tx| {
        tx.execute_batch("CREATE TABLE data(value); INSERT INTO data VALUES(randomblob(8388608))")
    })
    .unwrap();
    let capture = db.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(256 << 20);
    let store = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    for sequence in 1..=9 {
        store
            .append(leader, 2, vec![frame(sequence, segment, limits)], 0)
            .await
            .unwrap();
        assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    }
    let chunks = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks");
    let closed_last = std::fs::read_dir(&chunks)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            parse_chunk_name(entry.file_name().to_str().unwrap()).map(|(_, last)| last)
        })
        .max()
        .unwrap();
    assert!(closed_last < 9);
    store
        .append(leader, 2, vec![frame(10, segment, limits)], closed_last)
        .await
        .unwrap();
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    assert!(std::fs::read_dir(&chunks).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        name == "open.log"
            || parse_chunk_name(name.to_str().unwrap())
                .is_some_and(|(first, last)| first > closed_last && last < 10)
    }));
    store.seal(leader, 2).await.unwrap();
    drop(store);
    assert_eq!(disk.used(), 0);
    let cold = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    for sequence in closed_last + 1..=10 {
        let page = cold.read_tail_page(leader, 2, sequence).await.unwrap();
        assert_eq!(page.frames, vec![frame(sequence, segment, limits)]);
    }
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    db.close().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn warm_append_does_not_recount_an_unrelated_lane() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    database
        .transaction(|tx| tx.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(1 << 24);
    let store = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    store
        .append(leader, 2, vec![frame(1, segment, limits)], 0)
        .await
        .unwrap();
    let unrelated = lane_directory(
        root.path(),
        Lane {
            leader: SessionId::from_bytes([2; 16]),
            epoch: 2,
        },
    );
    std::fs::create_dir_all(&unrelated).unwrap();
    std::os::unix::fs::symlink("missing", unrelated.join("special")).unwrap();
    assert!(follower_bytes(root.path()).is_err());
    // A whole-store recount would turn this independently valid append into
    // an error. Its lane-local settlement cannot visit the unrelated entry.
    assert_eq!(
        store
            .append(leader, 2, vec![frame(2, segment, limits)], 0)
            .await
            .unwrap()
            .durable_through,
        2
    );
    std::fs::remove_dir_all(unrelated).unwrap();
    assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
    assert_eq!(disk.used(), store.retained_bytes());
    database.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_lanes_create_and_sync_shared_parent_directories() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    database
        .transaction(|tx| tx.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(1 << 24);
    let store = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    let inputs = (1..=32)
        .map(|id| {
            let leader = SessionId::from_bytes([id; 16]);
            (leader, scoped_frame(leader, 1, segment, limits))
        })
        .collect::<Vec<_>>();
    let mut jobs = tokio::task::JoinSet::new();
    for (leader, encoded) in &inputs {
        let store = store.clone();
        let leader = *leader;
        let encoded = encoded.clone();
        jobs.spawn(async move {
            store.append(leader, 2, vec![encoded], 0).await.unwrap();
            store.seal(leader, 2).await.unwrap();
        });
    }
    while let Some(result) = jobs.join_next().await {
        result.unwrap();
    }
    assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
    drop(store);
    assert_eq!(disk.used(), 0);
    let cold = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    for (leader, encoded) in inputs {
        assert_eq!(cold.read_tail(leader, 2, 1).await.unwrap(), vec![encoded]);
    }
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    database.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_cancelled_lane_does_not_hold_other_lanes_accounting() {
    struct Completed(Mutex<Option<tokio::sync::oneshot::Sender<bool>>>);
    impl crate::fleet::telemetry::CellTelemetry for Completed {
        fn follower_append(&self, timing: crate::fleet::telemetry::FollowerAppendTiming) {
            if timing.leader == Some(SessionId::from_bytes([1; 16]))
                && let Some(completed) = self.0.lock().unwrap().take()
            {
                completed.send(timing.succeeded).unwrap();
            }
        }
    }
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    database
        .transaction(|tx| tx.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(1 << 20);
    let telemetry = crate::fleet::telemetry::CellTelemetryHandle::default();
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    telemetry
        .install(Arc::new(Completed(Mutex::new(Some(completed_tx)))))
        .unwrap();
    let store =
        FollowerStore::open_with_telemetry(root.path().to_owned(), limits, disk.clone(), telemetry)
            .unwrap();
    let blocked = Lane {
        leader: SessionId::from_bytes([1; 16]),
        epoch: 2,
    };
    let lane_lock = store.lane_lock(blocked).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holding = tokio::task::spawn_blocking(move || {
        let _lane = lane_lock.lock().unwrap();
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.await.unwrap();
    let blocked_store = store.clone();
    let blocked_frame = scoped_frame(blocked.leader, 1, segment, limits);
    let waiting = tokio::spawn(async move {
        blocked_store
            .append(blocked.leader, 2, vec![blocked_frame], 0)
            .await
    });
    // Observe the dispatched worker's shared maintenance guard while its lane
    // is blocked. No sleep-based guess about which worker acquired a lock.
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.maintenance.try_write().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let other = SessionId::from_bytes([2; 16]);
    let progress = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.append(other, 2, vec![scoped_frame(other, 1, segment, limits)], 0),
    )
    .await;
    waiting.abort();
    let cancelled = waiting.await;
    release_tx.send(()).unwrap();
    holding.await.unwrap();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), completed_rx).await;
    assert!(entered.is_ok());
    assert!(cancelled.unwrap_err().is_cancelled());
    assert_eq!(progress.unwrap().unwrap().durable_through, 1);
    assert!(completed.unwrap().unwrap());
    // Cancellation of the async waiter does not cancel an already dispatched
    // durability worker. Sealing waits behind it and verifies its exact prefix.
    assert_eq!(
        store.seal(blocked.leader, 2).await.unwrap().durable_through,
        1
    );
    assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
    assert_eq!(disk.used(), store.retained_bytes());
    database.close().unwrap();
}

#[tokio::test]
async fn partial_prune_reserves_its_temporary_before_rewriting() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let frames = (1..=5)
        .map(|sequence| frame(sequence, segment, limits))
        .collect::<Vec<_>>();
    let record_bytes = frames[0].len() as u64 + RECORD_HEADER_BYTES as u64;
    assert!(frames.iter().all(|frame| frame.len() == frames[0].len()));
    let disk = cellule_ltx::DiskBudget::new(record_bytes * 7);
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let store = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    store
        .append(leader, 2, frames[..4].to_vec(), 0)
        .await
        .unwrap();
    let chunks = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks");
    let original = std::fs::read(chunks.join("open.log")).unwrap();
    let blocker = disk.try_reserve(record_bytes * 2).unwrap();

    // One new frame fits, but copying the two uncovered records would exceed
    // the remaining capacity while the original open file still exists.
    assert!(matches!(
        store.append(leader, 2, vec![frames[4].clone()], 2).await,
        Err(Error::Ltx(cellule_ltx::LtxError::Limit(
            cellule_ltx::LimitKind::LocalDiskBytes
        )))
    ));
    assert_eq!(std::fs::read(chunks.join("open.log")).unwrap(), original);
    assert!(!chunks.join("open.log.tmp").exists());
    assert_eq!(store.retained_bytes(), record_bytes * 4);
    assert_eq!(disk.used(), record_bytes * 6);

    drop(blocker);
    let receipt = store
        .append(leader, 2, vec![frames[4].clone()], 2)
        .await
        .unwrap();
    assert_eq!(receipt.base_sequence, 3);
    assert_eq!(receipt.durable_through, 5);
    assert_eq!(store.retained_bytes(), record_bytes * 3);
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    assert_eq!(disk.available(), record_bytes * 4);
    assert!(!chunks.join("open.log.tmp").exists());
    store.seal(leader, 2).await.unwrap();
    drop(store);
    assert_eq!(disk.used(), 0);
    let cold = FollowerStore::open(root.path().to_owned(), limits, disk.clone()).unwrap();
    assert_eq!(cold.quarantined_entries(), 0);
    assert_eq!(
        cold.read_tail(leader, 2, 3).await.unwrap(),
        frames[2..].to_vec()
    );
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    database.close().unwrap();
}

#[test]
fn open_reserves_only_existing_follower_bytes_and_rejects_an_undersized_budget() {
    let root = tempfile::TempDir::new().unwrap();
    std::fs::create_dir(root.path().join("followers")).unwrap();
    std::fs::write(root.path().join("followers/retained.log"), [0_u8; 17]).unwrap();
    std::fs::create_dir(root.path().join("sessions")).unwrap();
    std::fs::write(root.path().join("sessions/unrelated.sqlite"), [0_u8; 64]).unwrap();

    assert!(
        FollowerStore::open(
            root.path().to_owned(),
            cellule_ltx::Limits::default(),
            cellule_ltx::DiskBudget::new(16),
        )
        .is_err()
    );

    let store = FollowerStore::open(
        root.path().to_owned(),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(32),
    )
    .unwrap();
    assert_eq!(store.retained_bytes(), 17);
    assert_eq!(store.available_bytes(), 15);
    assert_eq!(store.quarantined_entries(), 1);
    assert!(!root.path().join("followers/retained.log").exists());
}
#[tokio::test]
async fn retired_lane_collection_requires_exact_grace_aged_candidate() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let encoded = frame(1, capture.segments.first().unwrap(), limits);
    let leader = SessionId::from_bytes([1; 16]);
    let root = tempfile::TempDir::new().unwrap();
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    store.append(leader, 2, vec![encoded], 0).await.unwrap();
    store.retire(leader, 2, 1).await.unwrap();

    let candidates = store.retired_lanes(i64::MAX, 1).await.unwrap();
    assert_eq!(candidates.len(), 1);
    let candidate = candidates[0];
    assert_eq!(candidate.leader(), leader);
    assert_eq!(candidate.epoch(), 2);
    assert_eq!(candidate.covered_through(), 1);
    assert!(
        store
            .remove_retired(candidate, candidate.retired_at_ms().saturating_sub(1))
            .await
            .is_err()
    );

    assert!(store.remove_retired(candidate, i64::MAX).await.unwrap());
    assert!(!store.remove_retired(candidate, i64::MAX).await.unwrap());
    assert_eq!(store.retained_bytes(), 0);
    assert!(store.retired_lanes(i64::MAX, 1).await.unwrap().is_empty());
    database.close().unwrap();
}
