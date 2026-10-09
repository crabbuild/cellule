//! Evidence must include failed attempts and must never run under storage locks.

use super::*;
use crate::fleet::telemetry::{CellTelemetry, CellTelemetryHandle, FollowerAppendTiming};

struct Recorder {
    retained: Arc<Mutex<DiskAccounting>>,
    observations: Mutex<Vec<FollowerAppendTiming>>,
}

impl CellTelemetry for Recorder {
    fn follower_append(&self, timing: FollowerAppendTiming) {
        let leader = timing.leader.unwrap();
        let epoch = timing.epoch;
        assert_eq!(leader, SessionId::from_bytes([1; 16]));
        assert_eq!(epoch, 2);
        assert!(
            self.retained.try_lock().is_ok(),
            "callback held accounting lock"
        );
        self.observations.lock().unwrap().push(timing);
    }
}

#[tokio::test]
async fn warm_appends_prune_and_marker_settle_exact_bytes_without_recounts() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE data(value)"))
        .unwrap();
    let capture = db.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let handle = CellTelemetryHandle::default();
    let disk = cellule_ltx::DiskBudget::new(1 << 24);
    let store = FollowerStore::open_with_telemetry(
        root.path().to_owned(),
        limits,
        disk.clone(),
        handle.clone(),
    )
    .unwrap();
    let recorder = Arc::new(Recorder {
        retained: Arc::clone(&store.retained),
        observations: Mutex::new(Vec::new()),
    });
    handle.install(recorder.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    for sequence in 1..=32 {
        assert_eq!(
            store
                .append(leader, 2, vec![frame(sequence, segment, limits)], 0)
                .await
                .unwrap()
                .durable_through,
            sequence
        );
        assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
        assert_eq!(disk.used(), store.retained_bytes());
    }
    assert_eq!(store.scan_count(), 1);
    // Partial rewrite followed by fully covered removal exercises both exact
    // deletion paths, including scratch that coexists until synced rename.
    store
        .append(leader, 2, vec![frame(33, segment, limits)], 16)
        .await
        .unwrap();
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    store
        .append(leader, 2, vec![frame(34, segment, limits)], 33)
        .await
        .unwrap();
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    {
        let observations = recorder.observations.lock().unwrap();
        assert_eq!(observations.len(), 34);
        assert_eq!(observations[0].recounts, 2);
        assert!(
            observations[1..]
                .iter()
                .all(|row| row.recounts == 0 && row.succeeded)
        );
    }
    store.seal(leader, 2).await.unwrap();
    assert_eq!(disk.used(), follower_bytes(root.path()).unwrap());
    assert_eq!(
        store.read_tail(leader, 2, 34).await.unwrap(),
        vec![frame(34, segment, limits)]
    );
    store.retire(leader, 2, 34).await.unwrap();
    assert_eq!(disk.used(), 8);
    let candidate = store.retired_lanes(i64::MAX, 1).await.unwrap()[0];
    store.remove_retired(candidate, i64::MAX).await.unwrap();
    assert_eq!(disk.used(), 0);
    db.close().unwrap();
}

#[tokio::test]
async fn append_evidence_counts_sync_recount_duplicates_and_failures() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE data(value)"))
        .unwrap();
    let capture = db.capture().unwrap();
    let encoded = frame(1, capture.segments.first().unwrap(), limits);
    let root = tempfile::TempDir::new().unwrap();
    let handle = CellTelemetryHandle::default();
    let store = FollowerStore::open_with_telemetry(
        root.path().join("follower"),
        limits,
        cellule_ltx::DiskBudget::new(1 << 20),
        handle.clone(),
    )
    .unwrap();
    let recorder = Arc::new(Recorder {
        retained: Arc::clone(&store.retained),
        observations: Mutex::new(Vec::new()),
    });
    handle.install(recorder.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    store
        .append(leader, 2, vec![encoded.clone()], 0)
        .await
        .unwrap();
    store
        .append(leader, 2, vec![encoded.clone()], 0)
        .await
        .unwrap();
    assert!(
        store
            .append(
                leader,
                2,
                vec![frame(3, capture.segments.first().unwrap(), limits)],
                0
            )
            .await
            .is_err()
    );

    {
        let observations = recorder.observations.lock().unwrap();
        assert_eq!(observations.len(), 3);
        assert_eq!(observations[0].frames, 1);
        assert_eq!(observations[0].encoded_bytes, encoded.len() as u64);
        assert!(observations[0].succeeded);
        assert_eq!(observations[0].data_sync_calls, 1);
        assert_eq!(observations[0].directory_sync_calls, 6); // Enrollment plus cold recovery.
        assert_eq!(observations[1].data_sync_calls, 0);
        assert_eq!(observations[1].directory_sync_calls, 0);
        assert!(observations[1].succeeded);
        assert!(!observations[2].succeeded);
        assert_eq!(observations[0].recounts, 2); // Cold accounting + recovery.
        assert_eq!(observations[1].recounts, 0); // Verified duplicate; no tree scan.
        assert_eq!(observations[2].recounts, 1); // Failed mutation forces reconciliation.
        for timing in observations.iter() {
            assert!(timing.total >= timing.accounting_hold);
            assert!(timing.total >= timing.append);
            assert!(timing.append >= timing.prune);
            assert!(timing.append >= timing.data_sync);
        }
    }
    assert_eq!(store.seal(leader, 2).await.unwrap().durable_through, 1);
    assert_eq!(store.read_tail(leader, 2, 1).await.unwrap(), vec![encoded]);
    db.close().unwrap();
}

#[tokio::test]
async fn failed_batch_prefix_is_synced_before_a_duplicate_retry_receipt() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE data(value)"))
        .unwrap();
    let capture = db.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let root = tempfile::TempDir::new().unwrap();
    let telemetry = CellTelemetryHandle::default();
    let store = FollowerStore::open_with_telemetry(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 20),
        telemetry.clone(),
    )
    .unwrap();
    let recorder = Arc::new(Recorder {
        retained: Arc::clone(&store.retained),
        observations: Mutex::new(Vec::new()),
    });
    telemetry.install(recorder.clone()).unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let first = frame(1, segment, limits);
    let second = frame(2, segment, limits);
    store
        .append(leader, 2, vec![first.clone()], 0)
        .await
        .unwrap();
    // The complete second record is written before the gap fails the batch;
    // that failure precedes the batch's final data sync and acknowledgement.
    assert!(
        store
            .append(
                leader,
                2,
                vec![second.clone(), frame(4, segment, limits)],
                0
            )
            .await
            .is_err()
    );
    assert_eq!(store.scan_count(), 1);
    assert_eq!(
        store
            .append(leader, 2, vec![second.clone()], 0)
            .await
            .unwrap()
            .durable_through,
        2
    );
    assert_eq!(store.scan_count(), 2);
    {
        let observed = recorder.observations.lock().unwrap();
        assert!(!observed[1].succeeded);
        assert_eq!(observed[1].data_sync_calls, 0);
        assert!(observed[2].succeeded);
        assert_eq!(observed[2].data_sync_calls, 1);
        assert_eq!(observed[2].directory_sync_calls, 1);
    }
    assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
    store.seal(leader, 2).await.unwrap();
    drop(store);
    let recovered = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 20),
    )
    .unwrap();
    assert_eq!(
        recovered.read_tail(leader, 2, 1).await.unwrap(),
        vec![first, second]
    );
    db.close().unwrap();
}
