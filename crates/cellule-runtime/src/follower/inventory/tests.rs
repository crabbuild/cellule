use super::*;

fn store(root: &Path) -> FollowerStore {
    FollowerStore::open(
        root.to_owned(),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(128 << 20),
    )
    .unwrap()
}

fn leader() -> SessionId {
    SessionId::from_bytes([1; 16])
}

fn enroll(root: &Path, epoch: u64) {
    ensure_lane_directories(
        root,
        Lane {
            leader: leader(),
            epoch,
        },
    )
    .unwrap();
}

fn used(store: &FollowerStore) -> u64 {
    *store.index_used.lock().unwrap()
}

#[tokio::test]
async fn persisted_cold_lanes_are_sorted_paged_and_charged_without_loading_tail_indexes() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    for epoch in (1..=255).rev() {
        enroll(root.path(), epoch);
    }
    assert!(store.lanes.lock().unwrap().is_empty());
    assert!(
        (MAX_INVENTORY_LANES + MAX_PAGE_ENTRIES) * std::mem::size_of::<FollowerLaneObservation>()
            < INVENTORY_BYTES as usize
    );
    let first = store.fleet_lanes_page(None, 128, 10).await.unwrap();
    assert_eq!(first.total_lanes(), 255);
    assert_eq!(first.unretired_lanes(), 255);
    assert_eq!(first.entries().len(), 128);
    assert_eq!(first.entries()[0].epoch, 1);
    assert_eq!(first.entries()[127].epoch, 128);
    assert_eq!(first.mode(), NodeMode::Active);
    let cursor = FollowerInventoryCursor::from_bytes(&first.next().unwrap().to_bytes()).unwrap();
    let second = store.fleet_lanes_page(Some(cursor), 128, 11).await.unwrap();
    assert_eq!(second.topology(), first.topology());
    assert_eq!(second.entries().len(), 127);
    assert_eq!(second.entries()[0].epoch, 129);
    assert_eq!(second.entries()[126].epoch, 255);
    assert!(second.next().is_none());
    assert_eq!(used(&store), 2 * INVENTORY_BYTES);
    assert!(store.lanes.lock().unwrap().is_empty());
    assert_eq!(store.scan_count(), 0);
    drop(first);
    drop(second);
    assert_eq!(used(&store), 0);
}

#[tokio::test]
async fn topology_or_store_replacement_rejects_cursor_and_releases_admission() {
    let root = tempfile::tempdir().unwrap();
    let first_store = store(root.path());
    enroll(root.path(), 1);
    enroll(root.path(), 2);
    let page = first_store.fleet_lanes_page(None, 1, 10).await.unwrap();
    let cursor = page.next().unwrap();
    drop(page);
    first_store.seal(leader(), 1).await.unwrap();
    assert!(
        first_store
            .fleet_lanes_page(Some(cursor), 1, 11)
            .await
            .is_err()
    );
    assert_eq!(used(&first_store), 0);
    let sealed = first_store.fleet_lanes_page(None, 1, 12).await.unwrap();
    assert_eq!(sealed.entries()[0].state, FollowerLaneState::Sealed);
    assert_eq!(sealed.entries()[0].sealed_through, Some(0));
    let cursor = sealed.next().unwrap();
    drop(sealed);
    let reopened = store(root.path());
    assert!(
        reopened
            .fleet_lanes_page(Some(cursor), 1, 13)
            .await
            .is_err()
    );
    assert_eq!(used(&reopened), 0);
    first_store.retire(leader(), 1, 0).await.unwrap();
    let retired = first_store.fleet_lanes_page(None, 128, 14).await.unwrap();
    assert_eq!(retired.entries()[0].state, FollowerLaneState::Retired);
    assert_eq!(retired.entries()[0].retired_through, Some(0));
    assert_eq!(retired.unretired_lanes(), 1);
    drop(retired);
    assert_eq!(used(&first_store), 0);
}

#[tokio::test]
async fn cordon_and_quarantine_remain_visible_without_manufacturing_empty_safety() {
    let root = tempfile::tempdir().unwrap();
    let gate = crate::fleet::admission::NodeAdmission::default();
    let store = store(root.path()).with_node_admission(gate.clone());
    enroll(root.path(), 1);
    gate.cordon().unwrap();
    std::fs::create_dir(root.path().join(FOLLOWER_QUARANTINE)).unwrap();
    std::fs::write(
        root.path().join(FOLLOWER_QUARANTINE).join("1.bad"),
        b"unknown",
    )
    .unwrap();
    let page = store.fleet_lanes_page(None, 128, 20).await.unwrap();
    assert_eq!(page.mode(), NodeMode::Cordoned);
    assert_eq!(page.total_lanes(), 1);
    assert_eq!(page.unretired_lanes(), 1);
    assert_eq!(page.quarantined_entries(), 1);
    assert_eq!(page.observed_at_ms(), 20);
    assert_eq!(page.entries()[0].leader, leader());
}

#[tokio::test]
async fn malformed_or_oversized_markers_fail_without_unbounded_read_or_leaked_page() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    enroll(root.path(), 1);
    let directory = lane_directory(
        root.path(),
        Lane {
            leader: leader(),
            epoch: 1,
        },
    );
    std::fs::write(directory.join("sealed"), [0; 9]).unwrap();
    assert!(store.fleet_lanes_page(None, 128, 1).await.is_err());
    assert_eq!(used(&store), 0);
    std::fs::write(directory.join("sealed"), 5_u64.to_le_bytes()).unwrap();
    std::fs::write(directory.join("retired"), 4_u64.to_le_bytes()).unwrap();
    assert!(store.fleet_lanes_page(None, 128, 1).await.is_err());
    assert_eq!(used(&store), 0);
    for limit in [0, 129, usize::MAX] {
        assert!(store.fleet_lanes_page(None, limit, 1).await.is_err());
    }
    assert!(store.fleet_lanes_page(None, 1, -1).await.is_err());
    assert!(FollowerInventoryCursor::from_bytes(&[0; 56]).is_err());
    assert!(FollowerInventoryCursor::from_bytes(&[1; 57]).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn special_directory_or_marker_cannot_be_reported_as_an_empty_or_retired_store() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    std::os::unix::fs::symlink(root.path().join("missing"), root.path().join("followers")).unwrap();
    assert!(store.fleet_lanes_page(None, 128, 1).await.is_err());
    assert_eq!(used(&store), 0);
    std::fs::remove_file(root.path().join("followers")).unwrap();
    enroll(root.path(), 1);
    let directory = lane_directory(
        root.path(),
        Lane {
            leader: leader(),
            epoch: 1,
        },
    );
    std::os::unix::fs::symlink(root.path().join("missing"), directory.join("retired")).unwrap();
    assert!(store.fleet_lanes_page(None, 128, 1).await.is_err());
    assert_eq!(used(&store), 0);
}

#[tokio::test]
async fn cancelled_page_keeps_memory_until_its_local_scan_finishes() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = {
        let retained = Arc::clone(&store.retained);
        std::thread::spawn(move || {
            let _disk = retained.lock().unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    };
    ready_rx.recv().unwrap();
    let task = {
        let store = store.clone();
        tokio::spawn(async move { store.fleet_lanes_page(None, 1, 1).await })
    };
    while used(&store) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert_eq!(used(&store), INVENTORY_BYTES);
    release_tx.send(()).unwrap();
    let _ = task.await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while used(&store) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    holder.join().unwrap();
}
