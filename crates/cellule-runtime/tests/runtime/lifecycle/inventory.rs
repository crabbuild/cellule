//! Public ownership inventory, residence tracking, and native-byte accounting.

use super::*;
use cellule_runtime::cell::actor::{CellInventoryCursor, CellInventoryEntry};
use cellule_runtime::fleet::operations::{DrainBlocker, MAX_PAGE_BYTES};

pub(super) async fn stable_owner(
    runtime: &CellRuntime,
) -> cellule_runtime::cell::actor::OwnedCellObservation {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let page = runtime.fleet_cells_page(None, 128).await.unwrap();
            if let Some(CellInventoryEntry::Owned(owner)) = page.entries().first()
                && owner.cost.is_some()
                && owner.stable_observations == 2
            {
                return (**owner).clone();
            }
            drop(page);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paged_inventory_preserves_all_owners_and_rejects_a_release_changed_cursor() {
    let first = fixture_for(b"inventory-first");
    let second = fixture_for(b"inventory-second");
    let session = SessionId::from_bytes([4; 16]);
    let pool = SqlWorkerPool::new(2, 10).unwrap();
    let runtime = CellRuntime::new(pool.clone(), 64 << 20, session).unwrap();
    let first_handle = bootstrap_on(&runtime, &first, session).await;
    let second_handle = bootstrap_on(&runtime, &second, session).await;
    let before = runtime.stats().retained_bytes();
    let page = runtime.fleet_cells_page(None, 1).await.unwrap();
    assert_eq!(page.session(), session);
    assert_eq!(page.owned_cells(), 2);
    assert_eq!(page.transitioning_cells(), 0);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(
        runtime.stats().retained_bytes(),
        before + MAX_PAGE_BYTES as usize
    );
    let cursor = CellInventoryCursor::from_bytes(&page.next().unwrap().to_bytes()).unwrap();
    let next = runtime.fleet_cells_page(Some(cursor), 1).await.unwrap();
    assert!(next.next().is_none());
    assert_eq!(next.topology(), page.topology());
    let mut actual = [page.entries()[0].cell(), next.entries()[0].cell()];
    actual.sort_by_key(|cell| *cell.as_bytes());
    let mut expected = [first.target.cell_id(), second.target.cell_id()];
    expected.sort_by_key(|cell| *cell.as_bytes());
    assert_eq!(actual, expected);
    drop(next);
    drop(page);
    assert_eq!(runtime.stats().retained_bytes(), before);
    first_handle.drain().await.unwrap();
    assert!(matches!(
        runtime.fleet_cells_page(Some(cursor), 1).await,
        Err(cellule_runtime::Error::Node(_))
    ));
    let remaining = runtime.fleet_cells_page(None, 128).await.unwrap();
    assert_eq!(remaining.owned_cells(), 1);
    assert_eq!(remaining.entries()[0].cell(), second.target.cell_id());
    drop(remaining);
    second_handle.drain().await.unwrap();
    let empty = runtime.fleet_cells_page(None, 128).await.unwrap();
    assert!(empty.entries().is_empty());
    assert!(empty.next().is_none());
    drop(empty);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_owner_stays_visible_and_later_use_cannot_reset_residence_time() {
    let fixture = fixture();
    let (runtime, handle, _pool) = activate_runtime(&fixture, 64 << 20).await;
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(initial) = &page.entries()[0] else {
        panic!("active owner omitted")
    };
    let since = initial.resident_since_ms;
    let topology = page.topology();
    assert_eq!(initial.target, fixture.target);
    assert!(initial.position.is_some());
    assert!(initial.cost.is_some());
    assert_eq!(initial.maintenance_cost, initial.cost);
    assert!(initial.database_bytes.is_some_and(|bytes| bytes > 0));
    assert!(initial.sampled_at_ms.is_some());
    assert!(initial.stable_observations >= 1);
    drop(page);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let started = Arc::new(Notify::new());
    let (release_tx, release_rx) = mpsc::channel();
    let query = {
        let handle = handle.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            handle
                .query(64, 64, move |_| {
                    started.notify_one();
                    release_rx.recv().unwrap();
                    Ok(Vec::new())
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let observation = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        runtime.fleet_cells_page(None, 128),
    )
    .await;
    // Release before assertions so a failed inventory cannot strand SQLite.
    release_tx.send(()).unwrap();
    query.await.unwrap().unwrap();
    let page = observation.unwrap().unwrap();
    assert_eq!(page.topology(), topology);
    assert_eq!(page.owned_cells(), 1);
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("busy owner omitted")
    };
    assert_eq!(owner.resident_since_ms, since);
    assert!(owner.last_used_ms > since);
    assert!(owner.blockers.contains(&DrainBlocker::BusyExecution));
    assert!(owner.maintenance_cost.is_some());
    drop(page);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_invalidates_demand_then_real_worker_samples_restore_stability() {
    let fixture = fixture_for(b"inventory-growing-cell");
    let (runtime, handle, _pool) = activate_runtime(&fixture, 64 << 20).await;
    let initial = stable_owner(&runtime).await;
    let initial_bytes = initial.database_bytes.unwrap();
    let started = Arc::new(Notify::new());
    let (release_tx, release_rx) = mpsc::channel();
    let clock = now_ms();
    let write = {
        let handle = handle.clone();
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            handle.execute(mutation_identity_window(81, clock, clock + 60_000),
                Digest::from_bytes([81; 32]), clock, 64, 64, move |transaction| {
                    started.notify_one();
                    release_rx.recv().unwrap();
                    transaction.execute_batch("CREATE TABLE payload(value BLOB NOT NULL); INSERT INTO payload VALUES(randomblob(1048576))")?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                }).await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let observed = runtime.fleet_cells_page(None, 128).await;
    release_tx.send(()).unwrap();
    let committed = write.await.unwrap().unwrap();
    let page = observed.unwrap();
    let CellInventoryEntry::Owned(during) = &page.entries()[0] else {
        panic!("executing owner omitted")
    };
    assert!(during.cost.is_none());
    assert_eq!(during.maintenance_cost, initial.cost);
    assert_eq!(during.stable_observations, 0);
    assert!(during.blockers.contains(&DrainBlocker::UnknownInventory));
    drop(page);
    let settled = stable_owner(&runtime).await;
    assert!(settled.database_bytes.unwrap() > initial_bytes);
    assert_eq!(settled.resident_since_ms, initial.resident_since_ms);
    assert_eq!(settled.generation, initial.generation);
    assert_eq!(
        settled.position.unwrap().root.commit_sequence,
        committed.commit_sequence()
    );
    assert_eq!(settled.cost, initial.cost);
    assert_eq!(settled.maintenance_cost, initial.maintenance_cost);
    assert!(settled.work_blocker.is_none());
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_schema_inventory_has_no_cost_and_cannot_release_its_owner() {
    let fixture = fixture_for(b"inventory-missing-queue-schema");
    let session = SessionId::from_bytes([4; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 64 << 20, session).unwrap();
    let _handle = bootstrap_role_on(
        &runtime,
        &fixture,
        session,
        CatalogRole::Queue,
        |transaction| {
            transaction.execute_batch("CREATE TABLE payload(value INTEGER)")?;
            Ok(())
        },
    )
    .await;
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("unknown owner omitted")
    };
    assert!(owner.cost.is_none());
    assert!(owner.database_bytes.is_none());
    assert_eq!(owner.stable_observations, 0);
    assert!(owner.blockers.contains(&DrainBlocker::UnknownInventory));
    let generation = owner.generation;
    drop(page);
    assert!(
        runtime
            .release_idle_cell(fixture.target.cell_id(), session, generation)
            .await
            .is_err()
    );
    let authority = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.value().owner.as_ref().unwrap().session, session);
    assert_eq!(runtime.unreleased_cell_count().await.unwrap(), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blob_owner_has_no_busy_maintenance_envelope_and_refusal_leaves_foreground_open() {
    use cellule_runtime::cell::actor::MaintenanceCellRelease;
    let fixture = fixture_for(b"inventory-unproven-blob-owner");
    let session = SessionId::from_bytes([4; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 1).unwrap(), 64 << 20, session).unwrap();
    let handle = bootstrap_role_on(
        &runtime,
        &fixture,
        session,
        CatalogRole::Blob,
        |transaction| {
            transaction.execute_batch(
                "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(42)",
            )?;
            Ok(())
        },
    )
    .await;
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("Blob owner omitted")
    };
    assert!(owner.maintenance_cost.is_none());
    let generation = owner.generation;
    let incarnation = owner.incarnation;
    let epoch = owner.position.as_ref().unwrap().epoch;
    drop(page);
    let result = runtime
        .release_maintenance_cell_at(
            fixture.target.cell_id(),
            session,
            generation,
            incarnation,
            epoch,
            tokio::time::Instant::now() + std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(matches!(
        result,
        MaintenanceCellRelease::Refused {
            blocker: DrainBlocker::UnknownInventory,
            error: None
        }
    ));
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("Blob owner omitted")
    };
    assert!(!owner.quiescing);
    drop(page);
    assert_eq!(
        handle
            .query(64, 64, |connection| {
                let value = connection
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        42_i64.to_be_bytes()
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
}
