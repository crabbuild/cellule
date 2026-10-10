use super::*;
use cellule_ltx::{CellReplica, CellStorageLayout, Db, Host, Limits};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

fn resources(memory: usize) -> ResourceLedger {
    ResourceLedger::new(
        ResourceCost::zero()
            .with_retained_bytes(memory)
            .with_publication_file_descriptors(64),
    )
}

#[tokio::test]
async fn fully_resident_worker_pool_can_admit_a_shared_capture() {
    let directory = tempfile::tempdir().unwrap();
    let pool = crate::SqlWorkerPool::new(1, 1).unwrap();
    let host = Host::default().with_local_disk_budget(cellule_ltx::DiskBudget::new(8 << 20));
    let runtime = crate::CellRuntime::new_with_replica_host(
        pool.clone(),
        4 << 20,
        crate::SessionId::from_bytes([9; 16]),
        host.clone(),
    )
    .unwrap();
    let ledger = pool.resource_ledger();
    let active = ledger.try_reserve(ResourceCost::active_cell()).unwrap();
    let coordinator = SharedPublication::new(ledger.clone(), Default::default());
    let replica = CellReplica::new(
        CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("resident-shared"),
            [3; 16],
        ),
        [1; 32],
        [1; 16],
        Limits::default(),
    )
    .unwrap()
    .with_host(host.clone());
    let mut db =
        Db::open_with_host(&directory.path().join("source"), Limits::default(), host).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let prepared = coordinator
        .submit(
            &replica,
            &cuts,
            directory.path().to_owned(),
            coordinator.admit().await.unwrap(),
        )
        .await
        .unwrap();
    assert!(
        prepared.is_some(),
        "resident Cell descriptors must not consume node publication headroom"
    );
    drop(prepared);
    coordinator.shutdown().await.unwrap();
    drop(cuts);
    drop(db);
    drop(replica);
    drop(active);
    runtime.shutdown().await.unwrap();
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
}

#[tokio::test]
async fn minimum_host_permits_drain_shared_work_after_a_sibling_waiter_cancels() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::default()
        .with_local_disk_budget(cellule_ltx::DiskBudget::new(8 << 20))
        .with_io_slots(Arc::new(Semaphore::new(1)))
        .with_job_slots(Arc::new(Semaphore::new(1)))
        .with_dirty_slots(Arc::new(Semaphore::new(1)))
        .with_recovery_slots(Arc::new(Semaphore::new(1)))
        .with_scratch_slots(Arc::new(Semaphore::new(1)));
    let disk = host.local_disk_budget();
    let store = Store::new(Arc::new(InMemory::new()));
    let ledger = resources(4 << 20);
    let coordinator = SharedPublication::new(ledger.clone(), Default::default());
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let mut replicas = Vec::new();
    let mut responses = Vec::new();
    let mut entries = Vec::new();
    // Preverify both inputs, then enqueue without yielding. The worker sees
    // the complete cohort rather than depending on workstation scheduling.
    for i in 1..=2 {
        let replica = CellReplica::new(
            CellStorageLayout::new(store.clone(), Path::from("shared-budget"), [3; 16]),
            [i; 32],
            [i; 16],
            Limits::default(),
        )
        .unwrap()
        .with_host(host.clone());
        let captures = replica.shared_captures(&cuts).await.unwrap().unwrap();
        let slot = coordinator.admit().await.unwrap();
        let memory = ledger
            .try_reserve(
                ResourceCost::zero()
                    .with_retained_bytes(512 << 10)
                    .with_publication_file_descriptors(3),
            )
            .unwrap();
        let (reply, response) = oneshot::channel();
        responses.push(response);
        replicas.push(replica);
        entries.push(Entry {
            captures,
            scratch: directory.path().to_owned(),
            accepted_at: Instant::now(),
            memory,
            _slot: slot,
            reply,
        });
    }
    for mut entry in entries {
        entry.accepted_at = Instant::now();
        coordinator
            .sender
            .try_send(Message::Capture(Box::new(entry)))
            .unwrap();
    }
    drop(responses.pop());
    let response = responses.pop().unwrap();
    let shared = tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let prepared = replicas[0]
        .prepare_shared(None, &shared.append, 1, 1)
        .await
        .unwrap();
    assert_eq!(
        replicas
            .iter()
            .map(|replica| replica.publication_cost().objects)
            .sum::<u64>(),
        2
    );
    assert_eq!(prepared.root().position, cuts.position);
    drop(shared);
    coordinator.shutdown().await.unwrap();
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    assert_eq!(disk.used(), 0);
    assert!(coordinator.admit().await.is_err());
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("shared-publication")
    }));
}

#[tokio::test]
async fn memory_pressure_falls_back_without_waiting_for_a_dirty_permit() {
    let directory = tempfile::tempdir().unwrap();
    let dirty = Arc::new(Semaphore::new(1));
    let host = Host::default().with_dirty_slots(dirty.clone());
    let permit = dirty.clone().acquire_owned().await.unwrap();
    let ledger = resources(1);
    let coordinator = SharedPublication::new(ledger.clone(), Default::default());
    let replica = CellReplica::new(
        CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("shared-pressure"),
            [3; 16],
        ),
        [1; 32],
        [2; 16],
        Limits::default(),
    )
    .unwrap()
    .with_host(host);
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let slot = coordinator.admit().await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        coordinator.submit(&replica, &cuts, directory.path().to_owned(), slot),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.is_none());
    assert_eq!(
        coordinator.slots.available_permits(),
        SHARED_PUBLICATION_ROWS
    );
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    drop(permit);
    let prepared = replica.prepare(None, &cuts, 1, 1).await.unwrap();
    assert_eq!(prepared.root().position, cuts.position);
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_fenced_cell_cannot_select_its_shared_proposal_or_block_a_sibling() {
    use crate::{
        control::authority::CellAuthority,
        control::{Control, Owner, Transition},
        identity::{CellId, Digest, IncarnationId, SessionId},
        publication::CellPublisher,
    };
    let directory = tempfile::tempdir().unwrap();
    let ledger = resources(8 << 20);
    let coordinator = SharedPublication::new(ledger.clone(), Default::default());
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("shared-fencing"),
        [3; 16],
    );
    let authority = CellAuthority::new(layout.clone());
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let mut publishers = Vec::new();
    for i in 1..=2 {
        let cell = CellId::from_bytes([i; 32]);
        let incarnation = IncarnationId::from_bytes([i; 16]);
        let control = Control::initial(
            cell,
            incarnation,
            Owner {
                session: SessionId::from_bytes([5; 16]),
                endpoint: "https://node.invalid".into(),
            },
            Digest::from_bytes([4; 32]),
            1,
        )
        .unwrap();
        layout
            .store()
            .create_strict(
                &layout.control_path(cell.as_bytes()),
                bytes::Bytes::from(control.encode().unwrap()),
            )
            .await
            .unwrap();
        let observed = authority.load(cell).await.unwrap().unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *cell.as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        publishers.push(
            CellPublisher::new(
                replica,
                authority.clone(),
                observed,
                directory.path().to_owned(),
            )
            .with_shared_publication(coordinator.clone()),
        );
    }
    let mut live = publishers.pop().unwrap();
    let mut stale = publishers.pop().unwrap();
    let a = stale.admit_publication().await.unwrap();
    let b = live.admit_publication().await.unwrap();
    let (a, b) = tokio::join!(
        stale.prepare_admitted_batch(a, &cuts, 1),
        live.prepare_admitted_batch(b, &cuts, 1)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    let observed = authority
        .load(stale.control().value().cell)
        .await
        .unwrap()
        .unwrap();
    let mut tombstone = observed.value().clone();
    tombstone.state = crate::control::ControlState::Tombstoned;
    tombstone.owner = None;
    tombstone.epoch += 1;
    tombstone.revision += 1;
    tombstone.progress += 1;
    authority
        .transition(&observed, tombstone, Transition::Tombstone)
        .await
        .unwrap();
    assert!(matches!(
        stale.publish_prepared(&a, None).await,
        Err(Error::Fenced)
    ));
    assert_eq!(live.publish_prepared(&b, None).await.unwrap(), b.root());
    assert!(
        authority
            .load(stale.control().value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .root
            .is_none()
    );
    live.replica.reachable_objects(&b.root()).await.unwrap();
    coordinator.shutdown().await.unwrap();
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
}
