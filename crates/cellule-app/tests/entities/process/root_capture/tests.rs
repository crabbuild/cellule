use super::*;
use crate::entities::{compiled_entities, entity_target, provision_entity};
use crate::performance_fixture::{node_session, now_ms};
use cellule_host::{CellNode, CellNodeBuilder};
use cellule_runtime::cell::actor::CellHandle;
use cellule_runtime::cell::executor::HandlerOutcome;
use cellule_runtime::cell::worker::SqlWorkerPool;
use cellule_runtime::node::lease::NodeLeaseGuard;
use cellule_runtime::{
    Digest,
    ltx::{CellStorageLayout, DiskBudget, Host},
};
use cellule_store::{StorageObservation, StorageObserver, StorageOperation, Store};
use object_store::memory::InMemory;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct FirstRead {
    armed: AtomicBool,
    observed: tokio::sync::Notify,
}
impl StorageObserver for FirstRead {
    fn started(&self, _: StorageOperation) {}

    fn finished(&self, observation: StorageObservation) {
        if observation.operation == StorageOperation::Get
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.observed.notify_one();
        }
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    node: CellNode,
    handle: CellHandle,
    authority: CellAuthority,
    original: Control,
    observer: Arc<FirstRead>,
    store: Store,
    layout: CellStorageLayout,
}
impl Fixture {
    async fn new() -> Self {
        let application = compiled_entities();
        let observer = Arc::new(FirstRead::default());
        let store = Store::new(Arc::new(InMemory::new())).with_storage_observer(observer.clone());
        let layout = CellStorageLayout::new(store.clone(), "root-capture".into(), [82; 16]);
        let root = tempfile::tempdir().unwrap();
        let node = CellNodeBuilder::new(application.clone())
            .with_runtime(SqlWorkerPool::new(1, 8).unwrap(), 64 << 20)
            .with_session(node_session(0))
            .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
            .build()
            .unwrap();
        node.install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let now = now_ms();
        node.install_node_lease_for_startup(NodeLeaseGuard::new(now, now + 60_000).unwrap())
            .unwrap();
        node.start().unwrap();
        let handle =
            provision_entity(&node, &layout, root.path(), 0, 0, "https://node-0".into()).await;
        let authority = CellAuthority::new(layout.clone());
        let original = authority
            .load(entity_target(&application, 0).cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .clone();
        Self {
            _root: root,
            node,
            handle,
            authority,
            original,
            observer,
            store,
            layout,
        }
    }
    async fn close(&self) {
        self.node.shutdown().await.unwrap();
        let stats = self.node.stats();
        assert_eq!(stats.active_cells(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.file_descriptors(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
    }

    fn admitted_authority(&self, reads: Arc<ReadGate>) -> CellAuthority {
        CellAuthority::new(CellStorageLayout::new(
            self.store.clone().with_read_admission(reads),
            "root-capture".into(),
            [82; 16],
        ))
    }

    async fn roster(&self, cells: usize) -> Vec<Control> {
        let mut values = vec![self.original.clone()];
        for entity in 1..cells {
            provision_entity(
                &self.node,
                &self.layout,
                self._root.path(),
                0,
                entity,
                "https://node-0".into(),
            )
            .await;
            values.push(
                self.authority
                    .load(entity_target(self.node.application(), entity).cell_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .value()
                    .clone(),
            );
        }
        values
    }
}

#[derive(Default)]
struct ReadGate {
    mode: usize,
    cancellation: CancellationToken,
    requests: AtomicUsize,
    active: AtomicUsize,
    entered: tokio::sync::Notify,
}
struct ActiveRead<'a>(&'a ReadGate);
impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct OriginalReadFailure;
impl std::fmt::Display for OriginalReadFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("original root observation read failure")
    }
}
impl std::error::Error for OriginalReadFailure {}

#[async_trait::async_trait]
impl cellule_store::ReadAdmission for ReadGate {
    fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    async fn request(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveRead(self);
        self.entered.notify_one();
        match self.mode {
            1 => std::future::pending().await,
            2 => Err(Box::new(OriginalReadFailure)),
            3 => {
                tokio::time::sleep(Duration::from_millis(600)).await;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn bytes(
        &self,
        _: u64,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[tokio::test]
async fn canonical_capture_waits_for_the_original_writer_to_publish_the_required_sequence() {
    let fixture = Fixture::new().await;
    let minimum = fixture.original.root.as_ref().unwrap().commit_sequence + 1;
    let minima = [minimum];
    fixture.observer.armed.store(true, Ordering::SeqCst);
    let capture = capture(
        &fixture.authority,
        std::slice::from_ref(&fixture.original),
        &minima,
    );
    tokio::pin!(capture);
    let advance = async {
        fixture.observer.observed.notified().await;
        fixture
            .handle
            .execute(
                crate::qualification_identity(98, now_ms()),
                Digest::from_bytes([99; 32]),
                now_ms(),
                1024,
                1024,
                |tx| {
                    tx.execute("CREATE TABLE root_capture_probe(value INTEGER)", [])?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            )
            .await
            .unwrap();
    };
    let (captured, ()) = tokio::join!(&mut capture, advance);
    fixture.close().await;
    let captured = captured.unwrap();
    assert_eq!(captured.values.len(), 1);
    assert!(captured.reads >= 2);
    assert_eq!(captured.values[0].epoch, fixture.original.epoch);
    assert_eq!(captured.values[0].owner, fixture.original.owner);
    assert!(captured.values[0].root.as_ref().unwrap().commit_sequence >= minimum);
    assert!(captured.elapsed_us <= DRAIN_GRACE_US);
    let control = &captured.values[0];
    let root = control.ltx_root().unwrap();
    let replica = cellule_ltx::CellReplica::new(
        fixture.layout.clone(),
        *control.cell.as_bytes(),
        *control.incarnation.as_bytes(),
        cellule_ltx::Limits::default(),
    )
    .unwrap()
    .with_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)));
    let restored = fixture._root.path().join("captured-root.sqlite");
    assert_eq!(
        replica
            .open_root(&root)
            .await
            .unwrap()
            .restore(&restored)
            .await
            .unwrap(),
        root.position
    );
    let database = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        database
            .query_row("SELECT count(*) FROM root_capture_probe", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        database
            .query_row(
                "SELECT count(*) FROM sys_requests WHERE commit_sequence = ?1",
                [minimum],
                |row| row.get::<_, u64>(0)
            )
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn every_original_writer_binding_is_required_even_when_the_root_already_covers_writes() {
    let fixture = Fixture::new().await;
    let mut originals = Vec::new();
    let mut changed = fixture.original.clone();
    changed.owner.as_mut().unwrap().endpoint = "https://unexpected-endpoint".into();
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.owner.as_mut().unwrap().session = node_session(1);
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.epoch += 1;
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.incarnation = cellule_runtime::identity::IncarnationId::from_bytes([99; 16]);
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.code = Digest::from_bytes([99; 32]);
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.schema += 1;
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.revision += 1;
    originals.push(changed);
    let mut changed = fixture.original.clone();
    changed.progress += 1;
    originals.push(changed);
    for original in &originals {
        assert!(matches!(
            capture(&fixture.authority, std::slice::from_ref(original), &[0]).await,
            Err(Error::Fenced)
        ));
    }
    let observed = fixture
        .authority
        .load(fixture.original.cell)
        .await
        .unwrap()
        .unwrap();
    fixture.close().await;
    assert_eq!(observed.value(), &fixture.original);
}

#[tokio::test]
async fn the_complete_roster_shares_one_original_deadline_including_read_admission() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster(4).await;
    let minimum = roster
        .iter()
        .map(|control| control.root.as_ref().unwrap().commit_sequence)
        .collect::<Vec<_>>();
    let reads = Arc::new(ReadGate {
        mode: 3,
        ..ReadGate::default()
    });
    let result = capture(
        &fixture.admitted_authority(reads.clone()),
        &roster,
        &minimum,
    )
    .await;
    fixture.close().await;
    assert!(matches!(result, Err(Error::Deadline)));
    assert_eq!(reads.requests.load(Ordering::SeqCst), 4);
    assert_eq!(reads.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_original_writer_that_never_covers_the_minimum_cannot_pass() {
    let fixture = Fixture::new().await;
    let minimum = fixture.original.root.as_ref().unwrap().commit_sequence + 1;
    let result = capture(
        &fixture.authority,
        std::slice::from_ref(&fixture.original),
        &[minimum],
    )
    .await;
    let observed = fixture
        .authority
        .load(fixture.original.cell)
        .await
        .unwrap()
        .unwrap();
    fixture.close().await;
    assert!(matches!(result, Err(Error::Deadline)));
    assert_eq!(observed.value(), &fixture.original);
}

#[tokio::test]
async fn capture_preserves_the_original_read_failure_instead_of_waiting_or_retrying() {
    let fixture = Fixture::new().await;
    let reads = Arc::new(ReadGate {
        mode: 2,
        ..ReadGate::default()
    });
    let result = capture(
        &fixture.admitted_authority(reads.clone()),
        std::slice::from_ref(&fixture.original),
        &[0],
    )
    .await;
    fixture.close().await;
    let error = result.err().unwrap();
    let mut source: &(dyn std::error::Error + 'static) = &error;
    while !source.is::<OriginalReadFailure>() {
        source = source.source().unwrap();
    }
    assert_eq!(reads.requests.load(Ordering::SeqCst), 1);
    assert_eq!(reads.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_capture_cancels_only_observation_and_retains_the_original_writer() {
    let fixture = Fixture::new().await;
    let reads = Arc::new(ReadGate {
        mode: 1,
        ..ReadGate::default()
    });
    let authority = fixture.admitted_authority(reads.clone());
    let mut capture = Box::pin(capture(
        &authority,
        std::slice::from_ref(&fixture.original),
        &[0],
    ));
    tokio::select! {
        result = &mut capture => panic!("unexpected capture result: {}", result.is_ok()),
        () = reads.entered.notified() => {},
    }
    assert_eq!(reads.active.load(Ordering::SeqCst), 1);
    drop(capture);
    assert_eq!(reads.active.load(Ordering::SeqCst), 0);
    let observed = fixture
        .authority
        .load(fixture.original.cell)
        .await
        .unwrap()
        .unwrap();
    fixture.close().await;
    assert_eq!(observed.value(), &fixture.original);
}

#[tokio::test]
async fn closed_missing_and_successor_authority_cannot_certify_the_original_serving_roster() {
    use cellule_runtime::control::{Owner, Transition};
    let fixture = Fixture::new().await;
    fixture.close().await;
    let result = capture(
        &fixture.authority,
        std::slice::from_ref(&fixture.original),
        &[0],
    )
    .await;
    assert!(matches!(result, Err(Error::Fenced)));
    let observed = fixture
        .authority
        .load(fixture.original.cell)
        .await
        .unwrap()
        .unwrap();
    let successor = observed
        .value()
        .takeover(Owner {
            session: node_session(1),
            endpoint: "https://node-1".into(),
        })
        .unwrap();
    fixture
        .authority
        .transition(&observed, successor, Transition::Takeover)
        .await
        .unwrap();
    let result = capture(
        &fixture.authority,
        std::slice::from_ref(&fixture.original),
        &[0],
    )
    .await;
    assert!(matches!(result, Err(Error::Fenced)));
    fixture
        .store
        .delete(
            &fixture
                .layout
                .control_path(fixture.original.cell.as_bytes()),
        )
        .await
        .unwrap();
    let result = capture(
        &fixture.authority,
        std::slice::from_ref(&fixture.original),
        &[0],
    )
    .await;
    assert!(matches!(result, Err(Error::CellNotActive)));
}

#[tokio::test]
async fn duplicate_empty_mismatched_and_unbounded_rosters_are_rejected_before_reading() {
    let fixture = Fixture::new().await;
    let reads = Arc::new(ReadGate {
        mode: 1,
        ..ReadGate::default()
    });
    let authority = fixture.admitted_authority(reads.clone());
    for (roster, minimum) in [
        (Vec::new(), Vec::new()),
        (vec![fixture.original.clone()], Vec::new()),
        (vec![fixture.original.clone(); 2], vec![0; 2]),
        (vec![fixture.original.clone(); 81], vec![0; 81]),
    ] {
        assert!(matches!(
            capture(&authority, &roster, &minimum).await,
            Err(Error::Control(_))
        ));
    }
    fixture.close().await;
    assert_eq!(reads.requests.load(Ordering::SeqCst), 0);
}
