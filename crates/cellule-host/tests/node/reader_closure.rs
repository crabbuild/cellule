//! Public host reader ownership through cancelled removal and drain deadlines.

use super::*;
use cellule_host::read_replicas::ReadReplicaManager;
use cellule_runtime::{
    CellRuntime,
    cell::{
        actor::CellHandle,
        catalog::{CatalogEntry, CellCatalog},
    },
    client::CellReadReplica,
    control::{Owner, authority::CellAuthority},
    identity::{CellTarget, IncarnationId, NamespaceId, TenantId},
    ltx::{CellReplica, CellStorageLayout},
    node::NodeDirectory,
    peer::PeerReplicaResolver,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{OperationDescriptor, Query, QueryContext},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{
    collections::BTreeMap,
    sync::{OnceLock, atomic::AtomicU64},
};
use tokio::sync::{Notify, oneshot};

type QueryGate = (Arc<Notify>, oneshot::Receiver<()>);
static QUERIES: Mutex<BTreeMap<u64, QueryGate>> = Mutex::new(BTreeMap::new());

struct Pause {
    id: u64,
    entered: Arc<Notify>,
    release: Option<oneshot::Sender<()>>,
}
impl Pause {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let entered = Arc::new(Notify::new());
        let (release, receive) = oneshot::channel();
        assert!(
            QUERIES
                .lock()
                .unwrap()
                .insert(id, (entered.clone(), receive))
                .is_none()
        );
        Self {
            id,
            entered,
            release: Some(release),
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.notified())
            .await
            .unwrap();
    }
    fn release(mut self) {
        self.release.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Pause {
    fn drop(&mut self) {
        QUERIES.lock().unwrap().remove(&self.id);
    }
}

struct ReadCounter;
impl Query for ReadCounter {
    const MODULE: &'static str = Module::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = u64;
    type Output = i64;
    fn execute(context: &mut QueryContext<'_>, token: u64) -> cellule_runtime::Result<i64> {
        if token != 0 {
            let (entered, receive) = QUERIES.lock().unwrap().remove(&token).unwrap();
            entered.notify_one();
            receive
                .blocking_recv()
                .map_err(|_| Error::Command("reader query gate dropped"))?;
        }
        let sets = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT value FROM counter".into(),
                parameters: vec![],
            }],
        })?;
        match sets
            .first()
            .and_then(|set| set.rows.first())
            .and_then(|row| row.first())
        {
            Some(SqlValue::Integer(value)) => Ok(*value),
            _ => Err(Error::Command("reader counter is missing")),
        }
    }
}
struct ReaderModule;
impl CellModule for ReaderModule {
    const NAME: &'static str = Module::NAME;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            queries: &[OperationDescriptor {
                id: 1,
                codec_version: 1,
                schema_min: 1,
                schema_max: 1,
                input_limit: 8,
                output_limit: 8,
            }],
            ..*Module.descriptor()
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_query::<ReadCounter>()
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    node: Arc<CellNode>,
    manager: ReadReplicaManager,
    peers: Vec<CellReadReplica>,
    source: CellRuntime,
    handles: Vec<CellHandle>,
}
async fn fixture() -> Fixture {
    fixture_with_store(Store::new(Arc::new(InMemory::new()))).await
}

async fn fixture_with_store(store: Store) -> Fixture {
    let mut app = cellule_app::ApplicationBuilder::new(
        "host-test",
        BuildDescriptor {
            source_revision: "reader-close".into(),
            cargo_lock_digest: Digest::from_bytes([7; 32]),
        },
    )
    .unwrap();
    app.register(ReaderModule).unwrap();
    let namespace = NamespaceId::from_bytes([2; 16]);
    app.cell_type(
        cellule_app::CellType::new("host-test", "host-test", namespace, CatalogRole::Sql, 1)
            .unwrap(),
    )
    .unwrap();
    let app = Arc::new(app.finish().unwrap());
    let code = app.registry().module_digests()[0];
    let layout = CellStorageLayout::new(store, Path::from("reader-closure"), [3; 16]);
    let directory = NodeDirectory::new(
        layout.clone(),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    );
    let now = super::inventory::clock();
    for id in [1, 2] {
        directory
            .create(super::inventory::advertisement(id, code, now), now)
            .await
            .unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    let limits = ReplicaLimits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..ReplicaLimits::default()
    };
    let node = Arc::new(
        CellNodeBuilder::new(app)
            .with_runtime(
                SqlWorkerPool::new(2, 8)
                    .unwrap()
                    .with_native_memory_limit(128 << 20)
                    .unwrap(),
                16 << 20,
            )
            .with_replica_host(
                ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
            )
            .with_session(SessionId::from_bytes([2; 16]))
            .build()
            .unwrap(),
    );
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let manager = node
        .install_read_replicas(
            layout.clone(),
            directory,
            root.path().join("readers"),
            limits,
        )
        .unwrap();
    node.install_node_lease(NodeLeaseGuard::new(now, now + 60_000).unwrap())
        .unwrap();
    let source = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 8).unwrap(),
        16 << 20,
        SessionId::from_bytes([1; 16]),
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
    )
    .unwrap();
    let catalog = CellCatalog::new(layout.clone(), TenantId::from_bytes([1; 16]));
    let authority = CellAuthority::new(layout.clone());
    let mut handles = Vec::new();
    let mut peers = Vec::new();
    for id in 1..=2 {
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            ApplicationId::from_bytes([3; 16]),
            namespace,
            &[id],
        )
        .unwrap();
        let incarnation = IncarnationId::from_bytes([id; 16]);
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let observed = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session: SessionId::from_bytes([1; 16]),
                    endpoint: "https://node.internal:8789".into(),
                },
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits,
        )
        .unwrap();
        handles.push(
            source
                .bootstrap(
                    proof,
                    replica,
                    authority.clone(),
                    observed,
                    root.path().join(format!("source-{id}.sqlite")),
                    |tx| {
                        tx.execute_batch(
                            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                        )?;
                        Ok(())
                    },
                )
                .await
                .unwrap(),
        );
        manager.set_target(&target, 0, 1).await.unwrap().unwrap();
        manager
            .activate(target.clone(), SessionId::from_bytes([1; 16]))
            .await
            .unwrap();
        peers.push(manager.resolve(target).await.unwrap());
    }
    Fixture {
        _root: root,
        node,
        manager,
        peers,
        source,
        handles,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_opening_is_joined_on_manager_closure_after_native_work_starts() {
    let runtime_slot = Arc::new(Mutex::new(None::<CellRuntime>));
    let slot = runtime_slot.clone();
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let (release, receive) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some(receive));
    let armed = Arc::new(AtomicBool::new(false));
    let once = armed.clone();
    let fixture = fixture_with_store(
        Store::new(Arc::new(InMemory::new())).with_read_request_observer(Arc::new(move |kind| {
            // Pause the VFS page fault in an actual admitted native open. The
            // source publisher and async preparation use different SQL ledgers.
            if kind == cellule_store::StorageReadKind::Range
                && slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|runtime| runtime.stats().worker_jobs() == 1)
                && !once.swap(true, Ordering::AcqRel)
            {
                let receive = gate.lock().unwrap().take().unwrap();
                signal.notify_one();
                // Dropping the test's sender also releases a failed assertion.
                let _ = receive.recv();
            }
        })),
    )
    .await;
    *runtime_slot.lock().unwrap() = Some(fixture.node.runtime());
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([2; 16]),
        &[1],
    )
    .unwrap();
    fixture.manager.remove(target.cell_id()).await.unwrap();
    let now = super::inventory::clock();
    fixture.handles[0]
        .execute(
            cellule_runtime::MutationIdentity {
                request_id: cellule_runtime::identity::RequestId::from_bytes([213; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([214; 32]),
            now,
            64,
            64,
            |tx| {
                // A changed schema page cannot hit the old view's page cache.
                tx.execute_batch("CREATE TABLE extra(value INTEGER)")?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await
        .unwrap();
    let source = fixture
        .manager
        .prepare_source(target, SessionId::from_bytes([1; 16]))
        .await
        .unwrap();
    let manager = fixture.manager.clone();
    let opening = tokio::spawn(async move { manager.activate_source(source).await });
    let entered = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    let mut closing = Box::pin(fixture.manager.shutdown());
    let pending = futures_util::poll!(closing.as_mut()).is_pending();
    let before = fixture.node.stats();
    let retained_open = !opening.is_finished();
    // Release every accepted job before checking any fixture assertion.
    let _ = release.send(());
    let result = opening.await.unwrap();
    closing.await.unwrap();
    fixture.node.shutdown().await.unwrap();
    runtime_slot.lock().unwrap().take();
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
    assert!(entered.is_ok() && armed.load(Ordering::Acquire) && pending && retained_open);
    assert_eq!(before.worker_jobs(), 1);
    assert!(matches!(result, Err(Error::RuntimeClosed)));
    assert_eq!(fixture.node.stats().resident_bytes(), 0);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().file_descriptors(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_host_activation_pins_the_enrollment_root_and_never_refreshes_an_existing_view() {
    let fixture = fixture().await;
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([2; 16]),
        &[1],
    )
    .unwrap();
    fixture.manager.remove(target.cell_id()).await.unwrap();
    let source = fixture
        .manager
        .prepare_source(target.clone(), SessionId::from_bytes([1; 16]))
        .await
        .unwrap();
    assert_eq!(source.target(), &target);
    let before = fixture.node.stats();
    let page = fixture
        .manager
        .fleet_readers_page(None, 128, super::inventory::clock())
        .await
        .unwrap();
    assert_eq!(page.total_views(), 1);
    drop(page);
    let now = super::inventory::clock();
    fixture.handles[0]
        .execute(
            cellule_runtime::MutationIdentity {
                request_id: cellule_runtime::identity::RequestId::from_bytes([211; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([212; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value=18", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.node.stats().resident_bytes(),
        before.resident_bytes()
    );
    let receipt = fixture
        .manager
        .activate_source(source.clone())
        .await
        .unwrap();
    assert_eq!(receipt.commit_sequence, source.root().commit_sequence);
    let peer = fixture.manager.resolve(target.clone()).await.unwrap();
    assert_eq!(peer.query::<ReadCounter>(None, 0).await.unwrap().output, 17);
    assert!(matches!(
        fixture.manager.activate_source(source.clone()).await,
        Err(Error::Control("read view is already installed"))
    ));
    assert_eq!(peer.query::<ReadCounter>(None, 0).await.unwrap().output, 17);
    let refreshed = fixture
        .manager
        .activate(target.clone(), SessionId::from_bytes([1; 16]))
        .await
        .unwrap();
    assert!(refreshed.commit_sequence > receipt.commit_sequence);
    assert_eq!(peer.query::<ReadCounter>(None, 0).await.unwrap().output, 18);
    assert!(matches!(
        fixture
            .manager
            .prepare_source(target.clone(), SessionId::from_bytes([3; 16]))
            .await,
        Err(Error::Fenced)
    ));
    fixture.node.runtime().node_admission().cordon().unwrap();
    fixture.manager.remove(target.cell_id()).await.unwrap();
    assert!(matches!(
        fixture.manager.activate_source(source).await,
        Err(Error::CellDraining)
    ));
    fixture.node.shutdown().await.unwrap();
    let stats = fixture.node.stats();
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_reader_removal_and_shutdown_keep_owned_views_until_native_queries_join() {
    for shutdown in [false, true] {
        let fixture = fixture().await;
        let pause = Pause::new();
        let token = pause.id;
        let peer = fixture.peers[0].clone();
        let query = tokio::spawn(async move { peer.query::<ReadCounter>(None, token).await });
        pause.entered().await;
        let cell = fixture.peers[0].receipt().await.cell;
        let first_pending = if shutdown {
            let close = fixture.manager.shutdown();
            tokio::pin!(close);
            futures_util::poll!(close.as_mut()).is_pending()
        } else {
            let close = fixture.manager.remove(cell);
            tokio::pin!(close);
            futures_util::poll!(close.as_mut()).is_pending()
        };
        let page = fixture
            .manager
            .fleet_readers_page(None, 128, super::inventory::clock())
            .await
            .unwrap();
        let retained_count = page.total_views();
        let terminal = page.closed();
        let joining = *page
            .entries()
            .iter()
            .find(|entry| entry.receipt().cell == cell)
            .unwrap();
        drop(page);
        let deadline_failed = if shutdown {
            fixture
                .node
                .drain_until(Some(Instant::now() + Duration::from_millis(25)))
                .await
                .is_err()
        } else {
            false
        };
        let before = fixture.node.stats();
        let state = fixture.node.state();
        // A red assertion cannot strand a native SQL callback or accepted drain.
        pause.release();
        assert!(matches!(query.await.unwrap(), Err(Error::Fenced)));
        if !shutdown {
            fixture.manager.remove(cell).await.unwrap();
        }
        fixture.node.shutdown().await.unwrap();
        assert!(first_pending);
        assert_eq!(retained_count, 2);
        assert_eq!(terminal, shutdown);
        assert!(joining.admission_closed());
        assert!(!joining.snapshot_attached());
        assert!(joining.retained_lifetimes() > 0);
        assert!(!joining.locally_joined());
        assert_eq!(before.worker_jobs(), 1);
        assert!(before.resident_bytes() > 0);
        if shutdown {
            assert!(deadline_failed);
            assert_eq!(state, NodeState::Draining);
        }
        assert_eq!(fixture.node.state(), NodeState::Stopped);
        let stats = fixture.node.stats();
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.file_descriptors(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
        for peer in &fixture.peers {
            assert!(peer.readiness().await.is_err());
            let observation = peer.lifecycle_observation().await;
            assert!(observation.locally_joined());
            assert_eq!(observation.retained_lifetimes(), 0);
        }
        for handle in fixture.handles {
            handle.drain().await.unwrap();
        }
        fixture.source.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn reader_lifetime_observation_keeps_cancelled_native_queries_visible_after_detachment() {
    let fixture = fixture().await;
    let pause = Pause::new();
    let token = pause.id;
    let retained_peer = fixture.peers[0].clone();
    let querying = retained_peer.clone();
    let query = tokio::spawn(async move { querying.query::<ReadCounter>(None, token).await });
    pause.entered().await;
    let open = retained_peer.lifecycle_observation().await;
    query.abort();
    let cancelled = query.await.unwrap_err().is_cancelled();
    let mut close = Box::pin(retained_peer.close_and_join());
    let pending = futures_util::poll!(close.as_mut()).is_pending();
    let joining = retained_peer.lifecycle_observation().await;
    let page = fixture
        .manager
        .fleet_readers_page(None, 128, super::inventory::clock())
        .await
        .unwrap();
    let managed = *page
        .entries()
        .iter()
        .find(|entry| entry.receipt().cell == open.receipt().cell)
        .unwrap();
    drop(page);
    let resources = fixture.node.stats();
    // Release the native callback before assertions so a red observation cannot
    // strand its original SQL job, lifetime or shutdown join.
    pause.release();
    let joined_receipt = close.await;
    let joined = retained_peer.lifecycle_observation().await;
    let refused = retained_peer.query::<ReadCounter>(None, 0).await;
    fixture.node.shutdown().await.unwrap();
    let after_shutdown = retained_peer.lifecycle_observation().await;
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
    assert!(cancelled && pending);
    assert!(!open.admission_closed() && open.snapshot_attached());
    assert!(open.retained_lifetimes() >= 2 && !open.locally_joined());
    assert!(joining.admission_closed() && !joining.snapshot_attached());
    assert!(joining.retained_lifetimes() > 0 && !joining.locally_joined());
    assert_eq!(managed, joining);
    assert_eq!(resources.worker_jobs(), 1);
    assert!(resources.resident_bytes() > 0);
    assert_eq!(joined_receipt, open.receipt());
    assert!(joined.locally_joined());
    assert_eq!(joined.retained_lifetimes(), 0);
    assert!(matches!(refused, Err(Error::Fenced)));
    assert_eq!(after_shutdown, joined);
    let resources = fixture.node.stats();
    assert_eq!(resources.active_cells(), 0);
    assert_eq!(resources.retained_bytes(), 0);
    assert_eq!(resources.resident_bytes(), 0);
    assert_eq!(resources.worker_jobs(), 0);
    assert_eq!(resources.file_descriptors(), 0);
    assert_eq!(resources.local_disk_reserved_bytes(), 0);
}

#[tokio::test]
async fn reader_inventory_continuation_rejects_peer_closure_outside_the_returned_page() {
    let fixture = fixture().await;
    let mut peers = Vec::new();
    for peer in &fixture.peers {
        peers.push((peer.receipt().await.cell, peer));
    }
    peers.sort_by_key(|(cell, _)| *cell.as_bytes());
    let (cell, peer) = peers.last().unwrap();
    let page = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let cursor = page.next().unwrap();
    let original_topology = page.topology();
    assert_ne!(page.entries()[0].receipt().cell, *cell);
    drop(page);
    peer.close();
    let closed_rejected = fixture
        .manager
        .fleet_readers_page(Some(cursor), 1, super::inventory::clock())
        .await
        .is_err();
    let closed = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let closed_topology = closed.topology();
    let cursor = closed.next().unwrap();
    drop(closed);
    let attached = peer.lifecycle_observation().await;
    peer.close_and_join().await;
    let detached_rejected = fixture
        .manager
        .fleet_readers_page(Some(cursor), 1, super::inventory::clock())
        .await
        .is_err();
    let joined = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let joined_topology = joined.topology();
    let cursor = joined.next().unwrap();
    drop(joined);
    let tail = fixture
        .manager
        .fleet_readers_page(Some(cursor), 1, super::inventory::clock())
        .await
        .unwrap();
    let total = tail.total_views();
    let observation = tail.entries()[0];
    let terminal = tail.next().is_none();
    drop(tail);
    fixture.node.shutdown().await.unwrap();
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
    assert!(closed_rejected && detached_rejected);
    assert_ne!(original_topology, closed_topology);
    assert_ne!(closed_topology, joined_topology);
    assert!(attached.admission_closed() && attached.snapshot_attached());
    assert!(!attached.locally_joined());
    assert!(observation.locally_joined() && terminal);
    assert_eq!(total, 2);
    let stats = fixture.node.stats();
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
}

#[tokio::test]
async fn reader_inventory_continuation_tracks_native_work_outside_the_returned_page() {
    let fixture = fixture().await;
    let mut peers = Vec::new();
    for peer in &fixture.peers {
        peers.push((peer.receipt().await.cell, peer.clone()));
    }
    peers.sort_by_key(|(cell, _)| *cell.as_bytes());
    let (_, peer) = peers.last().unwrap();
    let page = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let original = page.topology();
    let original_cursor = page.next().unwrap();
    drop(page);
    let pause = Pause::new();
    let token = pause.id;
    let querying = peer.clone();
    let query = tokio::spawn(async move { querying.query::<ReadCounter>(None, token).await });
    pause.entered().await;
    let start_rejected = fixture
        .manager
        .fleet_readers_page(Some(original_cursor), 1, super::inventory::clock())
        .await
        .is_err();
    let busy = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let busy_topology = busy.topology();
    let busy_cursor = busy.next().unwrap();
    drop(busy);
    let tail = fixture
        .manager
        .fleet_readers_page(Some(busy_cursor), 1, super::inventory::clock())
        .await;
    let busy_observation = tail.as_ref().ok().map(|page| page.entries()[0]);
    drop(tail);
    // Release the original native callback before assertions or query joining.
    pause.release();
    let result = query.await.unwrap();
    let quiesced = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let observation = peer.lifecycle_observation().await;
            if observation.retained_lifetimes() == 1 {
                break observation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let end_rejected = fixture
        .manager
        .fleet_readers_page(Some(busy_cursor), 1, super::inventory::clock())
        .await
        .is_err();
    let idle = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let idle_topology = idle.topology();
    drop(idle);
    fixture.node.shutdown().await.unwrap();
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
    assert!(start_rejected && end_rejected);
    assert_ne!(original, busy_topology);
    assert_eq!(original, idle_topology); // Matching intervals do not prove atomicity.
    assert_eq!(result.unwrap().output, 17);
    let busy_observation = busy_observation.unwrap();
    assert!(busy_observation.retained_lifetimes() > 1);
    let idle_observation = quiesced.unwrap();
    assert!(!idle_observation.admission_closed() && idle_observation.snapshot_attached());
    assert_eq!(busy_observation.receipt(), idle_observation.receipt());
    let stats = fixture.node.stats();
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
}

#[tokio::test]
async fn reader_inventory_continuation_rejects_peer_refresh_outside_the_returned_page() {
    let fixture = fixture().await;
    let mut peers = Vec::new();
    for (index, peer) in fixture.peers.iter().enumerate() {
        peers.push((peer.receipt().await.cell, index));
    }
    peers.sort_by_key(|(cell, _)| *cell.as_bytes());
    let (_, index) = *peers.last().unwrap();
    let peer = &fixture.peers[index];
    let page = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let original = page.topology();
    let cursor = page.next().unwrap();
    drop(page);
    let before = peer.receipt().await;
    let now = super::inventory::clock();
    fixture.handles[index]
        .execute(
            cellule_runtime::MutationIdentity {
                request_id: cellule_runtime::identity::RequestId::from_bytes([213; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([214; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value=19", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await
        .unwrap();
    let unpublished_here = fixture
        .manager
        .fleet_readers_page(Some(cursor), 1, super::inventory::clock())
        .await
        .unwrap();
    let still_original = unpublished_here.topology();
    drop(unpublished_here);
    let after = peer
        .refresh(&fixture._root.path().join("external-refresh.sqlite"))
        .await
        .unwrap();
    let refused = fixture
        .manager
        .fleet_readers_page(Some(cursor), 1, super::inventory::clock())
        .await
        .is_err();
    let fresh = fixture
        .manager
        .fleet_readers_page(None, 1, super::inventory::clock())
        .await
        .unwrap();
    let fresh_topology = fresh.topology();
    let fresh_cursor = fresh.next().unwrap();
    drop(fresh);
    let tail = fixture
        .manager
        .fleet_readers_page(Some(fresh_cursor), 1, super::inventory::clock())
        .await
        .unwrap();
    let position = tail.entries()[0].receipt();
    drop(tail);
    let value = peer.query::<ReadCounter>(None, 0).await.unwrap().output;
    fixture.node.shutdown().await.unwrap();
    for handle in fixture.handles {
        handle.drain().await.unwrap();
    }
    fixture.source.shutdown().await.unwrap();
    assert!(refused);
    assert_eq!(original, still_original);
    assert_ne!(original, fresh_topology);
    assert_eq!(before.cell, after.cell);
    assert_eq!(before.incarnation, after.incarnation);
    assert!(after.commit_sequence > before.commit_sequence);
    assert_eq!(position, after);
    assert_eq!(value, 19);
    let stats = fixture.node.stats();
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
}
