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
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("reader-closure"),
        [3; 16],
    );
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
            fixture.manager.remove(cell).await;
        }
        fixture.node.shutdown().await.unwrap();
        assert!(first_pending);
        assert_eq!(retained_count, 2);
        assert_eq!(terminal, shutdown);
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
        }
        for handle in fixture.handles {
            handle.drain().await.unwrap();
        }
        fixture.source.shutdown().await.unwrap();
    }
}
