//! A valid Fleet ACK remains usable while origin selection is delayed.
use super::actor::{Authority, transport};
use super::*;
use crate::cell::actor::CellRuntime;
use crate::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use crate::cell::executor::{HandlerOutcome, MutationIdentity};
use crate::cell::worker::SqlWorkerPool;
use crate::fleet::telemetry::{CellTelemetry, CommandResponseSource};
use crate::identity::{CellTarget, NamespaceId, RequestId, TenantId};
use crate::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority, NodeDurability,
};
use crate::node::log_shipper::{AssignedCapture, NodeLogShipper};
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

struct DelayedSelection {
    authority: Arc<Authority>,
    held: AtomicBool,
    entered: tokio::sync::Notify,
    changed: tokio::sync::Notify,
}

impl NodeBundleAuthority for DelayedSelection {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        NodeBundleAuthority::bind(self.authority.as_ref(), authority, observed)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        NodeBundleAuthority::close(self.authority.as_ref(), authority, observed, issued)
    }
}

impl NodeBundlePublicationAuthority for DelayedSelection {
    fn select<'a>(
        &'a self,
        captures: &'a [AssignedCapture],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async move {
            while self.held.load(Ordering::Acquire) {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                self.entered.notify_one();
                if self.held.load(Ordering::Acquire) {
                    changed.await;
                }
            }
            self.authority.select(captures, lease).await
        })
    }
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        self.authority.checkpoint(checkpoints)
    }
}

#[derive(Default)]
struct FleetResponses(AtomicUsize);
impl CellTelemetry for FleetResponses {
    fn command_response(&self, source: CommandResponseSource, _: Duration, _: Duration) {
        if source == CommandResponseSource::Fleet {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_selection_keeps_fleet_acks_visible_and_allows_prior_root_before_joined_drain() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let delayed = Arc::new(DelayedSelection {
        authority: authority.clone(),
        held: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    let peers = transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        peers,
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(2, 4).unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        pool.clone(),
        64 << 20,
        SessionId::from_bytes([1; 16]),
        cellule_ltx::Host::default(),
    )
    .unwrap();
    runtime.install_node_lease(f.lease.clone()).unwrap();
    runtime
        .install_node_durability(ApplicationId::from_bytes([9; 16]), durability.clone())
        .unwrap();
    durability
        .start_bundle_publication(delayed.clone())
        .unwrap();
    let responses = Arc::new(FleetResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([9; 16]),
        NamespaceId::from_bytes([13; 16]),
        b"delayed-selection",
    )
    .unwrap();
    let catalog = CellCatalog::new(f.layout.clone(), target.tenant());
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Application,
                Digest::from_bytes([12; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let cell_authority = CellAuthority::new(f.layout.clone());
    let control = cell_authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([4; 16]),
            Owner {
                session: SessionId::from_bytes([1; 16]),
                endpoint: "https://bundle.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let replica = CellReplica::new(
        f.layout.clone(),
        *target.cell_id().as_bytes(),
        [4; 16],
        Limits::default(),
    )
    .unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            replica.clone(),
            cell_authority.clone(),
            control,
            f.scratch.path().join("delayed.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let digest = Digest::from_bytes([9; 32]);
    let mut results = Vec::new();
    for byte in [1, 2] {
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes([byte; 16]),
            issued_at_ms: 10,
            expires_at_ms: 10_000,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            handle.execute(identity, digest, 20, 1024, 1024, |tx| {
                tx.execute("UPDATE counter SET value=value+1", [])?;
                Ok(HandlerOutcome::Success(b"ready".to_vec()))
            }),
        )
        .await
        .unwrap()
        .unwrap();
        results.push((identity, result));
        if byte == 1 {
            // The first exact cut is retired; its admitted root debt remains.
            tokio::time::timeout(Duration::from_secs(3), async {
                while pool.pending(target.cell_id()).await.unwrap().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            delayed.held.store(true, Ordering::Release);
        }
    }
    tokio::time::timeout(Duration::from_secs(3), delayed.entered.notified())
        .await
        .unwrap();
    assert!(
        responses.0.load(Ordering::Relaxed) > 0,
        "delayed cut must have a real Fleet ACK"
    );
    // Exercise the production ten-second wait failure without extending its
    // timeout or the original lease. The producer still owns the complete cut.
    tokio::time::sleep(Duration::from_secs(11)).await;
    super::managed::renew_actor_lease(&authority, &f.lease).await;
    assert_eq!(
        handle
            .query(1024, 1024, |connection| Ok(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0),)?
                .to_le_bytes()
                .to_vec()))
            .await
            .unwrap(),
        2_i64.to_le_bytes()
    );
    for (identity, result) in &results {
        assert_eq!(
            handle
                .execute(*identity, digest, 21, 1024, 1024, |_| panic!(
                    "Fleet retry must not execute twice"
                ))
                .await
                .unwrap(),
            *result
        );
    }
    let shutdown = runtime.shutdown();
    tokio::pin!(shutdown);
    // Draining forces the older selected root. It must prepare even though
    // the newer capture awaits selection; checkpoint/drain still join later.
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            result = &mut shutdown => panic!("unselected issued cut was not drained: {result:?}"),
            () = async {
                loop {
                    let control = cell_authority.load(target.cell_id()).await.unwrap().unwrap();
                    if control.value().ltx_root().unwrap().commit_sequence == 1 { break; }
                    tokio::task::yield_now().await;
                }
            } => (),
        }
    })
    .await
    .unwrap();
    delayed.held.store(false, Ordering::Release);
    delayed.changed.notify_waiters();
    tokio::time::timeout(Duration::from_secs(10), &mut shutdown)
        .await
        .unwrap()
        .unwrap();
    let control = cell_authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().state, ControlState::Idle);
    assert!(control.value().bundle_binding.is_none());
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, 2);
    let path = f.scratch.path().join("cold.sqlite");
    replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&path)
        .await
        .unwrap();
    let cold = cellule_ltx::rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        cold.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        cold.query_row(
            "SELECT COUNT(*) FROM sys_requests WHERE result=?1",
            [b"ready".as_slice()],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
}
