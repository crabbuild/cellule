//! Real signed follower RPCs, unpublished ACK, killed owner and canonical recovery.
//! The external driver owns three distinct VM/container roles and their native disks.
use super::*;
use cellule_ltx::{CellReplica, Host, Limits};
use cellule_runtime::{
    CatalogRole, CellTarget, MutationIdentity, NamespaceId, SqlWorkerPool, TenantId,
    cell::{
        catalog::{CatalogEntry, CellCatalog},
        executor::{HandlerOutcome, StoredOutcome},
    },
    control::{Owner as CellOwner, authority::CellAuthority},
    identity::{IncarnationId, RequestId},
    node::{
        log_recovery::{NodeLogRecovery, RecoveryCoordinator, recoverable_cells},
        log_transport::NodeLogTransport,
    },
    recovery::manifest::RecoveryManifestStore,
};
use cellule_store::Store;
use futures_util::stream::BoxStream;
use object_store::{ObjectStore, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};

const APPLICATION: ApplicationId = ApplicationId::from_bytes([62; 16]);
const CODE: Digest = Digest::from_bytes([65; 32]);
const INCARNATION: IncarnationId = IncarnationId::from_bytes([64; 16]);
const REQUEST: RequestId = RequestId::from_bytes([155; 16]);
const PAYLOAD: Digest = Digest::from_bytes([156; 32]);

#[derive(Debug)]
struct HeldData {
    inner: Arc<dyn ObjectStore>,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
}
impl std::fmt::Display for HeldData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldData")
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldData {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if path.as_ref().ends_with(".nodebundle") && self.armed.load(Ordering::Acquire) {
            self.entered.notify_one();
            // A killed owner cannot select this immutable upload. Authority and
            // enrollment operations still use the real strongly consistent store.
            std::future::pending::<()>().await;
        }
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn target() -> CellTarget {
    CellTarget::new(
        TenantId::from_bytes([61; 16]),
        APPLICATION,
        NamespaceId::from_bytes([63; 16]),
        b"remote-follower-tail",
    )
    .unwrap()
}
fn marker(name: &str, value: serde_json::Value) {
    let root = PathBuf::from(std::env::var("CELLULE_TEST_DEVICE_PATH").unwrap());
    let path = root.join(name);
    let mut file = std::fs::File::create(path).unwrap();
    use std::io::Write as _;
    file.write_all(serde_json::to_string_pretty(&value).unwrap().as_bytes())
        .unwrap();
    file.sync_all().unwrap();
    println!("Remote fault marker: {name}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "requires isolated RustFS, pinned TLS and an external driver owning three separate 1-vCPU/2-GiB VMs"]
async fn remote_crash_fixture_child() {
    // Refuse an incomplete invocation rather than quietly substituting memory.
    let role = std::env::var("CELLULE_REMOTE_FAULT_ROLE").unwrap();
    std::env::var("CELLULE_TEST_ENDPOINT").unwrap();
    let device = PathBuf::from(std::env::var("CELLULE_TEST_DEVICE_PATH").unwrap());
    let config = Config::from_env(false).unwrap().unwrap();
    let metrics = Arc::new(QueryMetrics::default());
    let (remote, prefix) = super::super::example_storage().await.unwrap();
    let held = Arc::new(HeldData {
        inner: remote.inner().clone(),
        armed: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
    });
    let store = if role == "owner" {
        Store::new(held.clone())
    } else {
        remote
    };
    let layout = cellule_ltx::CellStorageLayout::new(store, prefix, *APPLICATION.as_bytes());
    if role == "follower" {
        assert!(config.index == 1 || config.index == 2);
        let port = 8080 + u16::from(config.index) * 10;
        config
            .serve_follower(
                layout,
                CODE,
                format!("127.0.0.1:{port}").parse().unwrap(),
                metrics,
            )
            .await
            .unwrap();
        return;
    }
    assert_eq!(config.index, 0);
    let session = SessionId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
    let pool = SqlWorkerPool::new(1, 8)
        .unwrap()
        .with_native_memory_limit(512 << 20)
        .unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        pool,
        64 << 20,
        session,
        Host::default(),
    )
    .unwrap();
    runtime.install_telemetry(metrics.clone()).unwrap();
    let owner = config
        .start_owner(
            layout.clone(),
            CODE,
            session,
            APPLICATION,
            &runtime,
            metrics.clone(),
        )
        .await
        .unwrap();
    let directory = owner.directory();
    let target = target();
    let catalog = CellCatalog::new(layout.clone(), target.tenant());
    let authority = CellAuthority::new(layout.clone());
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *INCARNATION.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let current_owner = CellOwner {
        session,
        endpoint: owner.endpoint.clone(),
    };
    if role == "owner" {
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Application, CODE, 1).unwrap())
            .await
            .unwrap();
        let initial = authority
            .create_initial(&proof, INCARNATION, current_owner)
            .await
            .unwrap();
        let handle = runtime.bootstrap(proof, replica, authority.clone(), initial,
            device.join("owner.sqlite"), |tx| {
                tx.execute_batch("CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)")?;
                Ok(())
            }).await.unwrap();
        held.armed.store(true, Ordering::Release);
        let now = clock().unwrap();
        let identity = MutationIdentity {
            request_id: REQUEST,
            issued_at_ms: now,
            expires_at_ms: now.checked_add(120_000).unwrap(),
        };
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            handle.execute(identity, PAYLOAD, now, 1_024, 1_024, |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(1i64.to_be_bytes().to_vec()))
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            outcome,
            StoredOutcome::Success {
                result: 1i64.to_be_bytes().to_vec(),
                commit_sequence: 1
            }
        );
        tokio::time::timeout(Duration::from_secs(5), held.entered.notified())
            .await
            .unwrap();
        let original = authority.load(target.cell_id()).await.unwrap().unwrap();
        assert_eq!(original.value().ltx_root().unwrap().commit_sequence, 0);
        let advertised = directory
            .load(session, clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(advertised.advertisement().log().unwrap().active());
        assert_eq!(
            advertised
                .advertisement()
                .bundle_head()
                .map(|head| head.selected_through())
                .unwrap_or(0),
            0
        );
        marker(
            "acknowledged.json",
            serde_json::json!({
                "session": uuid::Uuid::from_bytes(*session.as_bytes()).to_string(),
                "issued_at_ms": identity.issued_at_ms, "expires_at_ms": identity.expires_at_ms,
                "commit_sequence": 1, "bucket_commit_sequence": 0,
                "native_epoch": advertised.advertisement().log().unwrap().epoch(),
                "required_followers": advertised.advertisement().log().unwrap().members().len(),
                "owner_expires_at_ms": advertised.advertisement().expires_at_ms(),
                "held_bundle_upload": true
            }),
        );
        std::future::pending::<()>().await;
        return;
    }
    assert_eq!(role, "recover");
    let before: serde_json::Value =
        serde_json::from_str(&std::env::var("CELLULE_REMOTE_FAULT_ACK").unwrap()).unwrap();
    let predecessor = SessionId::from_bytes(
        *uuid::Uuid::parse_str(before["session"].as_str().unwrap())
            .unwrap()
            .as_bytes(),
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        while directory
            .is_live(predecessor, clock().unwrap())
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap();
    // Expired advertisements cannot be loaded as live authority. The fencing
    // CAS returns the exact original membership that recovery must account for.
    let fenced = directory
        .claim_expired(predecessor, session, clock().unwrap())
        .await
        .unwrap();
    let members = fenced.log().unwrap().members().to_vec();
    assert_eq!(members.len(), 2);
    let mut peers = Vec::new();
    for member in members {
        if let Some(peer) = directory
            .resolve_node(member, clock().unwrap())
            .await
            .unwrap()
        {
            peers.push(peer);
        }
    }
    let transport: Arc<dyn NodeLogTransport> = Arc::new(
        transport::Transport::new(
            directory.clone(),
            session,
            config.tls().unwrap(),
            peers,
            metrics,
        )
        .unwrap(),
    );
    let recovery = NodeLogRecovery::from_fenced(transport, &fenced, Limits::default()).unwrap();
    let manifests = RecoveryManifestStore::new(layout, Limits::default());
    let inventory = recoverable_cells(&catalog, &authority, predecessor, 1)
        .await
        .unwrap();
    assert_eq!(inventory.len(), 1);
    let completed = RecoveryCoordinator::new(recovery, manifests.clone())
        .recover_and_seal(&directory, fenced, inventory, clock().unwrap())
        .await
        .unwrap();
    assert_eq!(completed.controls.len(), 1);
    let proof = catalog.lookup(target.cell_id()).await.unwrap().unwrap();
    let control = completed.controls.into_iter().next().unwrap();
    let handle = runtime
        .takeover_restored(
            proof,
            replica,
            authority,
            control,
            completed.takeover,
            manifests,
            device.join("recovered.sqlite"),
            current_owner,
        )
        .await
        .unwrap();
    let identity = MutationIdentity {
        request_id: REQUEST,
        issued_at_ms: before["issued_at_ms"].as_i64().unwrap(),
        expires_at_ms: before["expires_at_ms"].as_i64().unwrap(),
    };
    let retry = handle
        .execute(identity, PAYLOAD, clock().unwrap(), 1_024, 1_024, |_| {
            Err(Error::Control("recovered request executed twice"))
        })
        .await
        .unwrap();
    assert_eq!(
        retry,
        StoredOutcome::Success {
            result: 1i64.to_be_bytes().to_vec(),
            commit_sequence: 1
        }
    );
    assert_eq!(
        handle
            .query(64, 64, |tx| {
                let value: i64 = tx.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        1i64.to_be_bytes()
    );
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    owner.stop().await.unwrap();
    marker(
        "recovered.json",
        serde_json::json!({
            "commit_sequence": 1, "value": 1, "exact_retry": true,
            "reexecution": false, "runtime_joined": true, "owner_joined": true,
            "follower_transport": "original signed mTLS capacity transport"
        }),
    );
}
