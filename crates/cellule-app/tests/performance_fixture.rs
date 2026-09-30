use super::fleet::{balancer_round_trip, peer_round_trip, start_peer_servers};
use crate::*;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Mutex,
    time::{Duration, SystemTime},
};

use cellule_host::{CellNode, CellNodeBuilder};
use cellule_ltx::{CaptureTiming, LtxPhase};
use cellule_runtime::fleet::telemetry::{CellTelemetry, CommandResponseSource, PublicationTiming};
use cellule_runtime::node::lease::NodeLeaseGuard;
use cellule_runtime::node::log::DurabilitySource;
use cellule_runtime::peer::{
    EffectPeerClient, PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal,
    PeerRoundTrip, PeerSigner, PeerVerifier, VerifiedPeerRequest,
};
use tokio_util::sync::CancellationToken;

pub(super) fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

pub(super) fn identity(phase: u8, index: usize, step: u8) -> MutationIdentity {
    let mut bytes = [0; 16];
    bytes[0] = phase;
    bytes[1] = step;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    let now = now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes(bytes),
        issued_at_ms: now,
        expires_at_ms: now + 300_000,
    }
}

pub(super) fn item_id(index: usize) -> [u8; 16] {
    let mut id = [0; 16];
    id[8..].copy_from_slice(&(index as u64).to_be_bytes());
    id
}

pub(super) fn install_sql_tables(tx: &cellule_ltx::rusqlite::Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL); \
         CREATE TABLE invoice_receipts(schedule_id BLOB NOT NULL, occurrence INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(schedule_id, occurrence));",
    )?;
    Ok(())
}

pub(super) type Schema = for<'a> fn(&cellule_ltx::rusqlite::Transaction<'a>) -> Result<()>;

pub(super) fn perf_cells() -> [(NamespaceId, CatalogRole, &'static str, u8, Schema); 7] {
    [
        (
            SQL_NAMESPACE,
            CatalogRole::Sql,
            SQL_MODULE,
            40,
            install_sql_tables,
        ),
        (
            KV_NAMESPACE,
            CatalogRole::Kv,
            KV_MODULE,
            41,
            install_kv_schema,
        ),
        (
            BLOB_NAMESPACE,
            CatalogRole::Blob,
            BLOB_MODULE,
            42,
            install_blob_schema,
        ),
        (
            QUEUE_NAMESPACE,
            CatalogRole::Queue,
            QUEUE_MODULE,
            43,
            install_queue_schema,
        ),
        (
            DEAD_LETTER_NAMESPACE,
            CatalogRole::Queue,
            DEAD_LETTER_MODULE,
            44,
            install_queue_schema,
        ),
        (
            CRON_NAMESPACE,
            CatalogRole::Cron,
            CRON_MODULE,
            45,
            install_cron_schema,
        ),
        (
            WORKFLOW_NAMESPACE,
            CatalogRole::Workflow,
            WORKFLOW_MODULE,
            46,
            install_workflow_schema,
        ),
    ]
}

pub(super) fn owner_routes(
    tenant: TenantId,
    application: ApplicationId,
    owners: [SocketAddr; 3],
) -> HashMap<cellule_runtime::CellId, SocketAddr> {
    perf_cells()
        .into_iter()
        .enumerate()
        .map(|(index, (namespace, _, _, _, _))| {
            let target =
                CellTarget::new(tenant, application, namespace, &partition_for_shard(0)).unwrap();
            (target.cell_id(), owners[index % owners.len()])
        })
        .collect()
}

pub(super) struct PerfFixture {
    _directory: tempfile::TempDir,
    pub(super) nodes: Vec<CellNode>,
    pub(super) layout: Option<CellStorageLayout>,
    leases: Vec<NodeLeaseGuard>,
    pub(super) round_trip: Option<Arc<dyn PeerRoundTrip>>,
    pub(super) owned_handles: Vec<Vec<CellHandle>>,
    pub(super) durability: Vec<Arc<DurabilityRecorder>>,
    pub(super) typed: ApplicationHandle<ReferenceApplication>,
    pub(super) registry: Arc<Registry>,
    pub(super) client: CellClient,
    pub(super) sql_target: CellTarget,
    pub(super) cron_target: CellTarget,
    peer: EffectPeerClient,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
pub(super) struct DurabilityRecorder {
    proofs: Mutex<Vec<(DurabilitySource, Duration)>>,
    responses: Mutex<Vec<(i64, CommandResponseSource, Duration, Duration)>>,
    executions: Mutex<Vec<(i64, Duration, Duration, bool)>>,
    publications: Mutex<Vec<(i64, cellule_runtime::CellId, PublicationTiming)>>,
    phases: Mutex<Vec<(i64, LtxPhase, Duration, bool)>>,
    captures: Mutex<Vec<(i64, CaptureTiming, bool)>>,
    publication_costs: Mutex<Vec<(i64, u64, u64)>>,
    follower_appends: Mutex<Vec<(i64, bool, u64)>>,
    follower_network: Mutex<Vec<(i64, bool, u64, Duration)>>,
}

impl DurabilityRecorder {
    pub(super) fn object_waits(&self) -> Vec<Duration> {
        self.proofs
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(source, waited)| (*source == DurabilitySource::Object).then_some(*waited))
            .collect()
    }

    pub(super) fn responses(&self) -> Vec<(i64, CommandResponseSource, Duration, Duration)> {
        self.responses.lock().unwrap().clone()
    }

    pub(super) fn executions(&self) -> Vec<(i64, Duration, Duration, bool)> {
        self.executions.lock().unwrap().clone()
    }

    pub(super) fn publications(&self) -> Vec<(i64, cellule_runtime::CellId, PublicationTiming)> {
        self.publications.lock().unwrap().clone()
    }

    pub(super) fn phases(&self) -> Vec<(i64, LtxPhase, Duration, bool)> {
        self.phases.lock().unwrap().clone()
    }

    pub(super) fn captures(&self) -> Vec<(i64, CaptureTiming, bool)> {
        self.captures.lock().unwrap().clone()
    }

    pub(super) fn publication_costs(&self) -> Vec<(i64, u64, u64)> {
        self.publication_costs.lock().unwrap().clone()
    }

    pub(super) fn follower_appends(&self) -> Vec<(i64, bool, u64)> {
        self.follower_appends.lock().unwrap().clone()
    }

    pub(super) fn record_follower_network(
        &self,
        acknowledged: bool,
        bytes: u64,
        elapsed: Duration,
    ) {
        self.follower_network
            .lock()
            .unwrap()
            .push((now_ms(), acknowledged, bytes, elapsed));
    }

    pub(super) fn follower_network(&self) -> Vec<(i64, bool, u64, Duration)> {
        self.follower_network.lock().unwrap().clone()
    }
}

impl CellTelemetry for DurabilityRecorder {
    fn durability_proof(&self, source: DurabilitySource, waited: Duration) {
        self.proofs.lock().unwrap().push((source, waited));
    }

    fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: Duration,
        confirmation: Duration,
    ) {
        self.responses
            .lock()
            .unwrap()
            .push((now_ms(), source, elapsed, confirmation));
    }

    fn command_execution(
        &self,
        queue_wait: Duration,
        worker_round_trip: Duration,
        succeeded: bool,
    ) {
        self.executions
            .lock()
            .unwrap()
            .push((now_ms(), queue_wait, worker_round_trip, succeeded));
    }

    fn publication_completed(&self, cell: cellule_runtime::CellId, timing: PublicationTiming) {
        self.publications
            .lock()
            .unwrap()
            .push((now_ms(), cell, timing));
    }

    fn ltx_phase(&self, phase: LtxPhase, elapsed: Duration, succeeded: bool) {
        self.phases
            .lock()
            .unwrap()
            .push((now_ms(), phase, elapsed, succeeded));
    }

    fn ltx_capture(&self, timing: &CaptureTiming, succeeded: bool) {
        self.captures
            .lock()
            .unwrap()
            .push((now_ms(), *timing, succeeded));
    }

    fn publication_cost(&self, objects: u64, bytes: u64) {
        self.publication_costs
            .lock()
            .unwrap()
            .push((now_ms(), objects, bytes));
    }

    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        self.follower_appends
            .lock()
            .unwrap()
            .push((now_ms(), acknowledged, bytes));
    }
}

impl PerfFixture {
    pub(super) async fn start(nodes: usize) -> Self {
        Self::start_with_successor(nodes, None).await
    }

    pub(super) async fn start_with_store(
        nodes: usize,
        store: Store,
        root: object_store::path::Path,
    ) -> Self {
        Self::start_configured(nodes, None, store, root).await
    }

    pub(super) async fn start_with_successor(
        nodes: usize,
        successor: Option<Arc<cellule_app::CompiledApplication>>,
    ) -> Self {
        Self::start_configured(
            nodes,
            successor,
            Store::new(Arc::new(InMemory::new())),
            object_store::path::Path::from("reference-performance"),
        )
        .await
    }

    pub(super) async fn start_configured(
        nodes: usize,
        successor: Option<Arc<cellule_app::CompiledApplication>>,
        store: Store,
        root: object_store::path::Path,
    ) -> Self {
        assert!(nodes == 1 || nodes == 3);
        assert!(successor.is_none() || nodes == 3);
        let application = Arc::new(compiled());
        let registry = application.registry();
        let tenant = TenantId::from_bytes([81; 16]);
        let application_id = ApplicationId::from_bytes([82; 16]);
        let layout = CellStorageLayout::new(store.clone(), root, *application_id.as_bytes());
        let directory = tempfile::TempDir::new().unwrap();
        let mut leases = Vec::new();
        let mut durability = Vec::new();
        let hosts = (0..nodes)
            .map(|node| {
                let node_application = if node == 0 {
                    successor.as_ref().unwrap_or(&application)
                } else {
                    &application
                };
                let host = CellNodeBuilder::new(Arc::clone(node_application))
                    .with_runtime(SqlWorkerPool::new(4, 32).unwrap(), 64 * 1024 * 1024)
                    .with_session(node_session(node))
                    .with_replica_host(reference_host())
                    .build()
                    .unwrap();
                let recorder = Arc::new(DurabilityRecorder::default());
                host.install_telemetry(recorder.clone()).unwrap();
                durability.push(recorder);
                host.install_task_group(CancellationToken::new(), CancellationToken::new())
                    .unwrap();
                let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
                host.install_node_lease(lease.clone()).unwrap();
                leases.push(lease);
                host
            })
            .collect::<Vec<_>>();
        let cells = perf_cells();
        let mut handles = Vec::new();
        let mut owned = vec![Vec::new(); nodes];
        for (cell_index, (namespace, role, module, incarnation, schema)) in
            cells.into_iter().enumerate()
        {
            let node = cell_index % nodes;
            let handle = bootstrap_reference_cell(
                &hosts[node].runtime(),
                &registry,
                &layout,
                &directory,
                tenant,
                application_id,
                node_session(node),
                namespace,
                role,
                module,
                incarnation,
                schema,
            )
            .await
            .unwrap();
            owned[node].push(handle.clone());
            handles.push(handle);
        }
        let sql_handle = handles[0].clone();
        let signer = Arc::new(PeerSigner::new(
            cellule_runtime::SessionId::from_bytes([77; 16]),
            registry.release_digest(),
            SigningKey::from_bytes(&[78; 32]),
        ));
        let principal = PeerPrincipal {
            issuer: "reference-performance".into(),
            subject: "fleet-driver".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "reference.cron.deliver".into(),
            ],
        };
        let verifier = Arc::new(PeerVerifier::new(
            cellule_runtime::SessionId::from_bytes([77; 16]),
            registry.release_digest(),
            signer.verifying_key(),
        ));
        let (client, peer, servers, round_trip) = if nodes == 1 {
            let client = CellClient::local_many(Arc::clone(&registry), handles).unwrap();
            let dispatcher = Arc::new(PeerDispatcher::new(
                Arc::clone(&registry),
                Arc::new(SqlResolver {
                    target: CellTarget::new(
                        tenant,
                        application_id,
                        SQL_NAMESPACE,
                        &partition_for_shard(0),
                    )
                    .unwrap(),
                    handle: sql_handle,
                }),
                Arc::new(CronAuthorizer),
            ));
            let peer = EffectPeerClient::new(
                signer,
                principal,
                Arc::new(Loopback {
                    verifier,
                    dispatcher,
                }),
            );
            (client, peer, Vec::new(), None)
        } else {
            // A mixed release fleet must dispatch with each host's executable
            // registry; sharing the driver's registry would mask rollout bugs.
            let dispatchers = hosts
                .iter()
                .zip(owned.iter())
                .map(|(host, handles)| (host.application().registry(), handles.clone()))
                .collect();
            let (round_trip, servers) = start_peer_servers(verifier, dispatchers).await;
            let client = CellClient::peer(
                Arc::clone(&registry),
                Arc::clone(&signer),
                principal.clone(),
                Arc::clone(&round_trip),
            );
            let peer = EffectPeerClient::new(signer, principal, Arc::clone(&round_trip));
            (client, peer, servers, Some(round_trip))
        };
        let binding_node = usize::from(successor.is_some());
        let typed = hosts[binding_node]
            .application_handle::<ReferenceApplication>(client.clone(), tenant, application_id)
            .unwrap()
            .with_blob_artifact_store(BlobArtifactStore::new(store));
        let sql_target = CellTarget::new(
            tenant,
            application_id,
            SQL_NAMESPACE,
            &partition_for_shard(0),
        )
        .unwrap();
        let cron_target = CellTarget::new(
            tenant,
            application_id,
            CRON_NAMESPACE,
            &partition_for_shard(0),
        )
        .unwrap();
        Self {
            _directory: directory,
            nodes: hosts,
            layout: Some(layout),
            leases,
            round_trip,
            owned_handles: owned,
            durability,
            typed,
            registry,
            client,
            sql_target,
            cron_target,
            peer,
            servers,
        }
    }

    pub(super) fn cron_peer(&self) -> EffectPeerClient {
        self.peer.clone()
    }

    pub(super) fn lose_owner(&self, node: usize) {
        self.leases[node].fence();
        self.servers[node].abort();
    }

    pub(super) fn from_processes(
        directory: tempfile::TempDir,
        store: Store,
        owners: [SocketAddr; 3],
        balancer: Option<SocketAddr>,
    ) -> Self {
        let application = Arc::new(compiled());
        let registry = application.registry();
        let tenant = TenantId::from_bytes([81; 16]);
        let application_id = ApplicationId::from_bytes([82; 16]);
        let round_trip = if let Some(address) = balancer {
            balancer_round_trip(address)
        } else {
            peer_round_trip(owner_routes(tenant, application_id, owners))
        };
        let signer = Arc::new(PeerSigner::new(
            cellule_runtime::SessionId::from_bytes([77; 16]),
            registry.release_digest(),
            SigningKey::from_bytes(&[78; 32]),
        ));
        let principal = PeerPrincipal {
            issuer: "reference-performance".into(),
            subject: "fleet-driver".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "reference.cron.deliver".into(),
            ],
        };
        let client = CellClient::peer(
            Arc::clone(&registry),
            Arc::clone(&signer),
            principal.clone(),
            Arc::clone(&round_trip),
        );
        let peer = EffectPeerClient::new(signer, principal, round_trip);
        let typed = ApplicationHandle::new(client.clone(), application, tenant, application_id)
            .unwrap()
            .with_blob_artifact_store(BlobArtifactStore::new(store));
        let sql_target = CellTarget::new(
            tenant,
            application_id,
            SQL_NAMESPACE,
            &partition_for_shard(0),
        )
        .unwrap();
        let cron_target = CellTarget::new(
            tenant,
            application_id,
            CRON_NAMESPACE,
            &partition_for_shard(0),
        )
        .unwrap();
        Self {
            _directory: directory,
            nodes: Vec::new(),
            layout: None,
            leases: Vec::new(),
            round_trip: None,
            owned_handles: Vec::new(),
            durability: Vec::new(),
            typed,
            registry,
            client,
            sql_target,
            cron_target,
            peer,
            servers: Vec::new(),
        }
    }

    pub(super) async fn shutdown(&self) {
        for server in &self.servers {
            server.abort();
        }
        for (index, node) in self.nodes.iter().enumerate() {
            match node.shutdown().await {
                Ok(()) => {}
                // The simulated lost owner closes locally but cannot release
                // authority after its lease is fenced.
                Err(Error::Fenced) if self.leases[index].check().is_err() => {}
                Err(error) => panic!("CellNode shutdown failed: {error}"),
            }
        }
    }
}

pub(super) fn node_session(node: usize) -> cellule_runtime::SessionId {
    cellule_runtime::SessionId::from_bytes([24 + node as u8; 16])
}

struct SqlResolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for SqlResolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellHandle>> + Send + 'static>> {
        let allowed = target == self.target;
        let handle = self.handle.clone();
        Box::pin(async move {
            if allowed {
                Ok(handle)
            } else {
                Err(Error::CellNotActive)
            }
        })
    }
}

struct CronAuthorizer;

impl PeerAuthorizer for CronAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> Result<()> {
        if request.permits("reference.cron.deliver") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization("missing cron delivery action"))
        }
    }
}

struct Loopback {
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
}

impl PeerRoundTrip for Loopback {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        _remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>> {
        let verifier = Arc::clone(&self.verifier);
        let dispatcher = Arc::clone(&self.dispatcher);
        Box::pin(async move {
            let verified = verifier.verify(&request, now_ms())?;
            if verified.target() != &target {
                return Err(Error::Peer("round trip target changed"));
            }
            dispatcher.dispatch_bytes(&verified, now_ms()).await
        })
    }
}

// One explicit provider path for the public-host and process qualification lanes.
pub(super) fn rustfs_store() -> Store {
    let required = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    cellule_store::build_explicit_store(
        &required("CELLULE_TEST_BUCKET"),
        cellule_store::ObjectStoreCredentials::Aws {
            access_key_id: required("AWS_ACCESS_KEY_ID"),
            secret_access_key: required("AWS_SECRET_ACCESS_KEY"),
            session_token: None,
            region: "us-east-1".into(),
        },
        Some(&required("CELLULE_TEST_ENDPOINT")),
        true,
    )
    .unwrap()
}
