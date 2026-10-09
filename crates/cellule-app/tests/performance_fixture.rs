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
use cellule_runtime::SessionId;
use cellule_runtime::fleet::telemetry::{
    CellTelemetry, CommandResponseSource, ControlTransitionTiming, FollowerAppendTiming,
    NodeLogBatchTiming, NodeLogSubmissionTiming, PublicationTiming, QueryTiming, SqlSlotTiming,
};
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

// A stalled exporter has a fixed memory budget. Dropped observations are
// counted and invalidate qualification; callbacks never perform filesystem I/O.
pub(super) const TRACE_CAPACITY: usize = 65_536;

pub(super) struct Trace<T> {
    inner: Mutex<TraceBuffer<T>>,
}

struct TraceBuffer<T> {
    samples: Vec<T>,
    recorded: u64,
    dropped: u64,
}

impl<T> Default for Trace<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TraceBuffer {
                samples: Vec::new(),
                recorded: 0,
                dropped: 0,
            }),
        }
    }
}

impl<T> Trace<T> {
    pub(super) fn push(&self, sample: T) {
        let mut inner = self.inner.lock().unwrap();
        inner.recorded += 1;
        if inner.samples.len() == TRACE_CAPACITY {
            inner.dropped += 1;
        } else {
            inner.samples.push(sample);
        }
    }

    pub(super) fn drain(&self) -> Vec<T> {
        std::mem::take(&mut self.inner.lock().unwrap().samples)
    }

    pub(super) fn counts(&self) -> (u64, u64, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.recorded, inner.dropped, inner.samples.len())
    }

    fn take(&self) -> Self {
        let samples = self.drain();
        let recorded = samples.len() as u64;
        Self {
            inner: Mutex::new(TraceBuffer {
                samples,
                recorded,
                dropped: 0,
            }),
        }
    }
}

impl<T: Clone> Trace<T> {
    pub(super) fn snapshot(&self) -> Vec<T> {
        self.inner.lock().unwrap().samples.clone()
    }
}

#[test]
fn trace_drain_is_bounded_and_loss_remains_visible() {
    let trace = Trace::default();
    for sample in 0..TRACE_CAPACITY + 2 {
        trace.push(sample);
    }
    assert_eq!(
        trace.counts(),
        ((TRACE_CAPACITY + 2) as u64, 2, TRACE_CAPACITY)
    );
    assert_eq!(trace.drain(), (0..TRACE_CAPACITY).collect::<Vec<_>>());
    assert_eq!(trace.counts(), ((TRACE_CAPACITY + 2) as u64, 2, 0));
    trace.push(42);
    assert_eq!(trace.drain(), vec![42]);
    assert_eq!(trace.counts(), ((TRACE_CAPACITY + 3) as u64, 2, 0));
}

#[derive(Clone, Default)]
pub(super) struct TransportAppendTiming {
    pub pool_wait: Duration,
    pub member_resolution: Duration,
    pub encoding: Duration,
    pub address_resolution: Duration,
    pub connection: Duration,
    pub wire: Duration,
    pub verification: Duration,
    pub total: Duration,
    pub connection_attempts: u64,
}

#[derive(Default)]
pub(super) struct DurabilityRecorder {
    proofs: Trace<(DurabilitySource, Duration)>,
    responses: Trace<(i64, CommandResponseSource, Duration, Duration)>,
    executions: Trace<(i64, Duration, Duration, bool)>,
    queries: Trace<(i64, cellule_runtime::CellId, QueryTiming)>,
    sql_slots: Trace<(i64, SqlSlotTiming)>,
    publications: Trace<(i64, cellule_runtime::CellId, PublicationTiming)>,
    phases: Trace<(i64, LtxPhase, Duration, bool)>,
    captures: Trace<(i64, CaptureTiming, bool)>,
    publication_costs: Trace<(i64, u64, u64)>,
    follower_appends: Trace<(i64, bool, u64)>,
    follower_network: Trace<(i64, bool, u64, Duration)>,
    follower_transport: Trace<(i64, bool, u64, TransportAppendTiming)>,
    follower_store: Trace<(i64, SessionId, u64, FollowerAppendTiming)>,
    node_log_batches: Trace<(i64, NodeLogBatchTiming)>,
    node_log_submissions: Trace<(i64, cellule_runtime::CellId, NodeLogSubmissionTiming)>,
    control_transitions: Trace<(i64, cellule_runtime::CellId, ControlTransitionTiming)>,
    node_log_events: Trace<(i64, u64, &'static str, u64)>,
}

impl DurabilityRecorder {
    pub(super) fn drain(&self) -> Self {
        Self {
            proofs: self.proofs.take(),
            responses: self.responses.take(),
            executions: self.executions.take(),
            queries: self.queries.take(),
            sql_slots: self.sql_slots.take(),
            publications: self.publications.take(),
            phases: self.phases.take(),
            captures: self.captures.take(),
            publication_costs: self.publication_costs.take(),
            follower_appends: self.follower_appends.take(),
            follower_network: self.follower_network.take(),
            follower_transport: self.follower_transport.take(),
            follower_store: self.follower_store.take(),
            node_log_batches: self.node_log_batches.take(),
            node_log_submissions: self.node_log_submissions.take(),
            control_transitions: self.control_transitions.take(),
            node_log_events: self.node_log_events.take(),
        }
    }

    pub(super) fn trace_counts(&self) -> Vec<(&'static str, u64, u64, usize)> {
        vec![
            {
                let (recorded, dropped, buffered) = self.proofs.counts();
                ("proofs", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.responses.counts();
                ("responses", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.executions.counts();
                ("executions", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.queries.counts();
                ("queries", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.sql_slots.counts();
                ("sql_slots", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.publications.counts();
                ("publications", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.phases.counts();
                ("phases", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.captures.counts();
                ("captures", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.publication_costs.counts();
                ("publication_costs", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.follower_appends.counts();
                ("follower_appends", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.follower_network.counts();
                ("follower_network", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.follower_transport.counts();
                ("follower_transport", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.follower_store.counts();
                ("follower_store", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.node_log_batches.counts();
                ("node_log_batches", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.node_log_submissions.counts();
                ("node_log_submissions", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.control_transitions.counts();
                ("control_transitions", recorded, dropped, buffered)
            },
            {
                let (recorded, dropped, buffered) = self.node_log_events.counts();
                ("node_log_events", recorded, dropped, buffered)
            },
        ]
    }

    pub(super) fn object_waits(&self) -> Vec<Duration> {
        self.proofs
            .snapshot()
            .iter()
            .filter_map(|(source, waited)| (*source == DurabilitySource::Object).then_some(*waited))
            .collect()
    }

    pub(super) fn responses(&self) -> Vec<(i64, CommandResponseSource, Duration, Duration)> {
        self.responses.snapshot()
    }

    pub(super) fn executions(&self) -> Vec<(i64, Duration, Duration, bool)> {
        self.executions.snapshot()
    }

    pub(super) fn queries(&self) -> Vec<(i64, cellule_runtime::CellId, QueryTiming)> {
        self.queries.snapshot()
    }

    pub(super) fn sql_slots(&self) -> Vec<(i64, SqlSlotTiming)> {
        self.sql_slots.snapshot()
    }

    pub(super) fn publications(&self) -> Vec<(i64, cellule_runtime::CellId, PublicationTiming)> {
        self.publications.snapshot()
    }

    pub(super) fn phases(&self) -> Vec<(i64, LtxPhase, Duration, bool)> {
        self.phases.snapshot()
    }

    pub(super) fn captures(&self) -> Vec<(i64, CaptureTiming, bool)> {
        self.captures.snapshot()
    }

    pub(super) fn publication_costs(&self) -> Vec<(i64, u64, u64)> {
        self.publication_costs.snapshot()
    }

    pub(super) fn follower_appends(&self) -> Vec<(i64, bool, u64)> {
        self.follower_appends.snapshot()
    }

    pub(super) fn record_follower_network(
        &self,
        acknowledged: bool,
        bytes: u64,
        timing: TransportAppendTiming,
    ) {
        let at_ms = now_ms();
        self.follower_network
            .push((at_ms, acknowledged, bytes, timing.total));
        self.follower_transport
            .push((at_ms, acknowledged, bytes, timing));
    }

    pub(super) fn follower_network(&self) -> Vec<(i64, bool, u64, Duration)> {
        self.follower_network.snapshot()
    }

    pub(super) fn follower_transport(&self) -> Vec<(i64, bool, u64, TransportAppendTiming)> {
        self.follower_transport.snapshot()
    }

    pub(super) fn follower_store(&self) -> Vec<(i64, SessionId, u64, FollowerAppendTiming)> {
        self.follower_store.snapshot()
    }

    pub(super) fn node_log_batches(&self) -> Vec<(i64, NodeLogBatchTiming)> {
        self.node_log_batches.snapshot()
    }

    pub(super) fn node_log_submissions(
        &self,
    ) -> Vec<(i64, cellule_runtime::CellId, NodeLogSubmissionTiming)> {
        self.node_log_submissions.snapshot()
    }

    pub(super) fn control_transitions(
        &self,
    ) -> Vec<(i64, cellule_runtime::CellId, ControlTransitionTiming)> {
        self.control_transitions.snapshot()
    }

    pub(super) fn record_node_log_event(&self, epoch: u64, phase: &'static str, through: u64) {
        self.node_log_events.push((now_ms(), epoch, phase, through));
    }

    pub(super) fn node_log_events(&self) -> Vec<(i64, u64, &'static str, u64)> {
        self.node_log_events.snapshot()
    }
}

impl CellTelemetry for DurabilityRecorder {
    fn sql_slot_released(&self, timing: SqlSlotTiming) {
        self.sql_slots.push((now_ms(), timing));
    }

    fn query_completed(&self, cell: cellule_runtime::CellId, timing: QueryTiming) {
        self.queries.push((now_ms(), cell, timing));
    }

    fn durability_proof(&self, source: DurabilitySource, waited: Duration) {
        self.proofs.push((source, waited));
    }

    fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: Duration,
        confirmation: Duration,
    ) {
        self.responses
            .push((now_ms(), source, elapsed, confirmation));
    }

    fn command_execution(
        &self,
        queue_wait: Duration,
        worker_round_trip: Duration,
        succeeded: bool,
    ) {
        self.executions
            .push((now_ms(), queue_wait, worker_round_trip, succeeded));
    }

    fn publication_completed(&self, cell: cellule_runtime::CellId, timing: PublicationTiming) {
        self.publications.push((now_ms(), cell, timing));
    }

    fn ltx_phase(&self, phase: LtxPhase, elapsed: Duration, succeeded: bool) {
        self.phases.push((now_ms(), phase, elapsed, succeeded));
    }

    fn ltx_capture(&self, timing: &CaptureTiming, succeeded: bool) {
        self.captures.push((now_ms(), *timing, succeeded));
    }

    fn publication_cost(&self, objects: u64, bytes: u64) {
        self.publication_costs.push((now_ms(), objects, bytes));
    }

    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        self.follower_appends.push((now_ms(), acknowledged, bytes));
    }

    fn follower_append(&self, timing: FollowerAppendTiming) {
        let leader = timing.leader.unwrap();
        let epoch = timing.epoch;
        self.follower_store.push((now_ms(), leader, epoch, timing));
    }

    fn node_log_batch(&self, timing: NodeLogBatchTiming) {
        self.node_log_batches.push((now_ms(), timing));
    }

    fn node_log_submission(&self, cell: cellule_runtime::CellId, timing: NodeLogSubmissionTiming) {
        self.node_log_submissions.push((now_ms(), cell, timing));
    }

    fn control_transition(&self, cell: cellule_runtime::CellId, timing: ControlTransitionTiming) {
        self.control_transitions.push((now_ms(), cell, timing));
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
                Err(error)
                    if self.leases[index].check().is_err() && fenced_runtime_drain(&error) =>
                {
                    let stats = node.stats();
                    assert_eq!(stats.active_cells(), 0);
                    assert_eq!(stats.resident_bytes(), 0);
                    assert_eq!(stats.retained_bytes(), 0);
                    assert_eq!(stats.worker_jobs(), 0);
                    assert_eq!(stats.file_descriptors(), 0);
                    assert_eq!(stats.local_disk_reserved_bytes(), 0);
                }
                Err(error) => panic!("CellNode shutdown failed: {error}"),
            }
        }
    }
}

fn fenced_runtime_drain(error: &Error) -> bool {
    if matches!(error, Error::Fenced) {
        return true;
    }
    if !matches!(
        error,
        Error::Facility {
            name: "cell-runtime-drain",
            ..
        }
    ) {
        return false;
    }
    // Retained shutdown errors wrap the original runtime failure so repeated
    // callers can inspect it. Only the exact terminal fencing error is expected
    // after simulated owner loss; unrelated facility failures must still fail.
    let mut cause: &(dyn std::error::Error + 'static) = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    matches!(cause.downcast_ref::<Error>(), Some(Error::Fenced))
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
