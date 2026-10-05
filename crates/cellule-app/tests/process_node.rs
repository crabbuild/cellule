//! Public host and authoritative session lifecycle for the process fixture.

use super::performance_fixture::{DurabilityRecorder, node_session, now_ms};
use super::process_follower::{ProcessDurabilityProvider, ProcessEnrollment};
use crate::*;
use cellule_host::{CellNode, CellNodeBuilder, NodeDurabilitySupervisorConfig};
use cellule_runtime::node::lease::NodeLeaseGuard;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(super) fn directory(layout: &CellStorageLayout, registry: &Registry) -> NodeDirectory {
    NodeDirectory::new(
        layout.clone(),
        Digest::from_bytes([90; 32]),
        Digest::from_bytes([91; 32]),
        registry.release_digest(),
    )
}

pub(super) fn signing_key(node: usize) -> SigningKey {
    let mut seed = [93; 32];
    seed[0] = u8::try_from(node).unwrap();
    SigningKey::from_bytes(&seed)
}

pub(super) async fn start(
    node: usize,
    application: Arc<cellule_app::CompiledApplication>,
    layout: &CellStorageLayout,
    root: &std::path::Path,
    endpoint: String,
) -> (
    CellNode,
    Arc<DurabilityRecorder>,
    cellule_host::read_replicas::ReadReplicaManager,
) {
    start_configured(node, application, layout, root, endpoint, false).await
}

pub(super) async fn start_configured(
    node: usize,
    application: Arc<cellule_app::CompiledApplication>,
    layout: &CellStorageLayout,
    root: &std::path::Path,
    endpoint: String,
    follower_enabled: bool,
) -> (
    CellNode,
    Arc<DurabilityRecorder>,
    cellule_host::read_replicas::ReadReplicaManager,
) {
    let registry = application.registry();
    // Keep writer admission at 32 Cells while charging both the old and new
    // immutable snapshots during a refresh under the same node ledger.
    let pool = SqlWorkerPool::new(4, 32)
        .unwrap()
        .with_native_memory_limit(32 << 20)
        .unwrap();
    let mut builder = CellNodeBuilder::new(application)
        .with_runtime(pool, 64 * 1024 * 1024)
        .with_session(node_session(node))
        .with_replica_host(reference_host());
    if follower_enabled {
        builder = builder.with_follower_store(
            root.join("followers"),
            Limits::default(),
            DiskBudget::new(1 << 30),
        );
    }
    let host = builder.build().unwrap();
    let durability = Arc::new(DurabilityRecorder::default());
    host.install_telemetry(durability.clone()).unwrap();
    let directory = directory(layout, &registry);
    let signer = signing_key(node);
    let follower_signer = signer.clone();
    let advertisement = move |now: i64, progress| {
        NodeAdvertisement::sign(
            NodeId::from_bytes(*node_session(node).as_bytes()),
            node_session(node),
            endpoint.clone(),
            Digest::from_bytes([90; 32]),
            Digest::from_bytes([94; 32]),
            Digest::from_bytes([91; 32]),
            registry.release_digest(),
            &signer,
            progress,
            now,
            now + 15_000,
            registry.module_digests(),
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 64 * 1024 * 1024,
                free_disk_bytes: 1 << 30,
                job_credits: 32,
                follower_free_bytes: if follower_enabled { 1 << 30 } else { 0 },
                log_protocol: if follower_enabled {
                    cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION
                } else {
                    0
                },
                ..NodeCapacity::default()
            },
        )
    };
    let now = now_ms();
    let observed = directory
        .create(advertisement(now, 1).unwrap(), now)
        .await
        .unwrap();
    let lease = NodeLeaseGuard::new(now_ms(), observed.advertisement().expires_at_ms()).unwrap();
    let enrollment = ProcessEnrollment::new(
        observed,
        directory.clone(),
        node_session(node),
        follower_enabled.then(|| Arc::clone(&durability)),
    );
    let shutdown = CancellationToken::new();
    let tasks = host
        .install_task_group(CancellationToken::new(), shutdown.clone())
        .unwrap();
    host.install_node_lease_for_startup(lease.clone()).unwrap();
    if follower_enabled {
        let provider = Arc::new(ProcessDurabilityProvider::new(
            enrollment.clone(),
            follower_signer,
            lease.clone(),
            host.runtime().telemetry_handle(),
            Arc::clone(&durability),
        ));
        let configuration = NodeDurabilitySupervisorConfig::new(
            ApplicationId::from_bytes([82; 16]),
            Limits::default(),
            1 << 20,
            32,
            Duration::from_millis(250),
            Duration::from_secs(1),
            1_000_000,
        )
        .unwrap();
        host.install_node_durability_provider(provider, configuration)
            .unwrap();
    }
    let (renewed, first_renewal) = tokio::sync::oneshot::channel();
    let renewing = enrollment.clone();
    tasks
        .spawn_lease_maintenance(async move {
            let mut renewed = Some(renewed);
            let result: Result<()> = async {
                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                    let now = now_ms();
                    let next = advertisement(now, renewing.progress().await + 1)?;
                    // Finish the conditional write before observing shutdown so
                    // withdrawal always uses the latest acknowledged generation.
                    let expires_at =
                        tokio::time::timeout(lease.remaining(), renewing.refresh(next))
                            .await
                            .map_err(|_| Error::Deadline)??;
                    lease.renew(now_ms(), expires_at)?;
                    if let Some(renewed) = renewed.take() {
                        let _ = renewed.send(());
                    }
                }
                tokio::time::timeout(Duration::from_secs(20), renewing.withdraw())
                    .await
                    .map_err(|_| Error::Deadline)??;
                println!(
                    "PERF node_{node}_session_withdrawn: generation={}",
                    renewing.generation().await
                );
                Ok(())
            }
            .await;
            if let Err(error) = &result {
                eprintln!("[DEBUG-mixed-reader] node={node} lease_maintenance_error remaining_ms={} error={error:?}", lease.remaining().as_millis());
            }
            lease.fence();
            result
        })
        .unwrap();
    // Even a short smoke must exercise provider-backed renewal before it can
    // report readiness; successful drain then proves withdrawal of that version.
    first_renewal.await.unwrap();
    let readers = host
        .install_read_replicas(
            layout.clone(),
            self::directory(layout, &host.application().registry()),
            root.join("readers"),
            Limits::default(),
        )
        .unwrap();
    host.install_read_replica_recruitment(
        cellule_runtime::cell::application::ApplicationIdentity::new(
            TenantId::from_bytes([81; 16]),
            ApplicationId::from_bytes([82; 16]),
        ),
        cellule_runtime::peer::ReplicaPeerClient::new(
            host.application().registry(),
            Arc::new(cellule_runtime::peer::PeerSigner::new(
                node_session(node),
                host.application().registry().release_digest(),
                signing_key(node),
            )),
            cellule_runtime::peer::PeerPrincipal {
                issuer: "reference-runtime".into(),
                subject: format!("node-{node}"),
                actions: vec!["cell.replica.activate".into()],
            },
            Arc::new(EnrolledReplicaTransport),
        ),
    )
    .unwrap();
    host.start().unwrap();
    (host, durability, readers)
}

pub(super) struct EnrolledReplicaTransport;

impl cellule_runtime::peer::PeerRoundTrip for EnrolledReplicaTransport {
    fn send(
        &self,
        _: CellTarget,
        _: Vec<u8>,
        _: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>> {
        Box::pin(async { Err(Error::Peer("reader activation requires an enrolled node")) })
    }

    fn send_to_node(
        &self,
        _: CellTarget,
        node: NodeAdvertisement,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>> {
        let endpoint = node.endpoint().to_owned();
        Box::pin(async move {
            let address = endpoint
                .strip_prefix("https://")
                .ok_or(Error::Peer("reader endpoint is invalid"))?
                .parse()
                .map_err(|_| Error::Peer("reader endpoint has no socket address"))?;
            super::fleet::send_tcp(address, request, remaining_ms).await
        })
    }
}
