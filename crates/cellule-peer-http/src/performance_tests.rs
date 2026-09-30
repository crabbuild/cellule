//! Explicit RustFS measurements through actor routing and pinned mTLS peers.

use super::*;
use axum::{Router, body::Bytes, response::Response, routing::post};
use cellule_runtime::{
    cell::{
        actor::CellRuntime,
        catalog::{CatalogEntry, CatalogRole, CellCatalog},
        worker::SqlWorkerPool,
    },
    client::CellClient,
    fleet::telemetry::{CellTelemetry, PublicationTiming},
    identity::{ApplicationId, IncarnationId, NamespaceId, NodeId, RequestId, TenantId},
    ltx::{CellReplica, CellStorageLayout, Host, Limits},
    node::{NodeAdvertisement, NodeCapacity, NodeFailureDomain, lease::NodeLeaseGuard},
    peer::{
        PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal, PeerSigner, PeerVerifier,
        ResidentPeerCellResolver, VerifiedPeerRequest,
    },
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{
        BuildDescriptor, CellModule, Command, CommandContext, CommandResult, MigrationDescriptor,
        ModuleDescriptor, NamespaceDescriptor, OperationDescriptor, Query, QueryContext, Registry,
        RegistryBuilder,
    },
};
use cellule_store::{
    ObjectStoreCredentials, Store, build_explicit_store, test_support::CountingObjectStore,
};
use object_store::path::Path;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

const MODULE: &str = "routing-measurement";
const NAMESPACE: NamespaceId = NamespaceId::from_bytes([71; 16]);
const SCHEMA: &str = "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)";
const OPERATION: OperationDescriptor = OperationDescriptor {
    id: 1,
    codec_version: 1,
    schema_min: 1,
    schema_max: 1,
    input_limit: 64,
    output_limit: 64,
};
struct Increment;
impl Command for Increment {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = u64;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        _: (),
    ) -> cellule_runtime::Result<CommandResult<u64>> {
        let results = context.sql(&SqlBatch {
            statements: vec![
                SqlStatement {
                    sql: "UPDATE counter SET value=value+1".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "SELECT value FROM counter".into(),
                    parameters: vec![],
                },
            ],
        })?;
        Ok(CommandResult::Success(value(&results[1].rows[0][0])))
    }
}
struct Read;
impl Query for Read {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = u64;
    fn execute(context: &mut QueryContext<'_>, _: ()) -> cellule_runtime::Result<u64> {
        let results = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT value FROM counter".into(),
                parameters: vec![],
            }],
        })?;
        Ok(value(&results[0].rows[0][0]))
    }
}
fn value(value: &SqlValue) -> u64 {
    match value {
        SqlValue::Integer(value) => (*value).try_into().unwrap(),
        _ => panic!("integer"),
    }
}
struct Module;
impl CellModule for Module {
    const NAME: &'static str = MODULE;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: Digest::from_bytes([72; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: &[OPERATION],
            queries: &[OPERATION],
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: NAMESPACE,
                name: MODULE,
                role: CatalogRole::Application,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<Increment>()?;
        registry.bind_query::<Read>()
    }
}
struct Authorizer;

struct UncachedResident(CellRuntime, Arc<Registry>);
impl PeerCellResolver for UncachedResident {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<
        Box<
            dyn Future<Output = cellule_runtime::Result<cellule_runtime::cell::actor::CellHandle>>
                + Send
                + 'static,
        >,
    > {
        let runtime = self.0.clone();
        let registry = self.1.clone();
        Box::pin(async move {
            let role = registry
                .namespace_contract(target.namespace())
                .unwrap()
                .1
                .role;
            runtime
                .resident_handle(&target, role)
                .await?
                .ok_or(CellError::CellNotActive)
        })
    }
}

#[derive(Default)]
struct PublicationSamples(Mutex<HashMap<u64, PublicationTiming>>);
impl CellTelemetry for PublicationSamples {
    fn publication_completed(&self, _: CellId, timing: PublicationTiming) {
        self.0
            .lock()
            .unwrap()
            .insert(timing.commit_sequence, timing);
    }
}
impl PublicationSamples {
    fn report(&self, lane: &str, concurrency: usize, first: u64, last: u64) {
        let samples = self.0.lock().unwrap();
        let timings = (first..=last)
            .map(|sequence| samples[&sequence])
            .collect::<Vec<_>>();
        assert!(timings.iter().all(|timing| timing.succeeded));
        let mean = |phase: fn(&PublicationTiming) -> Duration| {
            timings
                .iter()
                .map(|timing| phase(timing).as_secs_f64() * 1000.0)
                .sum::<f64>()
                / timings.len() as f64
        };
        println!(
            "RUSTFS publication lane={lane} concurrency={concurrency} calls={} preparation_mean_ms={:.6} authority_mean_ms={:.6} total_mean_ms={:.6}",
            timings.len(),
            mean(|t| t.preparation),
            mean(|t| t.authority),
            mean(|t| t.total)
        );
    }
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.principal().subject != "measurement" {
            return Err(CellError::PeerAuthorization("fixture principal"));
        }
        Ok(())
    }
}
fn registry() -> Arc<Registry> {
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "routing-measurement".into(),
        cargo_lock_digest: Digest::from_bytes([73; 32]),
    });
    builder.register(Module).unwrap();
    Arc::new(builder.finish().unwrap())
}
fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
}
fn report(
    lane: &str,
    concurrency: usize,
    samples: &mut [Duration],
    elapsed: Duration,
    reads: usize,
    puts: usize,
    hops: usize,
) {
    samples.sort_unstable();
    let p = |q: usize| samples[(samples.len() * q).div_ceil(100) - 1].as_secs_f64() * 1_000.0;
    println!(
        "RUSTFS lane={lane} concurrency={concurrency} calls={} elapsed_s={:.6} throughput={:.3} p50_ms={:.6} p95_ms={:.6} p99_ms={:.6} reads={reads} puts={puts} hops={hops}",
        samples.len(),
        elapsed.as_secs_f64(),
        samples.len() as f64 / elapsed.as_secs_f64(),
        p(50),
        p(95),
        p(99)
    );
    if let Ok(directory) = std::env::var("CELLULE_PERF_EVIDENCE") {
        let output = samples
            .iter()
            .map(|sample| format!("{}\n", sample.as_nanos()))
            .collect::<String>();
        std::fs::write(
            std::path::Path::new(&directory).join(format!("{lane}-c{concurrency}.ns")),
            output,
        )
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated RustFS endpoint, bucket, prefix and explicit fixture credentials"]
async fn rustfs_owner_routing_latency_throughput() {
    run_rustfs_owner_routing_latency_throughput(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated RustFS endpoint, bucket, prefix and explicit fixture credentials"]
async fn rustfs_object_only_routing_latency_throughput() {
    run_rustfs_owner_routing_latency_throughput(false).await;
}

async fn run_rustfs_owner_routing_latency_throughput(leased: bool) {
    let Some((certificate_dir, tls)) = super::tests::generate_peer_identity("rustfs-routing")
    else {
        panic!("OpenSSL 3 is required for this explicit mTLS qualification");
    };
    let provider = build_explicit_store(
        &required("CELLULE_TEST_BUCKET"),
        ObjectStoreCredentials::Aws {
            access_key_id: required("AWS_ACCESS_KEY_ID"),
            secret_access_key: required("AWS_SECRET_ACCESS_KEY"),
            session_token: None,
            region: "us-east-1".into(),
        },
        Some(&required("CELLULE_TEST_ENDPOINT")),
        true,
    )
    .unwrap();
    let counted = Arc::new(CountingObjectStore::new(Arc::clone(provider.inner())));
    let tenant = TenantId::from_bytes([74; 16]);
    let application = ApplicationId::from_bytes([75; 16]);
    let target = CellTarget::new(tenant, application, NAMESPACE, b"counter").unwrap();
    let registry = registry();
    let layout = CellStorageLayout::new(
        Store::new(counted.clone()),
        Path::from(required("CELLULE_TEST_PREFIX")),
        *application.as_bytes(),
    );
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Application,
                registry.module_code(MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let owner_session = SessionId::from_bytes([76; 16]);
    let authority = CellAuthority::new(layout.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        tls.fleet(),
        Digest::from_bytes([78; 32]),
        registry.release_digest(),
    );
    let now = now_ms().unwrap();
    let enrolled = directory
        .create(
            NodeAdvertisement::sign(
                NodeId::from_bytes([79; 16]),
                owner_session,
                endpoint.clone(),
                tls.fleet(),
                tls.certificate(),
                Digest::from_bytes([78; 32]),
                registry.release_digest(),
                tls.signing_key(),
                1,
                now,
                now + 15_000,
                vec![registry.module_code(MODULE).unwrap()],
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity::default(),
            )
            .unwrap(),
            now,
        )
        .await
        .unwrap();
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([77; 16]),
            cellule_runtime::control::Owner {
                session: owner_session,
                endpoint: endpoint.clone(),
            },
        )
        .await
        .unwrap();
    let constructor = if leased {
        CellRuntime::new_with_replica_host_requiring_node_lease
    } else {
        CellRuntime::new_with_replica_host
    };
    let runtime = constructor(
        SqlWorkerPool::new(2, 32).unwrap(),
        16 << 20,
        owner_session,
        Host::default(),
    )
    .unwrap();
    let lease =
        NodeLeaseGuard::new(now_ms().unwrap(), enrolled.advertisement().expires_at_ms()).unwrap();
    if leased {
        runtime.install_node_lease(lease.clone()).unwrap();
    }
    let publications = Arc::new(PublicationSamples::default());
    runtime.install_telemetry(publications.clone()).unwrap();
    let disk = tempfile::tempdir().unwrap();
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        [77; 16],
        Limits::default(),
    )
    .unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            replica.clone(),
            authority.clone(),
            observed,
            disk.path().join("counter.sqlite"),
            |tx| {
                tx.execute_batch(SCHEMA)?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let (stop_heartbeat, mut heartbeat_stopped) = tokio::sync::oneshot::channel();
    let heartbeat_directory = directory.clone();
    let heartbeat_key = tls.signing_key().clone();
    let fleet = tls.fleet();
    let certificate = tls.certificate();
    let release = registry.release_digest();
    let code = registry.module_code(MODULE).unwrap();
    let heartbeat_lease = lease.clone();
    let heartbeat = tokio::spawn(async move {
        let mut enrolled = enrolled;
        let mut progress = 1;
        loop {
            tokio::select! {
                _ = &mut heartbeat_stopped => break,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            progress += 1;
            let now = now_ms().unwrap();
            let next = NodeAdvertisement::sign(
                NodeId::from_bytes([79; 16]),
                owner_session,
                endpoint.clone(),
                fleet,
                certificate,
                Digest::from_bytes([78; 32]),
                release,
                &heartbeat_key,
                progress,
                now,
                now + 15_000,
                vec![code],
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity::default(),
            )
            .unwrap();
            enrolled = heartbeat_directory
                .refresh(&enrolled, next, now)
                .await
                .unwrap();
            heartbeat_lease
                .renew(now_ms().unwrap(), enrolled.advertisement().expires_at_ms())
                .unwrap();
        }
    });
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([80; 16]),
        registry.release_digest(),
        tls.signing_key().clone(),
    ));
    let verifier = Arc::new(PeerVerifier::new(
        SessionId::from_bytes([80; 16]),
        registry.release_digest(),
        signer.verifying_key(),
    ));
    let dispatcher = Arc::new(PeerDispatcher::new(
        registry.clone(),
        Arc::new(ResidentPeerCellResolver::new(
            runtime.clone(),
            layout.clone(),
            registry.clone(),
        )),
        Arc::new(Authorizer),
    ));
    let uncached_dispatcher = Arc::new(PeerDispatcher::new(
        registry.clone(),
        Arc::new(UncachedResident(runtime.clone(), registry.clone())),
        Arc::new(Authorizer),
    ));
    let uncached_receiver = Arc::new(AtomicBool::new(false));
    let observed_receiver = uncached_receiver.clone();
    let hops = Arc::new(AtomicUsize::new(0));
    let observed_hops = hops.clone();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(move |body: Bytes| {
            let verifier = verifier.clone();
            let dispatcher = if observed_receiver.load(Ordering::Acquire) {
                uncached_dispatcher.clone()
            } else {
                dispatcher.clone()
            };
            let hops = observed_hops.clone();
            async move {
                hops.fetch_add(1, Ordering::Relaxed);
                let now = now_ms().unwrap();
                let verified = verifier.verify(&body, now).unwrap();
                let reply = dispatcher.dispatch_bytes(&verified, now).await.unwrap();
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, PROTOBUF_MEDIA_TYPE)
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(axum::body::Body::from(reply))
                    .unwrap()
            }
        }),
    );
    let clients = Arc::new(tls.client_identity());
    let server = tokio::spawn(async move {
        axum::serve(tls.listener(listener), router).await.unwrap();
    });
    let transport = Arc::new(PeerHttpRoundTrip::new(
        Arc::new(ApplicationIdentity::new(tenant, application)),
        authority.clone(),
        directory,
        clients,
        SessionId::from_bytes([80; 16]),
    ));
    let ingress = CellRuntime::new(
        SqlWorkerPool::new(1, 8).unwrap(),
        16 << 20,
        SessionId::from_bytes([80; 16]),
    )
    .unwrap();
    let peer = CellClient::runtime_with_peer(
        registry.clone(),
        ingress.clone(),
        layout.clone(),
        signer,
        PeerPrincipal {
            issuer: "https://fixture.example".into(),
            subject: "measurement".into(),
            actions: vec!["counter".into()],
        },
        transport.clone(),
    );
    let local = CellClient::local_runtime(registry.clone(), runtime.clone(), layout.clone());
    let queries = std::env::var("CELLULE_PERF_QUERIES")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(1024);
    let commands = std::env::var("CELLULE_PERF_COMMANDS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(128);
    assert!(
        queries >= 16
            && queries.is_multiple_of(16)
            && commands >= 16
            && commands.is_multiple_of(16)
    );
    let mut expected = 0_u64;
    for (route, client) in [("local", &local), ("forwarded", &peer)] {
        counted.reset();
        hops.store(0, Ordering::Relaxed);
        let cold = Instant::now();
        assert_eq!(
            client
                .query::<Read>(&target, None, ())
                .await
                .unwrap()
                .output,
            expected
        );
        let elapsed = cold.elapsed();
        report(
            &format!("{route}_query_cold"),
            1,
            &mut [elapsed],
            elapsed,
            counted.counts().body_requests(),
            counted.put_requests(),
            hops.load(Ordering::Relaxed),
        );
        // Keep warm-route samples separate from the 30-second Describe cache.
        // The fixture's observed contract is fixed; every request still crosses
        // the receiver's authority/lease and actor admission gates.
        let query_client =
            client
                .clone()
                .with_observed_description(cellule_runtime::client::CellDescription {
                    cell: target.cell_id(),
                    incarnation: IncarnationId::from_bytes([77; 16]),
                    code: registry.module_code(MODULE).unwrap(),
                    schema: 1,
                });
        if route == "local" {
            // Separate first-request throughput from warm reused-client throughput.
            // Each fresh transport must establish its description and route.
            for concurrency in [1, 16] {
                counted.reset();
                let started = Instant::now();
                let mut samples = Vec::new();
                for _ in 0..queries / concurrency {
                    let results = futures_util::future::join_all((0..concurrency).map(|_| {
                        let registry = registry.clone();
                        let runtime = runtime.clone();
                        let layout = layout.clone();
                        let target = &target;
                        async move {
                            let start = Instant::now();
                            let fresh = CellClient::local_runtime(registry, runtime, layout);
                            assert_eq!(
                                fresh.query::<Read>(target, None, ()).await.unwrap().output,
                                expected
                            );
                            start.elapsed()
                        }
                    }))
                    .await;
                    samples.extend(results);
                }
                report(
                    "local_query_fresh_client",
                    concurrency,
                    &mut samples,
                    started.elapsed(),
                    counted.counts().body_requests(),
                    counted.put_requests(),
                    0,
                );
            }
            // Adjacent direct-handle reads control for workstation scheduling
            // noise when evaluating the warm route's CPU overhead.
            let direct = CellClient::local(registry.clone(), handle.clone());
            let mut ratios = Vec::new();
            for index in 0..queries {
                let mut durations = [Duration::ZERO; 2];
                for lane in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
                    let selected = if lane == 0 { &direct } else { &query_client };
                    let started = Instant::now();
                    assert_eq!(
                        selected
                            .query::<Read>(&target, None, ())
                            .await
                            .unwrap()
                            .output,
                        expected
                    );
                    durations[lane] = started.elapsed();
                }
                ratios.push(durations[1].as_secs_f64() / durations[0].as_secs_f64());
            }
            ratios.sort_by(f64::total_cmp);
            println!(
                "RUSTFS warm_route_control calls={queries} median_runtime_to_direct_ratio={:.6}",
                ratios[queries / 2]
            );
        }
        for concurrency in [1, 16] {
            if route == "forwarded" {
                transport.routes().invalidate(&target, owner_session);
                transport.routes().route(&target).await.unwrap();
            }
            counted.reset();
            hops.store(0, Ordering::Relaxed);
            let started = Instant::now();
            let mut samples = Vec::new();
            for _ in 0..queries / concurrency {
                let results = futures_util::future::join_all((0..concurrency).map(|_| async {
                    let start = Instant::now();
                    let result = query_client.query::<Read>(&target, None, ()).await.unwrap();
                    assert_eq!(result.output, expected);
                    start.elapsed()
                }))
                .await;
                samples.extend(results);
            }
            report(
                &format!("{route}_query"),
                concurrency,
                &mut samples,
                started.elapsed(),
                counted.counts().body_requests(),
                counted.put_requests(),
                hops.load(Ordering::Relaxed),
            );
        }
        if route == "forwarded" {
            let mut ratios = Vec::new();
            for index in 0..queries {
                let mut durations = [Duration::ZERO; 2];
                for lane in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
                    uncached_receiver.store(lane == 0, Ordering::Release);
                    let started = Instant::now();
                    assert_eq!(
                        query_client
                            .query::<Read>(&target, None, ())
                            .await
                            .unwrap()
                            .output,
                        expected
                    );
                    durations[lane] = started.elapsed();
                }
                ratios.push(durations[1].as_secs_f64() / durations[0].as_secs_f64());
            }
            uncached_receiver.store(false, Ordering::Release);
            ratios.sort_by(f64::total_cmp);
            println!(
                "RUSTFS forwarded_route_control calls={queries} median_cached_to_uncached_ratio={:.6}",
                ratios[queries / 2]
            );
            // Force the same cold owner-hint burst in both revisions. The
            // shared adapter must enroll once, even when all callers miss.
            counted.reset();
            hops.store(0, Ordering::Relaxed);
            let started = Instant::now();
            let mut samples = Vec::new();
            for _ in 0..4 {
                transport.routes().invalidate(&target, owner_session);
                samples.extend(
                    futures_util::future::join_all((0..16).map(|_| async {
                        let started = Instant::now();
                        assert_eq!(
                            query_client
                                .query::<Read>(&target, None, ())
                                .await
                                .unwrap()
                                .output,
                            expected
                        );
                        started.elapsed()
                    }))
                    .await,
                );
            }
            report(
                "forwarded_query_uncached_route",
                16,
                &mut samples,
                started.elapsed(),
                counted.counts().body_requests(),
                counted.put_requests(),
                hops.load(Ordering::Relaxed),
            );
        }
        // Repeated bursts cross the former two-second route-cache lifetime.
        let mut samples = Vec::new();
        let mut reads = 0;
        let mut puts = 0;
        let mut peer_hops = 0;
        let burst_window = Instant::now();
        for _ in 0..12 {
            tokio::time::sleep(Duration::from_millis(2100)).await;
            if route == "forwarded" {
                // Isolate the receiver's expired admission-cache window from
                // the sender's independent 15-second background refresh.
                transport.routes().invalidate(&target, owner_session);
                transport.routes().route(&target).await.unwrap();
            }
            counted.reset();
            hops.store(0, Ordering::Relaxed);
            let results = futures_util::future::join_all((0..16).map(|_| async {
                let start = Instant::now();
                assert_eq!(
                    query_client
                        .query::<Read>(&target, None, ())
                        .await
                        .unwrap()
                        .output,
                    expected
                );
                start.elapsed()
            }))
            .await;
            samples.extend(results);
            reads += counted.counts().body_requests();
            puts += counted.put_requests();
            peer_hops += hops.load(Ordering::Relaxed);
        }
        let elapsed = burst_window.elapsed();
        report(
            &format!("{route}_query_expired_bursts"),
            16,
            &mut samples,
            elapsed,
            reads,
            puts,
            peer_hops,
        );
        let target = &target;
        for concurrency in [1, 16] {
            counted.reset();
            hops.store(0, Ordering::Relaxed);
            let started = Instant::now();
            let mut samples = Vec::new();
            let mut sequences = HashSet::new();
            for batch in 0..commands / concurrency {
                let results =
                    futures_util::future::join_all((0..concurrency).map(|slot| async move {
                        let index = batch * concurrency + slot;
                        let mut id = [0_u8; 16];
                        id[0] = if route == "local" { 1 } else { 2 };
                        id[1] = concurrency as u8;
                        id[8..].copy_from_slice(&(index as u64).to_be_bytes());
                        let now = now_ms().unwrap();
                        let start = Instant::now();
                        let committed = client
                            .command::<Increment>(
                                target,
                                cellule_runtime::MutationIdentity {
                                    request_id: RequestId::from_bytes(id),
                                    issued_at_ms: now,
                                    expires_at_ms: now + 300_000,
                                },
                                (),
                            )
                            .await
                            .unwrap();
                        assert_eq!(committed.output, committed.receipt.commit_sequence);
                        (start.elapsed(), committed.receipt)
                    }))
                    .await;
                for (sample, receipt) in results {
                    assert!(sequences.insert(receipt.commit_sequence));
                    samples.push(sample);
                }
            }
            expected += commands as u64;
            assert_eq!(
                client.query::<Read>(target, None, ()).await.unwrap().output,
                expected
            );
            report(
                &format!("{route}_command"),
                concurrency,
                &mut samples,
                started.elapsed(),
                counted.counts().body_requests(),
                counted.put_requests(),
                hops.load(Ordering::Relaxed),
            );
            publications.report(
                &format!("{route}_command"),
                concurrency,
                expected - commands as u64 + 1,
                expected,
            );
        }
    }
    // Paired writes use the same actor, immutable publication and authority
    // gate. Alternate order so provider pauses cannot always favor one route.
    let direct = CellClient::local(registry.clone(), handle.clone());
    let mut ratios = Vec::new();
    for index in 0..commands {
        let mut durations = [Duration::ZERO; 2];
        for lane in if index % 2 == 0 { [0, 1] } else { [1, 0] } {
            let mut id = [0_u8; 16];
            id[0] = 3;
            id[1] = lane;
            id[8..].copy_from_slice(&(index as u64).to_be_bytes());
            let now = now_ms().unwrap();
            let selected = if lane == 0 { &direct } else { &local };
            let started = Instant::now();
            let committed = selected
                .command::<Increment>(
                    &target,
                    cellule_runtime::MutationIdentity {
                        request_id: RequestId::from_bytes(id),
                        issued_at_ms: now,
                        expires_at_ms: now + 300_000,
                    },
                    (),
                )
                .await
                .unwrap();
            expected += 1;
            assert_eq!(committed.output, expected);
            assert_eq!(committed.receipt.commit_sequence, expected);
            durations[lane as usize] = started.elapsed();
        }
        ratios.push(durations[1].as_secs_f64() / durations[0].as_secs_f64());
    }
    ratios.sort_by(f64::total_cmp);
    println!(
        "RUSTFS write_route_control calls={commands} median_runtime_to_direct_ratio={:.6}",
        ratios[commands / 2]
    );
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    ingress.shutdown().await.unwrap();
    let final_control = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(
        final_control.value().root.as_ref().unwrap().commit_sequence,
        expected
    );
    // Authenticate every dependency at origin and reconstruct the pinned root.
    let root = final_control.value().ltx_root().unwrap();
    replica.reachable_objects(&root).await.unwrap();
    let restored = disk.path().join("restored.sqlite");
    let verified = replica.open_root(&root).await.unwrap();
    assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
    let recovery_session = SessionId::from_bytes([82; 16]);
    let recovered = CellRuntime::new(
        SqlWorkerPool::new(1, 8).unwrap(),
        16 << 20,
        recovery_session,
    )
    .unwrap();
    let proof = catalog.lookup(target.cell_id()).await.unwrap().unwrap();
    let recovered_handle = recovered
        .acquire_idle_restored(
            proof,
            replica,
            authority.clone(),
            final_control,
            disk.path().join("reopened.sqlite"),
            cellule_runtime::control::Owner {
                session: recovery_session,
                endpoint: "https://recovery.fixture".into(),
            },
        )
        .await
        .unwrap();
    let bytes = recovered_handle
        .query(8, 8, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(
        i64::from_be_bytes(bytes.try_into().unwrap()) as u64,
        expected
    );
    recovered_handle.drain().await.unwrap();
    recovered.shutdown().await.unwrap();
    stop_heartbeat.send(()).unwrap();
    heartbeat.await.unwrap();
    lease.fence();
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(certificate_dir).unwrap();
    println!(
        "RUSTFS correctness=passed commands={expected} final_sequence={expected} peer=mTLS+signed+authorized"
    );
}
