//! Routing reuse: a local route and description cache, resident peer
//! resolution, and the latency that repetition removes. Each test asserts the
//! provider reads a repeated invocation avoids, so a regression that
//! reintroduces them fails here.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, ResidentRouteOutcome, RouteCacheOutcome};
use cellule_runtime::peer::ResidentPeerCellResolver;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Telemetry sink that records only the routing counters under test.
#[derive(Default)]
struct RouteCounters {
    hits: AtomicUsize,
    misses: AtomicUsize,
    resident_lookups: AtomicUsize,
}

impl CellTelemetry for RouteCounters {
    fn resident_route(&self, _: ResidentRouteOutcome) {
        self.resident_lookups.fetch_add(1, Ordering::AcqRel);
    }

    fn route_cache(&self, outcome: RouteCacheOutcome) {
        let counter = match outcome {
            RouteCacheOutcome::Hit => &self.hits,
            RouteCacheOutcome::Miss => &self.misses,
        };
        counter.fetch_add(1, Ordering::AcqRel);
    }
}

async fn counted_fixture() -> (Fixture, Arc<CountingObjectStore>) {
    counted_fixture_with_lease(true).await
}

async fn counted_fixture_with_lease(leased: bool) -> (Fixture, Arc<CountingObjectStore>) {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_store_and_lease(
        Limits::default(),
        Store::new(counted.clone()),
        leased.then(|| cellule_runtime::node::lease::NodeLeaseGuard::new(0, 60_000).unwrap()),
    )
    .await;
    (fixture, counted)
}

fn local_client(fixture: &Fixture) -> CellClient {
    CellClient::local_runtime(
        Arc::clone(&fixture.registry),
        fixture.runtime.clone().expect("fixture runtime is present"),
        fixture.layout.clone(),
    )
}

#[tokio::test]
async fn unleased_local_and_peer_routes_reuse_catalog_but_read_fresh_control() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_store(Limits::default(), Store::new(counted.clone())).await;
    let client = local_client(&fixture);
    let control_path = fixture
        .layout
        .control_path(fixture.target.cell_id().as_bytes())
        .to_string();

    counted.reset();
    assert_eq!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap()
            .output,
        0
    );
    assert_eq!(
        counted.counts().body_requests(),
        2,
        "Describe and query each read authority"
    );
    assert!(
        counted
            .requests()
            .iter()
            .all(|read| read.location == control_path)
    );

    let inbound = ResidentPeerCellResolver::new(
        fixture.runtime.clone().unwrap(),
        fixture.layout.clone(),
        Arc::clone(&fixture.registry),
    );
    for _ in 0..2 {
        counted.reset();
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap();
        assert_eq!(counted.counts().body_requests(), 1);
        assert_eq!(counted.requests()[0].location, control_path);

        counted.reset();
        inbound.resolve(fixture.target.clone()).await.unwrap();
        assert_eq!(counted.counts().body_requests(), 1);
        assert_eq!(counted.requests()[0].location, control_path);
    }
    // Reusing catalog identity must never hide an origin outage or reuse the
    // last successful ownership observation across requests.
    let path = fixture
        .layout
        .control_path(fixture.target.cell_id().as_bytes());
    counted.block_body_reads_for(&path);
    // The injected provider failure is transient: retain the Store's normal
    // retry budget, and ensure every attempt still observes only authority.
    let attempts = usize::try_from(cellule_store::RetryPolicy::DEFAULT.max_attempts).unwrap();
    counted.reset();
    assert!(matches!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::Storage(_)
        ))
    ));
    assert_eq!(counted.counts().body_requests(), attempts);
    assert!(
        counted
            .requests()
            .iter()
            .all(|read| read.location == control_path)
    );
    counted.reset();
    assert!(matches!(
        inbound.resolve(fixture.target.clone()).await,
        Err(cellule_runtime::Error::Storage(_))
    ));
    assert_eq!(counted.counts().body_requests(), attempts);
    assert!(
        counted
            .requests()
            .iter()
            .all(|read| read.location == control_path)
    );
    counted.unblock_body_reads_for(&path);
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    fixture.handle().drain().await.unwrap();
    fixture.runtime.as_ref().unwrap().shutdown().await.unwrap();
}

#[tokio::test]
async fn repeated_local_query_reads_no_cell_metadata() {
    let (fixture, counted) = counted_fixture().await;
    counted.reset();
    let counters = Arc::new(RouteCounters::default());
    fixture
        .runtime
        .as_ref()
        .expect("fixture runtime is present")
        .install_telemetry(counters.clone())
        .unwrap();
    let client = local_client(&fixture);

    let first = client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    // Actor admission is the route proof under a live node lease, including
    // the first Describe and query. No periodically refreshed control cache.
    assert_eq!(counters.misses.load(Ordering::Acquire), 0);
    assert_eq!(counters.hits.load(Ordering::Acquire), 2);
    assert_eq!(counted.counts().body_requests(), 0);

    counted.reset();
    let second = client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    assert_eq!(second.output, first.output);
    assert_eq!(
        counted.counts().body_requests(),
        0,
        "a repeated local query must reuse the description and the route"
    );
    assert_eq!(counters.misses.load(Ordering::Acquire), 0);
    assert_eq!(counters.hits.load(Ordering::Acquire), 3);
    assert_eq!(counters.resident_lookups.load(Ordering::Acquire), 1);

    fixture.handle().drain().await.unwrap();
}

#[tokio::test]
async fn cached_route_never_serves_a_released_cell() {
    for leased in [false, true] {
        let (fixture, _counted) = counted_fixture_with_lease(leased).await;
        let client = local_client(&fixture);
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap();

        let runtime = fixture.runtime.as_ref().unwrap();
        let (cell, generation, _, _) = runtime.idle_transfer_candidates().await.unwrap()[0];
        runtime
            .release_idle_cell(cell, fixture.session, generation)
            .await
            .unwrap();

        let refused = client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap_err();
        assert!(matches!(refused, InvocationError::NotStarted(_)));
        // The cached description must not hide the loss on a later attempt either.
        assert!(
            client
                .query::<CountComments>(&fixture.target, None, ())
                .await
                .is_err()
        );
        let observed = fixture
            .authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let runtime = fixture.runtime.as_ref().unwrap();
        let replacement = runtime
            .acquire_idle_restored(
                fixture.proof.clone(),
                fixture.replica.clone(),
                fixture.authority.clone(),
                observed,
                fixture._directory.path().join("replacement.sqlite"),
                Owner {
                    session: fixture.session,
                    endpoint: "https://replacement.example".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query::<CountComments>(&fixture.target, None, ())
                .await
                .unwrap()
                .output,
            0
        );
        replacement.drain().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn resident_peer_resolver_reads_no_metadata_for_a_live_owner() {
    let (fixture, counted) = counted_fixture().await;
    let resolver = ResidentPeerCellResolver::new(
        fixture.runtime.clone().expect("fixture runtime is present"),
        fixture.layout.clone(),
        Arc::clone(&fixture.registry),
    );

    counted.reset();
    let handle = resolver.resolve(fixture.target.clone()).await.unwrap();
    assert_eq!(handle.cell_id(), fixture.target.cell_id());
    assert_eq!(
        counted.counts().body_requests(),
        0,
        "a receiver that already owns the Cell must not read catalog or control"
    );

    // A target this node does not own still falls back to verified storage reads.
    let unknown = CellTarget::new(
        fixture.target.tenant(),
        fixture.target.application(),
        NAMESPACE,
        b"repository-99",
    )
    .unwrap();
    counted.reset();
    assert!(matches!(
        resolver.resolve(unknown).await,
        Err(cellule_runtime::Error::Control(_))
    ));
    assert!(counted.counts().body_requests() > 0);

    fixture.handle().drain().await.unwrap();
}

/// Local latency model: one provider read costs 2 ms, the small-object GET p95
/// recorded by the release capacity run.
///
/// Compare unleased routing, which reads fresh catalog and control, with
/// lease-fenced resident routing over the same throttle. This mode comparison
/// checks the routing mechanism; matched revision measurements use RustFS.
#[tokio::test]
#[ignore = "manual routing latency model over a throttled provider"]
async fn routing_latency_model_over_a_two_millisecond_provider() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(2),
        ..ThrottleConfig::default()
    };
    let provider = Arc::new(ThrottledStore::new(Arc::clone(&counted), config));
    let fixture = fixture_with_store(Limits::default(), Store::new(provider)).await;
    let client = local_client(&fixture);

    counted.reset();
    let started = Instant::now();
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    let cold = started.elapsed();
    let cold_reads = counted.counts().body_requests();

    fixture.handle().drain().await.unwrap();
    let leased_counts = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let leased_provider = Arc::new(ThrottledStore::new(
        leased_counts.clone(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(2),
            ..ThrottleConfig::default()
        },
    ));
    let fixture = fixture_with_store_and_lease(
        Limits::default(),
        Store::new(leased_provider),
        Some(cellule_runtime::node::lease::NodeLeaseGuard::new(0, 60_000).unwrap()),
    )
    .await;
    let client = local_client(&fixture);
    let mut samples = Vec::with_capacity(20);
    for _ in 0..20 {
        leased_counts.reset();
        let started = Instant::now();
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap();
        samples.push(started.elapsed());
        assert_eq!(leased_counts.counts().body_requests(), 0);
    }
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95) / 100];
    println!(
        "unleased={cold:?} ({cold_reads} provider reads) | warm p50={p50:?} p95={p95:?} (0 provider reads)"
    );
    assert!(
        p50 * 4 < cold,
        "leased routing must avoid the unleased authority reads: cold={cold:?} warm p50={p50:?}"
    );

    fixture.handle().drain().await.unwrap();
}

/// Peer round trip that counts forwarded hops before delegating to loopback.
struct CountingRoundTrip {
    inner: Arc<LoopbackRoundTrip>,
    hops: Arc<AtomicUsize>,
}

impl PeerRoundTrip for CountingRoundTrip {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        self.hops.fetch_add(1, Ordering::AcqRel);
        self.inner.send(target, request, remaining_ms)
    }
}

/// A forwarded invocation used to describe and then dispatch: two peer hops.
/// The cached description removes the Describe hop, and the receiver resolves
/// the resident owner without metadata reads.
#[tokio::test]
async fn repeated_forwarded_command_skips_the_describe_hop() {
    let fixture = fixture().await;
    let caller = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 4).unwrap(),
        4 * 1024 * 1024,
        SessionId::from_bytes([64; 16]),
        cellule_runtime::ltx::Host::default(),
    )
    .unwrap();
    let lease = cellule_runtime::node::lease::NodeLeaseGuard::new(0, 60_000).unwrap();
    caller.install_node_lease(lease.clone()).unwrap();
    let counters = Arc::new(RouteCounters::default());
    caller.install_telemetry(counters.clone()).unwrap();
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([65; 16]),
        fixture.registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[66; 32]),
    ));
    let hops = Arc::new(AtomicUsize::new(0));
    let round_trip: Arc<dyn PeerRoundTrip> = Arc::new(CountingRoundTrip {
        inner: Arc::new(LoopbackRoundTrip {
            verifier: Arc::new(PeerVerifier::new(
                SessionId::from_bytes([65; 16]),
                fixture.registry.release_digest(),
                signer.verifying_key(),
            )),
            dispatcher: Arc::new(PeerDispatcher::new(
                Arc::clone(&fixture.registry),
                Arc::new(LocalResolver {
                    target: fixture.target.clone(),
                    handle: fixture.handle().clone(),
                }),
                Arc::new(RepositoryAuthorizer),
            )),
        }),
        hops: Arc::clone(&hops),
    });
    let remote = CellClient::runtime_with_peer(
        Arc::clone(&fixture.registry),
        caller.clone(),
        fixture.layout.clone(),
        Arc::clone(&signer),
        PeerPrincipal {
            issuer: "https://identity.example".into(),
            subject: "alice".into(),
            actions: vec!["repository.issue.create".into()],
        },
        Arc::clone(&round_trip),
    );

    hops.store(0, Ordering::Release);
    remote
        .command::<CreateComment>(&fixture.target, mutation_identity(71), b"first".to_vec())
        .await
        .unwrap();
    assert_eq!(
        hops.load(Ordering::Acquire),
        2,
        "a cold forwarded invocation describes before it dispatches"
    );

    hops.store(0, Ordering::Release);
    remote
        .command::<CreateComment>(&fixture.target, mutation_identity(72), b"second".to_vec())
        .await
        .unwrap();
    assert_eq!(
        hops.load(Ordering::Acquire),
        1,
        "a repeated forwarded invocation must reuse the observed description"
    );

    assert_eq!(
        counters.resident_lookups.load(Ordering::Acquire),
        0,
        "a nonowner must skip resident resolution before forwarding"
    );
    caller.shutdown().await.unwrap();
    lease.fence();
    fixture.handle().drain().await.unwrap();
}

#[tokio::test]
async fn unleased_route_rejects_authority_takeover() {
    let fixture = fixture().await;
    let client = local_client(&fixture);
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let old = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let successor = old
        .value()
        .takeover(Owner {
            session: SessionId::from_bytes([97; 16]),
            endpoint: "https://successor.internal".into(),
        })
        .unwrap();
    authority
        .transition(
            &old,
            successor,
            cellule_runtime::control::Transition::Takeover,
        )
        .await
        .unwrap();
    let runtime = fixture.runtime.as_ref().unwrap();
    let inbound =
        ResidentPeerCellResolver::new(runtime.clone(), fixture.layout.clone(), registry());
    assert!(
        inbound.resolve(fixture.target.clone()).await.is_err(),
        "unleased inbound resolver served after authority takeover"
    );
    let observed = client
        .query::<CountComments>(&fixture.target, None, ())
        .await;
    assert!(
        observed.is_err(),
        "cached owner served after authority takeover: {observed:?}"
    );
    assert!(matches!(
        runtime.shutdown().await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert_eq!(runtime.stats().active_cells(), 0);
}

struct ReplacingPeer {
    verifier: Arc<PeerVerifier>,
    description: CellDescription,
    replaced: Arc<std::sync::atomic::AtomicBool>,
}
impl PeerRoundTrip for ReplacingPeer {
    fn send(
        &self,
        _target: CellTarget,
        request: Vec<u8>,
        _remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let verifier = self.verifier.clone();
        let mut description = self.description;
        if self.replaced.load(Ordering::SeqCst) {
            description.incarnation = IncarnationId::from_bytes([98; 16]);
        }
        Box::pin(async move {
            let now = std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let verified = verifier.verify(&request, now)?;
            let Some(wire::peer_request::Operation::Read(read)) = verified.operation() else {
                panic!("expected a read");
            };
            let outcome = if matches!(
                read.operation,
                Some(wire::read_request::Operation::Describe(_))
            ) {
                wire::peer_reply::Outcome::Read(wire::ReadReply {
                    receipt: None,
                    result: Some(wire::read_reply::Result::Description(
                        wire::CellDescription {
                            cell_id: description.cell.as_bytes().to_vec(),
                            incarnation: description.incarnation.as_bytes().to_vec(),
                            code: description.code.as_bytes().to_vec(),
                            schema: description.schema,
                        },
                    )),
                })
            } else if read.expected.as_ref().unwrap().incarnation
                != description.incarnation.as_bytes()
            {
                wire::peer_reply::Outcome::Error(wire::Error {
                    code: wire::error::Code::Unavailable as i32,
                    outcome: wire::error::Outcome::NotStarted as i32,
                    message: "Cell owner is unavailable".into(),
                    retry_after_ms: 100,
                    application_details: Vec::new(),
                })
            } else {
                wire::peer_reply::Outcome::Read(wire::ReadReply {
                    receipt: Some(wire::Receipt {
                        cell_id: description.cell.as_bytes().to_vec(),
                        incarnation: description.incarnation.as_bytes().to_vec(),
                        commit_sequence: 1,
                    }),
                    result: Some(wire::read_reply::Result::CommandOutput(vec![0u8; 8])),
                })
            };
            cellule_runtime::peer::encode_peer_reply(&wire::PeerReply {
                outcome: Some(outcome),
            })
        })
    }
}
#[tokio::test]
async fn peer_refusal_refreshes_cached_description() {
    let fixture = fixture().await;
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([95; 16]),
        fixture.registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[96; 32]),
    ));
    let replaced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let peer = Arc::new(ReplacingPeer {
        verifier: Arc::new(PeerVerifier::new(
            SessionId::from_bytes([95; 16]),
            fixture.registry.release_digest(),
            signer.verifying_key(),
        )),
        description: CellDescription {
            cell: fixture.target.cell_id(),
            incarnation: fixture.incarnation,
            code: fixture.registry.module_code(MODULE).unwrap(),
            schema: 1,
        },
        replaced: replaced.clone(),
    });
    let client = CellClient::peer(
        fixture.registry.clone(),
        signer,
        PeerPrincipal {
            issuer: "https://identity.example".into(),
            subject: "alice".into(),
            actions: vec!["repository.issue.create".into()],
        },
        peer,
    );
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    replaced.store(true, Ordering::SeqCst);
    assert!(matches!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::CellNotActive
        ))
    ));
    let retried = client
        .query::<CountComments>(&fixture.target, None, ())
        .await;
    assert!(
        retried.is_ok(),
        "next call must re-describe after the peer refuses the old incarnation: {retried:?}"
    );
    fixture.handle().drain().await.unwrap();
}

#[tokio::test]
async fn leased_route_refuses_after_session_fencing() {
    let (fixture, counted) = counted_fixture().await;
    let client = local_client(&fixture);
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();
    fixture.node_lease.as_ref().unwrap().fence();
    counted.reset();
    assert!(matches!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await,
        Err(InvocationError::NotStarted(cellule_runtime::Error::Fenced))
    ));
    assert_eq!(counted.counts().body_requests(), 0);
    let runtime = fixture.runtime.as_ref().unwrap();
    let _ = runtime.shutdown().await;
    assert_eq!(runtime.stats().active_cells(), 0);
}
