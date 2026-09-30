//! Routing reuse: a local route and description cache, resident peer
//! resolution, and the latency that repetition removes. Each test asserts the
//! provider reads a repeated invocation avoids, so a regression that
//! reintroduces them fails here.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, RouteCacheOutcome};
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
}

impl CellTelemetry for RouteCounters {
    fn route_cache(&self, outcome: RouteCacheOutcome) {
        let counter = match outcome {
            RouteCacheOutcome::Hit => &self.hits,
            RouteCacheOutcome::Miss => &self.misses,
        };
        counter.fetch_add(1, Ordering::AcqRel);
    }
}

async fn counted_fixture() -> (Fixture, Arc<CountingObjectStore>) {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_store(Limits::default(), Store::new(counted.clone())).await;
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
async fn repeated_local_query_reads_no_cell_metadata() {
    let (fixture, counted) = counted_fixture().await;
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
    // The cold invocation reads catalog and control once for the route and once
    // for Describe; the query leg then reuses the cached route.
    assert_eq!(counters.misses.load(Ordering::Acquire), 1);
    assert_eq!(counters.hits.load(Ordering::Acquire), 1);
    assert!(counted.counts().body_requests() > 0);

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
    assert_eq!(counters.misses.load(Ordering::Acquire), 1);
    assert_eq!(counters.hits.load(Ordering::Acquire), 2);

    fixture.handle().drain().await.unwrap();
}

#[tokio::test]
async fn cached_route_never_serves_a_released_cell() {
    let (fixture, _counted) = counted_fixture().await;
    let client = local_client(&fixture);
    client
        .query::<CountComments>(&fixture.target, None, ())
        .await
        .unwrap();

    fixture.handle().drain().await.unwrap();

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
/// A cold invocation reads catalog pages and control to resolve the route and
/// Describe. A repeated invocation must resolve both from memory, so this test
/// fails if the metadata reads return to the request path.
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

    let mut samples = Vec::with_capacity(20);
    for _ in 0..20 {
        counted.reset();
        let started = Instant::now();
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap();
        samples.push(started.elapsed());
        assert_eq!(counted.counts().body_requests(), 0);
    }
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95) / 100];
    println!(
        "cold={cold:?} ({cold_reads} provider reads) | warm p50={p50:?} p95={p95:?} (0 provider reads)"
    );
    assert!(
        p50 * 4 < cold,
        "repeated routing must not pay provider latency: cold={cold:?} warm p50={p50:?}"
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
    let caller = CellRuntime::new(
        SqlWorkerPool::new(1, 4).unwrap(),
        4 * 1024 * 1024,
        SessionId::from_bytes([64; 16]),
    )
    .unwrap();
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

    caller.shutdown().await.unwrap();
    fixture.handle().drain().await.unwrap();
}
