use super::*;
use axum::{Router, body::Body, response::Response, routing::post};
use cellule_runtime::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use cellule_runtime::control::{ControlState, Owner, Transition};
use cellule_runtime::identity::IncarnationId;
use cellule_runtime::identity::{ApplicationId, NamespaceId, NodeId, TenantId};
use cellule_runtime::ltx::CellStorageLayout;
use cellule_runtime::node::{NodeCapacity, NodeFailureDomain};
use cellule_store::Store;
use cellule_store::test_support::CountingObjectStore;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

struct TestClients;

impl PeerHttpClientFactory for TestClients {
    fn client(&self, _: Digest, _: [u8; 32]) -> cellule_runtime::Result<reqwest::Client> {
        // HTTP fixtures exercise the round-trip classification only. Production
        // identity pinning is the responsibility of PeerTlsClient.
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(peer_transport)
    }
}

fn fixture() -> (PeerHttpRoundTrip, CellTarget, NodeAdvertisement) {
    let application = ApplicationId::from_bytes([1; 16]);
    let tenant = TenantId::from_bytes([2; 16]);
    let digest = Digest::from_bytes([3; 32]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("peer-tests"),
        *application.as_bytes(),
    );
    let transport = PeerHttpRoundTrip::new(
        Arc::new(ApplicationIdentity::new(tenant, application)),
        CellAuthority::new(layout.clone()),
        NodeDirectory::new(layout, digest, digest, digest),
        Arc::new(TestClients),
        SessionId::from_bytes([4; 16]),
    );
    let target =
        CellTarget::new(tenant, application, NamespaceId::from_bytes([5; 16]), b"p").unwrap();
    let now = now_ms().unwrap();
    let node = NodeAdvertisement::sign(
        NodeId::from_bytes([6; 16]),
        SessionId::from_bytes([7; 16]),
        "https://peer.test:443".into(),
        digest,
        digest,
        digest,
        digest,
        &SigningKey::from_bytes(&[8; 32]),
        1,
        now,
        now + 10_000,
        vec![digest],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )
    .unwrap();
    (transport, target, node)
}

#[tokio::test]
async fn both_routes_reject_oversized_requests_before_dispatch() {
    let (transport, target, node) = fixture();
    for direct in [false, true] {
        let request = vec![0; cellule_runtime::peer::MAX_PEER_REQUEST_BYTES + 1];
        let result = if direct {
            transport
                .send_to_node(target.clone(), node.clone(), request, 1_000)
                .await
        } else {
            transport.send(target.clone(), request, 1_000).await
        };
        assert!(
            matches!(
                result,
                Err(CellError::Peer("request exceeds peer byte limit"))
            ),
            "direct={direct}"
        );
    }
}

#[tokio::test]
async fn both_routes_reject_exhausted_deadlines_before_dispatch() {
    let (transport, target, node) = fixture();
    for direct in [false, true] {
        let result = if direct {
            transport
                .send_to_node(target.clone(), node.clone(), vec![], 0)
                .await
        } else {
            transport.send(target.clone(), vec![], 0).await
        };
        assert!(
            matches!(result, Err(CellError::Deadline)),
            "direct={direct}"
        );
    }
}

async fn http_attempt(
    status: StatusCode,
    delay: Option<&'static str>,
) -> cellule_runtime::Result<PeerHttpAttempt> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(move || async move {
            let mut response = Response::builder().status(status);
            if let Some(delay) = delay {
                response = response.header(header::RETRY_AFTER, delay);
            }
            response.body(Body::from("fixture response")).unwrap()
        }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let (transport, _, _) = fixture();
    let peer = RemotePeer {
        session: SessionId::from_bytes([9; 16]),
        endpoint: format!("http://{address}/").parse().unwrap(),
        certificate: Digest::from_bytes([10; 32]),
        public_key: [11; 32],
    };
    let result = transport.send_once(&peer, vec![1], 5_000).await;
    stop.send(()).unwrap();
    server.await.unwrap();
    result
}

#[tokio::test]
async fn admission_responses_preserve_retry_delay() {
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        assert!(matches!(
            http_attempt(status, Some("2")).await.unwrap(),
            PeerHttpAttempt::Retry(CellError::Capacity(_), delay) if delay == Duration::from_secs(2)
        ));
    }
}

#[tokio::test]
async fn server_failure_or_invalid_success_remains_unknown() {
    for status in [StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK] {
        assert!(matches!(
            http_attempt(status, None).await.unwrap(),
            PeerHttpAttempt::Unknown(_)
        ));
    }
}

#[tokio::test]
async fn authentication_refusal_is_not_an_owner_retry() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        assert!(matches!(
            http_attempt(status, None).await,
            Err(CellError::PeerAuthorization(_))
        ));
    }
}

async fn owner_lookup_fixture() -> (PeerHttpRoundTrip, CellTarget, Arc<CountingObjectStore>) {
    owner_lookup_fixture_with_endpoint(
        "https://owner.example:443".into(),
        Digest::from_bytes([23; 32]),
        Digest::from_bytes([23; 32]),
        SigningKey::from_bytes(&[27; 32]),
        Arc::new(TestClients),
        Arc::new(InMemory::new()),
    )
    .await
}

async fn owner_lookup_fixture_with_endpoint(
    endpoint: String,
    certificate: Digest,
    fleet: Digest,
    signing_key: SigningKey,
    clients: Arc<dyn PeerHttpClientFactory>,
    backend: Arc<dyn object_store::ObjectStore>,
) -> (PeerHttpRoundTrip, CellTarget, Arc<CountingObjectStore>) {
    let application = ApplicationId::from_bytes([21; 16]);
    let tenant = TenantId::from_bytes([22; 16]);
    let digest = Digest::from_bytes([23; 32]);
    let counted = Arc::new(CountingObjectStore::new(backend));
    let layout = CellStorageLayout::new(
        Store::new(counted.clone()),
        Path::from("owner-lookup-performance"),
        *application.as_bytes(),
    );
    let target = CellTarget::new(
        tenant,
        application,
        NamespaceId::from_bytes([24; 16]),
        b"partition",
    )
    .unwrap();
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let proof = catalog
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, digest, 1).unwrap())
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let directory = NodeDirectory::new(layout, fleet, digest, digest);
    let now = now_ms().unwrap();
    let advertisement = NodeAdvertisement::sign(
        NodeId::from_bytes([25; 16]),
        SessionId::from_bytes([26; 16]),
        endpoint,
        fleet,
        certificate,
        digest,
        digest,
        &signing_key,
        1,
        now,
        now + 30_000,
        vec![digest],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )
    .unwrap();
    directory.create(advertisement.clone(), now).await.unwrap();
    authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([28; 16]),
            Owner {
                session: advertisement.session(),
                endpoint: advertisement.endpoint().to_owned(),
            },
        )
        .await
        .unwrap();
    counted.reset();
    let transport = PeerHttpRoundTrip::new(
        Arc::new(ApplicationIdentity::new(tenant, application)),
        authority,
        directory,
        clients,
        SessionId::from_bytes([29; 16]),
    );
    (transport, target, counted)
}

#[tokio::test]
async fn concurrent_cold_routes_share_one_authority_and_enrollment_lookup() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let backend = ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(10),
            ..ThrottleConfig::default()
        },
    );
    let (transport, target, counted) = owner_lookup_fixture_with_endpoint(
        "https://owner.example:443".into(),
        Digest::from_bytes([23; 32]),
        Digest::from_bytes([23; 32]),
        SigningKey::from_bytes(&[27; 32]),
        Arc::new(TestClients),
        Arc::new(backend),
    )
    .await;
    let table = transport.routes();
    for _ in 0..2 {
        counted.reset();
        let routes = futures_util::future::join_all((0..16).map(|_| table.route(&target))).await;
        assert!(
            routes
                .into_iter()
                .all(|route| matches!(route.unwrap(), RouteDecision::Remote(_)))
        );
        assert_eq!(counted.counts().body_requests(), 2);
        table.invalidate(&target, SessionId::from_bytes([26; 16]));
    }
    // Cancelling a provider read releases the per-Cell gate immediately.
    counted.reset();
    assert!(
        tokio::time::timeout(Duration::from_millis(5), table.route(&target))
            .await
            .is_err()
    );
    assert!(matches!(
        table.route(&target).await.unwrap(),
        RouteDecision::Remote(_)
    ));
    assert_eq!(counted.counts().body_requests(), 3);
}

#[tokio::test]
async fn routing_table_shares_owner_hints_with_the_round_trip() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let table = transport.routes();
    assert!(matches!(
        table.route(&target).await.unwrap(),
        RouteDecision::Remote(_)
    ));
    assert_eq!(counted.counts().body_requests(), 2);
    counted.reset();
    // The forwarding path reuses the route the table already resolved.
    assert_eq!(
        transport.owner(&target).await.unwrap().session,
        SessionId::from_bytes([26; 16])
    );
    assert_eq!(counted.counts().body_requests(), 0);
    // A refusal drops the hint for both readers.
    table.invalidate(&target, SessionId::from_bytes([26; 16]));
    counted.reset();
    assert!(matches!(
        table.route(&target).await.unwrap(),
        RouteDecision::Remote(_)
    ));
    assert!(counted.counts().body_requests() > 0);
}

#[tokio::test]
async fn routing_table_reports_remote_routes_with_their_lease() {
    let (transport, target, _counted) = owner_lookup_fixture().await;
    let table = transport.routes();
    let RouteDecision::Remote(route) = table.route(&target).await.unwrap() else {
        panic!("fixture owner must be a remote route");
    };
    assert_eq!(route.session(), SessionId::from_bytes([26; 16]));
    assert_eq!(route.endpoint().host_str(), Some("owner.example"));
    assert_eq!(route.certificate(), Digest::from_bytes([23; 32]));
    assert!(route.is_live_at(now_ms().unwrap()));
}

#[tokio::test]
async fn routing_table_reports_local_and_unowned_cells() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let table = transport.routes();

    let observed = transport
        .hints
        .authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let local = observed
        .value()
        .takeover(Owner {
            session: transport.session,
            endpoint: "https://self.internal:8081".into(),
        })
        .unwrap();
    transport
        .hints
        .authority
        .transition(&observed, local, Transition::Takeover)
        .await
        .unwrap();
    counted.reset();
    // Ownership by this session is a transition, so it rests on an exact
    // control observation rather than a hint.
    assert!(matches!(
        table.route(&target).await.unwrap(),
        RouteDecision::Local
    ));
    assert_eq!(counted.counts().body_requests(), 1);

    let observed = transport
        .hints
        .authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let mut tombstoned = observed.value().clone();
    tombstoned.epoch += 1;
    tombstoned.revision += 1;
    tombstoned.progress += 1;
    tombstoned.state = ControlState::Tombstoned;
    tombstoned.owner = None;
    transport
        .hints
        .authority
        .transition(&observed, tombstoned, Transition::Tombstone)
        .await
        .unwrap();
    counted.reset();
    assert!(matches!(
        table.route(&target).await.unwrap(),
        RouteDecision::Unowned
    ));
    assert_eq!(counted.counts().body_requests(), 1);
}

#[tokio::test]
async fn exact_owner_lookup_reads_control_and_signed_session() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let owner = transport.owner(&target).await.unwrap();
    assert_eq!(owner.session, SessionId::from_bytes([26; 16]));
    assert_eq!(counted.counts().body_requests(), 2);
    transport.owner(&target).await.unwrap();
    assert_eq!(counted.counts().body_requests(), 2);
}

#[tokio::test]
async fn invalidated_hint_resolves_a_signed_owner_after_authority_takeover() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let old = transport.owner(&target).await.unwrap();
    let now = now_ms().unwrap();
    let successor = NodeAdvertisement::sign(
        NodeId::from_bytes([35; 16]),
        SessionId::from_bytes([36; 16]),
        "https://successor.example:443".into(),
        Digest::from_bytes([23; 32]),
        Digest::from_bytes([23; 32]),
        Digest::from_bytes([23; 32]),
        Digest::from_bytes([23; 32]),
        &SigningKey::from_bytes(&[37; 32]),
        1,
        now,
        now + 30_000,
        vec![Digest::from_bytes([23; 32])],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )
    .unwrap();
    transport
        .hints
        .directory
        .create(successor.clone(), now)
        .await
        .unwrap();
    let observed = transport
        .hints
        .authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let next = observed
        .value()
        .takeover(Owner {
            session: successor.session(),
            endpoint: successor.endpoint().to_owned(),
        })
        .unwrap();
    transport
        .hints
        .authority
        .transition(&observed, next, Transition::Takeover)
        .await
        .unwrap();
    counted.reset();
    assert_eq!(transport.owner(&target).await.unwrap().session, old.session);
    assert_eq!(counted.counts().body_requests(), 0);
    transport
        .hints
        .invalidate_owner(target.cell_id(), old.session);
    assert_eq!(
        transport.owner(&target).await.unwrap().session,
        successor.session()
    );
    assert_eq!(counted.counts().body_requests(), 2);
}

#[tokio::test]
async fn retired_owner_session_fails_closed_after_hint_invalidation() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let owner = transport.owner(&target).await.unwrap();
    let now = now_ms().unwrap();
    let enrolled = transport
        .hints
        .directory
        .load(owner.session, now)
        .await
        .unwrap()
        .unwrap();
    transport
        .hints
        .directory
        .withdraw(&enrolled, now)
        .await
        .unwrap();
    transport
        .hints
        .invalidate_owner(target.cell_id(), owner.session);
    counted.reset();
    assert!(transport.owner(&target).await.is_err());
    assert_eq!(counted.counts().body_requests(), 2);
}

#[tokio::test]
async fn tombstoned_cell_is_not_routed_from_an_invalidated_hint() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let old = transport.owner(&target).await.unwrap();
    let observed = transport
        .hints
        .authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let mut next = observed.value().clone();
    next.epoch += 1;
    next.revision += 1;
    next.progress += 1;
    next.state = ControlState::Tombstoned;
    next.owner = None;
    transport
        .hints
        .authority
        .transition(&observed, next, Transition::Tombstone)
        .await
        .unwrap();
    transport
        .hints
        .invalidate_owner(target.cell_id(), old.session);
    counted.reset();
    assert!(matches!(
        transport.owner(&target).await,
        Err(CellError::CellNotActive)
    ));
    assert_eq!(counted.counts().body_requests(), 1);
}

#[tokio::test]
async fn owner_hint_is_shared_scoped_and_invalidated_by_session() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let owner = transport.owner(&target).await.unwrap();
    let clone = transport.clone();
    clone.owner(&target).await.unwrap();
    assert_eq!(counted.counts().body_requests(), 2);

    let foreign = CellTarget::new(
        TenantId::from_bytes([31; 16]),
        target.application(),
        target.namespace(),
        target.partition(),
    )
    .unwrap();
    assert!(matches!(
        clone.owner(&foreign).await,
        Err(CellError::PeerAuthorization(_))
    ));
    assert_eq!(counted.counts().body_requests(), 2);

    transport
        .hints
        .invalidate_owner(target.cell_id(), SessionId::from_bytes([32; 16]));
    clone.owner(&target).await.unwrap();
    assert_eq!(counted.counts().body_requests(), 2);
    transport
        .hints
        .invalidate_owner(target.cell_id(), owner.session);
    clone.owner(&target).await.unwrap();
    assert_eq!(counted.counts().body_requests(), 4);
}

#[tokio::test]
async fn delayed_old_lookup_cannot_replace_new_owner_hint() {
    let (transport, target, _) = owner_lookup_fixture().await;
    let old_started = Instant::now();
    let new_started = old_started + Duration::from_millis(1);
    let now = now_ms().unwrap();
    let peer = RemotePeer {
        session: SessionId::from_bytes([33; 16]),
        endpoint: "https://new.example:443".parse().unwrap(),
        certificate: Digest::from_bytes([34; 32]),
        public_key: [35; 32],
    };
    transport.hints.remember_owner(
        target.cell_id(),
        peer.clone(),
        now + 10_000,
        now,
        new_started,
    );
    let old = RemotePeer {
        session: SessionId::from_bytes([36; 16]),
        ..peer
    };
    transport
        .hints
        .remember_owner(target.cell_id(), old, now + 10_000, now, old_started);
    assert_eq!(
        transport.owner(&target).await.unwrap().session,
        peer.session
    );
    transport
        .hints
        .invalidate_owner(target.cell_id(), peer.session);
    transport.hints.remember_owner(
        target.cell_id(),
        RemotePeer {
            session: SessionId::from_bytes([36; 16]),
            endpoint: "https://old.example:443".parse().unwrap(),
            certificate: Digest::from_bytes([34; 32]),
            public_key: [35; 32],
        },
        now + 10_000,
        now,
        old_started,
    );
    assert!(
        transport
            .hints
            .owners
            .lock()
            .unwrap()
            .get(&target.cell_id())
            .unwrap()
            .owner
            .is_none()
    );
}

#[tokio::test]
async fn near_expired_owner_lease_is_not_cached() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let now = now_ms().unwrap();
    let peer = RemotePeer {
        session: SessionId::from_bytes([37; 16]),
        endpoint: "https://old.example:443".parse().unwrap(),
        certificate: Digest::from_bytes([38; 32]),
        public_key: [39; 32],
    };
    transport
        .hints
        .remember_owner(target.cell_id(), peer, now + 500, now, Instant::now());
    assert_eq!(
        transport.owner(&target).await.unwrap().session,
        SessionId::from_bytes([26; 16])
    );
    assert_eq!(counted.counts().body_requests(), 2);
}

#[tokio::test]
async fn stale_owner_hint_is_served_while_one_refresh_runs() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    let owner = transport.owner(&target).await.unwrap();
    transport
        .hints
        .owners
        .lock()
        .unwrap()
        .get_mut(&target.cell_id())
        .unwrap()
        .expires_at = Instant::now() - Duration::from_millis(1);
    // The hint is past its refresh window but still inside the signed lease.
    let stale = transport
        .hints
        .cached_owner(target.cell_id(), now_ms().unwrap())
        .expect("the served hint must stay cached");
    assert!(!stale.fresh, "the hint must be past its refresh window");
    counted.reset();
    let calls = (0..5).map(|_| {
        let transport = transport.clone();
        let target = target.clone();
        async move { transport.owner(&target).await.unwrap() }
    });
    let owners = futures_util::future::join_all(calls).await;
    assert!(
        owners
            .iter()
            .all(|owner| owner.session == owners[0].session)
    );
    assert_eq!(owners[0].session, owner.session);
    // One background refresh revalidates the hint; concurrent callers share it
    // instead of each paying a synchronous control-and-session lookup.
    let mut refreshed = false;
    for _ in 0..200 {
        if transport
            .hints
            .cached_owner(target.cell_id(), now_ms().unwrap())
            .is_some_and(|route| route.fresh)
        {
            refreshed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(refreshed, "background refresh did not revalidate the hint");
    assert_eq!(counted.counts().body_requests(), 1);
}

#[tokio::test]
async fn background_refresh_reuses_the_enrolled_session_record() {
    let (transport, target, counted) = owner_lookup_fixture().await;
    transport.owner(&target).await.unwrap();
    assert_eq!(counted.counts().body_requests(), 2);
    counted.reset();
    // A hint refresh reuses the enrolled record and reads only control.
    let reused = transport
        .hints
        .lookup_owner(
            transport.scope.as_ref(),
            transport.session,
            &target,
            SessionReuse::Reuse,
        )
        .await
        .unwrap();
    assert!(matches!(reused, OwnerLookup::Remote(_, _)));
    assert_eq!(counted.counts().body_requests(), 1);
    counted.reset();
    // A synchronous lookup still proves enrollment from the signed record.
    let verified = transport
        .hints
        .lookup_owner(
            transport.scope.as_ref(),
            transport.session,
            &target,
            SessionReuse::Verify,
        )
        .await
        .unwrap();
    assert!(matches!(verified, OwnerLookup::Remote(_, _)));
    assert_eq!(counted.counts().body_requests(), 2);
}

#[tokio::test]
async fn owner_hint_cache_stays_bounded() {
    let (transport, target, _) = owner_lookup_fixture().await;
    let owner = transport.owner(&target).await.unwrap();
    let now = now_ms().unwrap();
    for index in 0..MAX_OWNER_HINTS + 32 {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
        transport.hints.remember_owner(
            CellId::from_bytes(bytes),
            owner.clone(),
            now + 10_000,
            now,
            Instant::now(),
        );
    }
    assert_eq!(
        transport.hints.owners.lock().unwrap().len(),
        MAX_OWNER_HINTS
    );
}

#[tokio::test]
async fn ambiguous_peer_response_does_not_retry_cached_route() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let hits = Arc::new(AtomicUsize::new(0));
    let server_hits = Arc::clone(&hits);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(move || {
            server_hits.fetch_add(1, Ordering::Relaxed);
            async {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("bad"))
                    .unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (transport, target, counted) = owner_lookup_fixture().await;
    let now = now_ms().unwrap();
    let peer = RemotePeer {
        session: SessionId::from_bytes([26; 16]),
        endpoint: format!("http://{address}/").parse().unwrap(),
        certificate: Digest::from_bytes([23; 32]),
        public_key: [27; 32],
    };
    transport
        .hints
        .remember_owner(target.cell_id(), peer, now + 10_000, now, Instant::now());
    assert!(matches!(
        transport.send_inner(target.clone(), vec![1], 1_000).await,
        Err(CellError::PeerTransportUnknown { .. })
    ));
    assert_eq!(hits.load(Ordering::Relaxed), 1);
    assert_eq!(counted.counts().body_requests(), 0);
    assert!(
        transport
            .hints
            .owners
            .lock()
            .unwrap()
            .get(&target.cell_id())
            .unwrap()
            .owner
            .is_none()
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_send_releases_owner_hint_for_next_request() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move || {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                async move {
                    started.notify_one();
                    release.notified().await;
                    Response::new(Body::empty())
                }
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (transport, target, counted) = owner_lookup_fixture().await;
    let now = now_ms().unwrap();
    let owner = RemotePeer {
        session: SessionId::from_bytes([26; 16]),
        endpoint: format!("http://{address}/").parse().unwrap(),
        certificate: Digest::from_bytes([23; 32]),
        public_key: [27; 32],
    };
    transport
        .hints
        .remember_owner(target.cell_id(), owner, now + 10_000, now, Instant::now());
    let sender = transport.clone();
    let request_target = target.clone();
    let request =
        tokio::spawn(async move { sender.send_inner(request_target, vec![1], 5_000).await });
    started.notified().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(
        transport.owner(&target).await.unwrap().session,
        SessionId::from_bytes([26; 16])
    );
    assert_eq!(counted.counts().body_requests(), 0);
    release.notify_one();
    server.abort();
}

#[tokio::test]
async fn not_started_refusal_refreshes_authority_without_resending_to_stale_peer() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let hits = Arc::new(AtomicUsize::new(0));
    let server_hits = Arc::clone(&hits);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(move || {
            server_hits.fetch_add(1, Ordering::Relaxed);
            async {
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::empty())
                    .unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (transport, target, counted) = owner_lookup_fixture().await;
    let now = now_ms().unwrap();
    let peer = RemotePeer {
        session: SessionId::from_bytes([26; 16]),
        endpoint: format!("http://{address}/").parse().unwrap(),
        certificate: Digest::from_bytes([23; 32]),
        public_key: [27; 32],
    };
    transport
        .hints
        .remember_owner(target.cell_id(), peer, now + 10_000, now, Instant::now());
    assert!(
        transport
            .send_inner(target.clone(), vec![1], 1_000)
            .await
            .is_err()
    );
    assert_eq!(hits.load(Ordering::Relaxed), 1);
    assert_eq!(counted.counts().body_requests(), 2);
    server.abort();
}

/// Returns the first OpenSSL 3 binary that can create Ed25519 keys.
fn peer_openssl() -> Option<String> {
    let mut candidates = Vec::new();
    if let Ok(configured) = std::env::var("CELLULE_TEST_OPENSSL") {
        candidates.push(configured);
    }
    candidates.extend(
        ["openssl", "/opt/homebrew/bin/openssl", "openssl3"]
            .into_iter()
            .map(str::to_owned),
    );
    candidates.into_iter().find(|binary| {
        std::process::Command::new(binary)
            .args(["version"])
            .output()
            .map(|output| {
                output.status.success()
                    && !String::from_utf8_lossy(&output.stdout).contains("LibreSSL")
            })
            .unwrap_or(false)
    })
}

/// Generates a CA and one Ed25519 leaf for a local mTLS peer fixture.
///
/// The leaf is valid for localhost client and server authentication under the
/// generated CA, which is the minimum the loader verifies before use.
pub(super) fn generate_peer_identity(label: &str) -> Option<(std::path::PathBuf, LoadedPeerTls)> {
    // macOS ships LibreSSL as `openssl`, which cannot create Ed25519 keys. Try
    // the usual OpenSSL 3 locations before reporting that the fixture is
    // unavailable, so the suite still runs wherever one is installed.
    let binary = peer_openssl()?;
    let certificate_dir = std::env::temp_dir().join(format!(
        "cellule-peer-{label}-{}-{}",
        std::process::id(),
        now_ms().unwrap()
    ));
    std::fs::create_dir_all(&certificate_dir).unwrap();
    let openssl = |args: &[&str]| {
        let output = std::process::Command::new(&binary)
            .args(args)
            .current_dir(&certificate_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };

    openssl(&[
        "req",
        "-x509",
        "-newkey",
        "ed25519",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-subj",
        "/CN=Cellule test CA",
        "-days",
        "1",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    ]);
    openssl(&[
        "req",
        "-new",
        "-newkey",
        "ed25519",
        "-nodes",
        "-keyout",
        "leaf.key",
        "-out",
        "leaf.csr",
        "-subj",
        "/CN=localhost",
    ]);
    std::fs::write(
        certificate_dir.join("leaf.ext"),
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature\n",
    )
    .unwrap();
    openssl(&[
        "x509",
        "-req",
        "-in",
        "leaf.csr",
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        "leaf.crt",
        "-days",
        "1",
        "-extfile",
        "leaf.ext",
    ]);
    let tls = LoadedPeerTls::load(
        &certificate_dir.join("leaf.crt"),
        &certificate_dir.join("leaf.key"),
        &certificate_dir.join("ca.crt"),
        "localhost",
    )
    .unwrap();
    Some((certificate_dir, tls))
}

/// The default listener keeps HTTP/1.1, so an HTTP/1.1-only peer keeps working.
#[tokio::test]
async fn default_listener_negotiates_http_1_1() {
    let Some((certificate_dir, tls)) = generate_peer_identity("alpn-h1") else {
        eprintln!("skipping: no OpenSSL 3 binary available for the peer identity fixture");
        return;
    };
    assert_eq!(
        peer_request_version(tls, false).await,
        http::Version::HTTP_11
    );
    std::fs::remove_dir_all(certificate_dir).unwrap();
}

/// Advertising h2 negotiates multiplexed HTTP/2 end to end over pinned mTLS.
#[tokio::test]
async fn http2_listener_negotiates_http_2() {
    let Some((certificate_dir, tls)) = generate_peer_identity("alpn-h2") else {
        eprintln!("skipping: no OpenSSL 3 binary available for the peer identity fixture");
        return;
    };
    assert_eq!(peer_request_version(tls, true).await, http::Version::HTTP_2);
    std::fs::remove_dir_all(certificate_dir).unwrap();
}

/// Serves one pinned-mTLS request and returns the protocol the peer spoke.
async fn peer_request_version(tls: LoadedPeerTls, http2: bool) -> http::Version {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("ok"))
                .unwrap()
        }),
    );
    let client = tls
        .client_identity()
        .client(
            tls.certificate(),
            tls.signing_key().verifying_key().to_bytes(),
        )
        .unwrap();
    let tls = if http2 {
        tls.with_http2().unwrap()
    } else {
        tls
    };
    let acceptor = tls.listener(listener);
    let server = tokio::spawn(async move { axum::serve(acceptor, router).await.unwrap() });
    let response = client
        .post(format!(
            "https://localhost:{}/internal/cells/v1/forward",
            address.port()
        ))
        .header(header::CONTENT_TYPE, PROTOBUF_MEDIA_TYPE)
        .body(vec![1_u8])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let version = response.version();
    server.abort();
    version
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual adapter owner-lookup baseline"]
async fn owner_lookup_performance() {
    use std::time::Instant;

    let mut raw = String::from("lane\tconcurrency\telapsed_us\n");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let Some((certificate_dir, tls)) = generate_peer_identity("bench") else {
        eprintln!("skipping: no OpenSSL 3 binary available for the peer identity fixture");
        return;
    };
    let (transport, target, counted) = owner_lookup_fixture_with_endpoint(
        format!("https://localhost:{}/", address.port()),
        tls.certificate(),
        tls.fleet(),
        tls.signing_key().clone(),
        Arc::new(tls.client_identity()),
        Arc::new(InMemory::new()),
    )
    .await;
    let response = cellule_runtime::peer::encode_peer_reply(&peer_wire::PeerReply {
        outcome: Some(peer_wire::peer_reply::Outcome::Read(peer_wire::ReadReply {
            receipt: None,
            result: Some(peer_wire::read_reply::Result::Description(
                peer_wire::CellDescription {
                    cell_id: target.cell_id().as_bytes().to_vec(),
                    incarnation: vec![28; 16],
                    code: vec![23; 32],
                    schema: 1,
                },
            )),
        })),
    })
    .unwrap();
    let response = Arc::new(response);
    let router = Router::new().route(
        "/internal/cells/v1/forward",
        post(move || {
            let response = Arc::clone(&response);
            async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, PROTOBUF_MEDIA_TYPE)
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::from(response.as_ref().clone()))
                    .unwrap()
            }
        }),
    );
    let tls_listener = tls.listener(listener);
    let server = tokio::spawn(async move { axum::serve(tls_listener, router).await.unwrap() });
    let peer = Arc::new(RemotePeer {
        session: SessionId::from_bytes([26; 16]),
        endpoint: format!("https://localhost:{}/", address.port())
            .parse()
            .unwrap(),
        certificate: tls.certificate(),
        public_key: tls.signing_key().verifying_key().to_bytes(),
    });
    let cold_before = counted.counts().body_requests();
    let cold_started = Instant::now();
    transport
        .send_inner(target.clone(), vec![1], 5_000)
        .await
        .unwrap();
    let cold_us = cold_started.elapsed().as_micros();
    let cold_reads = counted.counts().body_requests() - cold_before;
    raw.push_str(&format!("cold_adapter\t1\t{cold_us}\n"));
    println!("PERF cold_adapter elapsed_us={cold_us} reads={cold_reads}");
    for concurrency in [1_usize, 16] {
        let before = counted.counts().body_requests();
        let started = Instant::now();
        let mut samples = Vec::with_capacity(1_024);
        for _ in 0..(1_024 / concurrency) {
            let calls = (0..concurrency).map(|_| {
                let transport = transport.clone();
                let target = target.clone();
                async move {
                    let call = Instant::now();
                    transport.owner(&target).await.unwrap();
                    call.elapsed().as_micros()
                }
            });
            samples.extend(futures_util::future::join_all(calls).await);
        }
        let total = started.elapsed();
        let reads = counted.counts().body_requests() - before;
        assert_eq!(samples.len(), 1_024);
        assert!(reads <= 2 * samples.len());
        samples.sort_unstable();
        for sample in &samples {
            raw.push_str(&format!("owner_lookup\t{concurrency}\t{sample}\n"));
        }
        println!(
            "PERF owner_lookup concurrency={concurrency} calls={} elapsed_ms={:.3} throughput_per_s={:.1} p50_us={} p95_us={} p99_us={} reads={reads}",
            samples.len(),
            total.as_secs_f64() * 1_000.0,
            samples.len() as f64 / total.as_secs_f64(),
            samples[samples.len() / 2],
            samples[samples.len() * 95 / 100],
            samples[samples.len() * 99 / 100],
        );
        let started = Instant::now();
        let mut network_samples = Vec::with_capacity(1_024);
        for _ in 0..(1_024 / concurrency) {
            let calls = (0..concurrency).map(|_| {
                let transport = transport.clone();
                let peer = Arc::clone(&peer);
                async move {
                    let call = Instant::now();
                    assert!(matches!(
                        transport.send_once(&peer, vec![1], 5_000).await.unwrap(),
                        PeerHttpAttempt::Reply(_)
                    ));
                    call.elapsed().as_micros()
                }
            });
            network_samples.extend(futures_util::future::join_all(calls).await);
        }
        let network_total = started.elapsed();
        network_samples.sort_unstable();
        for sample in &network_samples {
            raw.push_str(&format!("peer_http\t{concurrency}\t{sample}\n"));
        }
        println!(
            "PERF peer_http concurrency={concurrency} calls={} elapsed_ms={:.3} throughput_per_s={:.1} p50_us={} p95_us={} p99_us={}",
            network_samples.len(),
            network_total.as_secs_f64() * 1_000.0,
            network_samples.len() as f64 / network_total.as_secs_f64(),
            network_samples[network_samples.len() / 2],
            network_samples[network_samples.len() * 95 / 100],
            network_samples[network_samples.len() * 99 / 100],
        );
        // Refresh an expired observation outside the warm lane. Every sample
        // below begins with the same live owner hint in the candidate.
        transport.owner(&target).await.unwrap();
        let reads_before = counted.counts().body_requests();
        let started = Instant::now();
        let mut full_samples = Vec::with_capacity(1_024);
        for _ in 0..(1_024 / concurrency) {
            let calls = (0..concurrency).map(|_| {
                let transport = transport.clone();
                let target = target.clone();
                async move {
                    let call = Instant::now();
                    transport.send_inner(target, vec![1], 5_000).await.unwrap();
                    call.elapsed().as_micros()
                }
            });
            full_samples.extend(futures_util::future::join_all(calls).await);
        }
        let full_total = started.elapsed();
        let reads = counted.counts().body_requests() - reads_before;
        full_samples.sort_unstable();
        assert_eq!(full_samples.len(), 1_024);
        for sample in &full_samples {
            raw.push_str(&format!("full_adapter\t{concurrency}\t{sample}\n"));
        }
        println!(
            "PERF full_adapter concurrency={concurrency} calls={} elapsed_ms={:.3} throughput_per_s={:.1} p50_us={} p95_us={} p99_us={} reads={reads}",
            full_samples.len(),
            full_total.as_secs_f64() * 1_000.0,
            full_samples.len() as f64 / full_total.as_secs_f64(),
            full_samples[full_samples.len() / 2],
            full_samples[full_samples.len() * 95 / 100],
            full_samples[full_samples.len() * 99 / 100],
        );
    }
    server.abort();
    let report = std::env::temp_dir().join(format!(
        "cellule-peer-owner-lookup-{}-{}.tsv",
        std::process::id(),
        now_ms().unwrap()
    ));
    std::fs::write(&report, raw).unwrap();
    println!("PERF raw={}", report.display());
    std::fs::remove_dir_all(certificate_dir).unwrap();
}
