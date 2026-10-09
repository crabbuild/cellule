//! Private follower process: mTLS, fresh enrollment and canonical fsynced storage.
use super::*;
use axum::{
    Extension, Router,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use bytes::Bytes;
use cellule_peer_http::PeerTlsIdentity;
use cellule_runtime::follower::FollowerStore;
use ed25519_dalek::VerifyingKey;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::TaskTracker;

#[derive(Clone)]
struct Server {
    member: NodeId,
    directory: NodeDirectory,
    tls: Arc<LoadedPeerTls>,
    store: FollowerStore,
    lease: NodeLeaseGuard,
    jobs: TaskTracker,
    metrics: Arc<QueryMetrics>,
}

pub(super) async fn serve(
    config: &Config,
    layout: cellule_ltx::CellStorageLayout,
    code: Digest,
    bind: std::net::SocketAddr,
    metrics: Arc<QueryMetrics>,
) -> Result<()> {
    let tls = config.tls()?;
    let directory = NodeDirectory::new(layout, tls.fleet(), code, code);
    let session = SessionId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
    let store = FollowerStore::open(
        config.root.join(format!("data-{}", config.index)),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(1 << 30),
    )?
    .with_telemetry(
        cellule_runtime::fleet::telemetry::CellTelemetryHandle::from_sink(metrics.clone()),
    );
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(transport_error)?;
    let endpoint = format!(
        "https://{}",
        listener.local_addr().map_err(transport_error)?
    );
    let enrollment = authority::Enrollment::start(
        directory.clone(),
        tls.clone(),
        config.index,
        session,
        endpoint.clone(),
        code,
        Some(store.clone()),
    )
    .await?;
    let jobs = TaskTracker::new();
    let state = Server {
        member: node(config.index),
        directory,
        tls: tls.clone(),
        store,
        lease: enrollment.authority.lease.clone(),
        jobs: jobs.clone(),
        metrics: metrics.clone(),
    };
    // Reserve the body/decoding lifetime before Axum buffers request bytes.
    // The follower has one ordered append lane; overload never allocates another body.
    let admission = Arc::new(Semaphore::new(1));
    let router = Router::new()
        .route(wire::PATH, post(handle))
        .layer(DefaultBodyLimit::max(wire::MAX_REQUEST_BYTES))
        .layer(middleware::from_fn(
            move |mut request: axum::extract::Request, next: middleware::Next| {
                let admission = admission.clone();
                async move {
                    let Ok(permit) = admission.try_acquire_owned() else {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    };
                    request.extensions_mut().insert(Arc::new(permit));
                    next.run(request).await
                }
            },
        ))
        .route(
            "/debug/metrics",
            get(|State(server): State<Server>| async move {
                axum::Json(server.metrics.window_snapshot())
            }),
        )
        .with_state(state);
    println!(
        "Follower service: {}; member: {:?}; session: {:?}",
        endpoint,
        node(config.index),
        session
    );
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
    let served = axum::serve(
        tls.listener(listener),
        router.into_make_service_with_connect_info::<PeerTlsIdentity>(),
    )
    .with_graceful_shutdown(async move {
        let _ = signal_tx.send(tokio::signal::ctrl_c().await);
    })
    .await;
    jobs.close();
    jobs.wait().await;
    println!("Follower metrics before drain: {}", metrics.snapshot());
    let stopped = enrollment.stop().await;
    println!("Follower metrics: {}", metrics.snapshot());
    served.map_err(transport_error)?;
    signal_rx
        .await
        .map_err(transport_error)?
        .map_err(transport_error)?;
    stopped
}

use axum::response::IntoResponse;

async fn handle(
    State(server): State<Server>,
    ConnectInfo(peer): ConnectInfo<PeerTlsIdentity>,
    Extension(permit): Extension<Arc<OwnedSemaphorePermit>>,
    encoded: Bytes,
) -> std::result::Result<Vec<u8>, StatusCode> {
    let jobs = server.jobs.clone();
    // A disconnected HTTP waiter cannot drop accepted native work or its budget.
    let result = jobs
        .spawn(async move {
            let _permit = permit;
            handle_inner(server, peer, encoded).await
        })
        .await
        .map_err(Error::FollowerWorkerJoin)
        .and_then(|result| result);
    result.map_err(|error| {
        eprintln!("Follower request failed: {error:?}");
        StatusCode::SERVICE_UNAVAILABLE
    })
}

async fn handle_inner(server: Server, peer: PeerTlsIdentity, encoded: Bytes) -> Result<Vec<u8>> {
    server.lease.check()?;
    let phase = std::time::Instant::now();
    let key = VerifyingKey::from_bytes(&peer.public_key()).map_err(Error::PeerSignature)?;
    let body = wire::verify(&encoded, &key, wire::REQUEST_DOMAIN)?;
    let request = wire::Request::decode(body.as_slice())?;
    server
        .metrics
        .peer_phase(PeerPhase::RequestVerify, phase.elapsed());
    let started_at = std::time::Instant::now();
    let started_ms = clock()?;
    validate_request(&request, started_ms, "before directory verification")?;
    let sender = SessionId::try_from(request.sender.as_slice())?;
    let leader = SessionId::try_from(request.leader.as_slice())?;
    if request.member != server.member.as_bytes() {
        return Err(Error::PeerAuthorization("capacity log member differs"));
    }
    let enrolled = server
        .metrics
        .enrollment(
            true,
            server
                .directory
                .peer_verifier(sender, peer.certificate(), peer.public_key(), clock()?),
        )
        .await;
    let enrolled = enrolled?;
    // Directory I/O consumed time. Preserve the accepted horizon across wall
    // clock rollback, while monotonic elapsed time still expires the request.
    let now = wire::request_time(started_ms, clock()?, started_at.elapsed())?;
    validate_request(&request, now, "after directory verification")?;
    server.lease.check()?;
    let mut reply = wire::Reply {
        member: server.member.as_bytes().to_vec(),
        request_digest: blake3::hash(&body).as_bytes().to_vec(),
        ..Default::default()
    };
    match request.operation {
        1 => {
            if sender != leader {
                return Err(Error::PeerAuthorization("capacity append sender differs"));
            }
            enrolled.authorize_log_append(
                server.member,
                request.epoch,
                request.covered_through,
                clock()?,
            )?;
            let phase = std::time::Instant::now();
            let result = server
                .store
                .append(
                    leader,
                    request.epoch,
                    request.frames.into_iter().map(Bytes::from).collect(),
                    request.covered_through,
                )
                .await;
            server
                .metrics
                .peer_phase(PeerPhase::DurableAppend, phase.elapsed());
            let receipt = result?;
            reply.base_sequence = receipt.base_sequence;
            reply.durable_through = receipt.durable_through;
        }
        3 => {
            if sender != leader {
                return Err(Error::PeerAuthorization("capacity retire sender differs"));
            }
            server
                .directory
                .authorize_log_retire(
                    leader,
                    server.member,
                    request.epoch,
                    request.covered_through,
                    clock()?,
                )
                .await?;
            wire::validate(&request, clock()?)?;
            server.lease.check()?;
            let receipt = server
                .store
                .retire(leader, request.epoch, request.covered_through)
                .await?;
            reply.base_sequence = receipt.base_sequence;
            reply.durable_through = receipt.durable_through;
        }
        2 | 4 => {
            server
                .directory
                .authorize_log_recovery(leader, sender, server.member, request.epoch, clock()?)
                .await?;
            wire::validate(&request, clock()?)?;
            server.lease.check()?;
            if request.operation == 2 {
                let receipt = server.store.seal(leader, request.epoch).await?;
                reply.base_sequence = receipt.base_sequence;
                reply.durable_through = receipt.durable_through;
            } else {
                let page = server
                    .store
                    .read_tail_page(leader, request.epoch, request.first_sequence)
                    .await?;
                reply.frames = page
                    .frames
                    .into_iter()
                    .map(|frame| frame.to_vec())
                    .collect();
                reply.next_sequence = page.next_sequence;
            }
        }
        _ => return Err(Error::PeerAuthorization("capacity operation differs")),
    }
    server.lease.check()?;
    let reply = wire::sign(
        reply.encode_to_vec(),
        server.tls.signing_key(),
        wire::RESPONSE_DOMAIN,
    );
    if reply.len() > wire::MAX_RESPONSE_BYTES {
        return Err(Error::Capacity("capacity follower response bytes"));
    }
    Ok(reply)
}

fn validate_request(request: &wire::Request, now: i64, phase: &str) -> Result<()> {
    wire::validate(request, now).inspect_err(|error| {
        // Keep rejected-envelope diagnostics bounded; never log frame payloads,
        // signatures or credentials. Both checks retain the signed deadline.
        eprintln!(
            "Follower request validation failed: {error:?}; phase={phase}; now_ms={now}; deadline_ms={}; operation={}; frames={}; sender_bytes={}; member_bytes={}; leader_bytes={}; epoch={}",
            request.deadline_ms,
            request.operation,
            request.frames.len(),
            request.sender.len(),
            request.member.len(),
            request.leader.len(),
            request.epoch,
        );
    })
}
