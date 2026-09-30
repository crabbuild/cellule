//! Owner-resolving HTTP transport for authenticated Cell peer requests.

mod tls;

pub use tls::{LoadedPeerTls, PeerTlsClient, PeerTlsIdentity, PeerTlsListener, TlsError};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::Error as CellError;
use cellule_runtime::cell::application::ApplicationIdentity;
use cellule_runtime::control::authority::CellAuthority;
use cellule_runtime::identity::{CellId, CellTarget, Digest, SessionId};
use cellule_runtime::node::{NodeAdvertisement, NodeDirectory};
use cellule_runtime::peer::{PeerRoundTrip, wire as peer_wire};
use futures_util::StreamExt;
use http::{StatusCode, header};

const PEER_FORWARD_PATH: &str = "internal/cells/v1/forward";
const MAX_PEER_CLIENTS: usize = 1_024;
const MAX_OWNER_HINTS: usize = 4_096;
const MAX_SESSION_HINTS: usize = 4_096;
/// How long one owner hint is served before a background refresh is wanted.
///
/// The hint is a routing shortcut, never authority: the receiver fences a
/// stale owner and the sender refreshes on the first refusal. The bound stays
/// well inside the signed node lease that caps every hint's hard lifetime.
const OWNER_HINT_TTL: Duration = Duration::from_secs(15);
/// How long a refused owner tombstone blocks an older in-flight lookup.
const OWNER_TOMBSTONE_TTL: Duration = Duration::from_secs(5);
const OWNER_LEASE_MARGIN_MS: i64 = 1_000;
/// Content type accepted by the private peer forwarding endpoint.
pub const PROTOBUF_MEDIA_TYPE: &str = "application/x-protobuf";

/// Builds an mTLS HTTP client pinned to one enrolled peer certificate and key.
pub trait PeerHttpClientFactory: Send + Sync + 'static {
    /// Builds a client that rejects any peer other than the pinned identity.
    fn client(
        &self,
        certificate: Digest,
        public_key: [u8; 32],
    ) -> cellule_runtime::Result<reqwest::Client>;
}

/// Restricts which Cell targets this node may forward through a peer.
pub trait PeerTargetScope: Send + Sync + 'static {
    /// Rejects a target outside the product's routing scope.
    fn check_target(&self, target: &CellTarget) -> cellule_runtime::Result<()>;
}

impl PeerTargetScope for ApplicationIdentity {
    fn check_target(&self, target: &CellTarget) -> cellule_runtime::Result<()> {
        if target.tenant() != self.tenant() || target.application() != self.application() {
            return Err(CellError::PeerAuthorization(
                "Cell target is outside the routed application",
            ));
        }
        Ok(())
    }
}

/// Sends signed Cell requests to the current enrolled owner over HTTP.
pub struct PeerHttpRoundTrip {
    scope: Arc<dyn PeerTargetScope>,
    tls: Arc<dyn PeerHttpClientFactory>,
    session: SessionId,
    clients: Arc<Mutex<VecDeque<CachedPeerClient>>>,
    hints: Arc<OwnerHints>,
}

impl PeerHttpRoundTrip {
    /// Binds one application, authority, fleet directory, and local session.
    #[must_use]
    pub fn new(
        scope: Arc<dyn PeerTargetScope>,
        authority: CellAuthority,
        directory: NodeDirectory,
        tls: Arc<dyn PeerHttpClientFactory>,
        session: SessionId,
    ) -> Self {
        Self {
            scope,
            tls,
            session,
            clients: Arc::new(Mutex::new(VecDeque::new())),
            hints: Arc::new(OwnerHints {
                authority,
                directory,
                owners: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                refreshing: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Returns a routing table that shares this round trip's owner hints.
    ///
    /// An ingress can resolve the owner once and dial it directly; the
    /// forwarding path then reuses the same observations instead of looking
    /// the owner up again.
    #[must_use]
    pub fn routes(&self) -> CellRouteTable {
        CellRouteTable {
            scope: Arc::clone(&self.scope),
            session: self.session,
            hints: Arc::clone(&self.hints),
        }
    }

    async fn send_inner(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        timeout_ms: u32,
    ) -> cellule_runtime::Result<Vec<u8>> {
        validate_request(&request, timeout_ms)?;
        let started = Instant::now();
        let mut last_retry: Option<(CellError, Duration)> = None;
        for _ in 0..2 {
            if let Some((_, delay)) = &last_retry {
                let remaining =
                    Duration::from_millis(u64::from(remaining_timeout(started, timeout_ms)?));
                if *delay >= remaining {
                    return Err(CellError::Deadline);
                }
                if !delay.is_zero() {
                    // Admission rejection has not started the operation. Pace
                    // its one retry, then reload ownership in case it moved.
                    tokio::time::sleep(*delay).await;
                }
            }
            let remaining_ms = remaining_timeout(started, timeout_ms)?;
            let owner =
                tokio::time::timeout(Duration::from_millis(u64::from(remaining_ms)), async {
                    if last_retry.is_some() {
                        self.refresh_owner(&target).await
                    } else {
                        self.owner(&target).await
                    }
                })
                .await
                .map_err(|_| CellError::Deadline)??;
            let remaining_ms = remaining_timeout(started, timeout_ms)?;
            match self.send_once(&owner, request.clone(), remaining_ms).await {
                Ok(PeerHttpAttempt::Reply(reply)) => return Ok(reply),
                Ok(PeerHttpAttempt::Retry(error, delay)) => {
                    self.hints.invalidate_owner(target.cell_id(), owner.session);
                    last_retry = Some((error, delay));
                }
                Ok(PeerHttpAttempt::Unknown(error)) => {
                    self.hints.invalidate_owner(target.cell_id(), owner.session);
                    return Err(CellError::PeerTransportUnknown {
                        context: "peer HTTP response was lost or invalid",
                        source: Box::new(error),
                    });
                }
                Err(error) => {
                    self.hints.invalidate_owner(target.cell_id(), owner.session);
                    return Err(error);
                }
            }
        }
        Err(last_retry.map_or(CellError::CellNotActive, |(error, _)| error))
    }

    async fn send_to_node_inner(
        &self,
        target: CellTarget,
        node: NodeAdvertisement,
        request: Vec<u8>,
        timeout_ms: u32,
    ) -> cellule_runtime::Result<Vec<u8>> {
        validate_request(&request, timeout_ms)?;
        self.scope.check_target(&target)?;
        if node.session() == self.session {
            return Err(CellError::CellNotActive);
        }
        let now_ms = now_ms()?;
        if node.expires_at_ms() <= now_ms || node.endpoint().is_empty() {
            return Err(CellError::CellNotActive);
        }
        let owner = RemotePeer {
            session: node.session(),
            endpoint: url::Url::parse(node.endpoint()).map_err(peer_transport)?,
            certificate: node.certificate(),
            public_key: node.verifying_key()?.to_bytes(),
        };
        match self.send_once(&owner, request, timeout_ms).await? {
            PeerHttpAttempt::Reply(reply) => Ok(reply),
            PeerHttpAttempt::Retry(error, _) => Err(error),
            PeerHttpAttempt::Unknown(error) => Err(CellError::PeerTransportUnknown {
                context: "peer HTTP activation response was lost or invalid",
                source: Box::new(error),
            }),
        }
    }

    async fn owner(&self, target: &CellTarget) -> cellule_runtime::Result<RemotePeer> {
        self.scope.check_target(target)?;
        let now = now_ms()?;
        match self.hints.cached_owner(target.cell_id(), now) {
            Some(route) => {
                if !route.fresh {
                    // The hint is still inside the signed node lease, so it
                    // names an enrolled endpoint. Serve it and refresh in the
                    // background so the next call needs no lookup. A moved
                    // owner refuses this request and the sender refreshes once
                    // inside the deadline.
                    self.hints
                        .spawn_refresh(Arc::clone(&self.scope), self.session, target.clone());
                }
                Ok(route.peer)
            }
            None => self.load_owner(target).await,
        }
    }

    async fn refresh_owner(&self, target: &CellTarget) -> cellule_runtime::Result<RemotePeer> {
        self.scope.check_target(target)?;
        self.load_owner(target).await
    }

    async fn load_owner(&self, target: &CellTarget) -> cellule_runtime::Result<RemotePeer> {
        match self
            .hints
            .lookup_owner(
                self.scope.as_ref(),
                self.session,
                target,
                SessionReuse::Verify,
            )
            .await?
        {
            OwnerLookup::Remote(peer, _) => Ok(peer),
            // A Cell this node owns, has no owner, or whose owner session is
            // not enrolled has no peer route to send through.
            OwnerLookup::Local | OwnerLookup::Unowned => Err(CellError::CellNotActive),
        }
    }

    fn client(&self, owner: &RemotePeer) -> cellule_runtime::Result<reqwest::Client> {
        {
            let clients = self
                .clients
                .lock()
                .map_err(|_| CellError::Peer("peer HTTP client cache is poisoned"))?;
            if let Some(cached) = clients.iter().find(|cached| cached.matches(owner)) {
                return Ok(cached.client.clone());
            }
        }

        let client = self.tls.client(owner.certificate, owner.public_key)?;
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| CellError::Peer("peer HTTP client cache is poisoned"))?;
        if let Some(cached) = clients.iter().find(|cached| cached.matches(owner)) {
            return Ok(cached.client.clone());
        }
        if clients.len() == MAX_PEER_CLIENTS {
            clients.pop_front();
        }
        clients.push_back(CachedPeerClient {
            session: owner.session,
            certificate: owner.certificate,
            public_key: owner.public_key,
            client: client.clone(),
        });
        Ok(client)
    }

    async fn send_once(
        &self,
        owner: &RemotePeer,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> cellule_runtime::Result<PeerHttpAttempt> {
        let client = self.client(owner)?;
        let url = owner
            .endpoint
            .join(PEER_FORWARD_PATH)
            .map_err(peer_transport)?;
        let response = match client
            .post(url)
            .header(header::CONTENT_TYPE, PROTOBUF_MEDIA_TYPE)
            .header(header::CACHE_CONTROL, "no-store")
            .timeout(Duration::from_millis(u64::from(remaining_ms)))
            .body(request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_connect() => {
                return Ok(PeerHttpAttempt::Retry(
                    peer_transport(error),
                    Duration::ZERO,
                ));
            }
            Err(error) => return Ok(PeerHttpAttempt::Unknown(peer_transport(error))),
        };
        match response.status() {
            StatusCode::OK => {}
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                // Receivers also use a bare 503 to refresh stale ownership.
                // Only an explicit delay identifies admission pressure here.
                if let Some(seconds) = response
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    return Ok(PeerHttpAttempt::Retry(
                        CellError::Capacity("peer HTTP admission"),
                        Duration::from_secs(seconds),
                    ));
                }
                return Ok(PeerHttpAttempt::Retry(
                    CellError::CellNotActive,
                    Duration::ZERO,
                ));
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(CellError::PeerAuthorization(
                    "remote node rejected the enrolled peer",
                ));
            }
            status if status.is_server_error() => {
                return Ok(PeerHttpAttempt::Unknown(CellError::Peer(
                    "remote peer returned a server error",
                )));
            }
            _ => return Err(CellError::Peer("remote peer rejected the HTTP request")),
        }
        if response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != Some(PROTOBUF_MEDIA_TYPE)
            || response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok())
                != Some("no-store")
            || response
                .content_length()
                .is_some_and(|length| length > cellule_runtime::peer::MAX_PEER_REQUEST_BYTES as u64)
        {
            return Ok(PeerHttpAttempt::Unknown(CellError::Peer(
                "remote peer response metadata is invalid",
            )));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => return Ok(PeerHttpAttempt::Unknown(peer_transport(error))),
            };
            if body.len().saturating_add(chunk.len())
                > cellule_runtime::peer::MAX_PEER_REQUEST_BYTES
            {
                return Ok(PeerHttpAttempt::Unknown(CellError::Peer(
                    "remote peer response exceeds the byte limit",
                )));
            }
            body.extend_from_slice(&chunk);
        }
        let decoded = match cellule_runtime::peer::decode_peer_reply(&body) {
            Ok(decoded) => decoded,
            Err(error) => return Ok(PeerHttpAttempt::Unknown(error)),
        };
        if matches!(
            decoded.outcome,
            Some(peer_wire::peer_reply::Outcome::Error(ref error))
                if error.code == peer_wire::error::Code::Unavailable as i32
                    && error.outcome == peer_wire::error::Outcome::NotStarted as i32
        ) {
            return Ok(PeerHttpAttempt::Retry(
                CellError::CellNotActive,
                Duration::ZERO,
            ));
        }
        Ok(PeerHttpAttempt::Reply(body))
    }
}

impl PeerRoundTrip for PeerHttpRoundTrip {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let round_trip = self.clone();
        Box::pin(async move { round_trip.send_inner(target, request, remaining_ms).await })
    }

    fn send_to_node(
        &self,
        target: CellTarget,
        node: NodeAdvertisement,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let round_trip = self.clone();
        Box::pin(async move {
            round_trip
                .send_to_node_inner(target, node, request, remaining_ms)
                .await
        })
    }
}

impl Clone for PeerHttpRoundTrip {
    fn clone(&self) -> Self {
        Self {
            scope: Arc::clone(&self.scope),
            tls: Arc::clone(&self.tls),
            session: self.session,
            clients: Arc::clone(&self.clients),
            hints: Arc::clone(&self.hints),
        }
    }
}

/// Shared owner-routing state: Cell hints, session hints, and refresh single-flight.
struct OwnerHints {
    authority: CellAuthority,
    directory: NodeDirectory,
    owners: Mutex<HashMap<CellId, CachedOwner>>,
    sessions: Mutex<HashMap<SessionId, CachedSession>>,
    refreshing: Mutex<HashSet<CellId>>,
}

/// A Cell owner hint, its lease bound, and whether it wants a refresh.
struct CachedRoute {
    peer: RemotePeer,
    lease_expires_at_ms: i64,
    /// Whether the hint is still inside its refresh window.
    fresh: bool,
}

/// The owner a lookup observed, before it is turned into a routing decision.
enum OwnerLookup {
    /// This node's own session owns the Cell.
    Local,
    /// Another enrolled session owns the Cell.
    Remote(RemotePeer, i64),
    /// The Cell has no current owner: idle, tombstoned, or absent.
    Unowned,
}

/// One lease-bound route to the session that currently owns a Cell.
///
/// A route is a routing hint, never authority: the receiving node fences a
/// stale owner, and a refused attempt invalidates the route in the table that
/// produced it.
#[derive(Clone, Debug)]
pub struct CellRoute {
    session: SessionId,
    endpoint: url::Url,
    certificate: Digest,
    public_key: [u8; 32],
    lease_expires_at_ms: i64,
}

impl CellRoute {
    /// Returns the owning node session.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }

    /// Returns the enrolled peer endpoint for that session.
    #[must_use]
    pub const fn endpoint(&self) -> &url::Url {
        &self.endpoint
    }

    /// Returns the certificate digest pinned by the owner's advertisement.
    #[must_use]
    pub const fn certificate(&self) -> Digest {
        self.certificate
    }

    /// Returns the public key pinned by the owner's advertisement.
    #[must_use]
    pub const fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    /// Returns whether the signed node lease still covers `now_ms`.
    #[must_use]
    pub fn is_live_at(&self, now_ms: i64) -> bool {
        self.lease_expires_at_ms > now_ms.saturating_add(OWNER_LEASE_MARGIN_MS)
    }

    fn from_peer(peer: &RemotePeer, lease_expires_at_ms: i64) -> Self {
        Self {
            session: peer.session,
            endpoint: peer.endpoint.clone(),
            certificate: peer.certificate,
            public_key: peer.public_key,
            lease_expires_at_ms,
        }
    }
}

/// The routing decision for one Cell target.
#[derive(Clone, Debug)]
pub enum RouteDecision {
    /// This node's session owns the Cell; serve it locally.
    Local,
    /// Another enrolled session owns it; dial the route or forward through it.
    Remote(CellRoute),
    /// Control proves no peer owner: the Cell is idle, tombstoned, or absent.
    ///
    /// An owner that is not currently enrolled fails with `CellNotActive`
    /// instead, so an ingress can tell a transient enrollment gap from a Cell
    /// that genuinely has no owner.
    Unowned,
}

/// Lease-bound `Cell -> node` routing table for an ingress.
///
/// The table shares its cache with the [`PeerHttpRoundTrip`] built from the
/// same state, so an ingress that routes directly to the owner and a forwarder
/// that resolves the owner itself never pay for the same lookup twice.
///
/// Remote routes are hinted: a fresh route resolves with no object read, a
/// route past its refresh window is still served while one background refresh
/// per Cell runs, and a route is never served past the signed node lease.
/// `Local` and `Unowned` are transitions, so they always rest on an exact
/// control observation.
#[derive(Clone)]
pub struct CellRouteTable {
    scope: Arc<dyn PeerTargetScope>,
    session: SessionId,
    hints: Arc<OwnerHints>,
}

impl CellRouteTable {
    /// Resolves the current route for one Cell target.
    ///
    /// A target outside this table's scope, or a malformed enrolled record,
    /// fails closed instead of returning a route.
    pub async fn route(&self, target: &CellTarget) -> cellule_runtime::Result<RouteDecision> {
        self.scope.check_target(target)?;
        let now = now_ms()?;
        if let Some(route) = self.hints.cached_owner(target.cell_id(), now) {
            if !route.fresh {
                // Still inside the signed lease: serve it and refresh behind
                // the caller. A refused attempt invalidates it.
                self.hints
                    .spawn_refresh(Arc::clone(&self.scope), self.session, target.clone());
            }
            return Ok(RouteDecision::Remote(CellRoute::from_peer(
                &route.peer,
                route.lease_expires_at_ms,
            )));
        }
        match self
            .hints
            .lookup_owner(
                self.scope.as_ref(),
                self.session,
                target,
                SessionReuse::Verify,
            )
            .await?
        {
            OwnerLookup::Local => Ok(RouteDecision::Local),
            OwnerLookup::Unowned => Ok(RouteDecision::Unowned),
            OwnerLookup::Remote(peer, lease_expires_at_ms) => Ok(RouteDecision::Remote(
                CellRoute::from_peer(&peer, lease_expires_at_ms),
            )),
        }
    }

    /// Drops one hinted route after a refusal or a known ownership change.
    ///
    /// The session argument guards against dropping a newer observation: a
    /// refusal reported for an older session leaves the current route in place.
    pub fn invalidate(&self, target: &CellTarget, session: SessionId) {
        self.hints.invalidate_owner(target.cell_id(), session);
    }
}

/// Whether a lookup may reuse an already enrolled session advertisement.
///
/// A synchronous lookup — a cold route or one after a refusal — always re-reads
/// the signed node record so a retired session fails closed. Only a background
/// refresh of a hint that is still inside its lease reuses the record: the
/// request that hint already serves is fenced by the receiver either way.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionReuse {
    /// Re-read the signed node record.
    Verify,
    /// Reuse the enrolled record while its lease margin holds.
    Reuse,
}

impl OwnerHints {
    /// Refreshes one stale hint without blocking its caller.
    ///
    /// At most one refresh per Cell is in flight; a failure leaves the current
    /// hint in place until its lease bound or the next refusal.
    fn spawn_refresh(
        self: &Arc<Self>,
        scope: Arc<dyn PeerTargetScope>,
        session: SessionId,
        target: CellTarget,
    ) {
        let cell = target.cell_id();
        {
            let Ok(mut refreshing) = self.refreshing.lock() else {
                return;
            };
            if !refreshing.insert(cell) {
                return;
            }
        }
        let hints = Arc::clone(self);
        tokio::spawn(async move {
            let _ = hints
                .lookup_owner(scope.as_ref(), session, &target, SessionReuse::Reuse)
                .await;
            if let Ok(mut refreshing) = hints.refreshing.lock() {
                refreshing.remove(&cell);
            }
        });
    }

    /// Loads and enrolls the current owner for one target.
    ///
    /// One control read is always required: it names the owner session and its
    /// enrolled endpoint. The signed advertisement behind that session is
    /// reused while its lease holds, so a refresh that lands on the same
    /// session costs one object read instead of two.
    async fn lookup_owner(
        &self,
        scope: &dyn PeerTargetScope,
        session: SessionId,
        target: &CellTarget,
        reuse: SessionReuse,
    ) -> cellule_runtime::Result<OwnerLookup> {
        scope.check_target(target)?;
        let lookup_started = Instant::now();
        let Some(control) = self.authority.load(target.cell_id()).await? else {
            return Ok(OwnerLookup::Unowned);
        };
        let Some(owner) = control.value().owner.as_ref() else {
            return Ok(OwnerLookup::Unowned);
        };
        if owner.session == session {
            return Ok(OwnerLookup::Local);
        }
        let now = now_ms()?;
        let enrolled = if reuse == SessionReuse::Reuse {
            self.cached_session(owner.session, &owner.endpoint, now)
        } else {
            None
        };
        let (endpoint, certificate, public_key, lease_expires_at_ms) = match enrolled {
            Some(enrolled) => enrolled,
            None => {
                let Some(loaded) = self.directory.load(owner.session, now).await? else {
                    // The control record names an owner session that is not
                    // currently enrolled, so no route to it exists.
                    return Ok(OwnerLookup::Unowned);
                };
                let advertisement = loaded.advertisement();
                if advertisement.endpoint() != owner.endpoint {
                    return Err(CellError::PeerAuthorization(
                        "Cell owner endpoint is not enrolled",
                    ));
                }
                let public_key = advertisement.verifying_key()?.to_bytes();
                let enrolled = EnrolledSession {
                    endpoint: advertisement.endpoint().to_string(),
                    certificate: advertisement.certificate(),
                    public_key,
                    lease_expires_at_ms: advertisement.expires_at_ms(),
                };
                self.remember_session(owner.session, &enrolled, lookup_started);
                (
                    enrolled.endpoint,
                    enrolled.certificate,
                    enrolled.public_key,
                    enrolled.lease_expires_at_ms,
                )
            }
        };
        let remote = RemotePeer {
            session: owner.session,
            endpoint: url::Url::parse(&endpoint).map_err(peer_transport)?,
            certificate,
            public_key,
        };
        self.remember_owner(
            target.cell_id(),
            remote.clone(),
            lease_expires_at_ms,
            now,
            lookup_started,
        );
        Ok(OwnerLookup::Remote(remote, lease_expires_at_ms))
    }

    fn cached_owner(&self, cell: CellId, now_ms: i64) -> Option<CachedRoute> {
        let Ok(mut owners) = self.owners.lock() else {
            return None;
        };
        let observed = owners.get(&cell)?;
        // Hard bound: never route past the signed node lease margin.
        if observed.lease_expires_at_ms <= now_ms.saturating_add(OWNER_LEASE_MARGIN_MS) {
            owners.remove(&cell);
            return None;
        }
        let owner = observed.owner.clone()?;
        Some(CachedRoute {
            peer: owner,
            lease_expires_at_ms: observed.lease_expires_at_ms,
            fresh: observed.expires_at > Instant::now(),
        })
    }

    fn cached_session(
        &self,
        session: SessionId,
        endpoint: &str,
        now_ms: i64,
    ) -> Option<(String, Digest, [u8; 32], i64)> {
        let mut sessions = self.sessions.lock().ok()?;
        let observed = sessions.get(&session)?;
        if observed.lease_expires_at_ms <= now_ms.saturating_add(OWNER_LEASE_MARGIN_MS) {
            sessions.remove(&session);
            return None;
        }
        if observed.endpoint != endpoint {
            return None;
        }
        Some((
            observed.endpoint.clone(),
            observed.certificate,
            observed.public_key,
            observed.lease_expires_at_ms,
        ))
    }

    fn remember_session(
        &self,
        session: SessionId,
        enrolled: &EnrolledSession,
        lookup_started: Instant,
    ) {
        let Ok(mut sessions) = self.sessions.lock() else {
            return;
        };
        if sessions
            .get(&session)
            .is_some_and(|current| current.lookup_started > lookup_started)
        {
            return;
        }
        if sessions.len() >= MAX_SESSION_HINTS
            && !sessions.contains_key(&session)
            && let Some(evicted) = sessions.keys().next().copied()
        {
            sessions.remove(&evicted);
        }
        sessions.insert(
            session,
            CachedSession {
                endpoint: enrolled.endpoint.clone(),
                certificate: enrolled.certificate,
                public_key: enrolled.public_key,
                lease_expires_at_ms: enrolled.lease_expires_at_ms,
                lookup_started,
            },
        );
    }

    fn remember_owner(
        &self,
        cell: CellId,
        owner: RemotePeer,
        lease_expires_at_ms: i64,
        observed_at_ms: i64,
        lookup_started: Instant,
    ) {
        let lease_margin = lease_expires_at_ms
            .saturating_sub(observed_at_ms)
            .saturating_sub(OWNER_LEASE_MARGIN_MS);
        let Ok(lease_margin) = u64::try_from(lease_margin) else {
            return;
        };
        if lease_margin == 0 {
            return;
        }
        let lifetime = OWNER_HINT_TTL.min(Duration::from_millis(lease_margin));
        let Some(expires_at) = Instant::now().checked_add(lifetime) else {
            return;
        };
        let Ok(mut owners) = self.owners.lock() else {
            return;
        };
        if owners
            .get(&cell)
            .is_some_and(|current| current.lookup_started > lookup_started)
        {
            return;
        }
        if owners.len() >= MAX_OWNER_HINTS
            && !owners.contains_key(&cell)
            && let Some(evicted) = owners.keys().next().copied()
        {
            owners.remove(&evicted);
        }
        owners.insert(
            cell,
            CachedOwner {
                owner: Some(owner),
                expires_at,
                lease_expires_at_ms,
                lookup_started,
            },
        );
    }

    fn invalidate_owner(&self, cell: CellId, session: SessionId) {
        let Ok(mut owners) = self.owners.lock() else {
            return;
        };
        if owners.get(&cell).is_some_and(|cached| {
            cached
                .owner
                .as_ref()
                .is_some_and(|owner| owner.session != session)
        }) {
            return;
        }
        let now = Instant::now();
        let Some(expires_at) = now.checked_add(OWNER_TOMBSTONE_TTL) else {
            return;
        };
        if owners.len() >= MAX_OWNER_HINTS
            && !owners.contains_key(&cell)
            && let Some(evicted) = owners.keys().next().copied()
        {
            owners.remove(&evicted);
        }
        // Keep a short tombstone so a lookup started before this refusal
        // cannot repopulate the invalidated session after the lock is released.
        owners.insert(
            cell,
            CachedOwner {
                owner: None,
                expires_at,
                lease_expires_at_ms: i64::MAX,
                lookup_started: now,
            },
        );
    }
}

#[derive(Clone)]
struct RemotePeer {
    session: SessionId,
    endpoint: url::Url,
    certificate: Digest,
    public_key: [u8; 32],
}

struct CachedOwner {
    owner: Option<RemotePeer>,
    expires_at: Instant,
    lease_expires_at_ms: i64,
    lookup_started: Instant,
}

struct CachedSession {
    endpoint: String,
    certificate: Digest,
    public_key: [u8; 32],
    lease_expires_at_ms: i64,
    lookup_started: Instant,
}

/// One verified signed advertisement, reduced to what routing needs.
struct EnrolledSession {
    endpoint: String,
    certificate: Digest,
    public_key: [u8; 32],
    lease_expires_at_ms: i64,
}

struct CachedPeerClient {
    session: SessionId,
    certificate: Digest,
    public_key: [u8; 32],
    client: reqwest::Client,
}

impl CachedPeerClient {
    fn matches(&self, owner: &RemotePeer) -> bool {
        self.session == owner.session
            && self.certificate == owner.certificate
            && self.public_key == owner.public_key
    }
}

enum PeerHttpAttempt {
    Reply(Vec<u8>),
    Retry(CellError, Duration),
    Unknown(CellError),
}

fn peer_transport(
    source: impl std::error::Error + Send + Sync + 'static,
) -> cellule_runtime::Error {
    CellError::PeerTransport {
        context: "peer HTTP transport failed",
        source: Box::new(source),
    }
}

fn now_ms() -> cellule_runtime::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CellError::Peer("system clock precedes Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| CellError::Peer("system clock exceeds peer time range"))
}

fn remaining_timeout(started: Instant, original_ms: u32) -> cellule_runtime::Result<u32> {
    let elapsed_ms = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
    original_ms
        .checked_sub(elapsed_ms)
        .filter(|remaining| *remaining > 0)
        .ok_or(CellError::Deadline)
}

fn validate_request(request: &[u8], timeout_ms: u32) -> cellule_runtime::Result<()> {
    // Direct-node activation shares the owner route's admission bounds. Reject
    // before consulting providers or dispatching a request with no time left.
    if request.len() > cellule_runtime::peer::MAX_PEER_REQUEST_BYTES {
        return Err(CellError::Peer("request exceeds peer byte limit"));
    }
    if timeout_ms == 0 {
        return Err(CellError::Deadline);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
