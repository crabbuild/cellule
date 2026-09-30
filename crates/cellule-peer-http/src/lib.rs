//! Owner-resolving HTTP transport for authenticated Cell peer requests.

mod tls;

pub use tls::{LoadedPeerTls, PeerTlsClient, PeerTlsIdentity, PeerTlsListener, TlsError};

use std::{
    collections::{HashMap, VecDeque},
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
const OWNER_HINT_LIFETIME: Duration = Duration::from_secs(5);
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
    authority: CellAuthority,
    directory: NodeDirectory,
    tls: Arc<dyn PeerHttpClientFactory>,
    session: SessionId,
    clients: Arc<Mutex<VecDeque<CachedPeerClient>>>,
    owners: Arc<Mutex<HashMap<CellId, CachedOwner>>>,
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
            authority,
            directory,
            tls,
            session,
            clients: Arc::new(Mutex::new(VecDeque::new())),
            owners: Arc::new(Mutex::new(HashMap::new())),
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
                    self.invalidate_owner(target.cell_id(), owner.session);
                    last_retry = Some((error, delay));
                }
                Ok(PeerHttpAttempt::Unknown(error)) => {
                    self.invalidate_owner(target.cell_id(), owner.session);
                    return Err(CellError::PeerTransportUnknown {
                        context: "peer HTTP response was lost or invalid",
                        source: Box::new(error),
                    });
                }
                Err(error) => {
                    self.invalidate_owner(target.cell_id(), owner.session);
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
        if let Some(owner) = self.cached_owner(target.cell_id(), now) {
            return Ok(owner);
        }
        self.load_owner(target).await
    }

    async fn refresh_owner(&self, target: &CellTarget) -> cellule_runtime::Result<RemotePeer> {
        self.scope.check_target(target)?;
        self.load_owner(target).await
    }

    async fn load_owner(&self, target: &CellTarget) -> cellule_runtime::Result<RemotePeer> {
        let lookup_started = Instant::now();
        let control = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(CellError::CellNotActive)?;
        let owner = control
            .value()
            .owner
            .as_ref()
            .ok_or(CellError::CellNotActive)?;
        if owner.session == self.session {
            return Err(CellError::CellNotActive);
        }
        let now_ms = now_ms()?;
        let enrolled = self
            .directory
            .load(owner.session, now_ms)
            .await?
            .ok_or(CellError::CellNotActive)?;
        let advertisement = enrolled.advertisement();
        if advertisement.endpoint() != owner.endpoint {
            return Err(CellError::PeerAuthorization(
                "Cell owner endpoint is not enrolled",
            ));
        }
        let remote = RemotePeer {
            session: owner.session,
            endpoint: url::Url::parse(advertisement.endpoint()).map_err(peer_transport)?,
            certificate: advertisement.certificate(),
            public_key: advertisement.verifying_key()?.to_bytes(),
        };
        self.remember_owner(
            target.cell_id(),
            remote.clone(),
            advertisement.expires_at_ms(),
            now_ms,
            lookup_started,
        );
        Ok(remote)
    }

    fn cached_owner(&self, cell: CellId, now_ms: i64) -> Option<RemotePeer> {
        let Ok(mut owners) = self.owners.lock() else {
            return None;
        };
        let observed = owners.get(&cell)?;
        if observed.expires_at <= Instant::now()
            || observed.lease_expires_at_ms <= now_ms.saturating_add(OWNER_LEASE_MARGIN_MS)
        {
            owners.remove(&cell);
            return None;
        }
        observed.owner.clone()
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
        let lifetime = OWNER_HINT_LIFETIME.min(Duration::from_millis(lease_margin));
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
        let Some(expires_at) = now.checked_add(OWNER_HINT_LIFETIME) else {
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
            authority: self.authority.clone(),
            directory: self.directory.clone(),
            tls: Arc::clone(&self.tls),
            session: self.session,
            clients: Arc::clone(&self.clients),
            owners: Arc::clone(&self.owners),
        }
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
