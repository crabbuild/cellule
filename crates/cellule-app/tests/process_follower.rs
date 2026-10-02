//! Test-only authenticated network transport for private-disk follower lanes.

use super::performance_fixture::{DurabilityRecorder, now_ms};
use bytes::Bytes;
use cellule_host::{FacilityResult, NodeDurabilityProvider};
use cellule_ltx::Limits;
use cellule_runtime::fleet::telemetry::CellTelemetryHandle;
use cellule_runtime::follower::{FollowerReceipt, FollowerStore, FollowerTailPage};
use cellule_runtime::identity::NodeId;
use cellule_runtime::node::durability::{NodeDurabilityConfig, NodeLogAuthority};
use cellule_runtime::node::lease::NodeLeaseGuard;
use cellule_runtime::node::log_transport::{
    AppendRequest, NodeLogTransport, RetireRequest, SealRequest, TailRequest,
};
use cellule_runtime::node::{NodeAdvertisement, NodeDirectory, VersionedNodeAdvertisement};
use cellule_runtime::{Error, Result, SessionId};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use futures_util::future::BoxFuture;
use prost::Message;
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
};

const REQUEST_DOMAIN: &[u8] = b"cellule.test-follower.request.v1\0";
const RESPONSE_DOMAIN: &[u8] = b"cellule.test-follower.response.v1\0";
const MAX_REQUEST_BYTES: usize = 8 << 20;
const MAX_RESPONSE_BYTES: usize = 2 << 20;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DEADLINE_AHEAD_MS: i64 = 10_000;
const MAX_APPEND_FRAMES: usize = 64;

#[derive(Clone, PartialEq, Message)]
struct SignedWire {
    #[prost(bytes = "vec", tag = "1")]
    body: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    signature: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RequestWire {
    #[prost(bytes = "vec", tag = "1")]
    sender: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    member: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    leader: Vec<u8>,
    #[prost(uint64, tag = "4")]
    epoch: u64,
    #[prost(uint32, tag = "5")]
    operation: u32,
    #[prost(bytes = "vec", repeated, tag = "6")]
    frames: Vec<Vec<u8>>,
    #[prost(uint64, tag = "7")]
    covered_through: u64,
    #[prost(uint64, tag = "8")]
    first_sequence: u64,
    #[prost(int64, tag = "9")]
    deadline_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ResponseWire {
    #[prost(bytes = "vec", tag = "1")]
    member: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    request_digest: Vec<u8>,
    #[prost(uint32, tag = "3")]
    status: u32,
    #[prost(uint64, tag = "4")]
    base_sequence: u64,
    #[prost(uint64, tag = "5")]
    durable_through: u64,
    #[prost(bytes = "vec", repeated, tag = "6")]
    frames: Vec<Vec<u8>>,
    #[prost(uint64, optional, tag = "7")]
    next_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Append = 1,
    Seal = 2,
    Retire = 3,
    TailPage = 4,
}

impl TryFrom<u32> for Operation {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            1 => Ok(Self::Append),
            2 => Ok(Self::Seal),
            3 => Ok(Self::Retire),
            4 => Ok(Self::TailPage),
            _ => Err(Error::PeerAuthorization("unsupported follower operation")),
        }
    }
}

fn session(bytes: &[u8]) -> Result<SessionId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Peer("invalid follower session"))?;
    Ok(SessionId::from_bytes(bytes))
}

fn node(bytes: &[u8]) -> Result<NodeId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Peer("invalid follower node"))?;
    Ok(NodeId::from_bytes(bytes))
}

fn validate_request(request: &RequestWire, member: NodeId, now: i64) -> Result<Operation> {
    let operation = Operation::try_from(request.operation)?;
    if node(&request.member)? != member
        || request.epoch == 0
        || request.deadline_ms <= now
        || request.deadline_ms > now.saturating_add(MAX_DEADLINE_AHEAD_MS)
    {
        return Err(Error::PeerAuthorization(
            "follower request scope or deadline is invalid",
        ));
    }
    if operation == Operation::Append
        && (request.frames.is_empty() || request.frames.len() > MAX_APPEND_FRAMES)
    {
        return Err(Error::PeerAuthorization("invalid follower append batch"));
    }
    if operation != Operation::Append && !request.frames.is_empty() {
        return Err(Error::PeerAuthorization("unexpected follower frames"));
    }
    Ok(operation)
}

fn signed(body: Vec<u8>, key: &SigningKey, domain: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(domain.len() + body.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&body);
    SignedWire {
        body,
        signature: key.sign(&message).to_bytes().to_vec(),
    }
    .encode_to_vec()
}

fn verify(wire: &[u8], key: ed25519_dalek::VerifyingKey, domain: &[u8]) -> Result<Vec<u8>> {
    let signed = SignedWire::decode(wire).map_err(|_| Error::Peer("invalid follower envelope"))?;
    let signature = Signature::from_slice(&signed.signature)
        .map_err(|_| Error::PeerAuthorization("invalid follower signature"))?;
    let mut message = Vec::with_capacity(domain.len() + signed.body.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(&signed.body);
    key.verify(&message, &signature)
        .map_err(|_| Error::PeerAuthorization("follower signature does not match enrollment"))?;
    Ok(signed.body)
}

async fn follower_address(node: &NodeAdvertisement) -> Result<SocketAddr> {
    let endpoint = node
        .endpoint()
        .strip_prefix("https://")
        .ok_or(Error::Peer("follower endpoint is invalid"))?;
    let (host, gateway_port) = endpoint
        .rsplit_once(':')
        .ok_or(Error::Peer("follower endpoint has no gateway port"))?;
    let follower_port = gateway_port
        .parse::<u16>()
        .ok()
        .and_then(|port| port.checked_add(1))
        .ok_or(Error::Peer("follower endpoint port is invalid"))?;
    tokio::net::lookup_host(format!("{host}:{follower_port}"))
        .await
        .map_err(transport_io)?
        .next()
        .ok_or(Error::Peer("follower endpoint has no socket address"))
}

fn transport_io(source: std::io::Error) -> Error {
    Error::PeerTransportUnknown {
        context: "follower fixture round trip",
        source: Box::new(source),
    }
}

async fn receive(socket: &mut TcpStream, maximum: usize) -> Result<Vec<u8>> {
    let mut length = [0; 4];
    socket.read_exact(&mut length).await.map_err(transport_io)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > maximum {
        return Err(Error::Peer("follower message exceeds byte limit"));
    }
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.map_err(transport_io)?;
    Ok(bytes)
}

async fn send(socket: &mut TcpStream, bytes: &[u8], maximum: usize) -> Result<()> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(Error::Peer("follower message exceeds byte limit"));
    }
    let length =
        u32::try_from(bytes.len()).map_err(|_| Error::Peer("follower message too large"))?;
    socket
        .write_all(&length.to_be_bytes())
        .await
        .map_err(transport_io)?;
    socket.write_all(bytes).await.map_err(transport_io)?;
    Ok(())
}

/// One test-only network transport. Every reply is signed by its enrolled member.
pub(super) struct ProcessFollowerTransport {
    session: SessionId,
    key: SigningKey,
    directory: NodeDirectory,
    members: StdMutex<HashMap<NodeId, CachedMember>>,
    observation: Option<Arc<DurabilityRecorder>>,
}

struct CachedMember {
    pinned_session: SessionId,
    pinned_key: [u8; 32],
    pinned_endpoint: String,
    advertisement: NodeAdvertisement,
    observed_at: Instant,
}

impl ProcessFollowerTransport {
    pub(super) fn new(
        session: SessionId,
        key: SigningKey,
        directory: NodeDirectory,
        members: HashMap<NodeId, NodeAdvertisement>,
        observation: Option<Arc<DurabilityRecorder>>,
    ) -> Result<Self> {
        Ok(Self {
            session,
            key,
            directory,
            observation,
            members: StdMutex::new(
                members
                    .into_iter()
                    .map(|(member, advertisement)| {
                        Ok((
                            member,
                            CachedMember {
                                pinned_session: advertisement.session(),
                                pinned_key: advertisement.verifying_key()?.to_bytes(),
                                pinned_endpoint: advertisement.endpoint().to_owned(),
                                advertisement,
                                observed_at: Instant::now(),
                            },
                        ))
                    })
                    .collect::<Result<HashMap<_, _>>>()?,
            ),
        })
    }

    async fn member(&self, member: NodeId) -> Result<NodeAdvertisement> {
        let (current, pinned_session, pinned_key, pinned_endpoint, observed_at) = {
            let members = self
                .members
                .lock()
                .map_err(|_| Error::Peer("follower member cache lock poisoned"))?;
            let cached = members
                .get(&member)
                .ok_or(Error::PeerAuthorization("follower member was not enrolled"))?;
            (
                cached.advertisement.clone(),
                cached.pinned_session,
                cached.pinned_key,
                cached.pinned_endpoint.clone(),
                cached.observed_at,
            )
        };
        let now = now_ms();
        if observed_at.elapsed() < Duration::from_secs(1)
            && current.expires_at_ms() > now.saturating_add(1_000)
        {
            return Ok(current);
        }
        let fresh = self
            .directory
            .resolve_node(member, now)
            .await?
            .ok_or(Error::Fenced)?;
        if fresh.session() != pinned_session
            || fresh.verifying_key()?.to_bytes() != pinned_key
            || fresh.endpoint() != pinned_endpoint
        {
            return Err(Error::Fenced);
        }
        let mut members = self
            .members
            .lock()
            .map_err(|_| Error::Peer("follower member cache lock poisoned"))?;
        let cached = members
            .get_mut(&member)
            .ok_or(Error::PeerAuthorization("follower member was not enrolled"))?;
        cached.advertisement = fresh.clone();
        cached.observed_at = Instant::now();
        Ok(fresh)
    }

    async fn round_trip(&self, member: NodeId, mut request: RequestWire) -> Result<ResponseWire> {
        let append = request.operation == Operation::Append as u32;
        let append_bytes = request.frames.iter().map(Vec::len).sum::<usize>();
        let started = Instant::now();
        let result = self.round_trip_inner(member, &mut request).await;
        if append && let Some(observation) = &self.observation {
            observation.record_follower_network(
                result.is_ok(),
                u64::try_from(append_bytes).unwrap_or(u64::MAX),
                started.elapsed(),
            );
        }
        result
    }

    async fn round_trip_inner(
        &self,
        member: NodeId,
        request: &mut RequestWire,
    ) -> Result<ResponseWire> {
        let enrolled = self.member(member).await?;
        if enrolled.node() != member || enrolled.expires_at_ms() <= now_ms() {
            return Err(Error::Fenced);
        }
        request.sender = self.session.as_bytes().to_vec();
        request.member = member.as_bytes().to_vec();
        request.deadline_ms = now_ms()
            .checked_add(MAX_DEADLINE_AHEAD_MS)
            .ok_or(Error::Deadline)?;
        let body = request.encode_to_vec();
        let request_digest = blake3::hash(&body);
        let encoded = signed(body, &self.key, REQUEST_DOMAIN);
        if encoded.len() > MAX_REQUEST_BYTES {
            return Err(Error::Peer("follower request exceeds byte limit"));
        }
        let address = follower_address(&enrolled).await?;
        let response = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let mut socket = TcpStream::connect(address).await.map_err(transport_io)?;
            send(&mut socket, &encoded, MAX_REQUEST_BYTES).await?;
            receive(&mut socket, MAX_RESPONSE_BYTES).await
        })
        .await
        .map_err(|source| Error::PeerTransportUnknown {
            context: "follower fixture deadline",
            source: Box::new(source),
        })??;
        let body = verify(&response, enrolled.verifying_key()?, RESPONSE_DOMAIN)?;
        let response = ResponseWire::decode(body.as_slice())
            .map_err(|_| Error::Peer("invalid follower response"))?;
        if response.member != member.as_bytes()
            || response.request_digest != request_digest.as_bytes()
        {
            return Err(Error::PeerAuthorization(
                "follower response was not bound to request",
            ));
        }
        match response.status {
            0 => Ok(response),
            1 => Err(Error::PeerAuthorization("follower request was refused")),
            _ => Err(Error::Peer("follower operation failed")),
        }
    }
}

fn request(operation: Operation, leader: SessionId, epoch: u64) -> RequestWire {
    RequestWire {
        sender: Vec::new(),
        member: Vec::new(),
        leader: leader.as_bytes().to_vec(),
        epoch,
        operation: operation as u32,
        frames: Vec::new(),
        covered_through: 0,
        first_sequence: 0,
        deadline_ms: 0,
    }
}

impl NodeLogTransport for ProcessFollowerTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        append: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            let mut message = request(Operation::Append, append.leader_session, append.log_epoch);
            message.frames = append
                .frames
                .into_iter()
                .map(|frame| frame.to_vec())
                .collect();
            message.covered_through = append.covered_through;
            let reply = self.round_trip(member, message).await?;
            Ok(FollowerReceipt {
                base_sequence: reply.base_sequence,
                durable_through: reply.durable_through,
            })
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        seal: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            let reply = self
                .round_trip(
                    member,
                    request(Operation::Seal, seal.leader_session, seal.log_epoch),
                )
                .await?;
            Ok(FollowerReceipt {
                base_sequence: reply.base_sequence,
                durable_through: reply.durable_through,
            })
        })
    }

    fn retire<'a>(
        &'a self,
        member: NodeId,
        retire: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            let mut message = request(Operation::Retire, retire.leader_session, retire.log_epoch);
            message.covered_through = retire.covered_through;
            let reply = self.round_trip(member, message).await?;
            Ok(FollowerReceipt {
                base_sequence: reply.base_sequence,
                durable_through: reply.durable_through,
            })
        })
    }

    fn tail<'a>(&'a self, member: NodeId, tail: TailRequest) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async move {
            let page = self.tail_page(member, tail).await?;
            if page.next_sequence.is_some() {
                return Err(Error::Peer("unbounded follower tail is unsupported"));
            }
            Ok(page.frames)
        })
    }

    fn tail_page<'a>(
        &'a self,
        member: NodeId,
        tail: TailRequest,
    ) -> BoxFuture<'a, Result<FollowerTailPage>> {
        Box::pin(async move {
            let mut message = request(Operation::TailPage, tail.leader_session, tail.log_epoch);
            message.first_sequence = tail.first_sequence;
            let reply = self.round_trip(member, message).await?;
            Ok(FollowerTailPage {
                frames: reply.frames.into_iter().map(Bytes::from).collect(),
                next_sequence: reply.next_sequence,
            })
        })
    }
}

/// Serves one node's durable store on the private Compose network.
pub(super) fn serve(
    listener: TcpListener,
    member: NodeId,
    store: FollowerStore,
    directory: NodeDirectory,
    key: SigningKey,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(512));
        while let Ok((socket, _)) = listener.accept().await {
            let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                continue;
            };
            let store = store.clone();
            let directory = directory.clone();
            let key = key.clone();
            tokio::spawn(async move {
                let _slot = slot;
                let _ = tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    serve_one(socket, member, store, directory, key),
                )
                .await;
            });
        }
    })
}

async fn serve_one(
    mut socket: TcpStream,
    member: NodeId,
    store: FollowerStore,
    directory: NodeDirectory,
    key: SigningKey,
) -> Result<()> {
    let encoded = receive(&mut socket, MAX_REQUEST_BYTES).await?;
    let envelope = SignedWire::decode(encoded.as_slice())
        .map_err(|_| Error::Peer("invalid follower envelope"))?;
    let request = RequestWire::decode(envelope.body.as_slice())
        .map_err(|_| Error::Peer("invalid follower request"))?;
    let sender = session(&request.sender)?;
    let leader = session(&request.leader)?;
    validate_request(&request, member, now_ms())?;
    let enrolled = directory
        .load(sender, now_ms())
        .await?
        .ok_or(Error::PeerAuthorization("follower sender is not live"))?;
    verify(
        &encoded,
        enrolled.advertisement().verifying_key()?,
        REQUEST_DOMAIN,
    )?;
    let digest = blake3::hash(&envelope.body);
    let result = execute(member, store, directory, sender, leader, request).await;
    let mut reply = ResponseWire {
        member: member.as_bytes().to_vec(),
        request_digest: digest.as_bytes().to_vec(),
        status: 0,
        base_sequence: 0,
        durable_through: 0,
        frames: Vec::new(),
        next_sequence: None,
    };
    match result {
        Ok(Reply::Receipt(receipt)) => {
            reply.base_sequence = receipt.base_sequence;
            reply.durable_through = receipt.durable_through;
        }
        Ok(Reply::Page(page)) => {
            reply.frames = page
                .frames
                .into_iter()
                .map(|frame| frame.to_vec())
                .collect();
            reply.next_sequence = page.next_sequence;
        }
        Err(Error::PeerAuthorization(_)) | Err(Error::Fenced) => reply.status = 1,
        Err(_) => reply.status = 2,
    }
    let encoded = signed(reply.encode_to_vec(), &key, RESPONSE_DOMAIN);
    send(&mut socket, &encoded, MAX_RESPONSE_BYTES).await
}

enum Reply {
    Receipt(FollowerReceipt),
    Page(FollowerTailPage),
}

async fn execute(
    member: NodeId,
    store: FollowerStore,
    directory: NodeDirectory,
    sender: SessionId,
    leader: SessionId,
    request: RequestWire,
) -> Result<Reply> {
    match Operation::try_from(request.operation)? {
        Operation::Append => {
            if sender != leader {
                return Err(Error::PeerAuthorization(
                    "invalid follower append sender or batch",
                ));
            }
            directory
                .authorize_log_append(
                    leader,
                    member,
                    request.epoch,
                    request.covered_through,
                    now_ms(),
                )
                .await?;
            store
                .append(
                    leader,
                    request.epoch,
                    request.frames.into_iter().map(Bytes::from).collect(),
                    request.covered_through,
                )
                .await
                .map(Reply::Receipt)
        }
        Operation::Retire => {
            if sender != leader {
                return Err(Error::PeerAuthorization(
                    "invalid follower retirement sender",
                ));
            }
            directory
                .authorize_log_retire(
                    leader,
                    member,
                    request.epoch,
                    request.covered_through,
                    now_ms(),
                )
                .await?;
            store
                .retire(leader, request.epoch, request.covered_through)
                .await
                .map(Reply::Receipt)
        }
        Operation::Seal | Operation::TailPage => {
            directory
                .authorize_log_recovery(leader, sender, member, request.epoch, now_ms())
                .await?;
            if request.operation == Operation::Seal as u32 {
                store.seal(leader, request.epoch).await.map(Reply::Receipt)
            } else {
                store
                    .read_tail_page(leader, request.epoch, request.first_sequence)
                    .await
                    .map(Reply::Page)
            }
        }
    }
}

/// One serialized authority view shared by heartbeats, enrollment, and log CAS.
#[derive(Clone)]
pub(super) struct ProcessEnrollment {
    pub(super) observed: Arc<Mutex<VersionedNodeAdvertisement>>,
    directory: NodeDirectory,
    session: SessionId,
    observation: Option<Arc<DurabilityRecorder>>,
}

impl ProcessEnrollment {
    pub(super) fn new(
        observed: VersionedNodeAdvertisement,
        directory: NodeDirectory,
        session: SessionId,
        observation: Option<Arc<DurabilityRecorder>>,
    ) -> Self {
        Self {
            observed: Arc::new(Mutex::new(observed)),
            directory,
            session,
            observation,
        }
    }

    fn record_log_event(&self, epoch: u64, phase: &'static str, through: u64) {
        if let Some(observation) = &self.observation {
            observation.record_node_log_event(epoch, phase, through);
        }
    }

    pub(super) async fn refresh(&self, next: NodeAdvertisement) -> Result<i64> {
        let mut observed = self.observed.lock().await;
        *observed = self.directory.refresh(&observed, next, now_ms()).await?;
        Ok(observed.advertisement().expires_at_ms())
    }

    pub(super) async fn withdraw(&self) -> Result<()> {
        let observed = self.observed.lock().await;
        self.directory
            .withdraw_after_drain(&observed, now_ms())
            .await
    }

    pub(super) async fn progress(&self) -> u64 {
        self.observed.lock().await.advertisement().progress()
    }

    pub(super) async fn generation(&self) -> u64 {
        self.observed.lock().await.advertisement().generation()
    }
}

impl NodeLogAuthority for ProcessEnrollment {
    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != epoch)
            {
                return Err(Error::Fenced);
            }
            *observed = self.directory.activate_log(&observed, now_ms()).await?;
            self.record_log_event(epoch, "active", 0);
            Ok(())
        })
    }

    fn advance_coverage<'a>(&'a self, epoch: u64, through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != epoch)
            {
                return Err(Error::Fenced);
            }
            *observed = self
                .directory
                .advance_log_coverage(&observed, through, now_ms())
                .await?;
            self.record_log_event(epoch, "coverage", through);
            Ok(())
        })
    }

    fn close<'a>(
        &'a self,
        retirement: &'a cellule_runtime::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>> {
        let barrier = retirement.barrier();
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != barrier.log_epoch())
            {
                return Err(Error::Fenced);
            }
            *observed = self
                .directory
                .close_log(&observed, barrier, now_ms())
                .await?;
            self.record_log_event(barrier.log_epoch(), "closed", barrier.covered_through());
            Ok(())
        })
    }
}

pub(super) struct ProcessDurabilityProvider {
    enrollment: ProcessEnrollment,
    key: SigningKey,
    lease: NodeLeaseGuard,
    telemetry: CellTelemetryHandle,
    observation: Arc<DurabilityRecorder>,
}

impl ProcessDurabilityProvider {
    pub(super) fn new(
        enrollment: ProcessEnrollment,
        key: SigningKey,
        lease: NodeLeaseGuard,
        telemetry: CellTelemetryHandle,
        observation: Arc<DurabilityRecorder>,
    ) -> Self {
        Self {
            enrollment,
            key,
            lease,
            telemetry,
            observation,
        }
    }
}

impl NodeDurabilityProvider for ProcessDurabilityProvider {
    fn recruit(
        self: Arc<Self>,
        limits: Limits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        Box::pin(async move {
            let result: Result<Option<NodeDurabilityConfig>> = async {
                let mut observed = self.enrollment.observed.lock().await;
                if observed.advertisement().log().is_none() {
                    // Keep the measured ensemble shape fixed at both followers.
                    if self
                        .enrollment
                        .directory
                        .live(now_ms(), live_node_limit)
                        .await?
                        .len()
                        < 3
                    {
                        return Ok(None);
                    }
                    let next = self
                        .enrollment
                        .directory
                        .try_recruit_log(
                            &observed,
                            1,
                            required_follower_bytes,
                            live_node_limit,
                            now_ms(),
                        )
                        .await?;
                    let Some(next) = next else {
                        return Ok(None);
                    };
                    *observed = next;
                    self.enrollment.record_log_event(1, "enrolled", 0);
                }
                let log = observed
                    .advertisement()
                    .log()
                    .ok_or(Error::Node("enrolled follower log disappeared"))?;
                let members = log.members().to_vec();
                if members.len() != 2 {
                    return Err(Error::Node("capacity lane requires two follower members"));
                }
                let epoch = log.epoch();
                let mut enrolled = HashMap::new();
                for &member in &members {
                    let advertisement = self
                        .enrollment
                        .directory
                        .resolve_node(member, now_ms())
                        .await?
                        .ok_or(Error::Node("follower member is no longer live"))?;
                    enrolled.insert(member, advertisement);
                }
                let transport: Arc<dyn NodeLogTransport> = Arc::new(ProcessFollowerTransport::new(
                    self.enrollment.session,
                    self.key.clone(),
                    self.enrollment.directory.clone(),
                    enrolled,
                    Some(Arc::clone(&self.observation)),
                )?);
                let authority: Arc<dyn NodeLogAuthority> = Arc::new(self.enrollment.clone());
                NodeDurabilityConfig::new(
                    self.enrollment.session,
                    NodeId::from_bytes(*self.enrollment.session.as_bytes()),
                    epoch,
                    members,
                    transport,
                    authority,
                    self.lease.clone(),
                    limits,
                    self.telemetry.clone(),
                )
                .map(Some)
            }
            .await;
            result.map_err(|source| Box::new(source) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_ltx::{Db, NodeFrameScope, encode_node_frame};
    use cellule_runtime::identity::Digest;
    use cellule_runtime::ltx::CellStorageLayout;
    use cellule_runtime::node::log_recovery::NodeLogRecovery;
    use cellule_runtime::node::{NODE_LOG_PROTOCOL_VERSION, NodeCapacity, NodeFailureDomain};
    use cellule_store::Store;
    use object_store::{memory::InMemory, path::Path};

    fn advertisement(
        node: NodeId,
        session: SessionId,
        key: &SigningKey,
        endpoint: String,
        issued_at: i64,
        lifetime_ms: i64,
    ) -> NodeAdvertisement {
        NodeAdvertisement::sign(
            node,
            session,
            endpoint,
            Digest::from_bytes([10; 32]),
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            Digest::from_bytes([13; 32]),
            key,
            1,
            issued_at,
            issued_at + lifetime_ms,
            vec![Digest::from_bytes([14; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 1 << 20,
                free_disk_bytes: 1 << 20,
                follower_free_bytes: 1 << 20,
                job_credits: 4,
                log_protocol: NODE_LOG_PROTOCOL_VERSION,
                ..NodeCapacity::default()
            },
        )
        .unwrap()
    }

    fn frame(limits: Limits) -> Bytes {
        let source = tempfile::TempDir::new().unwrap();
        let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
        database
            .transaction(|transaction| {
                transaction.execute_batch(
                    "CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL);\
                     INSERT INTO events(body) VALUES ('survives')",
                )
            })
            .unwrap();
        let capture = database.capture().unwrap();
        let segment = capture.segments.first().unwrap();
        let encoded = encode_node_frame(
            NodeFrameScope {
                leader_session: [1; 16],
                log_epoch: 1,
                node_sequence: 1,
                application: [3; 16],
                cell: [4; 32],
                incarnation: [5; 16],
                cell_epoch: 1,
                commit_sequence: 1,
            },
            segment.info().clone(),
            Bytes::from(std::fs::read(segment.path()).unwrap()),
            limits,
        )
        .unwrap()
        .encoded()
        .clone();
        database.close().unwrap();
        encoded
    }

    #[test]
    fn signed_envelope_binds_sender_key_domain_and_body() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let other = SigningKey::from_bytes(&[8; 32]);
        let encoded = signed(vec![1, 2, 3], &key, REQUEST_DOMAIN);
        assert_eq!(
            verify(&encoded, key.verifying_key(), REQUEST_DOMAIN).unwrap(),
            vec![1, 2, 3]
        );
        assert!(verify(&encoded, other.verifying_key(), REQUEST_DOMAIN).is_err());
        assert!(verify(&encoded, key.verifying_key(), RESPONSE_DOMAIN).is_err());
        let mut changed = SignedWire::decode(encoded.as_slice()).unwrap();
        changed.body.push(4);
        assert!(
            verify(
                &changed.encode_to_vec(),
                key.verifying_key(),
                REQUEST_DOMAIN
            )
            .is_err()
        );
    }

    #[test]
    fn request_scope_rejects_wrong_member_stale_epoch_deadline_and_batch() {
        let member = NodeId::from_bytes([1; 16]);
        let leader = SessionId::from_bytes([2; 16]);
        let mut append = request(Operation::Append, leader, 1);
        append.member = member.as_bytes().to_vec();
        append.frames.push(vec![1]);
        append.deadline_ms = 11_000;
        assert_eq!(
            validate_request(&append, member, 10_000).unwrap(),
            Operation::Append
        );
        append.member = NodeId::from_bytes([3; 16]).as_bytes().to_vec();
        assert!(validate_request(&append, member, 10_000).is_err());
        append.member = member.as_bytes().to_vec();
        append.epoch = 0;
        assert!(validate_request(&append, member, 10_000).is_err());
        append.epoch = 1;
        append.deadline_ms = 10_000;
        assert!(validate_request(&append, member, 10_000).is_err());
        append.deadline_ms = 20_001;
        assert!(validate_request(&append, member, 10_000).is_err());
        append.deadline_ms = 11_000;
        append.frames = vec![vec![1]; MAX_APPEND_FRAMES + 1];
        assert!(validate_request(&append, member, 10_000).is_err());
        append.operation = Operation::Retire as u32;
        assert!(validate_request(&append, member, 10_000).is_err());
    }

    #[tokio::test]
    async fn oversized_message_is_rejected_before_network_write() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        assert!(
            send(
                &mut socket,
                &vec![1; MAX_REQUEST_BYTES + 1],
                MAX_REQUEST_BYTES
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn owner_loss_seals_and_reads_exact_unpublished_network_tail() {
        let limits = Limits::default();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("follower-network-test"),
            [15; 16],
        );
        let directory = NodeDirectory::new(
            layout,
            Digest::from_bytes([10; 32]),
            Digest::from_bytes([12; 32]),
            Digest::from_bytes([13; 32]),
        );
        let leader = SessionId::from_bytes([1; 16]);
        let member = NodeId::from_bytes([2; 16]);
        let claimant = SessionId::from_bytes([3; 16]);
        let leader_key = SigningKey::from_bytes(&[21; 32]);
        let member_key = SigningKey::from_bytes(&[22; 32]);
        let claimant_key = SigningKey::from_bytes(&[23; 32]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_port = listener.local_addr().unwrap().port();
        let gateway_port = follower_port.checked_sub(1).unwrap();
        let endpoint = format!("https://127.0.0.1:{gateway_port}");
        let issued_at = now_ms();
        let leader_record = directory
            .create(
                advertisement(
                    NodeId::from_bytes([1; 16]),
                    leader,
                    &leader_key,
                    "https://127.0.0.1:8080".into(),
                    issued_at,
                    5_000,
                ),
                issued_at,
            )
            .await
            .unwrap();
        let member_record = directory
            .create(
                advertisement(
                    member,
                    SessionId::from_bytes([2; 16]),
                    &member_key,
                    endpoint,
                    now_ms(),
                    30_000,
                ),
                now_ms(),
            )
            .await
            .unwrap();
        let enrolled = directory
            .recruit_log(&leader_record, 1, 1, 3, now_ms())
            .await
            .unwrap();
        let root = tempfile::TempDir::new().unwrap();
        let store = FollowerStore::open(
            root.path().join("follower"),
            limits,
            cellule_ltx::DiskBudget::new(1 << 20),
        )
        .unwrap();
        let server = serve(listener, member, store, directory.clone(), member_key);
        let members = HashMap::from([(member, member_record.advertisement().clone())]);
        let transport = ProcessFollowerTransport::new(
            leader,
            leader_key.clone(),
            directory.clone(),
            members.clone(),
            None,
        )
        .unwrap();
        assert!(
            transport
                .append(
                    NodeId::from_bytes([9; 16]),
                    AppendRequest {
                        leader_session: leader,
                        log_epoch: 1,
                        frames: vec![Bytes::from_static(b"untrusted")],
                        covered_through: 0,
                    },
                )
                .await
                .is_err()
        );
        let expected = frame(limits);
        let receipt = transport
            .append(
                member,
                AppendRequest {
                    leader_session: leader,
                    log_epoch: 1,
                    frames: vec![expected.clone()],
                    covered_through: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(receipt.durable_through, 1);
        let mut interrupted = request(Operation::Append, leader, 1);
        interrupted.sender = leader.as_bytes().to_vec();
        interrupted.member = member.as_bytes().to_vec();
        interrupted.frames = vec![expected.to_vec()];
        interrupted.deadline_ms = now_ms() + MAX_DEADLINE_AHEAD_MS;
        let mut socket = TcpStream::connect(("127.0.0.1", follower_port))
            .await
            .unwrap();
        send(
            &mut socket,
            &signed(interrupted.encode_to_vec(), &leader_key, REQUEST_DOMAIN),
            MAX_REQUEST_BYTES,
        )
        .await
        .unwrap();
        drop(socket);
        let retried = transport
            .append(
                member,
                AppendRequest {
                    leader_session: leader,
                    log_epoch: 1,
                    frames: vec![expected.clone()],
                    covered_through: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(retried.durable_through, 1);
        let enrollment = ProcessEnrollment::new(enrolled, directory.clone(), leader, None);
        let next = advertisement(
            NodeId::from_bytes([1; 16]),
            leader,
            &leader_key,
            "https://127.0.0.1:8080".into(),
            now_ms(),
            5_000,
        );
        let (activated, refreshed) = tokio::join!(enrollment.activate(1), enrollment.refresh(next));
        activated.unwrap();
        refreshed.unwrap();
        assert!(
            directory
                .load(leader, now_ms())
                .await
                .unwrap()
                .unwrap()
                .advertisement()
                .log()
                .unwrap()
                .active()
        );
        assert!(
            transport
                .append(
                    member,
                    AppendRequest {
                        leader_session: leader,
                        log_epoch: 2,
                        frames: vec![expected.clone()],
                        covered_through: 0,
                    }
                )
                .await
                .is_err()
        );
        assert!(
            transport
                .retire(
                    member,
                    RetireRequest {
                        leader_session: leader,
                        log_epoch: 1,
                        covered_through: 1,
                    }
                )
                .await
                .is_err()
        );
        let claimant_record = directory
            .create(
                advertisement(
                    NodeId::from_bytes([3; 16]),
                    claimant,
                    &claimant_key,
                    "https://127.0.0.1:8082".into(),
                    now_ms(),
                    30_000,
                ),
                now_ms(),
            )
            .await
            .unwrap();
        let _ = claimant_record;
        tokio::time::sleep(Duration::from_millis(5_100)).await;
        let fenced = directory
            .claim_expired(leader, claimant, now_ms())
            .await
            .unwrap();
        assert_eq!(fenced.log().unwrap().tiered_through(), 0);
        let recovery_transport: Arc<dyn NodeLogTransport> = Arc::new(
            ProcessFollowerTransport::new(claimant, claimant_key, directory, members, None)
                .unwrap(),
        );
        let recovery = NodeLogRecovery::from_fenced(recovery_transport, &fenced, limits).unwrap();
        let sealed = recovery.ensure_sealed().await.unwrap();
        assert_eq!(sealed.frame_count(), 1);
        assert_eq!(sealed.frames[0].encoded(), &expected);
        server.abort();
        let _ = server.await;
    }
}
