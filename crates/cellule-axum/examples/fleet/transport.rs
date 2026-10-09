//! Bounded signed HTTP requests pinned to the originally enrolled follower boots.
use super::*;
use bytes::Bytes;
use cellule_runtime::follower::{FollowerReceipt, FollowerTailPage};
use cellule_runtime::node::NodeAdvertisement;
use cellule_runtime::node::log_transport::{
    AppendRequest, NodeLogTransport, RetireRequest, SealRequest, TailRequest,
};
use futures_util::{StreamExt, future::BoxFuture};
use prost::Message;
use std::collections::HashMap;

struct Member {
    original: NodeAdvertisement,
    client: reqwest::Client,
}

pub(super) struct Transport {
    directory: NodeDirectory,
    sender: SessionId,
    tls: Arc<LoadedPeerTls>,
    members: HashMap<NodeId, Member>,
    metrics: Arc<QueryMetrics>,
}

impl Transport {
    pub fn new(
        directory: NodeDirectory,
        sender: SessionId,
        tls: Arc<LoadedPeerTls>,
        members: Vec<NodeAdvertisement>,
        metrics: Arc<QueryMetrics>,
    ) -> Result<Self> {
        let mut enrolled = HashMap::new();
        for original in members {
            let client = tls
                .client_identity()
                .client(original.certificate(), original.verifying_key()?.to_bytes())
                .map_err(transport_error)?;
            if enrolled
                .insert(original.node(), Member { original, client })
                .is_some()
            {
                return Err(Error::Node("duplicate capacity follower"));
            }
        }
        Ok(Self {
            directory,
            sender,
            tls,
            members: enrolled,
            metrics,
        })
    }

    async fn round_trip(&self, member: NodeId, mut request: wire::Request) -> Result<wire::Reply> {
        let peer = self
            .members
            .get(&member)
            .ok_or(Error::PeerAuthorization("unknown capacity follower"))?;
        let fresh = self
            .metrics
            .enrollment(
                false,
                self.directory
                    .load_if_live(peer.original.session(), clock()?),
            )
            .await;
        let fresh = fresh?.ok_or(Error::Fenced)?;
        let actual = fresh.advertisement();
        if actual.node() != member
            || actual.certificate() != peer.original.certificate()
            || actual.endpoint() != peer.original.endpoint()
            || actual.verifying_key()? != peer.original.verifying_key()?
        {
            return Err(Error::Fenced);
        }
        request.sender = self.sender.as_bytes().to_vec();
        request.member = member.as_bytes().to_vec();
        request.deadline_ms = clock()?.checked_add(10_000).ok_or(Error::Deadline)?;
        let phase = std::time::Instant::now();
        let body = request.encode_to_vec();
        let digest = blake3::hash(&body);
        let encoded = wire::sign(body, self.tls.signing_key(), wire::REQUEST_DOMAIN);
        self.metrics
            .peer_phase(PeerPhase::RequestSign, phase.elapsed());
        if encoded.len() > wire::MAX_REQUEST_BYTES {
            return Err(Error::Capacity("capacity node-log request bytes"));
        }
        let phase = std::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let response = peer
                .client
                .post(format!(
                    "{}{}",
                    actual.endpoint().trim_end_matches('/'),
                    wire::PATH
                ))
                .header("content-type", "application/x-protobuf")
                .body(encoded)
                .send()
                .await
                .map_err(transport_error)?;
            if !response.status().is_success() {
                return Err(Error::Peer("capacity follower HTTP rejection"));
            }
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            while let Some(part) = stream.next().await {
                let part = part.map_err(transport_error)?;
                if bytes.len().saturating_add(part.len()) > wire::MAX_RESPONSE_BYTES {
                    return Err(Error::Capacity("capacity follower response bytes"));
                }
                bytes.extend_from_slice(&part);
            }
            Ok(bytes)
        })
        .await;
        self.metrics
            .peer_phase(PeerPhase::RoundTrip, phase.elapsed());
        let encoded = result.map_err(transport_error)??;
        let phase = std::time::Instant::now();
        let body = wire::verify(&encoded, &actual.verifying_key()?, wire::RESPONSE_DOMAIN)?;
        let reply = wire::Reply::decode(body.as_slice())?;
        if reply.member != member.as_bytes() || reply.request_digest != digest.as_bytes() {
            return Err(Error::PeerAuthorization(
                "capacity follower reply scope differs",
            ));
        }
        if reply.status != 0 {
            return Err(Error::Peer("capacity follower operation failed"));
        }
        self.metrics
            .peer_phase(PeerPhase::ReplyVerify, phase.elapsed());
        Ok(reply)
    }
}

fn request(operation: u32, leader: SessionId, epoch: u64) -> wire::Request {
    wire::Request {
        operation,
        leader: leader.as_bytes().to_vec(),
        epoch,
        ..Default::default()
    }
}

fn receipt(reply: wire::Reply) -> FollowerReceipt {
    FollowerReceipt {
        base_sequence: reply.base_sequence,
        durable_through: reply.durable_through,
    }
}

impl NodeLogTransport for Transport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        append: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            let mut message = request(1, append.leader_session, append.log_epoch);
            message.frames = append
                .frames
                .into_iter()
                .map(|frame| frame.to_vec())
                .collect();
            message.covered_through = append.covered_through;
            self.round_trip(member, message).await.map(receipt)
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        seal: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.round_trip(member, request(2, seal.leader_session, seal.log_epoch))
                .await
                .map(receipt)
        })
    }

    fn retire<'a>(
        &'a self,
        member: NodeId,
        retire: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            let mut message = request(3, retire.leader_session, retire.log_epoch);
            message.covered_through = retire.covered_through;
            self.round_trip(member, message).await.map(receipt)
        })
    }

    fn tail<'a>(&'a self, member: NodeId, tail: TailRequest) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async move {
            let page = self.tail_page(member, tail).await?;
            if page.next_sequence.is_some() {
                return Err(Error::Capacity("capacity unbounded follower tail"));
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
            let mut message = request(4, tail.leader_session, tail.log_epoch);
            message.first_sequence = tail.first_sequence;
            let reply = self.round_trip(member, message).await?;
            Ok(FollowerTailPage {
                frames: reply.frames.into_iter().map(Bytes::from).collect(),
                next_sequence: reply.next_sequence,
            })
        })
    }
}
