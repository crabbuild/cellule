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

struct ClientGrant {
    authorization: cellule_runtime::node::append_grant::NodeAppendGrant,
    expires: std::time::Instant,
}

struct Member {
    original: NodeAdvertisement,
    client: reqwest::Client,
    grant: tokio::sync::Mutex<Option<ClientGrant>>,
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
                .insert(
                    original.node(),
                    Member {
                        original,
                        client,
                        grant: tokio::sync::Mutex::new(None),
                    },
                )
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

    async fn round_trip(
        &self,
        member: NodeId,
        request: &mut wire::Request,
    ) -> Result<(wire::Reply, NodeAdvertisement)> {
        let peer = self
            .members
            .get(&member)
            .ok_or(Error::PeerAuthorization("unknown capacity follower"))?;
        let actual = if request.operation == 1 {
            // The installed signed grant pins this original receiver boot.
            // Only renewal reads directory enrollment; ordinary frames still
            // use pinned mTLS and exact signed request/response bytes.
            peer.original.clone()
        } else {
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
            actual.clone()
        };
        request.sender = self.sender.as_bytes().to_vec();
        request.member = member.as_bytes().to_vec();
        let deadline = clock()?.checked_add(10_000).ok_or(Error::Deadline)?;
        request.deadline_ms = if request.deadline_ms == 0 {
            deadline
        } else {
            request.deadline_ms.min(deadline)
        };
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
            let response = response.error_for_status().map_err(transport_error)?;
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
        Ok((reply, actual))
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

fn receipt((reply, _): (wire::Reply, NodeAdvertisement)) -> FollowerReceipt {
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
            let peer = self
                .members
                .get(&member)
                .ok_or(Error::PeerAuthorization("unknown capacity follower"))?;
            // Renewal cannot invalidate another append in flight on this member.
            let mut cached = peer.grant.lock().await;
            let first = append
                .frames
                .first()
                .ok_or(Error::Peer("empty capacity append"))?;
            let last = append
                .frames
                .last()
                .ok_or(Error::Peer("empty capacity append"))?;
            let first =
                cellule_ltx::inspect_node_frame(first.clone(), cellule_ltx::Limits::default())?
                    .scope()
                    .node_sequence;
            let last =
                cellule_ltx::inspect_node_frame(last.clone(), cellule_ltx::Limits::default())?
                    .scope()
                    .node_sequence;
            let mut message = request(1, append.leader_session, append.log_epoch);
            message.frames = append
                .frames
                .into_iter()
                .map(|frame| frame.to_vec())
                .collect();
            for attempt in 0..2 {
                let valid = cached.as_ref().is_some_and(|grant| {
                    std::time::Instant::now() < grant.expires
                        && grant
                            .authorization
                            .authorize(
                                self.sender,
                                self.tls.certificate(),
                                self.tls.signing_key().verifying_key().to_bytes(),
                                append.log_epoch,
                                first,
                                last,
                            )
                            .is_ok()
                });
                if !valid {
                    *cached = None;
                    let started = std::time::Instant::now();
                    let mut issue = request(5, append.leader_session, append.log_epoch);
                    issue.first_sequence = first;
                    let (reply, receiver) = self.round_trip(member, &mut issue).await?;
                    let authorization =
                        cellule_runtime::node::append_grant::NodeAppendGrant::verify(
                            &reply.grant,
                            &receiver,
                            clock()?,
                        )?;
                    authorization.authorize(
                        self.sender,
                        self.tls.certificate(),
                        self.tls.signing_key().verifying_key().to_bytes(),
                        append.log_epoch,
                        first,
                        last,
                    )?;
                    let lifetime = authorization
                        .expires_at_ms()
                        .checked_sub(authorization.issued_at_ms())
                        .ok_or(Error::Deadline)?;
                    let lifetime = u64::try_from(lifetime).map_err(|_| Error::Deadline)?;
                    *cached = Some(ClientGrant {
                        authorization,
                        expires: started + Duration::from_millis(lifetime),
                    });
                }
                let grant = cached.as_ref().ok_or(Error::Fenced)?;
                message.grant = grant.authorization.digest().as_bytes().to_vec();
                message.deadline_ms = grant.authorization.expires_at_ms();
                let result = self.round_trip(member, &mut message).await.map(receipt);
                match result {
                    Ok(receipt) => return Ok(receipt),
                    Err(error) => {
                        *cached = None;
                        if attempt == 1 {
                            return Err(error);
                        }
                        // A lost/expired first response may already have fsynced.
                        // Renew and replay the same frames once; native duplicate
                        // digests reconcile it without rerunning a command.
                    }
                }
            }
            Err(Error::Peer("capacity append retry bound"))
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        seal: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.round_trip(member, &mut request(2, seal.leader_session, seal.log_epoch))
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
            self.round_trip(member, &mut message).await.map(receipt)
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
            let (reply, _) = self.round_trip(member, &mut message).await?;
            Ok(FollowerTailPage {
                frames: reply.frames.into_iter().map(Bytes::from).collect(),
                next_sequence: reply.next_sequence,
            })
        })
    }
}
