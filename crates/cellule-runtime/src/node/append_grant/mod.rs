//! Receiver-signed append windows. A grant never supplies Cell authority.
use super::*;

#[cfg(test)]
mod tests;

const MAGIC: &[u8; 8] = b"CRBGRNT1";
const DOMAIN: &[u8] = b"crab.node-append-grant.v1\0";
/// Maximum locally measured lifetime of one append window.
pub const APPEND_GRANT_LIFETIME_MS: i64 = 5_000;
/// Maximum node sequences authorized by one fresh enrollment read.
pub const APPEND_GRANT_SEQUENCES: u64 = 512;
const BODY_BYTES: usize = 8 + 3 * 32 + 2 * (16 + 16 + 32 + 32) + 2 * 16 + 6 * 8;

/// Immutable receiver signature binding both physical nodes, boots and TLS keys.
///
/// Only fresh directory authorization can issue a grant. Decoding authenticates
/// a received grant; it cannot install one in the receiver's local registry.
#[derive(Clone)]
pub struct NodeAppendGrant {
    body: [u8; BODY_BYTES],
    signature: [u8; 64],
}

impl NodeAppendGrant {
    /// Verifies the exact receiver signature and the caller's original boot pin.
    pub fn verify(encoded: &[u8], receiver: &NodeAdvertisement, now_ms: i64) -> Result<Self> {
        if encoded.len() != BODY_BYTES + 64 || &encoded[..8] != MAGIC {
            return Err(Error::PeerAuthorization("invalid append grant encoding"));
        }
        receiver.validate_at(now_ms)?;
        let body: [u8; BODY_BYTES] = encoded[..BODY_BYTES]
            .try_into()
            .map_err(|_| Error::Peer("append grant body"))?;
        let signature: [u8; 64] = encoded[BODY_BYTES..]
            .try_into()
            .map_err(|_| Error::Peer("append grant signature"))?;
        receiver
            .verifying_key()?
            .verify_strict(&signing_bytes(&body), &Signature::from_bytes(&signature))
            .map_err(Error::PeerSignature)?;
        let grant = Self { body, signature };
        if grant.scope() != (receiver.fleet, receiver.image, receiver.release)
            || grant.receiver() != receiver.session
            || grant.receiver_node() != receiver.node
            || grant.receiver_certificate() != receiver.certificate
            || grant.receiver_key() != receiver.public_key
            || grant.expires_at_ms() > receiver.expires_at_ms
            || !grant.valid_at(now_ms)
            || grant.last_sequence().checked_sub(grant.first_sequence())
                != Some(APPEND_GRANT_SEQUENCES - 1)
            || grant.first_sequence() == 0
            || grant.covered_through() >= grant.first_sequence()
            || grant.log_epoch() == 0
            || !grant.members().contains(&receiver.node)
        {
            return Err(Error::PeerAuthorization(
                "append grant receiver or window differs",
            ));
        }
        Ok(grant)
    }

    /// Canonical fixed-width bytes including the receiver signature.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        [self.body.as_slice(), self.signature.as_slice()].concat()
    }
    /// Digest carried by every signed append request.
    #[must_use]
    pub fn digest(&self) -> Digest {
        Digest::from_bytes(*blake3::hash(&self.encode()).as_bytes())
    }
    /// Original leader boot authorized by the grant.
    #[must_use]
    pub fn leader(&self) -> SessionId {
        SessionId::from_bytes(self.bytes(120))
    }
    /// Exact receiver boot; a restarted process cannot adopt a registry entry.
    #[must_use]
    pub fn receiver(&self) -> SessionId {
        SessionId::from_bytes(self.bytes(216))
    }
    /// Node-log epoch of the authorized original ensemble.
    #[must_use]
    pub fn log_epoch(&self) -> u64 {
        self.number(328)
    }
    /// Inclusive first sequence of the authorization window.
    #[must_use]
    pub fn first_sequence(&self) -> u64 {
        self.number(336)
    }
    /// Inclusive final sequence of the authorization window.
    #[must_use]
    pub fn last_sequence(&self) -> u64 {
        self.number(344)
    }
    /// Fresh authority coverage floor; later request watermarks cannot prune more.
    #[must_use]
    pub fn covered_through(&self) -> u64 {
        self.number(352)
    }
    /// Signed issue time; local monotonic lifetime starts before issuance I/O.
    #[must_use]
    pub fn issued_at_ms(&self) -> i64 {
        self.number(360) as i64
    }
    /// Signed expiry, bounded by both observed node advertisements.
    #[must_use]
    pub fn expires_at_ms(&self) -> i64 {
        self.number(368) as i64
    }
    /// Whether wall-clock policy still permits this signed grant.
    #[must_use]
    pub fn valid_at(&self, now_ms: i64) -> bool {
        self.issued_at_ms() >= 0
            && self.issued_at_ms() <= now_ms
            && now_ms < self.expires_at_ms()
            && self
                .expires_at_ms()
                .checked_sub(self.issued_at_ms())
                .is_some_and(|ms| (1..=APPEND_GRANT_LIFETIME_MS).contains(&ms))
    }
    /// Validates the mTLS identity and frame window on an already signed request.
    pub fn authorize(
        &self,
        leader: SessionId,
        certificate: Digest,
        key: [u8; 32],
        epoch: u64,
        first: u64,
        last: u64,
    ) -> Result<()> {
        if leader != self.leader()
            || certificate != self.leader_certificate()
            || key != self.leader_key()
            || epoch != self.log_epoch()
            || first > last
            || first < self.first_sequence()
            || last > self.last_sequence()
        {
            return Err(Error::PeerAuthorization(
                "append grant sender or sequence differs",
            ));
        }
        Ok(())
    }
    fn bytes<const N: usize>(&self, offset: usize) -> [u8; N] {
        let mut out = [0; N];
        out.copy_from_slice(&self.body[offset..offset + N]);
        out
    }
    fn number(&self, offset: usize) -> u64 {
        u64::from_be_bytes(self.bytes(offset))
    }
    fn scope(&self) -> (Digest, Digest, Digest) {
        (
            Digest::from_bytes(self.bytes(8)),
            Digest::from_bytes(self.bytes(40)),
            Digest::from_bytes(self.bytes(72)),
        )
    }
    fn receiver_node(&self) -> NodeId {
        NodeId::from_bytes(self.bytes(200))
    }
    fn receiver_certificate(&self) -> Digest {
        Digest::from_bytes(self.bytes(264))
    }
    fn receiver_key(&self) -> [u8; 32] {
        self.bytes(232)
    }
    fn leader_certificate(&self) -> Digest {
        Digest::from_bytes(self.bytes(168))
    }
    fn leader_key(&self) -> [u8; 32] {
        self.bytes(136)
    }
    fn members(&self) -> [NodeId; 2] {
        [
            NodeId::from_bytes(self.bytes(296)),
            NodeId::from_bytes(self.bytes(312)),
        ]
    }
}

fn signing_bytes(body: &[u8]) -> Vec<u8> {
    [DOMAIN, blake3::hash(body).as_bytes()].concat()
}

impl NodeDirectory {
    #[expect(
        clippy::too_many_arguments,
        reason = "fresh mTLS and original receiver identities remain explicit"
    )]
    pub(crate) async fn issue_append_grant(
        &self,
        leader: SessionId,
        certificate: Digest,
        key: [u8; 32],
        receiver: SessionId,
        signing_key: &SigningKey,
        first: u64,
        epoch: u64,
        now_ms: i64,
    ) -> Result<NodeAppendGrant> {
        let enrollment = self.peer_verifier(leader, certificate, key, now_ms).await?;
        let receiver = self
            .load(receiver, now_ms)
            .await?
            .ok_or(Error::Fenced)?
            .advertisement;
        let leader = enrollment.into_append_grant_leader(receiver.node, epoch, now_ms)?;
        let log = leader.log.as_ref().ok_or(Error::Fenced)?;
        if receiver.public_key != signing_key.verifying_key().to_bytes() || first == 0 || now_ms < 0
        {
            return Err(Error::PeerAuthorization("append grant issuer differs"));
        }
        let last = first
            .checked_add(APPEND_GRANT_SEQUENCES - 1)
            .ok_or(Error::Capacity("append grant sequence"))?;
        let expires = now_ms
            .checked_add(APPEND_GRANT_LIFETIME_MS)
            .ok_or(Error::Deadline)?
            .min(leader.expires_at_ms)
            .min(receiver.expires_at_ms);
        let mut bytes = Vec::with_capacity(BODY_BYTES);
        bytes.extend_from_slice(MAGIC);
        for digest in [self.fleet, self.image, self.release] {
            bytes.extend_from_slice(digest.as_bytes());
        }
        for node in [&leader, &receiver] {
            bytes.extend_from_slice(node.node.as_bytes());
            bytes.extend_from_slice(node.session.as_bytes());
            bytes.extend_from_slice(&node.public_key);
            bytes.extend_from_slice(node.certificate.as_bytes());
        }
        let mut members = [[0u8; 16]; 2];
        for (slot, member) in members.iter_mut().zip(log.members()) {
            *slot = *member.as_bytes();
        }
        for member in members {
            bytes.extend_from_slice(&member);
        }
        for number in [
            epoch,
            first,
            last,
            // Publication may pass frames already queued by the source. Its
            // exact batch receipt still needs the first requested witness.
            // Fresh authority permits less pruning; it never permits more.
            log.tiered_through().min(first - 1),
            now_ms as u64,
            expires as u64,
        ] {
            bytes.extend_from_slice(&number.to_be_bytes());
        }
        let body: [u8; BODY_BYTES] = bytes
            .try_into()
            .map_err(|_| Error::Peer("append grant layout"))?;
        let signature = signing_key.sign(&signing_bytes(&body)).to_bytes();
        let grant = NodeAppendGrant { body, signature };
        if !grant.valid_at(now_ms) {
            return Err(Error::Fenced);
        }
        Ok(grant)
    }
}
