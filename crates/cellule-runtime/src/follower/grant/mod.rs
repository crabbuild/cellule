//! Locally registered grants serialize with durable lane closure and collection.
use super::*;
use crate::identity::Digest;
use crate::node::{
    NodeDirectory,
    append_grant::{APPEND_GRANT_LIFETIME_MS, NodeAppendGrant},
};
use ed25519_dalek::SigningKey;

#[cfg(test)]
mod tests;

pub(super) const MAX_APPEND_GRANTS: usize = 1_024;
#[derive(Default)]
pub(super) struct GrantState {
    pub(super) current: Option<RegisteredGrant>,
    pub(super) closed: bool,
}
pub(super) struct RegisteredGrant {
    grant: NodeAppendGrant,
    expires: Instant,
    _memory: IndexReservation,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

// Failed/cancelled authority I/O must not accumulate unauthenticated lane IDs.
// This only evicts an empty in-memory slot. Native files and closed slots stay
// under their ordinary lifecycle; a dispatched job or waiter prevents eviction.
struct TransientLane {
    lane: Lane,
    slot: LaneState,
    lanes: LaneMap,
}
impl Drop for TransientLane {
    fn drop(&mut self) {
        let Ok(memory) = self.slot.memory.try_lock() else {
            return;
        };
        let Ok(grant) = self.slot.grant.try_lock() else {
            return;
        };
        if memory.is_some() || grant.closed || grant.current.is_some() {
            return;
        }
        if let Ok(mut lanes) = self.lanes.lock()
            && Arc::strong_count(&self.slot) == 2
            && lanes
                .get(&self.lane)
                .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
        {
            lanes.remove(&self.lane);
        }
    }
}

/// Identity extracted from the authenticated and signed append request.
#[derive(Clone, Copy)]
pub struct AppendGrantPeer {
    /// Original leader boot, independently bound by the request signature.
    pub session: SessionId,
    /// Certificate digest supplied by the mTLS listener.
    pub certificate: Digest,
    /// Signing key extracted from the authenticated mTLS certificate.
    pub public_key: [u8; 32],
}
/// Fresh authority and receiver lease used for one local grant issuance.
pub struct AppendGrantIssuer<'a> {
    /// Canonical fleet/image/release directory.
    pub directory: &'a NodeDirectory,
    /// Receiver's advertised boot signing key.
    pub signing_key: &'a SigningKey,
    /// Current local node lease; proof release rechecks its terminal fence.
    pub lease: &'a crate::NodeLeaseGuard,
    /// Wall-clock sample taken before issuance I/O.
    pub now_ms: i64,
}
/// One append carrying an already authenticated grant digest.
pub struct GrantedFollowerAppend {
    /// Request sender's authenticated mTLS identity.
    pub peer: AppendGrantPeer,
    /// Exact original node-log epoch.
    pub log_epoch: u64,
    /// Digest of the receiver-issued signed grant.
    pub grant: Digest,
    /// Ordered, checksum-verified native frames; at most 64 per request.
    pub frames: Vec<Bytes>,
}

pub(super) struct GrantAppend {
    pub(super) peer: AppendGrantPeer,
    pub(super) digest: Digest,
    pub(super) lease: crate::NodeLeaseGuard,
}
impl GrantState {
    pub(super) fn authorize(&self, request: &GrantAppend, epoch: u64) -> Result<NodeAppendGrant> {
        request.lease.check()?;
        let current = self.current.as_ref().ok_or(Error::Fenced)?;
        if self.closed
            || Instant::now() >= current.expires
            || current.grant.digest() != request.digest
        {
            return Err(Error::Fenced);
        }
        let grant = &current.grant;
        grant.authorize(
            request.peer.session,
            request.peer.certificate,
            request.peer.public_key,
            epoch,
            grant.first_sequence(),
            grant.last_sequence(),
        )?;
        Ok(grant.clone())
    }
    pub(super) fn close(&mut self) {
        self.closed = true;
        self.current = None;
    }
}

impl FollowerStore {
    /// Binds grants to this process's fresh boot before the store serves traffic.
    /// Restart must use a new session even when its TLS key is persisted.
    #[must_use]
    pub fn with_append_grant_receiver(mut self, receiver: SessionId) -> Self {
        self.grant_receiver = Some(receiver);
        self
    }

    /// Issues and locally registers one signed window through fresh authority.
    ///
    /// The lifecycle gate precedes directory I/O and remains held through local
    /// registration. Seal, retirement, and collection use that same gate. A
    /// replayed signed token cannot install a new registry entry after closure.
    pub async fn open_append_grant(
        &self,
        peer: AppendGrantPeer,
        epoch: u64,
        first_sequence: u64,
        issuer: AppendGrantIssuer<'_>,
    ) -> Result<NodeAppendGrant> {
        issuer.lease.check()?;
        let receiver = self
            .grant_receiver
            .ok_or(Error::Node("append grant receiver is not installed"))?;
        let lane = Lane {
            leader: peer.session,
            epoch,
        };
        validate_lane(lane)?;
        let transient = TransientLane {
            lane,
            slot: self.lane_lock(lane)?,
            lanes: Arc::clone(&self.lanes),
        };
        let lock = &transient.slot;
        let mut state = lock.grant.clone().lock_owned().await;
        if state.closed {
            return Err(Error::Fenced);
        }
        state.current = None;
        let slot = self
            .grant_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity("append grants"))?;
        let memory = IndexReservation::new(&self.index_used, 1_024)?;
        let started = Instant::now();
        let grant = issuer
            .directory
            .issue_append_grant(
                peer.session,
                peer.certificate,
                peer.public_key,
                receiver,
                issuer.signing_key,
                first_sequence,
                epoch,
                issuer.now_ms,
            )
            .await?;
        let elapsed = i64::try_from(started.elapsed().as_millis()).map_err(|_| Error::Deadline)?;
        let now = issuer.now_ms.checked_add(elapsed).ok_or(Error::Deadline)?;
        issuer.lease.check()?;
        if !grant.valid_at(now) {
            return Err(Error::Fenced);
        }
        let lifetime = grant
            .expires_at_ms()
            .checked_sub(issuer.now_ms)
            .ok_or(Error::Deadline)?;
        if !(1..=APPEND_GRANT_LIFETIME_MS).contains(&lifetime) {
            return Err(Error::Fenced);
        }
        let root = self.root.clone();
        let lock = Arc::clone(lock);
        let closed = tokio::task::spawn_blocking(move || {
            let _native = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let directory = lane_directory(&root, lane);
            Ok::<bool, Error>(
                directory.join("sealed").exists() || directory.join("retired").exists(),
            )
        })
        .await
        .map_err(Error::FollowerWorkerJoin)??;
        if closed {
            state.close();
            return Err(Error::Fenced);
        }
        issuer.lease.check()?;
        let expires = started + std::time::Duration::from_millis(lifetime as u64);
        if Instant::now() >= expires {
            return Err(Error::Fenced);
        }
        state.current = Some(RegisteredGrant {
            grant: grant.clone(),
            expires,
            _memory: memory,
            _slot: slot,
        });
        Ok(grant)
    }

    /// Appends using its local signed window and the current receiver lease.
    /// Every frame remains signed at transport and fsynced at the native barrier.
    /// Request watermarks cannot advance beyond the fresh grant coverage floor.
    pub async fn append_granted(
        &self,
        request: GrantedFollowerAppend,
        lease: crate::NodeLeaseGuard,
    ) -> Result<FollowerReceipt> {
        self.append_inner(
            request.peer.session,
            request.log_epoch,
            request.frames,
            0,
            Some(GrantAppend {
                peer: request.peer,
                digest: request.grant,
                lease,
            }),
        )
        .await
    }
}
