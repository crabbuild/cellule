//! Fixed-size producer metadata; opaque signed ensembles are never deep-copied.
use super::*;
use cellule_runtime::node::NodeMode;

const PAGE_BYTES: usize = 1 << 20;
const MAX_EPOCHS: usize = MAX_FOLLOWER_ENROLLMENT_EPOCHS;
const MAX_MEMBERS: usize = 16;

#[cfg(test)]
mod tests;

/// Process-local continuation tied to every retained epoch and producer state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerEnrollmentInventoryCursor {
    topology: Digest,
    after: u64,
}
impl FollowerEnrollmentInventoryCursor {
    /// Encodes the fixed-width progress digest and last included epoch.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 40] {
        let mut bytes = [0; 40];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..].copy_from_slice(&self.after.to_le_bytes());
        bytes
    }
    /// Decodes a continuation; capture rechecks all original producer progress.
    pub fn from_bytes(bytes: &[u8]) -> cellule_runtime::Result<Self> {
        let bytes: &[u8; 40] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid follower enrollment cursor width"))?;
        let mut topology = [0; 32];
        topology.copy_from_slice(&bytes[..32]);
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&bytes[32..]);
        let after = u64::from_le_bytes(epoch);
        if topology == [0; 32] || after == 0 {
            return Err(Error::Node("zero follower enrollment cursor identity"));
        }
        Ok(Self {
            topology: Digest::from_bytes(topology),
            after,
        })
    }
}

/// Original requests and advisory progress for one supervisor-owned epoch.
/// Digests identify retained canonical inputs/proofs; they do not authenticate
/// them or establish current authority, native closure or replacement policy.
pub struct FollowerEnrollmentProgress {
    /// Exact original leader epoch, in ascending order within a page.
    pub epoch: u64,
    /// Digest of the original signed selection and conditional-write token.
    pub attempt: Digest,
    /// All original selected member requests, including unknown acceptance.
    pub members: Vec<FollowerEnrollmentMember>,
    /// Whether the original owner's enrollment CAS started.
    pub native_started: bool,
    /// Whether the original owner chose the nonexecution/refusal cleanup path.
    /// This flag alone supplies no durable exclusion fence.
    pub no_effect: bool,
    /// Whether configuration was delivered to the one native supervisor.
    pub delivered: bool,
    /// Digest of the original checked enrollment, when available.
    pub enrollment: Option<Digest>,
    /// Digest of the original-token conditional refusal, when available.
    pub refusal: Option<Digest>,
    /// Original joined member replies, including failures before canonical close.
    pub retirement: Option<Arc<NodeLogRetirementObservation>>,
    /// Whether the original canonical authority callback confirmed closure.
    pub native_closed: bool,
    /// Original native error, independently of journal failures.
    pub execution_error: Option<Arc<Error>>,
    /// Original journal error, retained across successful replay.
    pub journal_error: Option<Arc<Error>>,
}

/// Bounded advisory capture retaining one MiB from the node byte ledger.
/// Separate record locks make this an interval scan, not an atomic node barrier.
/// Zero epochs and an idle protocol do not prove a joined supervisor or no roles.
pub struct FollowerEnrollmentInventoryPage {
    scope: FleetScope,
    node: NodeId,
    session: SessionId,
    mode: NodeMode,
    observed_at_ms: i64,
    topology: Digest,
    total_epochs: usize,
    protocol_busy: bool,
    draining: bool,
    pending_epoch: Option<u64>,
    entries: Vec<FollowerEnrollmentProgress>,
    next: Option<FollowerEnrollmentInventoryCursor>,
    _memory: NodeByteReservation,
}
impl FollowerEnrollmentInventoryPage {
    /// Returns the installed journal namespace.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the exact original physical leader.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the installed leader boot.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns shared admission mode independently of local role counts.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns the caller's original capture time, without refreshing proof age.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Returns the fingerprint of this local producer capture.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Counts all retained original epochs, including those outside this page.
    #[must_use]
    pub const fn total_epochs(&self) -> usize {
        self.total_epochs
    }
    /// Reports a held protocol lane, including preparation before a row exists.
    /// It cannot distinguish preparation, acceptance, native dispatch or drain.
    #[must_use]
    pub const fn protocol_busy(&self) -> bool {
        self.protocol_busy
    }
    /// Reports permanent closure of this producer's recruitment admission.
    #[must_use]
    pub const fn draining(&self) -> bool {
        self.draining
    }
    /// Returns the original undelivered attempt, when registered locally.
    #[must_use]
    pub const fn pending_epoch(&self) -> Option<u64> {
        self.pending_epoch
    }
    /// Returns original requests and checked progress in ascending epoch order.
    #[must_use]
    pub fn entries(&self) -> &[FollowerEnrollmentProgress] {
        &self.entries
    }
    /// Continues only if every captured epoch and producer state still matches.
    #[must_use]
    pub const fn next(&self) -> Option<FollowerEnrollmentInventoryCursor> {
        self.next
    }
}

impl FleetFollowerEnrollment {
    pub(crate) fn page(
        &self,
        cursor: Option<FollowerEnrollmentInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> cellule_runtime::Result<FollowerEnrollmentInventoryPage> {
        if !(1..=MAX_EPOCHS).contains(&limit) || now_ms < 0 {
            return Err(Error::Node("invalid follower enrollment inventory bounds"));
        }
        // Reserve before cloning any retained index/progress. Member roles are
        // fixed-size Follower variants; signed inputs/proofs are hashed in place.
        let memory = self.runtime.try_reserve_node_bytes(PAGE_BYTES)?;
        let (records, draining, pending_epoch, last_epoch) = {
            let bank = self
                .bank
                .lock()
                .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?;
            if bank.epochs.len() > MAX_EPOCHS {
                return Err(Error::Capacity("follower enrollment inventory bound"));
            }
            (
                bank.epochs
                    .iter()
                    .map(|(epoch, row)| (*epoch, row.clone()))
                    .collect::<Vec<_>>(),
                bank.draining,
                bank.pending
                    .as_ref()
                    .map(|row| row.inputs.attempt.prepared().log().epoch()),
                bank.last_epoch,
            )
        };
        let protocol_busy = self.protocol.try_lock().is_err();
        let mode = self.runtime.node_admission().mode()?;
        let total_epochs = records.len();
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.follower-enrollment-inventory.v1\0");
        hash.update(self.scope.fleet.as_bytes());
        hash.update(self.scope.application.as_bytes());
        hash.update(self.node.as_bytes());
        hash.update(self.session.as_bytes());
        hash.update(&[mode as u8, u8::from(draining), u8::from(protocol_busy)]);
        hash.update(&last_epoch.to_le_bytes());
        hash.update(&pending_epoch.unwrap_or(0).to_le_bytes());
        hash.update(&(total_epochs as u64).to_le_bytes());
        let mut entries = Vec::with_capacity(limit.min(total_epochs));
        let mut after_found = cursor.is_none();
        let mut has_more = false;
        for (epoch, record) in records {
            if cursor.is_some_and(|cursor| cursor.after == epoch) {
                after_found = true;
            }
            let progress = record.progress()?;
            validate_members(&record, &progress, epoch)?;
            let attempt = record.inputs.attempt.evidence_digest()?;
            let enrollment = progress
                .enrollment
                .as_ref()
                .map(NodeLogEnrollmentProof::evidence_digest)
                .transpose()?;
            let refusal = progress
                .refusal
                .as_ref()
                .map(NodeLogEnrollmentRefusalProof::evidence_digest)
                .transpose()?;
            hash_progress(&mut hash, epoch, attempt, enrollment, refusal, &progress)?;
            if cursor.is_some_and(|cursor| epoch <= cursor.after) {
                continue;
            }
            if entries.len() == limit {
                has_more = true;
                continue;
            }
            entries.push(FollowerEnrollmentProgress {
                epoch,
                attempt,
                members: progress.members.clone(),
                native_started: progress.native_started,
                no_effect: progress.no_effect,
                delivered: progress.delivered,
                enrollment,
                refusal,
                retirement: progress.retirement.clone(),
                native_closed: progress.native_closed,
                execution_error: progress.execution_error.clone(),
                journal_error: progress.journal_error.clone(),
            });
        }
        let topology = Digest::from_bytes(*hash.finalize().as_bytes());
        if !after_found || cursor.is_some_and(|cursor| cursor.topology != topology) {
            return Err(Error::Node(
                "follower enrollment inventory changed; restart pagination",
            ));
        }
        let next = if has_more {
            entries
                .last()
                .map(|entry| FollowerEnrollmentInventoryCursor {
                    topology,
                    after: entry.epoch,
                })
        } else {
            None
        };
        Ok(FollowerEnrollmentInventoryPage {
            scope: self.scope,
            node: self.node,
            session: self.session,
            mode,
            observed_at_ms: now_ms,
            topology,
            total_epochs,
            protocol_busy,
            draining,
            pending_epoch,
            entries,
            next,
            _memory: memory,
        })
    }
}

fn validate_members(
    record: &Responsibility,
    progress: &Progress,
    epoch: u64,
) -> cellule_runtime::Result<()> {
    let prepared = record.inputs.attempt.prepared();
    let invalid_member = progress.members.iter().any(|member| {
        !matches!(member.spec.role, EnrollmentRole::Follower { log_epoch } if log_epoch == epoch)
            || member
                .accepted
                .as_ref()
                .is_some_and(|accepted| accepted.spec() != &member.spec)
    });
    if epoch != prepared.log().epoch()
        || progress.members.len() != prepared.followers().len()
        || progress.members.len() > MAX_MEMBERS
        || invalid_member
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

fn hash_progress(
    hash: &mut blake3::Hasher,
    epoch: u64,
    attempt: Digest,
    enrollment: Option<Digest>,
    refusal: Option<Digest>,
    progress: &Progress,
) -> cellule_runtime::Result<()> {
    hash.update(&epoch.to_le_bytes());
    hash.update(attempt.as_bytes());
    hash.update(&[
        u8::from(progress.native_started),
        u8::from(progress.no_effect),
        u8::from(progress.delivered),
        u8::from(progress.native_closed),
        u8::from(progress.execution_error.is_some()),
        u8::from(progress.journal_error.is_some()),
    ]);
    for digest in [enrollment, refusal] {
        hash.update(&[u8::from(digest.is_some())]);
        if let Some(digest) = digest {
            hash.update(digest.as_bytes());
        }
    }
    hash.update(&(progress.members.len() as u64).to_le_bytes());
    for member in &progress.members {
        hash.update(&member.spec.to_bytes().map_err(operation)?);
        hash.update(&[
            u8::from(member.accepted.is_some()),
            u8::from(member.published),
        ]);
        if let Some(accepted) = &member.accepted {
            hash.update(&accepted.to_bytes().map_err(operation)?);
        }
        match member.event {
            None => {
                hash.update(&[0]);
            }
            Some(event) => {
                let (tag, digest) = match event {
                    EnrollmentEvent::Established(digest) => (1, digest),
                    EnrollmentEvent::Refused(digest) => (2, digest),
                    EnrollmentEvent::Retired(digest) => (3, digest),
                };
                hash.update(&[tag]);
                hash.update(digest.as_bytes());
            }
        }
    }
    hash.update(&[u8::from(progress.retirement.is_some())]);
    if let Some(retirement) = &progress.retirement {
        // Each original joined observation is immutable. Bind its process-local
        // identity as well as the barrier; retry replacement must invalidate a
        // continuation even when only an original member error changes.
        hash.update(&(Arc::as_ptr(retirement) as usize).to_le_bytes());
        hash.update(retirement.barrier().leader_session().as_bytes());
        hash.update(&retirement.barrier().log_epoch().to_le_bytes());
        hash.update(&retirement.barrier().covered_through().to_le_bytes());
    }
    Ok(())
}
