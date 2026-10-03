//! Immutable follower replacement history; current authority remains separate.
use super::*;
use crate::identity::Digest;
use std::collections::HashSet;

mod validation;

/// Application redundancy policy in the fleet journal transaction domain.
/// The current native node-log protocol supports one or two members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerReplacementPolicy {
    pub(super) scope: FleetScope,
    pub(super) revision: u64,
    pub(super) minimum_members: u8,
}
impl FollowerReplacementPolicy {
    /// Validates shape; the application authorizes durable policy changes.
    pub fn new(scope: FleetScope, revision: u64, minimum_members: u8) -> Result<Self> {
        let policy = Self {
            scope,
            revision,
            minimum_members,
        };
        policy.validate()?;
        Ok(policy)
    }
    /// Journal/application namespace.
    #[must_use]
    pub const fn scope(self) -> FleetScope {
        self.scope
    }
    /// Monotonic policy revision; equal counts can still name different policy.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }
    /// Required native member count outside the maintenance node.
    #[must_use]
    pub const fn minimum_members(self) -> u8 {
        self.minimum_members
    }
}

/// Complete original Established replacement request and signed boot identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerReplacementWitness {
    /// Original member enrollment, including acceptance and establishment history.
    pub enrollment: EnrollmentRecord,
    /// Immutable signed boot identity, excluding renewable measurements.
    pub boot_identity: Digest,
}

/// Historical live-owner rotation and complete replacement ensemble.
/// Decoding supplies no native retirement, authentication or finalization rights.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerEvacuationRecord {
    pub(super) operation: MaintenanceOperation,
    pub(super) head_digest: Digest,
    pub(super) registry: RegistryVersion,
    pub(super) policy: FollowerReplacementPolicy,
    pub(super) original_key: Digest,
    pub(super) original_digest: Digest,
    pub(super) retired: Vec<EnrollmentRecord>,
    pub(super) covered_through: u64,
    pub(super) source_boot: Digest,
    pub(super) replacement_epoch: u64,
    pub(super) replacement_evidence: Digest,
    pub(super) replacements: Vec<FollowerReplacementWitness>,
    pub(super) started_at_ms: i64,
    pub(super) finished_at_ms: i64,
}
impl FollowerEvacuationRecord {
    /// Builds bounded history from trusted native capture; shape alone is advisory.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation: MaintenanceOperation,
        barrier: (Digest, RegistryVersion),
        policy: FollowerReplacementPolicy,
        original: (Digest, Digest),
        mut retired: Vec<EnrollmentRecord>,
        covered_through: u64,
        source_boot: Digest,
        replacement: (u64, Digest),
        mut replacements: Vec<FollowerReplacementWitness>,
        interval: (i64, i64),
    ) -> Result<Self> {
        retired.sort_by_key(|row| *row.spec().target.node.as_bytes());
        replacements.sort_by_key(|entry| *entry.enrollment.spec().target.node.as_bytes());
        let record = Self {
            operation,
            head_digest: barrier.0,
            registry: barrier.1,
            policy,
            original_key: original.0,
            original_digest: original.1,
            retired,
            covered_through,
            source_boot,
            replacement_epoch: replacement.0,
            replacement_evidence: replacement.1,
            replacements,
            started_at_ms: interval.0,
            finished_at_ms: interval.1,
        };
        record.validate()?;
        Ok(record)
    }
    /// Full original operation, including its session and deadline.
    #[must_use]
    pub fn operation(&self) -> &MaintenanceOperation {
        &self.operation
    }
    /// Full original journal head identity.
    #[must_use]
    pub const fn head_digest(&self) -> Digest {
        self.head_digest
    }
    /// Original complete registry barrier.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
    /// Authoritative application policy observed during this capture.
    #[must_use]
    pub const fn policy(&self) -> FollowerReplacementPolicy {
        self.policy
    }
    /// Exact donor request key; latest pointers never conflate sibling members.
    #[must_use]
    pub const fn original_key(&self) -> Digest {
        self.original_key
    }
    /// Digest of the original Established donor request, before retirement.
    #[must_use]
    pub const fn original_digest(&self) -> Digest {
        self.original_digest
    }
    /// Complete original retired ensemble in physical member order.
    #[must_use]
    pub fn retired(&self) -> &[EnrollmentRecord] {
        &self.retired
    }
    /// Original contiguous object-covered retirement watermark.
    #[must_use]
    pub const fn covered_through(&self) -> u64 {
        self.covered_through
    }
    /// Immutable signed original leader boot identity.
    #[must_use]
    pub const fn source_boot(&self) -> Digest {
        self.source_boot
    }
    /// Newer canonical ensemble epoch captured after original rotation.
    #[must_use]
    pub const fn replacement_epoch(&self) -> u64 {
        self.replacement_epoch
    }
    /// Original canonical enrollment proof identity, without restamping.
    #[must_use]
    pub const fn replacement_evidence(&self) -> Digest {
        self.replacement_evidence
    }
    /// Every original Established replacement and signed boot identity.
    #[must_use]
    pub fn replacements(&self) -> &[FollowerReplacementWitness] {
        &self.replacements
    }
    /// Original source endpoint; decoding has already validated its presence.
    pub fn source(&self) -> Result<EnrollmentEndpoint> {
        self.retired
            .first()
            .and_then(|row| row.spec().source)
            .ok_or(OperationError::Invalid("follower source is absent"))
    }
    /// Original epoch; every original member belongs to this same epoch.
    pub fn original_epoch(&self) -> Result<u64> {
        match self.retired.first().map(|row| &row.spec().role) {
            Some(EnrollmentRole::Follower { log_epoch }) => Ok(*log_epoch),
            _ => Err(OperationError::Invalid("follower original epoch is absent")),
        }
    }
    /// Original capture interval; replay cannot renew it.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
    /// Canonical immutable content identity.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests;
