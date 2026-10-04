//! Durable historical reader replacement evidence, never current authority.
use super::*;
use crate::{
    control::{Control, ControlState},
    identity::{Digest, NodeId, SessionId},
    read_policy::MAX_READERS,
};
use std::collections::HashSet;

mod validation;
pub(super) const MAX_READER_PAGES: usize = (MAX_READERS as usize).div_ceil(MAX_PAGE_ENTRIES);

/// Original enrolled reader boot and observed prefix outside the donor.
/// Digests bind authenticated adapter observations; decoding does not prove them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderReplacementWitness {
    /// Physical replacement node, distinct from every other replacement.
    pub node: NodeId,
    /// Exact original receiver boot.
    pub session: SessionId,
    /// Immutable signed boot identity, excluding renewable sample fields.
    pub boot_identity: Digest,
    /// Exact original reader enrollment request key.
    pub enrollment_key: Digest,
    /// Digest of that complete Established row, including original history.
    pub enrollment_digest: Digest,
    /// Ready native prefix in the manifest's Cell incarnation.
    pub commit_sequence: u64,
}

/// Immutable replacement page with an exact manifest basis and ordinal.
/// At most 128 entries; the complete manifest validates all pages together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderEvacuationPage {
    pub(super) basis: Digest,
    pub(super) ordinal: u32,
    pub(super) entries: Vec<ReaderReplacementWitness>,
}
impl ReaderEvacuationPage {
    /// Exact immutable capture basis shared by every page.
    #[must_use]
    pub const fn basis(&self) -> Digest {
        self.basis
    }
    /// Zero-based ordinal; skipped, repeated and reordered pages refuse.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    /// Original replacement identities and observed prefixes.
    #[must_use]
    pub fn entries(&self) -> &[ReaderReplacementWitness] {
        &self.entries
    }
    /// Canonical content identity checked against the manifest.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
}

/// Bounded immutable reader evacuation manifest plus paged replacement evidence.
/// Historical capture is persisted once; fresh readiness, authority, policy,
/// roster and operation checks are required before consuming it as settlement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderEvacuationRecord {
    pub(super) operation: MaintenanceOperation,
    pub(super) head_digest: Digest,
    pub(super) registry: RegistryVersion,
    pub(super) retired: EnrollmentRecord,
    pub(super) original_digest: Digest,
    pub(super) authority: Control,
    pub(super) policy_revision: Option<u64>,
    pub(super) desired_readers: u16,
    pub(super) minimum_sequence: u64,
    pub(super) started_at_ms: i64,
    pub(super) finished_at_ms: i64,
    pub(super) pages: Vec<Digest>,
}
impl ReaderEvacuationRecord {
    /// Validates shape and builds canonical pages for the entire policy bound.
    /// This constructor supplies no native closure or adapter authentication.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation: MaintenanceOperation,
        barrier: (Digest, RegistryVersion),
        retired: EnrollmentRecord,
        original_digest: Digest,
        authority: Control,
        policy_revision: Option<u64>,
        desired_readers: u16,
        minimum_sequence: u64,
        interval: (i64, i64),
        mut replacements: Vec<ReaderReplacementWitness>,
    ) -> Result<(Self, Vec<ReaderEvacuationPage>)> {
        let mut record = Self {
            operation,
            head_digest: barrier.0,
            registry: barrier.1,
            retired,
            original_digest,
            authority,
            policy_revision,
            desired_readers,
            minimum_sequence,
            started_at_ms: interval.0,
            finished_at_ms: interval.1,
            pages: Vec::new(),
        };
        record.validate_basis()?;
        if replacements.len() != usize::from(desired_readers) {
            return Err(OperationError::Invalid("reader replacement count differs"));
        }
        replacements.sort_by_key(|replacement| *replacement.node.as_bytes());
        let basis = record.basis_digest()?;
        let pages: Vec<_> = replacements
            .chunks(MAX_PAGE_ENTRIES)
            .enumerate()
            .map(|(ordinal, entries)| ReaderEvacuationPage {
                basis,
                ordinal: ordinal as u32,
                entries: entries.to_vec(),
            })
            .collect();
        record.pages = pages
            .iter()
            .map(ReaderEvacuationPage::digest)
            .collect::<Result<_>>()?;
        record.validate_pages(&pages)?;
        Ok((record, pages))
    }
    /// Exact original operation; deadline/session extensions require fresh capture.
    #[must_use]
    pub fn operation(&self) -> &MaintenanceOperation {
        &self.operation
    }
    /// Digest of the full original head checked before capture publication.
    #[must_use]
    pub const fn head_digest(&self) -> Digest {
        self.head_digest
    }
    /// Exact original registry version, retained independently of fresh rechecks.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
    /// Original Retired request retaining acceptance and establishment history.
    #[must_use]
    pub fn retired(&self) -> &EnrollmentRecord {
        &self.retired
    }
    /// Digest of the original Established request supplied to native closure.
    #[must_use]
    pub const fn original_digest(&self) -> Digest {
        self.original_digest
    }
    /// Original serving authority capture, without ownership permission.
    #[must_use]
    pub fn authority(&self) -> &Control {
        &self.authority
    }
    /// Exact policy revision, or absence meaning zero desired readers.
    #[must_use]
    pub const fn policy_revision(&self) -> Option<u64> {
        self.policy_revision
    }
    /// Original required replacement count.
    #[must_use]
    pub const fn desired_readers(&self) -> u16 {
        self.desired_readers
    }
    /// Prefix of the exact retired native view that every replacement must cover.
    #[must_use]
    pub const fn minimum_sequence(&self) -> u64 {
        self.minimum_sequence
    }
    /// Original capture interval; replay cannot refresh it.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
    /// Complete ordered page identities; empty only under zero-reader policy.
    #[must_use]
    pub fn pages(&self) -> &[Digest] {
        &self.pages
    }
    /// Canonical immutable capture identity, including every page digest.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }

    /// Requires every exact page and unique physical boot, never a partial list.
    pub fn validate_pages(&self, pages: &[ReaderEvacuationPage]) -> Result<()> {
        self.validate()?;
        if pages.len() != self.pages.len() {
            return Err(OperationError::Invalid(
                "reader replacement pages incomplete",
            ));
        }
        let basis = self.basis_digest()?;
        let mut nodes = HashSet::new();
        let mut sessions = HashSet::new();
        let mut requests = HashSet::new();
        let mut after = None;
        let mut count = 0;
        for (ordinal, (page, digest)) in pages.iter().zip(&self.pages).enumerate() {
            page.validate()?;
            let remaining = usize::from(self.desired_readers) - count;
            if page.basis != basis
                || page.ordinal as usize != ordinal
                || page.digest()? != *digest
                || page.entries.len() != remaining.min(MAX_PAGE_ENTRIES)
            {
                return Err(OperationError::Conflict);
            }
            for entry in &page.entries {
                let node = *entry.node.as_bytes();
                if after.is_some_and(|previous| node <= previous)
                    || !nodes.insert(entry.node)
                    || !sessions.insert(entry.session)
                    || !requests.insert(entry.enrollment_key)
                    || entry.node == self.operation.node()
                    || entry.session == self.operation.session()
                    || entry.session == self.retired.spec().target.session
                    || self
                        .authority
                        .owner
                        .as_ref()
                        .is_some_and(|owner| owner.session == entry.session)
                    || entry.commit_sequence < self.minimum_sequence
                {
                    return Err(OperationError::Invalid(
                        "reader replacement identity or prefix differs",
                    ));
                }
                after = Some(node);
            }
            count += page.entries.len();
        }
        if count != usize::from(self.desired_readers) {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
