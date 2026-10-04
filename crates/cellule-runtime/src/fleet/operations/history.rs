use crate::identity::Digest;

use super::{
    FleetScope, MAX_PAGE_ENTRIES, MoveAttempt, OperationError, OperationId, Result, nonzero,
};

/// Immutable progress chain reachable from the CAS-published fleet head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressHead {
    /// Digest of the last canonical page published with permit retirement.
    pub digest: Digest,
    /// Monotonic page number across this fleet journal.
    pub sequence: u64,
}

impl ProgressHead {
    pub(super) fn validate(self) -> Result<()> {
        if self.sequence == 0 || !nonzero(self.digest.as_bytes()) {
            return Err(OperationError::Invalid("invalid progress head"));
        }
        Ok(())
    }
}

/// Bounded immutable terminal attempts, including incarnation and movement time.
///
/// The adapter must persist this page before CAS-publishing the head that
/// retires its attempts. A page PUT alone is not committed progress. A failed
/// CAS leaves an unreachable page and retains all original movement permits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressPage {
    pub(super) scope: FleetScope,
    pub(super) operation: OperationId,
    pub(super) sequence: u64,
    pub(super) previous: Option<Digest>,
    pub(super) entries: Vec<MoveAttempt>,
}

impl ProgressPage {
    /// Constructs a canonical page containing only completed, cleaned attempts.
    pub fn new(
        scope: FleetScope,
        operation: OperationId,
        sequence: u64,
        previous: Option<Digest>,
        entries: Vec<MoveAttempt>,
    ) -> Result<Self> {
        let page = Self {
            scope,
            operation,
            sequence,
            previous,
            entries,
        };
        page.validate()?;
        Ok(page)
    }

    /// Returns the exact journal scope.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the operation whose history this page contains.
    #[must_use]
    pub const fn operation(&self) -> OperationId {
        self.operation
    }
    /// Returns the monotonically assigned fleet page number.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the preceding committed page's digest.
    #[must_use]
    pub const fn previous(&self) -> Option<Digest> {
        self.previous
    }
    /// Returns bounded terminal attempts in ascending allocation order.
    #[must_use]
    pub fn entries(&self) -> &[MoveAttempt] {
        &self.entries
    }
    /// Returns the digest the adapter must publish with the successor head.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if !nonzero(self.operation.as_bytes())
            || self.sequence == 0
            || (self.sequence == 1) != self.previous.is_none()
            || self
                .previous
                .is_some_and(|digest| !nonzero(digest.as_bytes()))
            || self.entries.is_empty()
            || self.entries.len() > MAX_PAGE_ENTRIES
        {
            return Err(OperationError::Invalid("invalid progress page"));
        }
        let mut previous = 0;
        for attempt in &self.entries {
            attempt.validate()?;
            if attempt
                .recovered()
                .is_some_and(|e| e.recovery.basis().scope != self.scope)
            {
                return Err(OperationError::Invalid(
                    "recovery history belongs to another fleet",
                ));
            }
            if !attempt.can_retire()
                || attempt.spec.id.operation != self.operation
                || attempt.spec.target.application() != self.scope.application
                || attempt.spec.id.sequence <= previous
            {
                return Err(OperationError::Invalid(
                    "unproven or unordered historical attempt",
                ));
            }
            previous = attempt.spec.id.sequence;
        }
        Ok(())
    }
}
