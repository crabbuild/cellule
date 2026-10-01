use crate::identity::{Digest, NodeId};

use super::{
    EnrollmentRecord, FleetHead, FleetScope, MAX_PAGE_ENTRIES, MaintenancePhase, MoveAttemptSpec,
    NodeIntent, OperationError, Result, nonzero,
};
use crate::node::NodeMode;

/// Revision shared by retained physical-node intents and all enrollment rows.
/// The adapter advances it in the same transaction as every registry mutation.
/// Readers must recheck it after collecting every required observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegistryVersion {
    pub(super) scope: FleetScope,
    pub(super) revision: u64,
    pub(super) bootstrap_revision: Option<u64>,
    pub(super) scheduling_enabled: bool,
}

impl RegistryVersion {
    /// Creates an unbootstrapped empty registry. It cannot prove complete coverage.
    pub fn new(scope: FleetScope) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            scope,
            revision: 0,
            bootstrap_revision: None,
            scheduling_enabled: false,
        })
    }
    /// Calculates the revision committed with a row or intent mutation.
    pub fn advance(self, expected_revision: u64) -> Result<Self> {
        self.validate()?;
        if expected_revision != self.revision {
            return Err(OperationError::Conflict);
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(OperationError::Invalid("registry revision overflow"))?;
        Ok(Self { revision, ..self })
    }
    /// Records the controlled initial coverage barrier. The application pauses
    /// enrollment and imports every existing obligation before committing this.
    /// Merely invoking this method supplies no evidence that coverage is complete.
    pub fn bootstrap(self, expected_revision: u64) -> Result<Self> {
        self.validate()?;
        if expected_revision != self.revision {
            return Err(OperationError::Conflict);
        }
        if self.bootstrap_revision.is_some() {
            return Ok(self);
        }
        let mut next = self.advance(expected_revision)?;
        next.bootstrap_revision = Some(next.revision);
        Ok(next)
    }

    /// Stops or resumes allocation of new planned movement in the same registry
    /// transaction. Accepted actions, permits, and retained cordons are unchanged.
    /// Enabling requires initial coverage; it is not deployment qualification.
    pub fn set_scheduling(self, expected_revision: u64, enabled: bool) -> Result<Self> {
        self.validate()?;
        if expected_revision != self.revision {
            return Err(OperationError::Conflict);
        }
        if enabled && self.bootstrap_revision.is_none() {
            return Err(OperationError::Invalid(
                "cannot schedule before registry bootstrap",
            ));
        }
        if enabled == self.scheduling_enabled {
            return Ok(self);
        }
        Ok(Self {
            scheduling_enabled: enabled,
            ..self.advance(expected_revision)?
        })
    }

    /// Returns whether the shared transaction may allocate a new move permit.
    /// Existing accepted attempts still reconcile while this is false.
    #[must_use]
    pub const fn scheduling_enabled(self) -> bool {
        self.scheduling_enabled
    }

    /// Checks allocation against the retained rows inside the head transaction.
    /// Invoke before the head's `Allocate` transition; that transition still
    /// checks controller fencing, sequence, deadline, and count/byte permits.
    /// A draining source may move only for its current evacuation operation.
    pub fn authorize_allocation(
        self,
        head: &FleetHead,
        spec: &MoveAttemptSpec,
        source: &NodeIntent,
        destination: &NodeIntent,
    ) -> Result<()> {
        self.validate()?;
        spec.validate()?;
        source.validate()?;
        destination.validate()?;
        if !self.scheduling_enabled {
            return Err(OperationError::Stopped);
        }
        if head.scope() != self.scope
            || spec.target.application() != self.scope.application
            || source.scope() != self.scope
            || destination.scope() != self.scope
            || source.node() != spec.source_node
            || source.session() != spec.source
            || destination.node() != spec.destination_node
            || destination.session() != spec.destination
            || destination.mode() != NodeMode::Active
        {
            return Err(OperationError::Conflict);
        }
        if source.mode() != NodeMode::Active
            && head.maintenance().is_none_or(|operation| {
                source.operation() != Some(operation.id())
                    || spec.id.operation != operation.id()
                    || operation.node() != source.node()
                    || operation.session() != source.session()
                    || operation.intent_revision() != source.revision()
                    || operation.phase() != MaintenancePhase::Evacuating
            })
        {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
    /// Returns the journal/application namespace.
    #[must_use]
    pub const fn scope(self) -> FleetScope {
        self.scope
    }
    /// Returns the stable-scan revision, including failed-session obligations.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }
    /// Returns the first committed coverage barrier, retained after mutations.
    #[must_use]
    pub const fn bootstrap_revision(self) -> Option<u64> {
        self.bootstrap_revision
    }
    /// Requires an identical bootstrapped revision after collecting observations.
    /// A filtered scan or omitted row cannot satisfy this check on its own.
    pub fn confirm(self, current: Self) -> Result<()> {
        self.validate()?;
        current.validate()?;
        if self.bootstrap_revision.is_none() {
            return Err(OperationError::Invalid(
                "registry coverage is unbootstrapped",
            ));
        }
        if self != current {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
    pub(super) fn validate(self) -> Result<()> {
        self.scope.validate()?;
        if (self.scheduling_enabled && self.bootstrap_revision.is_none())
            || self
                .bootstrap_revision
                .is_some_and(|revision| revision == 0 || revision > self.revision)
        {
            return Err(OperationError::Invalid(
                "invalid registry bootstrap revision",
            ));
        }
        Ok(())
    }
}

/// Bounded retained physical-node intents. Cursors bind to the shared version.
/// The adapter includes older cordons, not only the head's current operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentPage {
    pub(super) version: RegistryVersion,
    pub(super) after: Option<NodeId>,
    pub(super) entries: Vec<NodeIntent>,
    pub(super) next: Option<NodeId>,
}

impl IntentPage {
    /// Builds a sorted registry page from one consistent transaction.
    pub fn new(
        version: RegistryVersion,
        after: Option<NodeId>,
        entries: Vec<NodeIntent>,
        next: Option<NodeId>,
    ) -> Result<Self> {
        let page = Self {
            version,
            after,
            entries,
            next,
        };
        page.validate()?;
        Ok(page)
    }
    /// Returns the revision to repeat in the next request and final barrier.
    #[must_use]
    pub const fn version(&self) -> RegistryVersion {
        self.version
    }
    /// Returns the exclusive starting physical identity requested by this page.
    #[must_use]
    pub const fn after(&self) -> Option<NodeId> {
        self.after
    }
    /// Returns at most 128 intents in ascending physical-node order.
    #[must_use]
    pub fn entries(&self) -> &[NodeIntent] {
        &self.entries
    }
    /// Returns the last emitted key only when another page remains.
    #[must_use]
    pub const fn next(&self) -> Option<NodeId> {
        self.next
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.version.validate()?;
        if self.version.revision == 0 && !self.entries.is_empty() {
            return Err(OperationError::Invalid(
                "rows exist before the first registry commit",
            ));
        }
        let mut keys = Vec::with_capacity(self.entries.len().min(MAX_PAGE_ENTRIES));
        if self.entries.len() > MAX_PAGE_ENTRIES {
            return Err(OperationError::Invalid("intent page exceeds entry bound"));
        }
        for intent in &self.entries {
            intent.validate()?;
            if intent.scope() != self.version.scope {
                return Err(OperationError::Invalid("intent page scope differs"));
            }
            keys.push(*intent.node().as_bytes());
        }
        validate_page(
            self.after.map(|v| *v.as_bytes()),
            &keys,
            self.next.map(|v| *v.as_bytes()),
        )
    }
}

/// Bounded complete-registry scan, including pending, retired, and failed boots.
/// Tombstones remain visible to preserve request identity and original evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentPage {
    pub(super) version: RegistryVersion,
    pub(super) after: Option<Digest>,
    pub(super) entries: Vec<EnrollmentRecord>,
    pub(super) next: Option<Digest>,
}

impl EnrollmentPage {
    /// Builds a sorted page without treating lease expiry as retirement.
    pub fn new(
        version: RegistryVersion,
        after: Option<Digest>,
        entries: Vec<EnrollmentRecord>,
        next: Option<Digest>,
    ) -> Result<Self> {
        let page = Self {
            version,
            after,
            entries,
            next,
        };
        page.validate()?;
        Ok(page)
    }
    /// Returns the shared revision to recheck after the scan.
    #[must_use]
    pub const fn version(&self) -> RegistryVersion {
        self.version
    }
    /// Returns the exclusive request-index starting key.
    #[must_use]
    pub const fn after(&self) -> Option<Digest> {
        self.after
    }
    /// Returns at most 128 obligations in ascending stable-index order.
    #[must_use]
    pub fn entries(&self) -> &[EnrollmentRecord] {
        &self.entries
    }
    /// Returns the last emitted key only when another page remains.
    #[must_use]
    pub const fn next(&self) -> Option<Digest> {
        self.next
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.version.validate()?;
        if self.version.revision == 0 && !self.entries.is_empty() {
            return Err(OperationError::Invalid(
                "rows exist before the first registry commit",
            ));
        }
        let mut keys = Vec::with_capacity(self.entries.len().min(MAX_PAGE_ENTRIES));
        if self.entries.len() > MAX_PAGE_ENTRIES {
            return Err(OperationError::Invalid(
                "enrollment page exceeds entry bound",
            ));
        }
        for record in &self.entries {
            record.validate()?;
            if record.spec().scope != self.version.scope {
                return Err(OperationError::Invalid("enrollment page scope differs"));
            }
            keys.push(*record.spec().key()?.as_bytes());
        }
        validate_page(
            self.after.map(|v| *v.as_bytes()),
            &keys,
            self.next.map(|v| *v.as_bytes()),
        )
    }
}

fn validate_page<const N: usize>(
    after: Option<[u8; N]>,
    keys: &[[u8; N]],
    next: Option<[u8; N]>,
) -> Result<()> {
    if after.is_some_and(|v| !nonzero(&v))
        || next.is_some_and(|v| !nonzero(&v))
        || keys.len() > MAX_PAGE_ENTRIES
        || keys.windows(2).any(|w| w[0] >= w[1])
        || keys
            .first()
            .is_some_and(|key| after.is_some_and(|a| *key <= a))
        || next.is_some_and(|n| keys.last().copied() != Some(n))
        || (keys.is_empty() && after.is_some())
    {
        return Err(OperationError::Invalid(
            "invalid registry page cursor or ordering",
        ));
    }
    Ok(())
}
