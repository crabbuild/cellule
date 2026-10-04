//! Source reader joining composed with actual successor and current reader policy.
use super::*;
use crate::{
    CellNode,
    read_replicas::{ReaderEnrollmentRetirement, ReaderReplacement},
};
use cellule_runtime::{
    cell::{actor::CellServingObservation, catalog::CatalogProof},
    control::{
        Control,
        authority::{CellAuthority, VerifiedRootPrefix},
    },
    fleet::operations::EnrollmentRecord,
    identity::NodeId,
    ltx::CellReplica,
};
use std::sync::Arc;

mod collection;
mod digest;

/// Authenticated mapping of one retained native retirement to its current writer.
/// Providers retain this exact allocation through their existing accepted-work
/// owner. Losing native evidence cannot be repaired from a terminal journal row.
pub struct FleetSourceReaderInputs {
    /// Exact locally joined original reader and last installed root.
    pub retirement: Arc<ReaderEnrollmentRetirement>,
    /// Authenticated physical destination of the current native writer.
    pub node: NodeId,
    /// Actual existing managed host; collection starts no writer acquisition.
    pub host: Arc<CellNode>,
    /// Canonical catalog proof for the original target.
    pub catalog: CatalogProof,
    /// Application-authenticated canonical authority backend.
    pub authority: CellAuthority,
    /// Original Cell/incarnation origin through the existing native I/O path.
    pub replica: CellReplica,
}

/// Read-only application lookup of original native evidence and current writer.
/// Authenticate both physical endpoints and all canonical backend mappings.
/// Process restart exclusion and durable accepted-work retention remain separate
/// requirements; this interface cannot reconstruct a lost native join witness.
pub trait FleetSourceReaderSuccessors: Send + Sync {
    /// None is unknown, never empty work. Return the same retained allocation on
    /// global recheck; substituting a new capsule or mapping fences collection.
    fn successor<'a>(
        &'a self,
        retired: &'a EnrollmentRecord,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<Arc<FleetSourceReaderInputs>>>;
}

/// Fresh current policy for one exact source-side native reader retirement.
/// This grants no original-writer/process/accepted-work or finalization rights.
pub struct FleetSourceReaderCheck {
    inputs: Arc<FleetSourceReaderInputs>,
    serving: CellServingObservation,
    writer_boot: Digest,
    origin: VerifiedRootPrefix,
    authority: Control,
    policy_revision: Option<u64>,
    desired_readers: u16,
    replacements: Vec<ReaderReplacement>,
}
impl FleetSourceReaderCheck {
    /// Original charged local retirement retained throughout this observation.
    #[must_use]
    pub fn retirement(&self) -> &ReaderEnrollmentRetirement {
        &self.inputs.retirement
    }
    /// Authenticated physical successor.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.inputs.node
    }
    /// Immutable signed identity of the actual successor boot.
    #[must_use]
    pub const fn boot_identity(&self) -> Digest {
        self.writer_boot
    }
    /// Actual compiled release checked against the signed successor.
    #[must_use]
    pub fn release(&self) -> Digest {
        self.inputs.host.application().registry().release_digest()
    }
    /// Actual managed native successor observed around all I/O.
    #[must_use]
    pub fn serving(&self) -> &CellServingObservation {
        &self.serving
    }
    /// Exact final installed prefix and complete current origin verification.
    #[must_use]
    pub fn origin(&self) -> &VerifiedRootPrefix {
        &self.origin
    }
    /// Current canonical reader policy revision; absent differs from explicit zero.
    #[must_use]
    pub const fn policy_revision(&self) -> Option<u64> {
        self.policy_revision
    }
    /// Current desired reader count.
    #[must_use]
    pub const fn desired_readers(&self) -> u16 {
        self.desired_readers
    }
    /// Actual ready, enrolled replacements selected by the canonical policy.
    #[must_use]
    pub fn replacements(&self) -> &[ReaderReplacement] {
        &self.replacements
    }
}

/// Complete lookup of eligible source reader requests at one full roster barrier.
/// Missing evidence stays absent for the full maintenance matcher to report.
/// Applications account bounded collector copies; retained retirement capsules
/// and native origin reads retain their existing runtime admission charges.
pub struct FleetSourceReaderPolicies {
    snapshot: FleetJournalSnapshot,
    roster: Digest,
    original: Digest,
    interval: (i64, i64),
    checks: Vec<FleetSourceReaderCheck>,
}
impl FleetSourceReaderPolicies {
    /// Exact full head and registry checked before and after collection.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Full roster including original terminal and Pending rows.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// Exact immutable original/current request capture consumed by this lookup.
    #[must_use]
    pub const fn original_digest(&self) -> Digest {
        self.original
    }
    /// Checked requests only; unknown requests remain obligations in the matcher.
    #[must_use]
    pub fn checks(&self) -> &[FleetSourceReaderCheck] {
        &self.checks
    }
    /// Original fresh interval, separate from historical native retirement times.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.interval
    }
}
