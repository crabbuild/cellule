//! Source reader joining composed with actual successor and current reader policy.
use super::*;
use crate::{
    CellNode,
    fleet::FleetFailedReaderClosure,
    read_replicas::{ReaderEnrollmentRetirement, ReaderReplacement},
};
use cellule_runtime::{
    Error, Result,
    cell::{actor::CellServingObservation, catalog::CatalogProof},
    control::{
        Control,
        authority::{CellAuthority, VerifiedRootPrefix},
    },
    fleet::operations::{EnrollmentRecord, EnrollmentRole},
    identity::NodeId,
    ltx::CellReplica,
};
use std::sync::Arc;

mod collection;
mod digest;

/// Durable native or failed-process closure for one original reader request.
/// A failed-process closure proves nonexecution only; current source lineage and
/// replacement policy still have to pass the same successor checks.
#[derive(Clone)]
pub enum FleetSourceReaderRetirement {
    /// Native removal joined all local query/refresh work and retained its final root.
    Native(Arc<ReaderEnrollmentRetirement>),
    /// Application-confirmed original process and accepted-work closure.
    Failed(Arc<FleetFailedReaderClosure>),
}
impl FleetSourceReaderRetirement {
    /// Exact first accepted reader row used by the retirement proof.
    #[must_use]
    pub fn original(&self) -> &EnrollmentRecord {
        match self {
            Self::Native(retirement) => retirement.original(),
            Self::Failed(closure) => closure.original(),
        }
    }
    /// Exact terminal row confirmed by the retirement proof.
    #[must_use]
    pub fn retired(&self) -> &EnrollmentRecord {
        match self {
            Self::Native(retirement) => retirement.retired(),
            Self::Failed(closure) => closure.reader(),
        }
    }
    /// Original bounded proof interval, distinct from the current lookup interval.
    #[must_use]
    pub fn interval(&self) -> (i64, i64) {
        match self {
            Self::Native(retirement) => retirement.interval(),
            Self::Failed(closure) => closure.interval(),
        }
    }
    /// Exact native final root, or the original failed reader's pinned opening root.
    pub fn root(&self) -> Result<cellule_runtime::ltx::RootRef> {
        match self {
            Self::Native(retirement) => Ok(retirement.root()),
            Self::Failed(closure) => {
                let EnrollmentRole::Reader { target, position } = &closure.original().spec().role
                else {
                    return Err(Error::Fenced);
                };
                Ok(position.root.to_ltx(target.cell_id(), position.incarnation))
            }
        }
    }
    /// Lowest prefix this exact retirement requires from its current successor.
    pub fn minimum_commit_sequence(&self) -> u64 {
        match self {
            Self::Native(retirement) => retirement.receipt().commit_sequence,
            Self::Failed(closure) => match &closure.original().spec().role {
                EnrollmentRole::Reader { position, .. } => position.root.commit_sequence,
                _ => 0,
            },
        }
    }
    /// Failed-process evidence when the old receiver lifetime no longer exists.
    #[must_use]
    pub fn failed_process(&self) -> Option<&FleetFailedReaderClosure> {
        match self {
            Self::Native(_) => None,
            Self::Failed(closure) => Some(closure.as_ref()),
        }
    }
    /// Native join capsule when the old receiver remains available.
    #[must_use]
    pub fn native(&self) -> Option<&ReaderEnrollmentRetirement> {
        match self {
            Self::Native(retirement) => Some(retirement.as_ref()),
            Self::Failed(_) => None,
        }
    }
}

/// Authenticated mapping of one retained retirement to its current writer.
/// Providers retain this exact allocation through their existing accepted-work
/// owner. Failed receiver closures are not a substitute for current source or
/// replacement evidence.
pub struct FleetSourceReaderInputs {
    /// Exact native join or application-authenticated failed receiver closure.
    pub retirement: FleetSourceReaderRetirement,
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

/// Read-only application lookup of original closure evidence and current writer.
/// Authenticate both physical endpoints and all canonical backend mappings.
/// Process restart exclusion and durable accepted-work retention remain the
/// provider's responsibility; None remains unknown and cannot infer closure.
pub trait FleetSourceReaderSuccessors: Send + Sync {
    /// None is unknown, never empty work. Return the same retained allocation on
    /// global recheck; substituting a new capsule or mapping fences collection.
    fn successor<'a>(
        &'a self,
        retired: &'a EnrollmentRecord,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<Arc<FleetSourceReaderInputs>>>;
}

/// Fresh current policy for one exact native or failed-process reader closure.
/// This grants no original-writer, accepted-work or finalization rights.
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
    /// Exact native/process retirement evidence retained throughout this observation.
    #[must_use]
    pub fn retirement(&self) -> &FleetSourceReaderRetirement {
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
