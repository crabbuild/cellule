//! Complete current native successors for every retained original writer.
use super::*;
use crate::CellNode;
use cellule_runtime::{
    cell::{actor::CellServingObservation, catalog::CatalogProof},
    control::authority::{VerifiedRecoveryPrefix, VerifiedRootPrefix},
    fleet::operations::OriginalWriterObservation,
    identity::NodeId,
    ltx::CellReplica,
    recovery::manifest::RecoveryManifestStore,
};

mod collection;
mod digest;
mod prefix;

/// Read-only trusted composition for an original writer's current successor.
/// Authenticate the physical host and every canonical backend independently.
/// No caller-provided path, acquisition or recovery effect is accepted here.
#[derive(Clone)]
pub struct FleetOriginalWriterSuccessorInputs {
    /// Authenticated physical node hosting this original Cell's current writer.
    pub node: NodeId,
    /// The actual existing host; its native admission and startup are checked.
    pub host: Arc<CellNode>,
    /// Canonical catalog proof for the exact original tenant/application target.
    pub catalog: CatalogProof,
    /// Canonical authority in that target's application backend.
    pub authority: CellAuthority,
    /// Exact Cell/incarnation origin through the existing shared runtime I/O.
    pub replica: CellReplica,
    /// Authenticated historical manifest backend for an inherited overlay.
    pub manifests: RecoveryManifestStore,
}

/// Application-authenticated lookup across every original application/tenant.
/// Lookup is read-only; it cannot recover/acquire/bootstrap a writer. Providers
/// retain accepted work through their existing finite owner and authenticate
/// original target, physical/session identity and all canonical backend mappings.
pub trait FleetOriginalWriterSuccessors: Send + Sync {
    /// Resolves an actual existing native successor at this full journal barrier.
    fn successor<'a>(
        &'a self,
        original: &'a OriginalWriterObservation,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, FleetOriginalWriterSuccessorInputs>;
}

/// Exact original root and every required suffix verified against one current
/// native writer. Origin availability is always checked, including rootless
/// original owners. This point observation grants no retention pin or shutdown.
pub struct FleetOriginalWriterSuccessorProof {
    original: OriginalWriterObservation,
    node: NodeId,
    release: Digest,
    serving: CellServingObservation,
    origin: VerifiedRootPrefix,
    suffixes: Vec<VerifiedRecoveryPrefix>,
}
impl FleetOriginalWriterSuccessorProof {
    /// Exact original epoch/Control, without substituting a newer owner.
    #[must_use]
    pub fn original(&self) -> &OriginalWriterObservation {
        &self.original
    }
    /// Authenticated current physical destination.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Compiled release of the actual native host checked against its signed boot.
    #[must_use]
    pub const fn release(&self) -> Digest {
        self.release
    }
    /// Native FIFO/authority/generation observation repeated after origin work.
    #[must_use]
    pub fn serving(&self) -> &CellServingObservation {
        &self.serving
    }
    /// Original root derivation, or current origin identity for a rootless input.
    #[must_use]
    pub fn origin(&self) -> &VerifiedRootPrefix {
        &self.origin
    }
    /// Original sealed and inherited overlays, retaining their manifest epochs.
    #[must_use]
    pub fn suffixes(&self) -> &[VerifiedRecoveryPrefix] {
        &self.suffixes
    }
}

/// Complete original writer/suffix/current-serving collection. All rows are
/// verified before return, followed by a global successor recheck and original
/// process/log/full-journal revalidation. A missing row or failed proof refuses
/// the entire set; an empty result requires a committed complete empty input.
/// Reader/follower replacement policy and accepted-work barriers remain separate
/// requirements before role settlement or finalization. Applications account
/// bounded collector buffers; native origin memory/I/O uses existing admission.
pub struct FleetOriginalWriterSuccessorInventory {
    original: FleetOriginalBootSuffixInventory,
    proofs: Vec<FleetOriginalWriterSuccessorProof>,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetOriginalWriterSuccessorInventory {
    /// Complete original boot/process/writer/manifest basis confirmed afterwards.
    #[must_use]
    pub fn original(&self) -> &FleetOriginalBootSuffixInventory {
        &self.original
    }
    /// Every original epoch, including object-covered and rootless writers.
    #[must_use]
    pub fn proofs(&self) -> &[FleetOriginalWriterSuccessorProof] {
        &self.proofs
    }
    /// Original collection through the final global rechecks, without restamping.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}
