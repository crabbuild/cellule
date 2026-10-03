//! Durable follower replacement history in the existing fleet journal domain.
use super::*;
use cellule_runtime::{
    fleet::operations::{FollowerEvacuationRecord, FollowerReplacementPolicy, OperationId},
    identity::Digest,
};

mod native;
mod publication;
mod refresh;
mod verification;
pub use publication::FleetFollowerEvacuationPublication;
pub use refresh::FleetFollowerEvacuationCandidate;
pub use verification::{FleetFollowerEvacuationCheck, FleetFollowerEvacuationVerifier};

/// Immutable follower captures and authoritative policy share the fleet registry.
/// Each mutation must use the same transaction domain and accepted backend owner
/// as enrollment/actions. Applications authorize policy changes and account buffers.
pub trait FleetFollowerEvacuationJournal: FleetJournal {
    /// Loads current application redundancy policy at the complete expected barrier.
    /// Absence is an explicit blocker; it never means zero required redundancy.
    fn follower_replacement_policy<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<FollowerReplacementPolicy>>;
    /// Atomically compares the complete barrier and live controller, then stores
    /// policy revision one or exactly the current revision plus one. Advance the
    /// registry in the same transaction; never silently lower application policy.
    fn set_follower_replacement_policy<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        policy: FollowerReplacementPolicy,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FollowerReplacementPolicy>;
    /// Commits complete original/replacement rows and the latest request pointer
    /// after full barrier/current operation/controller/policy and intent checks.
    /// Exact historical replay returns original bytes without advancing the
    /// registry or restoring a superseded pointer; original times never refresh.
    fn persist_follower_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a FollowerEvacuationRecord,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FollowerEvacuationRecord>;
    /// Loads immutable exact history; validate its requested scope and full digest.
    fn load_follower_evacuation(
        &self,
        scope: cellule_runtime::fleet::operations::FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<FollowerEvacuationRecord>>;
    /// Reads the current pointer at the full barrier. Orphan PUTs grant no evidence.
    fn latest_follower_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        original: Digest,
    ) -> FleetAdapterFuture<'a, Option<FollowerEvacuationRecord>>;
}
