//! Durable reader replacement history and independent current confirmation.
use super::*;
use cellule_runtime::{
    fleet::operations::{OperationId, ReaderEvacuationPage, ReaderEvacuationRecord},
    identity::Digest,
};

mod current;
mod source;
pub use source::{
    FleetSourceReaderCheck, FleetSourceReaderInputs, FleetSourceReaderPolicies,
    FleetSourceReaderRetirement, FleetSourceReaderSuccessors,
};
mod maintenance;
mod publication;
mod refresh;
pub use refresh::FleetReaderEvacuationCandidate;
mod verification;
pub use publication::FleetReaderEvacuationPublication;
pub use verification::{FleetReaderEvacuationCheck, FleetReaderEvacuationVerifier};

/// Reader evidence transactions in the existing fleet journal domain.
/// Immutable manifests/pages and the latest per-operation/request pointer commit
/// together with registry advancement. Historical records never grant settlement.
pub trait FleetReaderEvacuationJournal: FleetJournal {
    /// Atomically compares the full original head/registry, current Evacuating/Closing
    /// operation, live controller, exact Retired original and Established
    /// replacement rows/intents, then commits every checked page and manifest.
    /// Advance the shared registry with the latest pointer. Exact duplicate
    /// manifests return original history without advancing or restoring an old
    /// pointer; ambiguous replies require lookup of that same digest first.
    fn persist_reader_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a ReaderEvacuationRecord,
        pages: &'a [ReaderEvacuationPage],
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ReaderEvacuationRecord>;
    /// Loads immutable historical metadata, checking requested scope/digest.
    fn load_reader_evacuation(
        &self,
        scope: cellule_runtime::fleet::operations::FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ReaderEvacuationRecord>>;
    /// Loads an immutable page; its digest, basis and ordinal must still be
    /// verified against the complete manifest before consuming any entries.
    fn load_reader_evacuation_page(
        &self,
        scope: cellule_runtime::fleet::operations::FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ReaderEvacuationPage>>;
    /// Reads the latest committed witness for one original request at the full
    /// expected snapshot. No filtered registry view or orphan PUT contributes.
    fn latest_reader_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        original: Digest,
    ) -> FleetAdapterFuture<'a, Option<ReaderEvacuationRecord>>;
}
