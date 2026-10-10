//! One ordered catalog lifetime shared by immutable staging and live selection.
use super::*;
use crate::node::bundle::{BundlePreparation, PreparedNodeBundle, StagedNodeBundle};

/// Original catalog ordering held across at most two publication cohorts.
/// Immutable operations may overlap; selection remains ordered and freshly
/// verified. Hold no heartbeat/state lock through staging or upload.
pub trait NodeBundlePublicationRound: Send + Sync {
    /// Encodes a complete cohort, optionally following the exact staged first
    /// cohort. It grants no availability or authority. Checkpoints are applied
    /// only after observing their original complete materialized root.
    fn stage<'a>(
        &'a self,
        previous: Option<&'a StagedNodeBundle>,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        preparation: Option<&'a mut BundlePreparation>,
    ) -> BoxFuture<'a, Result<Arc<StagedNodeBundle>>>;
    /// Uploads immutable bytes. The managed producer joins accepted I/O even
    /// after fencing or a sibling failure. No PUT result grants coverage.
    fn upload<'a>(
        &'a self,
        staged: Arc<StagedNodeBundle>,
    ) -> BoxFuture<'a, Result<PreparedNodeBundle>>;
    /// Verifies complete fresh origin dependencies and selects the exact next
    /// predecessor. Preserve intervening heartbeat/coverage updates and check
    /// the original process lease before and after I/O and CAS.
    fn select<'a>(
        &'a self,
        prepared: &'a PreparedNodeBundle,
        prefixes: &'a [&'a BundleCoverageProof],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>>;
}

/// Canonical authority for the runtime-owned publication producer.
/// Binding, checkpoint, close and publication rounds share catalog ordering;
/// heartbeat renewal continues on the separate original state lock.
pub trait NodeBundlePublicationAuthority: NodeBundleAuthority {
    /// Maximum selected receipt allocation for a cohort, before overlapping
    /// I/O starts. Use `NodeDirectory::bundle_receipt_memory_bound` for the
    /// canonical bounds and origin path. The producer charges it on its original
    /// ledger before starting overlapping work and verifies every actual cost.
    fn receipt_memory_bound(&self, captures: usize) -> Result<usize>;
    /// Acquires the original catalog ordering and observes its current head.
    /// Release this lifetime before waiting for credit or servicing a standalone
    /// checkpoint. The runtime joins all I/O dispatched through this round.
    fn begin_round<'a>(&'a self)
    -> BoxFuture<'a, Result<Box<dyn NodeBundlePublicationRound + 'a>>>;
    /// Selects one cohort through the same staging/upload/selection path used by
    /// the managed pipeline. Manual callers retain admission and join ownership.
    fn select<'a>(
        &'a self,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        prefixes: &'a [&'a BundleCoverageProof],
        preparation: Option<&'a mut BundlePreparation>,
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async move {
            lease.check()?;
            let round = self.begin_round().await?;
            let staged = round
                .stage(None, captures, checkpoints, preparation)
                .await?;
            let uploaded = round.upload(staged).await?;
            round.select(&uploaded, prefixes, lease).await
        })
    }
    /// Joins exact materialized roots. Skip an obsolete notification only after
    /// observing a newer original root whose callback remains a joined obligation.
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>>;
}
