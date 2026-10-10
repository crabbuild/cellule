//! Fault hooks on the same typed publication round used by the managed worker.
use super::*;
use crate::node::durability::{BundleCheckpoint, NodeBundlePublicationRound};
use crate::node::log_shipper::AssignedCapture;
use futures_util::future::BoxFuture;
use std::sync::Mutex;

pub(super) trait Hook: Sync {
    fn before_stage<'a>(
        &'a self,
        _checkpoints: &'a [BundleCheckpoint],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn before_select<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn after_select<'a>(
        &'a self,
        _checkpoints: usize,
        proofs: Vec<BundleCoverageProof>,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async { Ok(proofs) })
    }
}
pub(super) struct HookedRound<'a> {
    original: Box<dyn NodeBundlePublicationRound + 'a>,
    hook: &'a dyn Hook,
    checkpoints: Mutex<std::collections::BTreeMap<[u8; 32], usize>>,
}
impl<'a> HookedRound<'a> {
    pub(super) fn wrap(
        original: Box<dyn NodeBundlePublicationRound + 'a>,
        hook: &'a dyn Hook,
    ) -> Box<dyn NodeBundlePublicationRound + 'a> {
        Box::new(Self {
            original,
            hook,
            checkpoints: Mutex::default(),
        })
    }
}
impl NodeBundlePublicationRound for HookedRound<'_> {
    fn stage<'a>(
        &'a self,
        previous: Option<&'a StagedNodeBundle>,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        preparation: Option<&'a mut BundlePreparation>,
    ) -> BoxFuture<'a, Result<Arc<StagedNodeBundle>>> {
        Box::pin(async move {
            self.hook.before_stage(checkpoints).await?;
            let staged = self
                .original
                .stage(previous, captures, checkpoints, preparation)
                .await?;
            self.checkpoints
                .lock()
                .unwrap()
                .insert(*staged.head().digest().as_bytes(), checkpoints.len());
            Ok(staged)
        })
    }
    fn upload<'a>(
        &'a self,
        staged: Arc<StagedNodeBundle>,
    ) -> BoxFuture<'a, Result<PreparedNodeBundle>> {
        self.original.upload(staged)
    }
    fn select<'a>(
        &'a self,
        prepared: &'a PreparedNodeBundle,
        prefixes: &'a [&'a BundleCoverageProof],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async move {
            self.hook.before_select().await?;
            let proofs = self.original.select(prepared, prefixes, lease).await?;
            let count = self
                .checkpoints
                .lock()
                .unwrap()
                .remove(prepared.head().digest().as_bytes())
                .unwrap();
            self.hook.after_select(count, proofs).await
        })
    }
}
