//! Resource lifetime after original root I/O, before its checkpoint joins.
use super::actor::Authority;
use super::*;
use crate::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority,
    NodeBundlePublicationRound,
};
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, Ordering};

// Original root/proof obligations still have the same 21 MiB ceiling. The
// independently admitted preparation cache stays owned by the live publisher.
pub(super) const HELD_METADATA_CEILING: usize = (21 << 20) + PREPARATION_BYTES;

#[derive(Default)]
pub(super) struct CheckpointGate {
    pub(super) entered: tokio::sync::Notify,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}

impl CheckpointGate {
    async fn wait(&self, checkpoints: &[BundleCheckpoint]) {
        if checkpoints.is_empty() {
            return;
        }
        self.entered.notify_one();
        while !self.released.load(Ordering::Acquire) {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.released.load(Ordering::Acquire) {
                changed.await;
            }
        }
    }

    pub(super) fn resume(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

pub(super) struct HeldCheckpoints {
    pub(super) original: Arc<Authority>,
    pub(super) gate: Arc<CheckpointGate>,
}

impl NodeBundleAuthority for HeldCheckpoints {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        NodeBundleAuthority::bind(self.original.as_ref(), authority, observed)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        NodeBundleAuthority::close(self.original.as_ref(), authority, observed, issued)
    }
}

impl NodeBundlePublicationAuthority for HeldCheckpoints {
    fn receipt_memory_bound(&self, captures: usize) -> Result<usize> {
        self.original.receipt_memory_bound(captures)
    }
    fn begin_round<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn NodeBundlePublicationRound + 'a>>> {
        Box::pin(async move {
            Ok(super::publication_hooks::HookedRound::wrap(
                self.original.begin_round().await?,
                self,
            ))
        })
    }
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.gate.wait(checkpoints).await;
            self.original.checkpoint(checkpoints).await
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_root_working_credit_is_released_before_held_original_checkpoint() {
    super::managed::managed_actor_case(
        215,
        64 << 20,
        true,
        true,
        Some(Arc::new(CheckpointGate::default())),
    )
    .await;
}

impl super::publication_hooks::Hook for HeldCheckpoints {
    fn before_stage<'a>(
        &'a self,
        checkpoints: &'a [BundleCheckpoint],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.gate.wait(checkpoints).await;
            Ok(())
        })
    }
}
