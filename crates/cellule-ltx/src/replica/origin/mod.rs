//! One fresh small-root verification operation; scheduling stays with the host.
use super::*;
use std::time::Instant;

const WORKING_BYTES: usize = 512 << 10;

/// A single origin verification operation begun with a fresh exact root read.
///
/// This retains bounded root metadata, not verified dependency availability.
/// Complete it before using the inventory; no writer, retention or durability
/// authority is granted. It is consumed once and cannot substitute a cached
/// proof for fresh reads in a later operation. Scheduling and memory admission
/// belong to the embedding runtime.
#[must_use = "complete origin verification before relying on the dependency inventory"]
pub struct RootOriginVerification {
    replica: CellReplica,
    root: RootRef,
    document: RootDocument,
    max_objects: usize,
    started: Instant,
}

impl CellReplica {
    /// Begins fresh verification of a root with a bounded small working set.
    ///
    /// Reads and authenticates the exact root record without the metadata cache.
    /// A packed leaf graph with at most 32 inline descriptors returns a one-use
    /// operation. Other graphs return `None`; callers must use the complete
    /// [`Self::reachable_objects_bounded`] path with its original admission.
    /// No partial inventory or availability proof is returned.
    pub async fn small_root_origin_verification(
        &self,
        root: &RootRef,
        max_objects: usize,
    ) -> Result<Option<RootOriginVerification>> {
        if max_objects == 0 {
            return Err(LtxError::Limit(crate::LimitKind::RootInventoryObjects));
        }
        let started = self.host.now_monotonic();
        let (document, _) = self.read_root_document(root, false).await?;
        if !eligible(&document) {
            return Ok(None);
        }
        Ok(Some(RootOriginVerification {
            replica: self.clone(),
            root: *root,
            document,
            max_objects,
            started,
        }))
    }
}

fn eligible(document: &RootDocument) -> bool {
    document.segment_pages.is_empty()
        && document.directory_height == 0
        && document.segments.len() <= MAX_INLINE_SEGMENTS
        && document.segments.iter().all(|descriptor| {
            matches!(
                descriptor.object_kind(),
                CellObjectKind::Packed | CellObjectKind::SharedPacked
            )
        })
}

impl RootOriginVerification {
    /// Working-set charge including metadata, one small origin body and scratch.
    ///
    /// Retained root/descriptor/inventory tables and a leaf directory are bounded
    /// independently of database size. Only one packed body, at most 256 KiB,
    /// is read at a time. Hosts must admit this charge before overlapping work.
    #[must_use]
    pub const fn working_bytes(&self) -> usize {
        WORKING_BYTES
    }

    /// Authenticates every origin dependency and returns the complete inventory.
    ///
    /// Uses the canonical graph, packed-body, directory and inventory verifier.
    /// Any missing, corrupt or out-of-scope dependency fails with its source
    /// error. Bodies do not survive this operation; cancellation drops pending
    /// origin reads and the caller must retain its admission through completion.
    pub async fn verify(self) -> Result<Vec<RootObjectRef>> {
        let Self {
            replica,
            root,
            document,
            max_objects,
            started,
        } = self;
        let graph = replica
            .load_graph_document(&root, false, document, None)
            .await;
        replica
            .host
            .observe_ltx_phase(crate::LtxPhase::RootOpen, started, graph.is_ok());
        replica
            .inventory_graph(&root, Some(max_objects), graph?)
            .await
    }
}

#[cfg(test)]
mod tests;
