//! Reuse the exact original manifest after a partial materialization.
use super::*;

impl RecoveryManifestStore {
    pub(crate) async fn pin_reconciled(
        &self,
        leader: SessionId,
        epoch: u64,
        tails: Vec<RecoveredCellTail>,
        existing: &[RecoveryOverlayRef],
    ) -> Result<PinnedRecoveryCells> {
        let Some(first) = existing.first() else {
            return self.pin_with_summary(leader, epoch, tails).await;
        };
        if existing.iter().any(|reference| {
            reference.leader_session != leader
                || reference.log_epoch != epoch
                || reference.manifest_digest != first.manifest_digest
        }) {
            return Err(Error::Control(
                "recovered session produced multiple manifests",
            ));
        }
        let manifest = self
            .load_manifest_body(leader, epoch, first.manifest_digest)
            .await?;
        let mut cells = Vec::with_capacity(tails.len());
        for tail in tails {
            let base = tail.overlay.predecessor();
            let row = manifest
                .cells
                .iter()
                .find(|row| {
                    row.scope()
                        == (
                            tail.application,
                            base.cell,
                            base.incarnation,
                            tail.cell_epoch,
                        )
                })
                .ok_or(Error::Node("original recovery manifest omits pending Cell"))?;
            if row.predecessor != base
                || row.first_node_sequence != tail.first_node_sequence
                || row.last_node_sequence != tail.last_node_sequence
                || row.final_position != tail.overlay.final_position()
                || row.final_commit_sequence != tail.overlay.final_commit_sequence()
                || row.bundle_digest != tail.overlay.bundle().digest()
            {
                return Err(Error::Node(
                    "original recovery manifest differs from verified tail",
                ));
            }
            // The newly rebuilt bytes authenticate this exact original object.
            // Already materialized siblings remain in that manifest; a subset
            // retry cannot replace the pending controls with a different digest.
            cells.push(row.pinned(leader, epoch, first.manifest_digest));
        }
        Ok(PinnedRecoveryCells {
            cells,
            summary: RecoveryPublicationSummary {
                object_reads: 1,
                ..Default::default()
            },
        })
    }
}
