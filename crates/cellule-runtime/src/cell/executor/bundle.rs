//! Exact selected-capture cleanup before independently joined root materialization.

use super::*;
use crate::publication::VerifiedBundleCapture;

impl CellExecutor {
    pub(crate) fn release_bundle_captures(
        &mut self,
        captures: &[VerifiedBundleCapture],
    ) -> Result<()> {
        if self.fenced || self.pending_migration.is_some() || self.bundle_materialization.is_some()
        {
            return Err(Error::PendingPublication);
        }
        let newest = captures
            .last()
            .ok_or(Error::Control("bundle cleanup lacks captures"))?;
        let pending = self
            .pending
            .get(captures.len() - 1)
            .ok_or(Error::PendingPublication)?;
        let selected = newest.selected();
        if selected.proof.commit_sequence() != pending.outcome.commit_sequence()
            || selected.proof.position() != pending.cuts.position
        {
            return Err(Error::Control("bundle cleanup endpoint differs"));
        }
        // Validate the whole oldest prefix before deleting any file. A selected
        // suffix, endpoint match alone, or another original assignment cannot
        // retire an earlier unresolved capture.
        for (pending, capture) in self.pending.iter().zip(captures) {
            if pending.prepared.is_some()
                || capture.selected().proof.binding() != selected.proof.binding()
            {
                return Err(Error::Control(
                    "bundle cleanup changes publication ownership",
                ));
            }
            capture.verify_pending(self.cell, self.incarnation, pending)?;
        }
        for pending in self.pending.iter().take(captures.len()) {
            self.db.prune_captured(&pending.cuts)?;
        }
        // Keep outcomes and coordination pending until the actor's sole
        // publisher materializes the exact endpoint. Proven visibility may
        // advance now; an unproven later suffix still blocks queries/retries.
        for pending in self.pending.iter_mut().take(captures.len()) {
            self.pending_bytes = self
                .pending_bytes
                .checked_sub(pending.retained_bytes())
                .ok_or(Error::Control("bundle cleanup accounting underflow"))?;
            pending.cuts.segments = Vec::new();
            pending.durable = true;
        }
        self.bundle_materialization = Some(std::sync::Arc::clone(selected));
        Ok(())
    }

    pub(crate) fn bind_bundle_materialized(&mut self, root: &cellule_ltx::RootRef) -> Result<()> {
        let selected = self
            .bundle_materialization
            .as_ref()
            .ok_or(Error::PendingPublication)?;
        if root.cell != *self.cell.as_bytes()
            || root.incarnation != *self.incarnation.as_bytes()
            || root.commit_sequence != selected.proof.commit_sequence()
            || root.position != selected.proof.position()
        {
            return Err(Error::Control(
                "materialized root differs from bundle cleanup",
            ));
        }
        let index = self
            .pending
            .iter()
            .position(|pending| pending.outcome.commit_sequence() == root.commit_sequence)
            .ok_or(Error::PendingPublication)?;
        if self.pending.iter().take(index + 1).any(|pending| {
            !pending.durable || !pending.cuts.segments.is_empty() || pending.prepared.is_some()
        }) {
            return Err(Error::Control("materialized bundle omits retained capture"));
        }
        for pending in self.pending.iter_mut().take(index + 1) {
            pending.prepared = Some(*root);
        }
        Ok(())
    }
}
