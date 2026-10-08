//! Exact selected-capture cleanup before independently joined root materialization.

use super::*;
use crate::publication::VerifiedBundleCapture;

impl CellExecutor {
    pub(crate) fn release_bundle_captures(
        &mut self,
        captures: &[VerifiedBundleCapture],
    ) -> Result<Vec<StoredOutcome>> {
        if self.fenced || self.pending_migration.is_some() {
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
        if let Some(previous) = &self.bundle_materialization {
            selected.proof.continues_selected_prefix(&previous.proof)?;
        }
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
        // The original proof now owns this exact recoverable range. Outcomes
        // remain in SQLite's durable retry ledger, rather than one heap entry
        // per selected command. A later unproven suffix remains in `pending`.
        let mut outcomes = Vec::with_capacity(captures.len());
        for pending in self.pending.drain(..captures.len()) {
            self.pending_bytes = self
                .pending_bytes
                .checked_sub(pending.retained_bytes())
                .ok_or(Error::Control("bundle cleanup accounting underflow"))?;
            outcomes.push(pending.outcome);
        }
        self.bundle_materialization = Some(std::sync::Arc::clone(selected));
        Ok(outcomes)
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
        self.published_sequence = root.commit_sequence;
        self.bundle_materialization = None;
        Ok(())
    }
}
