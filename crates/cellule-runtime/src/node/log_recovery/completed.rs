//! Read-only adoption of an already committed recovery result.
use super::*;

impl RecoveryCoordinator {
    pub(super) async fn resume_completed(
        &self,
        directory: &NodeDirectory,
        fenced: &FencedNodeSession,
        cells: &[RecoveryCell],
        now_ms: i64,
    ) -> Result<Option<CompletedNodeRecovery>> {
        let started = std::time::Instant::now();
        let recovered = match directory
            .recovered_session(fenced.node(), fenced.session(), fenced.claimant(), now_ms)
            .await
        {
            Ok(recovered) => recovered,
            Err(Error::Control("node session log recovery is incomplete")) => return Ok(None),
            Err(source) => return Err(source),
        };
        let sealed = SealedNodeLog {
            session: fenced.session(),
            log: recovered.log().cloned().ok_or(Error::Fenced)?,
        };
        let takeover = NodeTakeoverProof::after_recovery(fenced, &sealed)?;
        let mut controls = Vec::new();
        for cell in cells {
            let current = cell
                .authority
                .load(cell.observed.value().cell)
                .await?
                .ok_or(Error::Fenced)?;
            if current.value() != cell.observed.value()
                || *cell.application.as_bytes() == [0; 16]
                || current.value().ltx_root().is_none()
                || current.value().owner.as_ref().map(|owner| owner.session)
                    != Some(fenced.session())
            {
                return Err(Error::Fenced);
            }
            if current.value().recovery.is_some() {
                controls.push(current);
            }
        }
        let manifest = pinned_manifest(&controls, fenced, self.recovery.log_epoch)?;
        if sealed.log().recovery_manifest() != manifest {
            return Err(Error::Fenced);
        }
        if let Some(manifest) = manifest {
            let inventory = self
                .manifests
                .load_manifest(fenced.session(), self.recovery.log_epoch, manifest)
                .await?;
            if inventory.cells().len() != controls.len() {
                return Err(Error::Fenced);
            }
            for pin in inventory.cells() {
                cells
                    .iter()
                    .find(|cell| {
                        cell.application == pin.application
                            && cell.observed.value().cell == pin.cell
                            && cell.observed.value().incarnation == pin.incarnation
                            && cell.observed.value().epoch == pin.cell_epoch
                            && cell.observed.value().recovery.as_ref() == Some(&pin.recovery)
                    })
                    .ok_or(Error::Fenced)?;
                // A terminal pointer does not prove its immutable payload is
                // still available. Reopen the exact original overlay.
                self.manifests
                    .for_application(pin.application)
                    .load_overlay(pin.cell, pin.incarnation, &pin.recovery)
                    .await?;
            }
        }
        let now = crate::node::bundle::recovery::logical_now(now_ms, started)?;
        let current = directory
            .recovered_session(fenced.node(), fenced.session(), fenced.claimant(), now)
            .await?;
        if current != recovered {
            return Err(Error::Fenced);
        }
        // Sealing clears the transient recovery claim. A repeated successful
        // call adopts only the exact terminal result; it renews no old claim
        // and performs no Cell, manifest or node authority writes.
        Ok(Some(CompletedNodeRecovery {
            sealed,
            controls,
            takeover,
        }))
    }
}
