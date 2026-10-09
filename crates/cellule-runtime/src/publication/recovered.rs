//! Selected overlays enter the same bounded producer and exact root factory.
use super::*;

impl CellPublisher {
    pub(super) async fn prepare_recovered(
        &mut self,
        overlay: &cellule_ltx::RecoveryOverlay,
    ) -> Result<cellule_ltx::PreparedRoot> {
        let result = match self.admit_publication().await? {
            PublicationPermit::Direct(replica) => {
                self.prepare_recovered_inputs(*replica, overlay, None).await
            }
            PublicationPermit::Shared(slot) => {
                let coordinator = self
                    .shared_publication
                    .clone()
                    .ok_or(Error::Control("shared publication is unavailable"))?;
                let replica = self.replica.clone();
                let submission = coordinator.submit_recovered(
                    &replica,
                    overlay,
                    self.scratch_directory.clone(),
                    slot,
                );
                tokio::pin!(submission);
                let shared = loop {
                    tokio::select! {
                        result = &mut submission => break result,
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.renew_at)) => self.renew().await?,
                    }
                };
                self.check_node_lease()?;
                let shared = match shared {
                    Ok(shared) => shared,
                    // A failed sibling/upload cannot invalidate this Cell's
                    // original overlay. The direct factory rechecks its exact
                    // inputs and retains its own source error on failure.
                    Err(Error::Shared(source)) if matches!(source.as_ref(), Error::Ltx(_)) => None,
                    Err(error) => return Err(error),
                };
                self.prepare_recovered_inputs(self.replica.clone(), overlay, shared.as_ref())
                    .await
            }
        };
        self.record_publication_cost();
        result
    }

    async fn prepare_recovered_inputs(
        &mut self,
        replica: cellule_ltx::CellReplica,
        overlay: &cellule_ltx::RecoveryOverlay,
        shared: Option<&shared::SharedPrepared>,
    ) -> Result<cellule_ltx::PreparedRoot> {
        self.check_node_lease()?;
        let (replica, confirmation) = lineage::replica(replica, &self.authority);
        let base = overlay.predecessor();
        let schema = self.observed.value().schema;
        let preparation = match shared {
            Some(shared) => futures_util::future::Either::Left(replica.prepare_shared(
                Some(&base),
                &shared.append,
                overlay.final_commit_sequence(),
                schema,
            )),
            None => futures_util::future::Either::Right(
                replica.prepare_recovered_overlay(overlay, schema),
            ),
        };
        tokio::pin!(preparation);
        let prepared = loop {
            tokio::select! {
                result = &mut preparation => break result.map_err(lineage::error)?,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.renew_at)) => self.renew().await?,
            }
        };
        if prepared.predecessor() != Some(base)
            || prepared.root().position != overlay.final_position()
            || prepared.root().commit_sequence != overlay.final_commit_sequence()
        {
            return Err(cellule_ltx::LtxError::ChecksumMismatch.into());
        }
        self.lineage_confirmed = *confirmation
            .lock()
            .map_err(|_| Error::Peer("root lineage confirmation lock poisoned"))?;
        Ok(prepared)
    }
}
