//! Periodic repair through the original activation and enrollment owners.
use super::*;
use cellule_runtime::cell::actor::NodeByteReservation;

struct ReconciliationCells {
    cells: Vec<CellId>,
    _memory: NodeByteReservation,
}

impl ReadReplicaManager {
    /// Refreshes admitted snapshots and reconciles retained enrollment results.
    ///
    /// This loop discovers no new Cells. It scans installed views and original
    /// producer requests, including joined attempts that never opened a view.
    /// Cancellation interrupts waits; accepted opening and cleanup keep their
    /// canonical owners. Application replacement policy remains independent.
    pub async fn run(&self, cancellation: CancellationToken) -> Result<()> {
        let mut tick = tokio::time::interval(RECONCILE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cursor = 0_usize;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = self.closed.cancelled() => return Ok(()),
                _ = tick.tick() => {}
            }
            if self.closed.is_cancelled() {
                return Ok(());
            }
            if let Some(enrollment) = self.bound_enrollment()?
                && let Err(error) = enrollment.reap().await
            {
                // Preserve failed joins in their original bank, while allowing
                // independent responsibilities to publish or close normally.
                tracing::warn!(error = %error, "read enrollment join remains failed");
            }
            let readers = match self.reconciliation_cells().await {
                Ok(readers) => readers,
                Err(error @ Error::Capacity(_)) => {
                    tracing::warn!(error = %error, "read reconciliation inventory refused");
                    continue;
                }
                Err(error) => return Err(error),
            };
            let count = readers.cells.len().min(RECONCILE_BATCH);
            for _ in 0..count {
                let index = cursor % readers.cells.len();
                let cell = readers.cells[index];
                // Advance before I/O: a failed journal or unavailable Cell must
                // not repeatedly consume the first position of every batch.
                cursor = (index + 1) % readers.cells.len();
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = self.closed.cancelled() => return Ok(()),
                    result = tokio::time::timeout(RECONCILE_DEADLINE, self.refresh_selected(cell)) => {
                        match result {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => tracing::warn!(?cell, error = %error, "read replica reconciliation failed"),
                            Err(_) => tracing::warn!(?cell, "read replica reconciliation deadline exceeded"),
                        }
                    }
                }
            }
        }
    }

    async fn reconciliation_cells(&self) -> Result<ReconciliationCells> {
        let active = self.active.read().await;
        if active.views.len() > MAX_READ_VIEWS {
            return Err(Error::Capacity("read reconciliation view bound"));
        }
        let (mut cells, memory) = match self.bound_enrollment()? {
            Some(enrollment) => {
                enrollment.reconciliation_cells(&self.runtime, active.views.keys().copied())?
            }
            None => {
                let memory = self.runtime.try_reserve_node_bytes(
                    active.views.len() * std::mem::size_of::<CellId>() + 4096,
                )?;
                (active.views.keys().copied().collect::<Vec<_>>(), memory)
            }
        };
        drop(active);
        cells.sort_unstable_by_key(|cell| *cell.as_bytes());
        cells.dedup();
        Ok(ReconciliationCells {
            cells,
            _memory: memory,
        })
    }

    async fn refresh_selected(&self, cell: CellId) -> Result<()> {
        // A local record can be reconciled only after its original activation
        // releases this lane. No absence scan may race an accepted native open.
        let _activation = self.activation.lock().await;
        self.ensure_open()?;
        let current = self.active.read().await.views.get(&cell).cloned();
        let Some(reader) = current else {
            if let Some(enrollment) = self.bound_enrollment()? {
                // Canonical retirement distinguishes never-started exclusion
                // from joined native refusal. Unjoined task failures stay blocked.
                enrollment.retire(cell, None).await?;
            }
            return Ok(());
        };
        if reader.lifecycle_observation().await.admission_closed() {
            // A failed/cancelled removal retained the fenced view. Resume its
            // same join and journal event before attempting any remote refresh.
            return self.remove_locked(cell).await;
        }
        if !self.still_selected(cell).await? {
            return self.remove_locked(cell).await;
        }
        if let Some(enrollment) = self.bound_enrollment()? {
            // Republish the original opening proof; never reopen or replace its
            // pinned request just because the first result reply was lost.
            enrollment.established(cell).await?;
        }
        let path = self.destination(cell).await?;
        match reader.refresh(&path).await {
            Ok(_) => Ok(()),
            Err(Error::Fenced) => {
                // Keep verified warm bytes after owner death. Queries still
                // require a live owner; changed authority evicts the view.
                if reader.readiness().await.is_err() {
                    self.remove_locked(cell).await?;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}
