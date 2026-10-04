//! Fresh observation of an already committed original retirement, without effects.
use super::*;

impl FleetFailedBootRetirement {
    /// Reconfirms an already Retired original boot without publishing any event.
    ///
    /// Capture the original retained request at the current complete roster,
    /// then call this from the observation adapter. Both original process reads,
    /// terminal authority, every related responsibility and physical follower
    /// reference must agree with that exact head/registry. Missing retirement
    /// cannot be converted into absence or completed by this read. Reuse the
    /// original process request across recovery and controller reconstruction.
    /// This supplies no writer relocation, replacement policy or finalization.
    pub async fn confirm(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFailedBootClosure> {
        let mut last = self.finished_at_ms;
        let mut now = || {
            let next = clock()?;
            interval(self.started_at_ms, next)?;
            if next < last {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        now()?;
        self.confirm_snapshot(journal, deadline).await?;
        let roster = FleetRoster::collect(journal, &self.snapshot, deadline).await?;
        let boot = records::select(&roster, &self.request.boot)?;
        let process = self.request.confirm_process(processes, deadline).await?;
        records::result(&self.request.boot, &boot, records::retirement(&process))?;
        // A provider read may suspend. The ordinary publication final check is
        // also the read-only path: retain this exact committed row, repeat all
        // canonical/reference/process reads, and refuse a different full barrier.
        self.confirm_snapshot(journal, deadline).await?;
        let closure = self
            .finish(
                journal,
                directory,
                processes,
                claimant,
                deadline,
                &mut now,
                &process,
                &Ok(boot),
            )
            .await?;
        if closure.snapshot() != &self.snapshot {
            return Err(Error::Fenced);
        }
        Ok(closure)
    }
}
