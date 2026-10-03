use super::*;

/// Fresh complete-roster, permanent-fence and original-process confirmation.
///
/// Original process identity does not depend on recovery progress. This interval
/// proof supplies no affected-Cell inventory, log closure or role settlement.
pub struct FleetFailedBootProcessConfirmation {
    snapshot: FleetJournalSnapshot,
    fence: NodeSessionFence,
    process: FleetFailedBootProcessEvidence,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetFailedBootProcessConfirmation {
    /// Full journal barrier checked around the process provider reads.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original permanent fence; it makes no terminal-log assertion.
    #[must_use]
    pub const fn fence(&self) -> &NodeSessionFence {
        &self.fence
    }
    /// Independently authenticated, durable original lifetime evidence.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Fresh monotonic confirmation interval, without restamping the request.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

impl FleetFailedBootProcessRequest {
    /// Captures a terminal original boot/log basis. Existing v1 request digests
    /// and retirement event identities remain unchanged. Related reader/follower
    /// rows may be unresolved; this request supplies no process closure itself.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        original: &EnrollmentRecord,
        claimant: SessionId,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        Self::capture_basis(
            journal, directory, roster, original, claimant, deadline, clock, true,
        )
        .await
    }

    /// Captures original process identity immediately after permanent fencing,
    /// while the leader log and related roles may still need recovery/retirement.
    /// The v2 digest binds the immutable boot and permanent fence, excluding log
    /// phase, manifest, claimant, registry and observation times. It remains the
    /// same through recovery, claim adoption and terminal retirement. Obtain
    /// actual process/accepted-work joining through [`Self::confirm`]; this read
    /// starts no process, recovery or native effect.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_fenced(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        original: &EnrollmentRecord,
        claimant: SessionId,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        Self::capture_basis(
            journal, directory, roster, original, claimant, deadline, clock, false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn capture_basis(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        original: &EnrollmentRecord,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
        terminal: bool,
    ) -> Result<Self> {
        let started_at_ms = clock()?;
        if directory.fleet() != roster.snapshot().head().scope().fleet
            || roster.snapshot().registry().bootstrap_revision().is_none()
        {
            return Err(Error::Fenced);
        }
        roster.confirm(journal, deadline).await?;
        let boot = records::boot(roster, original)?;
        let endpoint = boot.spec().target;
        let canonical = if terminal {
            Some(
                bounded(
                    deadline,
                    directory.closed_session(
                        endpoint.node,
                        endpoint.session,
                        claimant,
                        started_at_ms,
                    ),
                )
                .await?,
            )
        } else {
            None
        };
        let fence = match &canonical {
            Some(closure) => closure.fence(),
            None => {
                bounded(
                    deadline,
                    directory.fenced_session(
                        endpoint.node,
                        endpoint.session,
                        claimant,
                        started_at_ms,
                    ),
                )
                .await?
            }
        };
        roster.confirm(journal, deadline).await?;
        let finished_at_ms = clock()?;
        interval(started_at_ms, finished_at_ms)?;
        let digest = match &canonical {
            Some(closure) => records::request_digest(&boot, closure)?,
            None => records::fenced_request_digest(&boot, &fence)?,
        };
        Ok(Self {
            boot,
            canonical,
            fence,
            digest,
            snapshot: roster.snapshot().clone(),
            started_at_ms,
            finished_at_ms,
        })
    }

    /// Freshly confirms the original process before dependent recovery/inventory
    /// effects. Collects the complete current roster, rechecks its original boot,
    /// canonical basis and provider twice, then confirms the full barrier. The
    /// read-only provider owns actual authentication and original native/external
    /// lifetime joining. Cancellation starts no replacement effect here.
    #[allow(clippy::too_many_arguments)]
    pub async fn confirm(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFailedBootProcessConfirmation> {
        let started_at_ms = clock()?;
        let mut last = started_at_ms;
        if started_at_ms < self.finished_at_ms {
            return Err(Error::Deadline);
        }
        interval(started_at_ms, started_at_ms)?;
        let mut now = || {
            let next = clock()?;
            if next < last {
                return Err(Error::Deadline);
            }
            interval(started_at_ms, next)?;
            last = next;
            Ok(next)
        };
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(adapter_error)
        })
        .await?;
        let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
        if snapshot.registry().bootstrap_revision().is_none() {
            return Err(Error::Fenced);
        }
        records::boot(&roster, &self.boot)?;
        self.confirm_canonical(directory, claimant, now()?, deadline)
            .await?;
        let process = self.confirm_process(processes, deadline).await?;
        roster.confirm(journal, deadline).await?;
        self.confirm_canonical(directory, claimant, now()?, deadline)
            .await?;
        if self.confirm_process(processes, deadline).await? != process {
            return Err(Error::Control("original failed process evidence changed"));
        }
        // A suspended second provider read cannot hide a changed journal or
        // expired claimant. Keep this final basis check after both reads.
        roster.confirm(journal, deadline).await?;
        self.confirm_canonical(directory, claimant, now()?, deadline)
            .await?;
        let finished_at_ms = now()?;
        Ok(FleetFailedBootProcessConfirmation {
            snapshot,
            fence: self.fence.clone(),
            process,
            started_at_ms,
            finished_at_ms,
        })
    }
}
