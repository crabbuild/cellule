use super::*;

impl FleetFailedBootRetirement {
    /// Confirms durable original process evidence before publishing boot closure
    /// through the existing journal. Rereads complete roster, canonical fence,
    /// physical follower references and process evidence afterward. Cancellation
    /// or a waiter deadline does not join native/backend work; its existing
    /// application/adapter owners retain and join accepted work. Fresh recapture
    /// adopts a committed lost reply with unchanged evidence and original times.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFailedBootPublication> {
        let mut last = self.finished_at_ms;
        let mut clock = || {
            let next = clock()?;
            if next < last {
                return Err(Error::Deadline);
            }
            interval(self.started_at_ms, next)?;
            last = next;
            Ok(next)
        };
        let now = clock()?;
        self.confirm_snapshot(journal, deadline).await?;
        self.request
            .confirm_canonical(directory, claimant, now, deadline)
            .await?;
        let process = self.request.confirm_process(processes, deadline).await?;
        let evidence = records::retirement(&process);
        if self.request.boot.status() == EnrollmentStatus::Retired
            && self.request.boot.settlement_evidence() != Some(evidence)
        {
            return Err(Error::Control("failed boot retirement evidence differs"));
        }
        // The provider read can suspend. Recheck the entire original barrier
        // before committing any evidence; a new related role cannot be ignored.
        self.confirm_snapshot(journal, deadline).await?;
        let now = clock()?;
        self.request
            .confirm_canonical(directory, claimant, now, deadline)
            .await?;
        let record = bounded(deadline, async {
            let returned = journal
                .publish_enrollment_result(
                    &self.request.boot,
                    EnrollmentEvent::Retired(evidence),
                    now,
                )
                .await
                .map_err(adapter_error)?;
            records::result(&self.request.boot, &returned, evidence)?;
            Ok(returned)
        })
        .await
        .map_err(Arc::new);
        let closure = self
            .finish(
                journal, directory, processes, claimant, deadline, &mut clock, &process, &record,
            )
            .await
            .map_err(Arc::new);
        Ok(FleetFailedBootPublication {
            process,
            record,
            closure,
        })
    }

    async fn confirm_snapshot(&self, journal: &dyn FleetJournal, deadline: Instant) -> Result<()> {
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(adapter_error)
        })
        .await?;
        if snapshot != self.snapshot {
            return Err(Error::Fenced);
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    async fn finish(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        claimant: SessionId,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
        process: &FleetFailedBootProcessEvidence,
        record: &std::result::Result<EnrollmentRecord, Arc<Error>>,
    ) -> Result<FleetFailedBootClosure> {
        let returned = record
            .as_ref()
            .map_err(|source| retained(Arc::clone(source)))?;
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(adapter_error)
        })
        .await?;
        let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
        let boot = records::select(&roster, &self.request.boot)?;
        if &boot != returned {
            return Err(Error::Control("failed boot publication changed"));
        }
        records::result(&self.request.boot, &boot, records::retirement(process))?;
        records::references(
            directory,
            &roster,
            boot.spec().target.node,
            boot.spec().target.session,
            deadline,
            clock,
        )
        .await?;
        self.request
            .confirm_canonical(directory, claimant, clock()?, deadline)
            .await?;
        if &self.request.confirm_process(processes, deadline).await? != process {
            return Err(Error::Control("original failed process evidence changed"));
        }
        roster.confirm(journal, deadline).await?;
        let finished_at_ms = clock()?;
        Ok(FleetFailedBootClosure {
            snapshot,
            digest: records::closure_digest(&boot, process)?,
            boot,
            canonical: self.request.canonical.clone(),
            process: process.clone(),
            started_at_ms: self.started_at_ms,
            finished_at_ms,
        })
    }
}

impl FleetFailedBootProcessRequest {
    pub(super) async fn confirm_canonical(
        &self,
        directory: &NodeDirectory,
        claimant: SessionId,
        now: i64,
        deadline: Instant,
    ) -> Result<()> {
        if directory.fleet() != self.snapshot.head().scope().fleet {
            return Err(Error::Fenced);
        }
        let endpoint = self.boot.spec().target;
        let canonical = bounded(
            deadline,
            directory.closed_session(endpoint.node, endpoint.session, claimant, now),
        )
        .await?;
        if canonical != self.canonical {
            return Err(Error::Fenced);
        }
        Ok(())
    }
    pub(super) async fn confirm_process(
        &self,
        processes: &dyn FleetFailedBootProcesses,
        deadline: Instant,
    ) -> Result<FleetFailedBootProcessEvidence> {
        let process = bounded(deadline, async {
            processes.confirm_stopped(self).await.map_err(adapter_error)
        })
        .await?;
        if process.request != self.digest
            || process.witness.as_bytes().iter().all(|byte| *byte == 0)
        {
            return Err(Error::Fenced);
        }
        Ok(process)
    }
}
