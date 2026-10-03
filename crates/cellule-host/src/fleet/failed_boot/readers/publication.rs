use super::*;

impl FleetFailedReaderRetirement {
    /// Confirms the exact original receiver's durable process and accepted-work
    /// closure, then publishes through the existing enrollment journal. No
    /// native drain or new task starts here. The adapter owns and joins accepted
    /// backend work across cancellation; fresh recapture adopts lost replies
    /// with the same event, original timestamps and establishment history.
    pub async fn publish(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFailedReaderPublication> {
        let (started_at_ms, mut last) = self.interval();
        let mut clock = || {
            let next = clock()?;
            if next < last {
                return Err(Error::Deadline);
            }
            interval(started_at_ms, next)?;
            last = next;
            Ok(next)
        };
        let now = clock()?;
        self.confirm_snapshot(journal, deadline).await?;
        self.request
            .confirm_canonical(directory, claimant, now, deadline)
            .await?;
        let process = self.request.confirm_process(processes, deadline).await?;
        let evidence = records::retirement(&self.reader, &process)?;
        if self.reader.status() == EnrollmentStatus::Retired
            && self.reader.settlement_evidence() != Some(evidence)
        {
            return Err(Error::Control("failed reader retirement evidence differs"));
        }
        // The provider can suspend. Any change to the original complete barrier
        // requires recapture before effects; expiry is never process evidence.
        self.confirm_snapshot(journal, deadline).await?;
        let now = clock()?;
        self.request
            .confirm_canonical(directory, claimant, now, deadline)
            .await?;
        let record = bounded(deadline, async {
            let returned = journal
                .publish_enrollment_result(&self.reader, EnrollmentEvent::Retired(evidence), now)
                .await
                .map_err(adapter_error)?;
            records::result(&self.reader, &returned, evidence)?;
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
        Ok(FleetFailedReaderPublication {
            process,
            record,
            closure,
        })
    }

    async fn confirm_snapshot(&self, journal: &dyn FleetJournal, deadline: Instant) -> Result<()> {
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot().head().scope())
                .await
                .map_err(adapter_error)
        })
        .await?;
        if &snapshot != self.snapshot() {
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
    ) -> Result<FleetFailedReaderClosure> {
        let returned = record
            .as_ref()
            .map_err(|source| retained(Arc::clone(source)))?;
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot().head().scope())
                .await
                .map_err(adapter_error)
        })
        .await?;
        let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
        super::super::records::boot(&roster, &self.request.boot)?;
        let reader = records::select(&roster, &self.request, &self.reader)?;
        if &reader != returned {
            return Err(Error::Control("failed reader publication changed"));
        }
        records::result(
            &self.reader,
            &reader,
            records::retirement(&self.reader, process)?,
        )?;
        self.request
            .confirm_canonical(directory, claimant, clock()?, deadline)
            .await?;
        if &self.request.confirm_process(processes, deadline).await? != process {
            return Err(Error::Control("original failed receiver evidence changed"));
        }
        roster.confirm(journal, deadline).await?;
        let finished_at_ms = clock()?;
        Ok(FleetFailedReaderClosure {
            snapshot,
            digest: records::closure_digest(&reader, process)?,
            reader,
            process: process.clone(),
            started_at_ms: self.request.started_at_ms,
            finished_at_ms,
        })
    }
}
