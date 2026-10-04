//! Confirmed native closure precedes every member's durable retirement event.
use super::*;
use futures_util::future::BoxFuture;

pub(super) struct EnrollmentAuthority {
    // The producer bank owns live obligations. A weak callback prevents a
    // cycle through an undelivered epoch's retained runtime cleanup object.
    pub(super) record: std::sync::Weak<Responsibility>,
    pub(super) journal: Arc<dyn FleetJournal>,
    pub(super) interval: Duration,
}
impl EnrollmentAuthority {
    fn record(&self) -> cellule_runtime::Result<Arc<Responsibility>> {
        self.record
            .upgrade()
            .ok_or(Error::Node("follower enrollment owner missing"))
    }
}

impl NodeLogAuthority for EnrollmentAuthority {
    fn requires_confirmed_retirement(&self) -> bool {
        true
    }
    fn observe_shutdown_failure(&self, error: Arc<Error>) -> cellule_runtime::Result<()> {
        let record = self.record()?;
        let mut progress = record.progress()?;
        if progress.execution_error.is_none() {
            progress.execution_error = Some(error);
        }
        Ok(())
    }
    fn observe_retirement(
        &self,
        observation: Arc<NodeLogRetirementObservation>,
    ) -> cellule_runtime::Result<()> {
        let record = self.record()?;
        let prepared = record.inputs.attempt.prepared();
        if observation.barrier().leader_session() != prepared.source().session()
            || observation.barrier().log_epoch() != prepared.log().epoch()
            || observation.barrier().members() != prepared.log().members()
        {
            return Err(Error::Fenced);
        }
        if let Err(error) = observation.confirmed() {
            record.remember(error, false)?;
        }
        let mut progress = record.progress()?;
        if !progress.native_closed {
            progress.retirement = Some(observation);
        }
        Ok(())
    }

    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let record = self.record()?;
            if record.inputs.attempt.prepared().log().epoch() != epoch {
                return Err(Error::Fenced);
            }
            record
                .inputs
                .authority
                .activate(epoch)
                .await
                .map_err(|error| {
                    record
                        .remember(error, false)
                        .map(retained)
                        .unwrap_or_else(|error| error)
                })
        })
    }
    fn advance_coverage<'a>(
        &'a self,
        epoch: u64,
        through: u64,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let record = self.record()?;
            if record.inputs.attempt.prepared().log().epoch() != epoch {
                return Err(Error::Fenced);
            }
            record
                .inputs
                .authority
                .advance_coverage(epoch, through)
                .await
                .map_err(|error| {
                    record
                        .remember(error, false)
                        .map(retained)
                        .unwrap_or_else(|error| error)
                })
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a NodeLogRetirementObservation,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            retirement.confirmed()?;
            let record = self.record()?;
            let _closing = record.closing.lock().await;
            let prepared = record.inputs.attempt.prepared();
            let barrier = retirement.barrier();
            if barrier.leader_session() != prepared.source().session()
                || barrier.log_epoch() != prepared.log().epoch()
                || barrier.members() != prepared.log().members()
            {
                return Err(Error::Fenced);
            }
            {
                let mut progress = record.progress()?;
                if let Some(original) = &progress.retirement {
                    if original.barrier() != barrier {
                        return Err(Error::Fenced);
                    }
                } else {
                    // Keep the original full observation before either CAS or
                    // journal await. Member RPCs cannot be repeated after close.
                    progress.retirement = Some(Arc::new(retirement.clone()));
                }
            }
            loop {
                if !record.progress()?.native_closed {
                    match record.inputs.authority.close(retirement).await {
                        Ok(()) => {
                            // Record the successful canonical close before fallible
                            // evidence construction. A later error must not replay CAS.
                            record.progress()?.native_closed = true;
                        }
                        Err(error) => {
                            record.remember(error, false)?;
                            tokio::time::sleep(self.interval).await;
                            continue;
                        }
                    }
                }
                let members = record.progress()?.members.clone();
                for (index, member) in members.iter().enumerate() {
                    if !matches!(member.event, Some(EnrollmentEvent::Retired(_))) {
                        let event = EnrollmentEvent::Retired(evidence(
                            &record,
                            member,
                            b"confirmed-closed",
                        )?);
                        let mut progress = record.progress()?;
                        progress.members[index].event = Some(event);
                        progress.members[index].published = false;
                    }
                }
                match protocol::publish(&self.journal, &record).await {
                    Ok(()) => {
                        record.finished()?;
                        return Ok(());
                    }
                    Err(error) => {
                        record.remember(error, true)?;
                        // This accepted closure is owned by the retained runtime
                        // or supervisor join. A deadline cancels only its waiter.
                        tokio::time::sleep(self.interval).await;
                    }
                }
            }
        })
    }
}
