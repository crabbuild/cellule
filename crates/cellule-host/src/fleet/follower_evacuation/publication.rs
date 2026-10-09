use super::verification::{adapter, bounded, interval};
use super::*;
use crate::FollowerEvacuation;
use cellule_runtime::{Error, Result};
use std::sync::Arc;
use tokio::time::Instant;

/// Original durable response and independent fresh confirmation, including errors.
pub struct FleetFollowerEvacuationPublication {
    record: std::result::Result<FollowerEvacuationRecord, Arc<Error>>,
    check: std::result::Result<FleetFollowerEvacuationCheck, Arc<Error>>,
}
impl FleetFollowerEvacuationPublication {
    /// Publishes the native rotation's exact full ensemble under current policy.
    /// Accepted backend work remains owned after cancellation or ambiguous replies.
    pub async fn publish(
        capture: &FollowerEvacuation,
        policy: FollowerReplacementPolicy,
        journal: &dyn FleetFollowerEvacuationJournal,
        verifier: &FleetFollowerEvacuationVerifier,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let record = capture.durable_record(policy)?;
        Self::publish_record(
            capture.snapshot(),
            &record,
            journal,
            verifier,
            deadline,
            clock,
        )
        .await
    }
    /// Commits fresh policy/ensemble metadata without repeating original retirement.
    pub async fn publish_refreshed(
        candidate: &FleetFollowerEvacuationCandidate,
        journal: &dyn FleetFollowerEvacuationJournal,
        verifier: &FleetFollowerEvacuationVerifier,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        Self::publish_record(
            candidate.snapshot(),
            candidate.record(),
            journal,
            verifier,
            deadline,
            clock,
        )
        .await
    }
    async fn publish_record(
        snapshot: &FleetJournalSnapshot,
        record: &FollowerEvacuationRecord,
        journal: &dyn FleetFollowerEvacuationJournal,
        verifier: &FleetFollowerEvacuationVerifier,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        if let Some(original) = bounded(deadline, async {
            journal
                .load_follower_evacuation(
                    record.policy().scope(),
                    record.digest().map_err(operation)?,
                )
                .await
                .map_err(adapter)
        })
        .await?
        {
            if &original != record {
                return Err(Error::Fenced);
            }
            let check = verifier
                .recheck(journal, &original, deadline, &mut clock)
                .await
                .map_err(Arc::new);
            return Ok(Self {
                record: Ok(original),
                check,
            });
        }
        let (started, mut last) = record.interval();
        let mut clock = || {
            let now = clock()?;
            interval(started, last, now)?;
            last = now;
            Ok(now)
        };
        verifier
            .candidate(journal, record, snapshot, deadline, &mut clock)
            .await?;
        let now = clock()?;
        let returned = bounded(deadline, async {
            let returned = journal
                .persist_follower_evacuation(snapshot, record, now)
                .await
                .map_err(adapter)?;
            if &returned != record {
                return Err(Error::Fenced);
            }
            Ok(returned)
        })
        .await
        .map_err(Arc::new);
        let check = match &returned {
            Ok(record) => verifier
                .recheck(journal, record, deadline, &mut clock)
                .await
                .map_err(Arc::new),
            Err(error) => Err(Arc::clone(error)),
        };
        Ok(Self {
            record: returned,
            check,
        })
    }
    /// Original returned history or unchanged shared source failure.
    pub fn record(&self) -> std::result::Result<&FollowerEvacuationRecord, Arc<Error>> {
        self.record.as_ref().map_err(Arc::clone)
    }
    /// Independent current confirmation; historical success alone cannot settle a role.
    pub fn confirmed(&self) -> std::result::Result<&FleetFollowerEvacuationCheck, Arc<Error>> {
        self.check.as_ref().map_err(Arc::clone)
    }
}
