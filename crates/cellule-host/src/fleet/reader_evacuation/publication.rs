use super::verification::{adapter, bounded};
use super::*;
use crate::read_replicas::ReaderEvacuation;
use cellule_runtime::{Error, Result};
use std::sync::Arc;
use tokio::time::Instant;

/// Durable response and independent final reader-policy confirmation.
pub struct FleetReaderEvacuationPublication {
    record: std::result::Result<ReaderEvacuationRecord, Arc<Error>>,
    check: std::result::Result<FleetReaderEvacuationCheck, Arc<Error>>,
}
impl FleetReaderEvacuationPublication {
    /// Publishes the native capsule's exact immutable history after current
    /// policy/readiness confirmation. Backend ownership survives cancellation;
    /// lookup/adoption uses the same digest after an ambiguous reply. A final
    /// failure retains the actual committed record and its separate source error.
    pub async fn publish(
        capture: &ReaderEvacuation,
        journal: &dyn FleetReaderEvacuationJournal,
        verifier: &FleetReaderEvacuationVerifier,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let (record, pages) = capture.durable_record()?;
        Self::publish_record(
            capture.snapshot(),
            &record,
            &pages,
            journal,
            verifier,
            deadline,
            &mut clock,
        )
        .await
    }
    /// Commits a refreshed policy candidate without repeating original native closure.
    pub async fn publish_refreshed(
        candidate: &FleetReaderEvacuationCandidate,
        journal: &dyn FleetReaderEvacuationJournal,
        verifier: &FleetReaderEvacuationVerifier,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        Self::publish_record(
            candidate.snapshot(),
            candidate.record(),
            candidate.pages(),
            journal,
            verifier,
            deadline,
            clock,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn publish_record(
        snapshot: &FleetJournalSnapshot,
        record: &ReaderEvacuationRecord,
        pages: &[ReaderEvacuationPage],
        journal: &dyn FleetReaderEvacuationJournal,
        verifier: &FleetReaderEvacuationVerifier,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        if let Some(original) = bounded(deadline, async {
            journal
                .load_reader_evacuation(
                    record.retired().spec().scope,
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
            let next = clock()?;
            if next < last || started < 0 || next - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        verifier
            .candidate(journal, record, pages, snapshot, deadline, &mut clock)
            .await?;
        let now = clock()?;
        let returned = bounded(deadline, async {
            let returned = journal
                .persist_reader_evacuation(snapshot, record, pages, now)
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
            Ok(returned) => verifier
                .recheck(journal, returned, deadline, &mut clock)
                .await
                .map_err(Arc::new),
            Err(error) => Err(Arc::clone(error)),
        };
        Ok(Self {
            record: returned,
            check,
        })
    }
    /// Original durable response or unchanged shared publication source error.
    pub fn record(&self) -> std::result::Result<&ReaderEvacuationRecord, Arc<Error>> {
        self.record.as_ref().map_err(Arc::clone)
    }
    /// Complete fresh post-publication confirmation or its original shared error.
    pub fn confirmed(&self) -> std::result::Result<&FleetReaderEvacuationCheck, Arc<Error>> {
        self.check.as_ref().map_err(Arc::clone)
    }
}
