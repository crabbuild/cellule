//! Original receiver lifetime closure; replacement policy remains independent.
use super::*;

mod publication;
mod records;

/// Exact reader responsibility on a canonically fenced original receiver boot.
/// Capture starts no native effect. Publication requires independently joined
/// original process and accepted-work evidence from the application provider.
/// Source failure cannot authorize retirement on a live receiver.
pub struct FleetFailedReaderRetirement {
    request: FleetFailedBootProcessRequest,
    reader: EnrollmentRecord,
}
impl FleetFailedReaderRetirement {
    /// Captures a Pending, Established or replayed Retired reader at the same
    /// complete bootstrap barrier as its original Established receiver boot.
    /// Other unresolved roles remain obligations and prevent boot retirement.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        boot: &EnrollmentRecord,
        original: &EnrollmentRecord,
        claimant: SessionId,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let request = FleetFailedBootProcessRequest::capture(
            journal, directory, roster, boot, claimant, deadline, clock,
        )
        .await?;
        let reader = records::select(roster, &request, original)?;
        Ok(Self { request, reader })
    }
    /// Full original head and registry barrier.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        self.request.snapshot()
    }
    /// Exact original receiver process request; role publication does not
    /// alter its immutable identity or authorize the receiver's boot closure.
    #[must_use]
    pub const fn request(&self) -> &FleetFailedBootProcessRequest {
        &self.request
    }
    /// Original reader request, acceptance and retained establishment history.
    #[must_use]
    pub const fn reader(&self) -> &EnrollmentRecord {
        &self.reader
    }
    /// Original collection interval, without restamping on publication.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.request.interval()
    }
}

/// Retains the journal response and final confirmation independently.
/// A committed row remains inspectable after a final-check failure; a lost
/// reply retains its original source error for reconstruction and adoption.
pub struct FleetFailedReaderPublication {
    process: FleetFailedBootProcessEvidence,
    record: std::result::Result<EnrollmentRecord, Arc<Error>>,
    closure: std::result::Result<FleetFailedReaderClosure, Arc<Error>>,
}
impl FleetFailedReaderPublication {
    /// Durable original receiver lifetime witness used by publication.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Original returned row or retained publication source error.
    pub fn record(&self) -> std::result::Result<&EnrollmentRecord, Arc<Error>> {
        self.record.as_ref().map_err(Arc::clone)
    }
    /// Checked original reader retirement after all final reads.
    pub fn confirmed(&self) -> Result<&FleetFailedReaderClosure> {
        self.closure
            .as_ref()
            .map_err(|source| retained(Arc::clone(source)))
    }
    /// Original final-check failure without discarding the journal response.
    #[must_use]
    pub fn closure_error(&self) -> Option<Arc<Error>> {
        self.closure.as_ref().err().cloned()
    }
}

/// Interval evidence that one original reader's receiver cannot execute again.
/// This does not prove replacement redundancy, writer relocation, other role
/// closure, boot retirement or completed physical maintenance.
pub struct FleetFailedReaderClosure {
    snapshot: FleetJournalSnapshot,
    reader: EnrollmentRecord,
    process: FleetFailedBootProcessEvidence,
    digest: Digest,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetFailedReaderClosure {
    /// Complete post-publication barrier, confirmed around authority/provider reads.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original retired reader, including acceptance and establishment history.
    #[must_use]
    pub const fn reader(&self) -> &EnrollmentRecord {
        &self.reader
    }
    /// Application-authenticated original receiver lifetime evidence.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Stable identity of the original terminal row and process witness.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original capture through final confirmation, bounded to thirty seconds.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}
