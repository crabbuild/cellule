//! Independent original-work confirmation for terminal unknown acceptances.
use super::{FleetMaintenanceEnrollments, FleetRoster, enrollment_digest, operation, requests};
use crate::fleet::{FleetAdapterFuture, FleetJournal, FleetJournalSnapshot};
use cellule_runtime::fleet::operations::{EnrollmentStatus, MaintenancePhase};
use cellule_runtime::{Error, Result, fleet::operations::EnrollmentRecord, identity::Digest};
use tokio::time::{Instant, timeout_at};

/// Exact original acceptance and terminal exclusion to confirm independently.
/// Mutable head and collection times are excluded from the stable request digest.
pub struct FleetEnrollmentNonexecutionRequest {
    original: Option<EnrollmentRecord>,
    terminal: EnrollmentRecord,
    digest: Digest,
}
impl FleetEnrollmentNonexecutionRequest {
    /// Selects a required terminal request with no retained establishment history.
    /// This checks identity only; a Refused/Retired row cannot itself prove that
    /// accepted work was joined or that a native effect never committed.
    pub fn new(
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        key: Digest,
    ) -> Result<Self> {
        let request = requests::required(original, roster)?
            .into_iter()
            .find(|request| {
                request
                    .current
                    .spec()
                    .key()
                    .is_ok_and(|candidate| candidate == key)
            })
            .ok_or(Error::Fenced)?;
        Self::select(original, &request)
    }
    fn select(
        original: &FleetMaintenanceEnrollments,
        request: &requests::RequiredRequest<'_>,
    ) -> Result<Self> {
        if !eligible(request) {
            return Err(Error::Fenced);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-enrollment-nonexecution-request.v1\0");
        hash.update(original.original().digest().map_err(operation)?.as_bytes());
        hash.update(&[u8::from(request.original.is_some())]);
        if let Some(accepted) = request.original {
            hash.update(enrollment_digest(accepted)?.as_bytes());
        }
        hash.update(enrollment_digest(request.current)?.as_bytes());
        Ok(Self {
            original: request.original.cloned(),
            terminal: request.current.clone(),
            digest: Digest::from_bytes(*hash.finalize().as_bytes()),
        })
    }
    /// Immutable first-evacuation acceptance, if present.
    #[must_use]
    pub fn original(&self) -> Option<&EnrollmentRecord> {
        self.original.as_ref()
    }
    /// Exact current terminal row; preserve its original settlement witness.
    #[must_use]
    pub const fn terminal(&self) -> &EnrollmentRecord {
        &self.terminal
    }
    /// Stable original/terminal request identity across head/controller changes.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Application-authenticated original nonexecution evidence retained durably.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetEnrollmentNonexecutionEvidence {
    request: Digest,
    witness: Digest,
}
impl FleetEnrollmentNonexecutionEvidence {
    /// Checks shape and the exact terminal settlement witness. Construction does
    /// not authenticate evidence or join work; the provider owns those duties.
    pub fn new(request: &FleetEnrollmentNonexecutionRequest, witness: Digest) -> Result<Self> {
        if witness.as_bytes().iter().all(|byte| *byte == 0)
            || request.terminal.settlement_evidence() != Some(witness)
        {
            return Err(Error::Fenced);
        }
        Ok(Self {
            request: request.digest,
            witness,
        })
    }
    /// Exact original request independently confirmed by the provider.
    #[must_use]
    pub const fn request_digest(&self) -> Digest {
        self.request
    }
    /// Original durable settlement evidence, never minted during confirmation.
    #[must_use]
    pub const fn witness(&self) -> Digest {
        self.witness
    }
}

/// Read-only authenticated provider of retained original enrollment evidence.
///
/// Confirm the exact original role, both endpoints, acceptance and terminal
/// witness. Confirm earlier joining of every originally accepted native/external producer task and
/// prove that its ordinary role effect never committed or installed a role.
/// Exclude delayed execution/restart of the same request. Retain that evidence
/// durably and return the same binding after provider/controller restart. Neither
/// a terminal journal row, absent inventory, successful recovery, timeout nor
/// lease expiry supplies this proof. This read must start no cancellation,
/// closure or replacement effect. A committed role needs native retirement and
/// current replacement policy instead, even if establishment publication was lost.
pub trait FleetEnrollmentNonexecution: Send + Sync {
    /// Missing retained evidence is unknown and remains a coverage blocker.
    /// Preserve source failures; never turn a read error into None.
    fn confirm_unexecuted<'a>(
        &'a self,
        request: &'a FleetEnrollmentNonexecutionRequest,
    ) -> FleetAdapterFuture<'a, Option<FleetEnrollmentNonexecutionEvidence>>;
}

/// Fresh checked original nonexecution at the complete maintenance roster barrier.
/// Applications account bounded copies of at most 10,000 original/terminal rows.
pub struct FleetMaintenanceNonexecution {
    snapshot: FleetJournalSnapshot,
    roster: Digest,
    original: Digest,
    checks: Vec<(
        FleetEnrollmentNonexecutionRequest,
        FleetEnrollmentNonexecutionEvidence,
    )>,
    interval: (i64, i64),
    digest: Digest,
}
impl FleetMaintenanceNonexecution {
    /// Enumerates the same complete request set as policy matching. Confirms
    /// every eligible terminal request twice through the independent provider,
    /// then rechecks the full head/registry. One monotonic interval and deadline
    /// bound the whole read. Missing evidence remains absent, never certified.
    pub async fn collect(
        journal: &dyn FleetJournal,
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        provider: &dyn FleetEnrollmentNonexecution,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let now = clock()?;
            if started < 0 || now < last || now - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = now;
            Ok(now)
        };
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        timeout_at(deadline, async {
            let required = requests::required(original, roster)?;
            let maintenance = roster
                .snapshot()
                .head()
                .maintenance()
                .ok_or(Error::Fenced)?;
            if !matches!(
                maintenance.phase(),
                MaintenancePhase::Evacuating | MaintenancePhase::Closing
            ) {
                return Err(Error::Fenced);
            }
            if started < original.interval().1 || started >= maintenance.deadline_ms() {
                return Err(Error::Deadline);
            }
            roster.confirm(journal, deadline).await?;
            let mut checks = Vec::new();
            for request in required.iter().filter(|request| eligible(request)) {
                clock()?;
                let request = FleetEnrollmentNonexecutionRequest::select(original, request)?;
                let first = provider
                    .confirm_unexecuted(&request)
                    .await
                    .map_err(adapter)?;
                clock()?;
                let second = provider
                    .confirm_unexecuted(&request)
                    .await
                    .map_err(adapter)?;
                if first != second {
                    return Err(Error::Control("original nonexecution evidence changed"));
                }
                if let Some(evidence) = first {
                    if evidence.request != request.digest
                        || request.terminal.settlement_evidence() != Some(evidence.witness)
                    {
                        return Err(Error::Fenced);
                    }
                    checks.push((request, evidence));
                }
            }
            roster.confirm(journal, deadline).await?;
            let interval = (started, clock()?);
            if interval.1 >= maintenance.deadline_ms() {
                return Err(Error::Deadline);
            }
            let original = original.digest()?;
            let roster_digest = roster.digest()?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"cellule.fleet-maintenance-nonexecution.v1\0");
            hash.update(original.as_bytes());
            hash.update(roster_digest.as_bytes());
            for time in [interval.0, interval.1] {
                hash.update(&time.to_be_bytes());
            }
            hash.update(&(checks.len() as u64).to_be_bytes());
            for (request, evidence) in &checks {
                hash.update(request.digest.as_bytes());
                hash.update(evidence.witness.as_bytes());
            }
            Ok(Self {
                snapshot: roster.snapshot().clone(),
                roster: roster_digest,
                original,
                checks,
                interval,
                digest: Digest::from_bytes(*hash.finalize().as_bytes()),
            })
        })
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-maintenance-nonexecution-deadline",
            source: Box::new(source),
        })?
    }
    /// Exact full head and registry checked around all provider reads.
    #[must_use]
    pub const fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Full roster identity, including original terminal rows.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// The exact original capture used for enumeration, including its interval.
    #[must_use]
    pub const fn original_digest(&self) -> Digest {
        self.original
    }
    /// Canonical bound checks and original collection interval.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original whole-collection interval, never refreshed on attachment.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.interval
    }
    /// Exact independently confirmed original requests and retained evidence.
    pub fn checks(
        &self,
    ) -> impl Iterator<
        Item = (
            &FleetEnrollmentNonexecutionRequest,
            &FleetEnrollmentNonexecutionEvidence,
        ),
    > {
        self.checks
            .iter()
            .map(|(request, evidence)| (request, evidence))
    }
}
fn eligible(request: &requests::RequiredRequest<'_>) -> bool {
    matches!(
        request.current.status(),
        EnrollmentStatus::Refused | EnrollmentStatus::Retired
    ) && request.current.established_evidence().is_none()
        && request.current.settlement_evidence().is_some()
        && request
            .original
            .is_none_or(|row| row.established_evidence().is_none())
}
fn adapter(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-enrollment-nonexecution-provider",
        source,
    }
}
