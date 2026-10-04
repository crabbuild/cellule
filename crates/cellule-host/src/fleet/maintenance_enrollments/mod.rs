//! Complete original journal responsibilities retained through fresh observation.
use super::{FleetJournal, FleetJournalSnapshot, FleetRoster, operation};
use cellule_runtime::fleet::operations::{
    EnrollmentRecord, EnrollmentStatus, MaintenanceEnrollmentInventory, MaintenanceEnrollmentPage,
};
use cellule_runtime::identity::Digest;
use cellule_runtime::{Error, Result};
use std::collections::HashMap;
use std::future::Future;
use tokio::time::{Instant, timeout_at};

/// Complete original maintenance role set matched against one fresh full roster.
///
/// Collectors retain the original operation, acceptances, pages and timestamps
/// after rows retire. Applications account bounded retained buffers (at most
/// 10,000 entries). This proves committed original coverage and compatible row
/// progress; current authority, replacement policy, native/external joining and
/// finalization remain separate requirements. A missing manifest is unknown.
pub struct FleetMaintenanceEnrollments {
    snapshot: FleetJournalSnapshot,
    roster: Digest,
    original: MaintenanceEnrollmentInventory,
    pages: Vec<MaintenanceEnrollmentPage>,
    interval: (i64, i64),
}
impl FleetMaintenanceEnrollments {
    /// Reads every immutable original page, validates its committed manifest and
    /// matches every original acceptance with the retained current roster. Full
    /// head/registry rechecks surround collection; no retirement or effect starts.
    pub async fn collect(
        journal: &dyn FleetJournal,
        roster: &FleetRoster,
        deadline: Instant,
        clock: impl Fn() -> Result<i64>,
    ) -> Result<Self> {
        let started = clock()?;
        roster.confirm(journal, deadline).await?;
        let maintenance = roster
            .snapshot()
            .head()
            .maintenance()
            .ok_or(Error::Fenced)?;
        let original = call(
            deadline,
            journal.maintenance_enrollments(roster.snapshot(), maintenance.id()),
        )
        .await?
        .ok_or(Error::Control(
            "original maintenance enrollments are unknown",
        ))?;
        if original.operation().id() != maintenance.id()
            || original.operation().node() != maintenance.node()
            || original.operation().request_digest() != maintenance.request_digest()
            || original.operation().created_at_ms() != maintenance.created_at_ms()
            || original.registry().scope() != roster.snapshot().registry().scope()
            || original.registry().bootstrap_revision()
                != roster.snapshot().registry().bootstrap_revision()
            || original.registry().revision() > roster.snapshot().registry().revision()
        {
            return Err(Error::Fenced);
        }
        let mut pages = Vec::with_capacity(original.pages().len());
        for digest in original.pages() {
            let page = call(
                deadline,
                journal.maintenance_enrollment_page(original.registry().scope(), *digest),
            )
            .await?
            .ok_or(Error::Control(
                "original maintenance enrollment page is missing",
            ))?;
            pages.push(page);
        }
        original.validate_pages(&pages).map_err(operation)?;
        let current = roster
            .enrollments()
            .iter()
            .map(|row| Ok((*row.spec().key().map_err(operation)?.as_bytes(), row)))
            .collect::<Result<HashMap<_, _>>>()?;
        for row in pages.iter().flat_map(|page| page.entries()) {
            let current = current
                .get(row.spec().key().map_err(operation)?.as_bytes())
                .ok_or(Error::Fenced)?;
            row.validate_replay(current.spec()).map_err(operation)?;
            if row.accepted_at_ms() != current.accepted_at_ms()
                || current.updated_at_ms() < row.updated_at_ms()
                || (row.status() == EnrollmentStatus::Established
                    && (!matches!(
                        current.status(),
                        EnrollmentStatus::Established | EnrollmentStatus::Retired
                    ) || row.established_evidence() != current.established_evidence()))
                || (current.status() == EnrollmentStatus::Pending && row != *current)
            {
                return Err(Error::Fenced);
            }
        }
        roster.confirm(journal, deadline).await?;
        let finished = clock()?;
        if started < 0
            || finished < started
            || finished - started > 30_000
            || original.captured_at_ms() > started
        {
            return Err(Error::Fenced);
        }
        Ok(Self {
            snapshot: roster.snapshot().clone(),
            roster: roster.digest()?,
            original,
            pages,
            interval: (started, finished),
        })
    }
    /// Fresh exact head/registry against which original row progress was checked.
    #[must_use]
    pub const fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Full current retained roster, including every terminal row.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// Immutable original set; no timestamp, boot or acceptance is substituted.
    #[must_use]
    pub const fn original(&self) -> &MaintenanceEnrollmentInventory {
        &self.original
    }
    /// Every original unresolved reader/follower acceptance at either endpoint.
    pub fn entries(&self) -> impl Iterator<Item = &EnrollmentRecord> {
        self.pages.iter().flat_map(|page| page.entries())
    }
    /// Original fresh read interval; this never restamps the persisted capture.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.interval
    }
    /// Binds original metadata and fresh full observation, without settlement rights.
    pub fn digest(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-maintenance-enrollments.v1\0");
        hash.update(self.original.digest().map_err(operation)?.as_bytes());
        hash.update(self.roster.as_bytes());
        for bytes in [
            self.snapshot.head().to_bytes().map_err(operation)?,
            self.snapshot.registry().to_bytes().map_err(operation)?,
        ] {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(&bytes);
        }
        for time in [self.interval.0, self.interval.1] {
            hash.update(&time.to_be_bytes());
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}
async fn call<T>(
    deadline: Instant,
    future: impl Future<Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>>,
) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(Error::Control(
            "maintenance enrollment capture deadline expired",
        ));
    }
    timeout_at(deadline, future)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-maintenance-enrollments-deadline",
            source: Box::new(source),
        })?
        .map_err(|source| Error::Facility {
            name: "fleet-maintenance-enrollments-journal",
            source,
        })
}
