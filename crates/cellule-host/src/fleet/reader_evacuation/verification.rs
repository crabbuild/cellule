use super::*;
use crate::read_replicas::ReaderReplacement;
use cellule_runtime::{
    Error, Result,
    client::Receipt,
    control::{Control, authority::CellAuthority},
    fleet::operations::{EnrollmentRole, MaintenancePhase},
    node::{NodeDirectory, NodeMode},
    peer::ReplicaPeerClient,
    read_policy::ReadPolicyStore,
};
use tokio::time::{Instant, timeout_at};

/// Canonical readers of current authority, policy, signed boots and native status.
/// It starts no role effect or task; the application authenticates peer routing
/// and accounts bounded metadata and its existing accepted backend work.
#[derive(Clone)]
pub struct FleetReaderEvacuationVerifier {
    pub(super) directory: NodeDirectory,
    pub(super) authority: CellAuthority,
    pub(super) policy: ReadPolicyStore,
    pub(super) peer: ReplicaPeerClient,
}
impl FleetReaderEvacuationVerifier {
    /// Binds the existing canonical handles and authenticated native peer client.
    #[must_use]
    pub fn new(
        directory: NodeDirectory,
        authority: CellAuthority,
        policy: ReadPolicyStore,
        peer: ReplicaPeerClient,
    ) -> Self {
        Self {
            directory,
            authority,
            policy,
            peer,
        }
    }
    /// Reloads the latest durable manifest and every exact page, then confirms
    /// current policy, authority, enrollment, selected boots and native prefixes
    /// twice around the full roster barrier. Historical times stay unchanged;
    /// this result has a separate fresh interval and grants no node finalization.
    pub async fn recheck(
        &self,
        journal: &dyn FleetReaderEvacuationJournal,
        record: &ReaderEvacuationRecord,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetReaderEvacuationCheck> {
        let scope = record.retired().spec().scope;
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let next = clock()?;
            if next < last || started < 0 || next - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let snapshot = journal.load_snapshot(scope).await.map_err(adapter)?;
            if journal
                .latest_reader_evacuation(
                    &snapshot,
                    record.operation().id(),
                    record.retired().spec().key().map_err(operation)?,
                )
                .await
                .map_err(adapter)?
                .as_ref()
                != Some(record)
            {
                return Err(Error::Fenced);
            }
            let mut pages = Vec::with_capacity(record.pages().len());
            for digest in record.pages() {
                pages.push(
                    journal
                        .load_reader_evacuation_page(scope, *digest)
                        .await
                        .map_err(adapter)?
                        .ok_or(Error::Fenced)?,
                );
            }
            self.confirm(
                journal, record, &pages, &snapshot, deadline, &mut clock, started,
            )
            .await
        })
        .await
    }
    /// Confirms a newly constructed native candidate before its first durable
    /// publication. The caller still owns its original collection interval.
    pub(super) async fn candidate(
        &self,
        journal: &dyn FleetReaderEvacuationJournal,
        record: &ReaderEvacuationRecord,
        pages: &[ReaderEvacuationPage],
        snapshot: &FleetJournalSnapshot,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<FleetReaderEvacuationCheck> {
        let started = clock()?;
        bounded(
            deadline,
            self.confirm(journal, record, pages, snapshot, deadline, clock, started),
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn confirm(
        &self,
        journal: &dyn FleetReaderEvacuationJournal,
        record: &ReaderEvacuationRecord,
        pages: &[ReaderEvacuationPage],
        snapshot: &FleetJournalSnapshot,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
        started: i64,
    ) -> Result<FleetReaderEvacuationCheck> {
        record.validate_pages(pages).map_err(operation)?;
        let operation_record = record.operation();
        let scope = record.retired().spec().scope;
        let current = snapshot.head().maintenance().ok_or(Error::Fenced)?;
        if self.directory.fleet() != scope.fleet
            || current.id() != operation_record.id()
            || current.node() != operation_record.node()
            || current.session() != operation_record.session()
            || current.intent_revision() != operation_record.intent_revision()
            || current.deadline_ms() != operation_record.deadline_ms()
            || !matches!(
                current.phase(),
                MaintenancePhase::Evacuating | MaintenancePhase::Closing
            )
            || snapshot.registry().bootstrap_revision().is_none()
        {
            return Err(Error::Fenced);
        }
        let roster = FleetRoster::collect(journal, snapshot, deadline).await?;
        let intent = roster
            .intents()
            .iter()
            .find(|intent| intent.node() == current.node())
            .ok_or(Error::Fenced)?;
        if intent.session() != current.session()
            || intent.revision() != current.intent_revision()
            || intent.mode() != NodeMode::Draining
            || !roster
                .enrollments()
                .iter()
                .any(|row| row == record.retired())
        {
            return Err(Error::Fenced);
        }
        let EnrollmentRole::Reader { target, position } = &record.retired().spec().role else {
            return Err(Error::Fenced);
        };
        let minimum = Receipt {
            cell: target.cell_id(),
            incarnation: position.incarnation,
            commit_sequence: record.minimum_sequence(),
        };
        let witnesses: Vec<_> = pages
            .iter()
            .flat_map(|page| page.entries())
            .cloned()
            .collect();
        let (authority, replacements) = self
            .confirm_current(
                journal,
                &roster,
                super::current::CurrentReaderPolicy {
                    target,
                    authority: record.authority(),
                    minimum,
                    policy_revision: record.policy_revision(),
                    desired_readers: record.desired_readers(),
                    witnesses: &witnesses,
                    operation_deadline_ms: current.deadline_ms(),
                },
                deadline,
                clock,
            )
            .await?;
        let finished = clock()?;
        if finished < started || finished - started > 30_000 || finished >= current.deadline_ms() {
            return Err(Error::Deadline);
        }
        Ok(FleetReaderEvacuationCheck {
            snapshot: snapshot.clone(),
            record: record.digest().map_err(operation)?,
            history: record.clone(),
            roster_digest: roster.digest()?,
            authority,
            replacements,
            started_at_ms: started,
            finished_at_ms: finished,
        })
    }
}

/// Fresh confirmation of one persisted reader obligation; other roles remain.
pub struct FleetReaderEvacuationCheck {
    snapshot: FleetJournalSnapshot,
    record: Digest,
    history: ReaderEvacuationRecord,
    roster_digest: Digest,
    authority: Control,
    replacements: Vec<ReaderReplacement>,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetReaderEvacuationCheck {
    /// Complete immutable history whose policy and replacements were rechecked.
    /// Its original times are separate from this confirmation's fresh interval.
    #[must_use]
    pub fn record(&self) -> &ReaderEvacuationRecord {
        &self.history
    }
    /// Exact final journal barrier; compare it again before dependent actions.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Immutable durable history confirmed by this fresh observation.
    #[must_use]
    pub const fn record_digest(&self) -> Digest {
        self.record
    }
    /// Full roster identity, including original terminal and Pending rows.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster_digest
    }
    /// Current serving authority, without ownership rights.
    #[must_use]
    pub fn authority(&self) -> &Control {
        &self.authority
    }
    /// Actual selected/probed replacements and their nonregressing prefixes.
    #[must_use]
    pub fn replacements(&self) -> &[ReaderReplacement] {
        &self.replacements
    }
    /// Separate fresh interval; the persisted record keeps its original times.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}
pub(super) fn adapter(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-reader-evacuation-journal",
        source,
    }
}
pub(super) async fn bounded<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(Error::Deadline);
    }
    timeout_at(deadline, future)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-reader-evacuation-deadline",
            source: Box::new(source),
        })?
}
