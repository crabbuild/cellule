use super::*;
use crate::durability::enrollment::maintenance::boot_identity;
use cellule_runtime::{
    Error, Result,
    fleet::operations::MaintenancePhase,
    node::{NodeAdvertisement, NodeDirectory, NodeMode, log_state::NodeLogPhase},
};
use tokio::time::{Instant, timeout_at};

/// Read-only canonical revalidation; uses no second supervisor or native task.
#[derive(Clone)]
pub struct FleetFollowerEvacuationVerifier {
    pub(super) directory: NodeDirectory,
    pub(super) transport: std::sync::Arc<dyn FleetSnapshotTransport>,
}
impl FleetFollowerEvacuationVerifier {
    /// Binds the existing signed canonical directory. Applications pin signing
    /// keys and authentic management endpoints before consuming observations.
    #[must_use]
    pub fn new(
        directory: NodeDirectory,
        transport: std::sync::Arc<dyn FleetSnapshotTransport>,
    ) -> Self {
        Self {
            directory,
            transport,
        }
    }
    /// Reloads latest history, current policy, complete roster and exact signed
    /// ensemble twice. Fresh interval metadata never renews historical times.
    /// Native/foreign inventories and failed processes remain separate barriers.
    pub async fn recheck(
        &self,
        journal: &dyn FleetFollowerEvacuationJournal,
        record: &FollowerEvacuationRecord,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFollowerEvacuationCheck> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let now = clock()?;
            interval(started, last, now)?;
            last = now;
            Ok(now)
        };
        bounded(deadline, async {
            let snapshot = journal
                .load_snapshot(record.policy().scope())
                .await
                .map_err(adapter)?;
            if journal
                .latest_follower_evacuation(
                    &snapshot,
                    record.operation().id(),
                    record.original_key(),
                )
                .await
                .map_err(adapter)?
                .as_ref()
                != Some(record)
            {
                return Err(Error::Fenced);
            }
            self.confirm(journal, record, &snapshot, deadline, &mut clock, started)
                .await
        })
        .await
    }
    pub(super) async fn candidate(
        &self,
        journal: &dyn FleetFollowerEvacuationJournal,
        record: &FollowerEvacuationRecord,
        snapshot: &FleetJournalSnapshot,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<FleetFollowerEvacuationCheck> {
        let started = clock()?;
        bounded(
            deadline,
            self.confirm(journal, record, snapshot, deadline, clock, started),
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn confirm(
        &self,
        journal: &dyn FleetFollowerEvacuationJournal,
        record: &FollowerEvacuationRecord,
        snapshot: &FleetJournalSnapshot,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
        started: i64,
    ) -> Result<FleetFollowerEvacuationCheck> {
        record.to_bytes().map_err(operation)?;
        let original = record.operation();
        let current = snapshot.head().maintenance().ok_or(Error::Fenced)?;
        if self.directory.fleet() != record.policy().scope().fleet
            || snapshot.head().scope() != record.policy().scope()
            || snapshot.registry().bootstrap_revision().is_none()
            || current.id() != original.id()
            || current.node() != original.node()
            || current.session() != original.session()
            || current.intent_revision() != original.intent_revision()
            || current.deadline_ms() != original.deadline_ms()
            || !matches!(
                current.phase(),
                MaintenancePhase::Evacuating | MaintenancePhase::Closing
            )
        {
            return Err(Error::Fenced);
        }
        let roster = FleetRoster::collect(journal, snapshot, deadline).await?;
        let intent = roster
            .intents()
            .iter()
            .find(|row| row.node() == current.node())
            .ok_or(Error::Fenced)?;
        if intent.session() != current.session()
            || intent.revision() != current.intent_revision()
            || intent.mode() != NodeMode::Draining
        {
            return Err(Error::Fenced);
        }
        for row in record.retired() {
            if !roster.enrollments().iter().any(|current| current == row) {
                return Err(Error::Fenced);
            }
        }
        let source = record.source().map_err(operation)?;
        for (epoch, expected) in [
            (
                record.original_epoch().map_err(operation)?,
                record.retired().iter().collect::<Vec<_>>(),
            ),
            (
                record.replacement_epoch(),
                record
                    .replacements()
                    .iter()
                    .map(|entry| &entry.enrollment)
                    .collect::<Vec<_>>(),
            ),
        ] {
            let all = roster
                .enrollments()
                .iter()
                .filter(|row| {
                    row.spec().source.is_some_and(|endpoint| {
                        endpoint.node == source.node && endpoint.session == source.session
                    }) && row.spec().role
                        == (cellule_runtime::fleet::operations::EnrollmentRole::Follower {
                            log_epoch: epoch,
                        })
                })
                .collect::<Vec<_>>();
            if all.len() != expected.len() || all.iter().any(|row| !expected.contains(row)) {
                return Err(Error::Fenced);
            }
        }
        let mut authority: Option<NodeAdvertisement> = None;
        let mut native = Vec::new();
        let mut last = started;
        for _ in 0..2 {
            let now = clock()?;
            interval(started, last, now)?;
            last = now;
            if now >= current.deadline_ms() {
                return Err(Error::Deadline);
            }
            if journal
                .follower_replacement_policy(snapshot)
                .await
                .map_err(adapter)?
                != Some(record.policy())
            {
                return Err(Error::Fenced);
            }
            native = self
                .collect_native(&roster, record, deadline, clock)
                .await?;
            let observed = self.observe_directory(&roster, record, clock).await?;
            if authority
                .as_ref()
                .is_some_and(|previous| !nonregressing(previous, &observed))
            {
                return Err(Error::Fenced);
            }
            if journal
                .follower_replacement_policy(snapshot)
                .await
                .map_err(adapter)?
                != Some(record.policy())
            {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            // Reobserve every original boot after suspended journal reads. The
            // source is read again after all members, including the final pass.
            self.recheck_native(&roster, &mut native, deadline, clock)
                .await?;
            let after = self.observe_directory(&roster, record, clock).await?;
            if !nonregressing(&observed, &after) {
                return Err(Error::Fenced);
            }
            authority = Some(after);
            if journal
                .follower_replacement_policy(snapshot)
                .await
                .map_err(adapter)?
                != Some(record.policy())
            {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
        }
        let finished = clock()?;
        interval(started, last, finished)?;
        if finished >= current.deadline_ms() {
            return Err(Error::Deadline);
        }
        Ok(FleetFollowerEvacuationCheck {
            snapshot: snapshot.clone(),
            record: record.digest().map_err(operation)?,
            roster_digest: roster.digest()?,
            authority: authority.ok_or(Error::Fenced)?,
            started_at_ms: started,
            finished_at_ms: finished,
            native,
        })
    }
    async fn observe_directory(
        &self,
        roster: &FleetRoster,
        record: &FollowerEvacuationRecord,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<NodeAdvertisement> {
        let source = record.source().map_err(operation)?;
        let expected_members = record
            .replacements()
            .iter()
            .map(|entry| entry.enrollment.spec().target.node)
            .collect::<Vec<_>>();
        let source_boot = roster.boot(source.node, source.session)?;
        if source_boot.intent().mode() != NodeMode::Active {
            return Err(Error::Fenced);
        }
        let valid = |leader: &NodeAdvertisement, now: i64| -> Result<bool> {
            Ok(leader.node() == source.node
                && boot_identity(leader)? == record.source_boot()
                && leader.accepts_new_roles(now)
                && leader.log().is_some_and(|log| {
                    log.phase() == NodeLogPhase::Open
                        && log.epoch() == record.replacement_epoch()
                        && log.members() == expected_members
                }))
        };
        let leader = self
            .directory
            .load_if_live(source.session, clock()?)
            .await?
            .ok_or(Error::Fenced)?;
        if !valid(leader.advertisement(), clock()?)? {
            return Err(Error::Fenced);
        }
        for entry in record.replacements() {
            let row = &entry.enrollment;
            let endpoint = row.spec().target;
            if !roster.enrollments().iter().any(|current| current == row)
                || row.spec().source.is_none_or(|endpoint| {
                    endpoint.intent_revision != source_boot.intent().revision()
                })
            {
                return Err(Error::Fenced);
            }
            let boot = roster.boot(endpoint.node, endpoint.session)?;
            let signed = self
                .directory
                .load_if_live(endpoint.session, clock()?)
                .await?
                .ok_or(Error::Fenced)?;
            if boot.intent().mode() != NodeMode::Active
                || boot.intent().revision() != endpoint.intent_revision
                || signed.advertisement().node() != endpoint.node
                || boot_identity(signed.advertisement())? != entry.boot_identity
                || !signed.advertisement().accepts_new_roles(clock()?)
            {
                return Err(Error::Fenced);
            }
        }
        let after = self
            .directory
            .load_if_live(source.session, clock()?)
            .await?
            .ok_or(Error::Fenced)?;
        if !valid(after.advertisement(), clock()?)?
            || !nonregressing(leader.advertisement(), after.advertisement())
        {
            return Err(Error::Fenced);
        }
        Ok(after.advertisement().clone())
    }
}
/// Separate fresh canonical confirmation of one persisted follower obligation.
pub struct FleetFollowerEvacuationCheck {
    snapshot: FleetJournalSnapshot,
    record: Digest,
    roster_digest: Digest,
    authority: NodeAdvertisement,
    started_at_ms: i64,
    finished_at_ms: i64,
    native: Vec<FleetNodeInventory>,
}
impl FleetFollowerEvacuationCheck {
    /// Original complete source/member native traversals with all-category
    /// rechecks after authority discovery. Other physical roles remain separate.
    #[must_use]
    pub fn native(&self) -> &[FleetNodeInventory] {
        &self.native
    }
    /// Full final barrier; dependent transactions compare it again.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Exact immutable record independently confirmed here.
    #[must_use]
    pub const fn record_digest(&self) -> Digest {
        self.record
    }
    /// Complete retained roster identity, including Pending and terminal rows.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster_digest
    }
    /// Current signed canonical leader/log, without ownership permission.
    #[must_use]
    pub fn authority(&self) -> &NodeAdvertisement {
        &self.authority
    }
    /// Fresh confirmation interval, separate from original capture times.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}
pub(super) fn adapter(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-follower-evacuation-journal",
        source,
    }
}
pub(super) fn interval(start: i64, last: i64, now: i64) -> Result<()> {
    if start < 0 || now < last || now - start > 30_000 {
        return Err(Error::Deadline);
    }
    Ok(())
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
            name: "fleet-follower-evacuation-deadline",
            source: Box::new(source),
        })?
}

fn nonregressing(before: &NodeAdvertisement, after: &NodeAdvertisement) -> bool {
    after.generation() >= before.generation()
        && after.issued_at_ms() >= before.issued_at_ms()
        && after.log().is_some_and(|log| {
            before
                .log()
                .is_some_and(|old| log.tiered_through() >= old.tiered_through())
        })
}
