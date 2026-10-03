use super::verification::{adapter, bounded, interval};
use super::*;
use crate::durability::enrollment::maintenance::boot_identity;
use cellule_runtime::{
    Error, Result,
    fleet::operations::{
        EnrollmentRole, EnrollmentStatus, FollowerReplacementWitness, MaintenancePhase,
    },
    node::log_state::NodeLogPhase,
};
use tokio::time::Instant;

/// Fresh current ensemble/policy capture retaining the original native retirement.
pub struct FleetFollowerEvacuationCandidate {
    snapshot: FleetJournalSnapshot,
    record: FollowerEvacuationRecord,
}
impl FleetFollowerEvacuationCandidate {
    /// Complete original barrier used by this fresh candidate.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// New immutable metadata; all original retired rows/timestamps stay unchanged.
    #[must_use]
    pub fn record(&self) -> &FollowerEvacuationRecord {
        &self.record
    }
}
impl FleetFollowerEvacuationVerifier {
    /// Refreshes committed live-owner history after policy, ensemble or operation
    /// adoption. Ordinary recruitment supplies current replacements. This starts
    /// no rotation, native retirement, recovery or producer work. A failed original
    /// leader still requires canonical recovery and separate affected-Cell evidence.
    pub async fn refresh(
        &self,
        journal: &dyn FleetFollowerEvacuationJournal,
        original: &FollowerEvacuationRecord,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetFollowerEvacuationCandidate> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let now = clock()?;
            interval(started, last, now)?;
            last = now;
            Ok(now)
        };
        bounded(deadline, async {
            let scope = original.policy().scope();
            if journal
                .load_follower_evacuation(scope, original.digest().map_err(operation)?)
                .await
                .map_err(adapter)?
                .as_ref()
                != Some(original)
            {
                return Err(Error::Fenced);
            }
            let snapshot = journal.load_snapshot(scope).await.map_err(adapter)?;
            let maintenance = snapshot.head().maintenance().ok_or(Error::Fenced)?;
            if maintenance.id() != original.operation().id()
                || maintenance.node() != original.operation().node()
                || !matches!(
                    maintenance.phase(),
                    MaintenancePhase::Evacuating | MaintenancePhase::Closing
                )
            {
                return Err(Error::Fenced);
            }
            let policy = journal
                .follower_replacement_policy(&snapshot)
                .await
                .map_err(adapter)?
                .ok_or(Error::Fenced)?;
            let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
            for row in original.retired() {
                if !roster.enrollments().iter().any(|current| current == row) {
                    return Err(Error::Fenced);
                }
            }
            let source = original.source().map_err(operation)?;
            let leader = self
                .directory
                .load_if_live(source.session, clock()?)
                .await?
                .ok_or(Error::Fenced)?;
            let leader = leader.advertisement();
            if leader.node() != source.node || boot_identity(leader)? != original.source_boot() {
                return Err(Error::Fenced);
            }
            let original_epoch = original.original_epoch().map_err(operation)?;
            let log = leader
                .log()
                .filter(|log| log.phase() == NodeLogPhase::Open && log.epoch() > original_epoch)
                .ok_or(Error::Fenced)?;
            let mut replacements = Vec::with_capacity(log.members().len());
            for member in log.members() {
                let mut rows = roster.enrollments().iter().filter(|row| {
                    row.status() == EnrollmentStatus::Established
                        && row.spec().source.is_some_and(|endpoint| {
                            endpoint.node == source.node && endpoint.session == source.session
                        })
                        && row.spec().target.node == *member
                        && row.spec().role
                            == (EnrollmentRole::Follower {
                                log_epoch: log.epoch(),
                            })
                });
                let row = rows.next().ok_or(Error::Fenced)?;
                if rows.next().is_some() {
                    return Err(Error::Fenced);
                }
                let boot = self
                    .directory
                    .load_if_live(row.spec().target.session, clock()?)
                    .await?
                    .ok_or(Error::Fenced)?;
                replacements.push(FollowerReplacementWitness {
                    enrollment: row.clone(),
                    boot_identity: boot_identity(boot.advertisement())?,
                });
            }
            // This is a fresh canonical observation identity, distinct from the
            // original enrollment proof and from native retirement history.
            let mut evidence = blake3::Hasher::new();
            evidence.update(b"cellule.follower-maintenance-observation.v1\0");
            evidence.update(boot_identity(leader)?.as_bytes());
            evidence.update(&leader.generation().to_le_bytes());
            evidence.update(&leader.issued_at_ms().to_le_bytes());
            evidence.update(&leader.expires_at_ms().to_le_bytes());
            evidence.update(&log.epoch().to_le_bytes());
            evidence.update(&log.tiered_through().to_le_bytes());
            for member in log.members() {
                evidence.update(member.as_bytes());
            }
            let evidence = Digest::from_bytes(*evidence.finalize().as_bytes());
            let record = FollowerEvacuationRecord::new(
                maintenance.clone(),
                (
                    Digest::from_bytes(
                        *blake3::hash(&snapshot.head().to_bytes().map_err(operation)?).as_bytes(),
                    ),
                    snapshot.registry(),
                ),
                policy,
                (original.original_key(), original.original_digest()),
                original.retired().to_vec(),
                original.covered_through(),
                original.source_boot(),
                (log.epoch(), evidence),
                replacements,
                (started, clock()?),
            )
            .map_err(operation)?;
            self.candidate(journal, &record, &snapshot, deadline, &mut clock)
                .await?;
            // Include the complete revalidation interval without changing native history.
            let record = FollowerEvacuationRecord::new(
                maintenance.clone(),
                (record.head_digest(), record.registry()),
                policy,
                (record.original_key(), record.original_digest()),
                record.retired().to_vec(),
                record.covered_through(),
                record.source_boot(),
                (record.replacement_epoch(), record.replacement_evidence()),
                record.replacements().to_vec(),
                (started, clock()?),
            )
            .map_err(operation)?;
            Ok(FleetFollowerEvacuationCandidate { snapshot, record })
        })
        .await
    }
}
