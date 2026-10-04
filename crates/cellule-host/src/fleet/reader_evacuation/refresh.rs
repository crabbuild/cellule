//! New immutable policy capture from a previously committed native closure.
use super::verification::{adapter, bounded};
use super::*;
use crate::read_replicas::maintenance::{boot_identity, validate_replacement};
use cellule_runtime::{
    Error, Result,
    client::{CellDescription, Receipt},
    control::ControlState,
    fleet::operations::{EnrollmentRole, MaintenancePhase, ReaderReplacementWitness},
};
use tokio::time::Instant;

/// Fresh policy candidate retaining the same original native retirement.
/// This value starts no opening, refresh, closure or task. The normal live-owner
/// recruiter supplies replacements; publication commits only this metadata.
pub struct FleetReaderEvacuationCandidate {
    snapshot: FleetJournalSnapshot,
    record: ReaderEvacuationRecord,
    pages: Vec<ReaderEvacuationPage>,
}
impl FleetReaderEvacuationCandidate {
    /// Original full barrier used for this new capture.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// New immutable policy history; original retirement timestamps stay intact.
    #[must_use]
    pub fn record(&self) -> &ReaderEvacuationRecord {
        &self.record
    }
    /// Complete canonical pages for the new policy capture.
    #[must_use]
    pub fn pages(&self) -> &[ReaderEvacuationPage] {
        &self.pages
    }
}
impl FleetReaderEvacuationVerifier {
    /// Reobserves replacements after policy/boot/owner changes or operation
    /// deadline/session adoption. Requires the exact previously committed
    /// native retirement and all its historical pages. It never repeats native
    /// closure or treats an absent record as an empty responsibility. Closing
    /// operations may restore current redundancy before terminal finalization.
    pub async fn refresh(
        &self,
        journal: &dyn FleetReaderEvacuationJournal,
        original: &ReaderEvacuationRecord,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetReaderEvacuationCandidate> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let next = clock()?;
            if started < 0 || next < last || next - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let scope = original.retired().spec().scope;
            if journal
                .load_reader_evacuation(scope, original.digest().map_err(operation)?)
                .await
                .map_err(adapter)?
                .as_ref()
                != Some(original)
            {
                return Err(Error::Fenced);
            }
            let mut historical = Vec::with_capacity(original.pages().len());
            for digest in original.pages() {
                historical.push(
                    journal
                        .load_reader_evacuation_page(scope, *digest)
                        .await
                        .map_err(adapter)?
                        .ok_or(Error::Fenced)?,
                );
            }
            original.validate_pages(&historical).map_err(operation)?;
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
            let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
            if !roster
                .enrollments()
                .iter()
                .any(|row| row == original.retired())
            {
                return Err(Error::Fenced);
            }
            let EnrollmentRole::Reader { target, position } = &original.retired().spec().role
            else {
                return Err(Error::Fenced);
            };
            let authority = self
                .authority
                .load(target.cell_id())
                .await?
                .ok_or(Error::Fenced)?;
            let authority = authority.value();
            if authority.state != ControlState::Serving
                || authority.recovery.is_some()
                || authority.incarnation != position.incarnation
                || authority
                    .root
                    .as_ref()
                    .is_none_or(|root| root.commit_sequence < original.minimum_sequence())
            {
                return Err(Error::Fenced);
            }
            let policy = self
                .policy
                .load(authority.cell)
                .await?
                .map(|row| row.value());
            if policy.is_some_and(|policy| policy.incarnation() != authority.incarnation) {
                return Err(Error::Fenced);
            }
            let desired = policy.map_or(0, |policy| policy.desired_readers());
            let minimum = Receipt {
                cell: authority.cell,
                incarnation: authority.incarnation,
                commit_sequence: original.minimum_sequence().max(
                    authority
                        .root
                        .as_ref()
                        .ok_or(Error::Fenced)?
                        .commit_sequence,
                ),
            };
            let selected = self
                .directory
                .select_readers(
                    minimum.cell,
                    authority.owner.as_ref().ok_or(Error::Fenced)?.session,
                    authority.code,
                    usize::from(desired),
                    clock()?,
                    10_000,
                )
                .await?;
            if selected.len() != usize::from(desired) {
                return Err(Error::ReplicaUnavailable);
            }
            let expected = CellDescription {
                cell: minimum.cell,
                incarnation: minimum.incarnation,
                code: authority.code,
                schema: authority.schema,
            };
            let mut replacements = Vec::with_capacity(selected.len());
            for node in selected {
                if node.node() == maintenance.node() {
                    return Err(Error::Fenced);
                }
                let (enrollment_key, enrollment_digest) =
                    validate_replacement(&roster, &node, target, minimum)?;
                let (receipt, ready) = self.peer.status(target, node.clone(), expected).await?;
                if !ready
                    || receipt.cell != minimum.cell
                    || receipt.incarnation != minimum.incarnation
                    || receipt.commit_sequence < minimum.commit_sequence
                {
                    return Err(Error::ReplicaUnavailable);
                }
                replacements.push(ReaderReplacementWitness {
                    node: node.node(),
                    session: node.session(),
                    boot_identity: boot_identity(&node)?,
                    enrollment_key,
                    enrollment_digest,
                    commit_sequence: receipt.commit_sequence,
                });
            }
            roster.confirm(journal, deadline).await?;
            let (record, pages) = ReaderEvacuationRecord::new(
                maintenance.clone(),
                (
                    Digest::from_bytes(
                        *blake3::hash(&snapshot.head().to_bytes().map_err(operation)?).as_bytes(),
                    ),
                    snapshot.registry(),
                ),
                original.retired().clone(),
                original.original_digest(),
                authority.clone(),
                policy.map(|policy| policy.revision()),
                desired,
                minimum.commit_sequence,
                (started, clock()?),
                replacements,
            )
            .map_err(operation)?;
            let check = self
                .candidate(journal, &record, &pages, &snapshot, deadline, &mut clock)
                .await?;
            let (record, pages) = ReaderEvacuationRecord::new(
                maintenance.clone(),
                (
                    Digest::from_bytes(
                        *blake3::hash(&snapshot.head().to_bytes().map_err(operation)?).as_bytes(),
                    ),
                    snapshot.registry(),
                ),
                original.retired().clone(),
                original.original_digest(),
                check.authority().clone(),
                policy.map(|policy| policy.revision()),
                desired,
                minimum.commit_sequence,
                (started, clock()?),
                check
                    .replacements()
                    .iter()
                    .map(|entry| ReaderReplacementWitness {
                        node: entry.node,
                        session: entry.session,
                        boot_identity: entry.boot_identity,
                        enrollment_key: entry.enrollment_key,
                        enrollment_digest: entry.enrollment_digest,
                        commit_sequence: entry.receipt.commit_sequence,
                    })
                    .collect(),
            )
            .map_err(operation)?;
            Ok(FleetReaderEvacuationCandidate {
                snapshot,
                record,
                pages,
            })
        })
        .await
    }
}
