//! Complete original sealed suffix inputs, bound to committed owner history.
use super::*;
use cellule_runtime::{
    node::NodeSessionRecovery,
    recovery::manifest::{RecoveryManifestInventory, RecoveryManifestStore},
};
use std::collections::BTreeMap;

/// Every suffix in the original boot's canonical sealed log and its complete
/// committed writer set. All applications are retained; no resident/current-
/// owner filter can omit a manifest row. Original Controls also retain inherited
/// overlays from earlier boots, which require their own exact successor proofs.
///
/// This checks metadata inputs and fresh original process/operation barriers.
/// It does not verify bundles, materialization, current native serving, role
/// settlement or finalization. The application authenticates canonical storage
/// and process mappings and accounts bounded manifest/collector memory.
pub struct FleetOriginalBootSuffixInventory {
    snapshot: FleetJournalSnapshot,
    writers: FleetOriginalWriterInventory,
    recovered: NodeSessionRecovery,
    manifest: Option<RecoveryManifestInventory>,
    process: FleetFailedBootProcessEvidence,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetOriginalBootSuffixInventory {
    /// Reads the canonical recovered log and complete digest-verified manifest,
    /// matches every row to its exact original application/Cell/incarnation/epoch
    /// and predecessor, then repeats process, fence, log and full journal checks.
    /// Missing commitment, unresolved recovery, omitted owners and provider errors
    /// refuse. This starts no capture publication, recovery or acquisition effect.
    #[allow(clippy::too_many_arguments)]
    pub async fn collect(
        journal: &dyn FleetOriginalWriterJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        manifests: &RecoveryManifestStore,
        request: &FleetFailedBootProcessRequest,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started = clock()?;
        if started < request.interval().1 {
            return Err(Error::Deadline);
        }
        let mut last = started;
        let mut now = || {
            let next = clock()?;
            interval(started, next)?;
            if next < last {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let joined = request
                .confirm(journal, directory, processes, claimant, deadline, &mut now)
                .await?;
            let snapshot = joined.snapshot().clone();
            let operation = snapshot.head().maintenance().ok_or(Error::Fenced)?;
            let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
            capture::check_operation(&snapshot, &roster, request, now()?)?;
            let writers = FleetOriginalWriterInventory::load(
                journal,
                &snapshot,
                operation.id(),
                request.digest(),
                deadline,
            )
            .await?
            .ok_or(Error::Control("original writer inventory is not committed"))?;
            let basis = writers.record().basis();
            if basis.boot.spec() != request.boot().spec()
                || basis.boot.established_evidence() != request.boot().established_evidence()
                || basis.process_witness != joined.process().witness()
                || basis.interval.1 > started
                || basis.operation.node() != operation.node()
                || basis.operation.session() != operation.session()
                || basis.operation.intent_revision() != operation.intent_revision()
                || basis.operation.deadline_ms() != operation.deadline_ms()
            {
                return Err(Error::Fenced);
            }
            let recovered = directory
                .recovered_session(
                    request.fence().node(),
                    request.fence().session(),
                    claimant,
                    now()?,
                )
                .await?;
            if recovered.fence() != request.fence() {
                return Err(Error::Fenced);
            }
            let manifest = match recovered.log().and_then(|log| log.recovery_manifest()) {
                Some(digest) => {
                    let log = recovered.log().ok_or(Error::Fenced)?;
                    Some(
                        manifests
                            .load_manifest(request.fence().session(), log.epoch(), digest)
                            .await?,
                    )
                }
                None => None,
            };
            match_suffixes(&writers, manifest.as_ref(), request)?;
            // Manifest I/O may suspend. Recheck the exact original lifetime and
            // every canonical input afterwards; no historical time is refreshed.
            let final_join = request
                .confirm(journal, directory, processes, claimant, deadline, &mut now)
                .await?;
            if final_join.snapshot() != &snapshot || final_join.process() != joined.process() {
                return Err(Error::Fenced);
            }
            if directory
                .recovered_session(
                    request.fence().node(),
                    request.fence().session(),
                    claimant,
                    now()?,
                )
                .await?
                != recovered
            {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            capture::check_operation(&snapshot, &roster, request, now()?)?;
            let finished = now()?;
            Ok(Self {
                snapshot,
                writers,
                recovered,
                manifest,
                process: joined.process().clone(),
                started_at_ms: started,
                finished_at_ms: finished,
            })
        })
        .await
    }
    /// Full original operation/registry read barrier confirmed after collection.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// All committed original owners, including object-covered/rootless writers.
    #[must_use]
    pub fn writers(&self) -> &FleetOriginalWriterInventory {
        &self.writers
    }
    /// Exact canonical original boot fence and sealed/retired log.
    #[must_use]
    pub const fn recovered(&self) -> &NodeSessionRecovery {
        &self.recovered
    }
    /// Complete original manifest across applications. None is canonical no-
    /// suffix evidence for this log, not absence of writers or inherited overlays.
    #[must_use]
    pub fn manifest(&self) -> Option<&RecoveryManifestInventory> {
        self.manifest.as_ref()
    }
    /// Original immutable process evidence freshly reconfirmed on both sides.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Current metadata collection interval, without restamping historical input.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

fn match_suffixes(
    writers: &FleetOriginalWriterInventory,
    manifest: Option<&RecoveryManifestInventory>,
    request: &FleetFailedBootProcessRequest,
) -> Result<()> {
    let original = writers
        .writers()
        .map(|row| {
            (
                (
                    *row.target.application().as_bytes(),
                    *row.control.cell.as_bytes(),
                    *row.control.incarnation.as_bytes(),
                    row.control.epoch,
                ),
                row,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut matched = BTreeMap::new();
    if let Some(manifest) = manifest {
        if manifest.cells().len() > cellule_runtime::fleet::operations::MAX_ORIGINAL_WRITERS {
            return Err(Error::Capacity(
                "original sealed suffix inventory exceeds bound",
            ));
        }
        for suffix in manifest.cells() {
            let key = (
                *suffix.application.as_bytes(),
                *suffix.cell.as_bytes(),
                *suffix.incarnation.as_bytes(),
                suffix.cell_epoch,
            );
            let owner = original.get(&key).ok_or(Error::Control(
                "sealed suffix is absent from original writer set",
            ))?;
            if owner.control.root.as_ref() != Some(&suffix.recovery.predecessor)
                || suffix.recovery.leader_session != request.fence().session()
                || owner
                    .control
                    .recovery
                    .as_ref()
                    .is_some_and(|overlay| overlay != &suffix.recovery)
                || matched.insert(key, &suffix.recovery).is_some()
            {
                return Err(Error::Control("sealed suffix differs from original writer"));
            }
        }
    }
    for (key, owner) in original {
        if let Some(overlay) = &owner.control.recovery
            && overlay.leader_session == request.fence().session()
            && matched.get(&key).copied() != Some(overlay)
        {
            return Err(Error::Control(
                "original pinned suffix is absent from sealed manifest",
            ));
        }
    }
    Ok(())
}
