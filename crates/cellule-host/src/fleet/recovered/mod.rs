//! Durable enrollment publication after canonical recovered ensemble retirement.
use super::{FleetJournal, FleetJournalSnapshot, FleetRoster, operation};
use cellule_runtime::{
    Error, Result,
    fleet::operations::EnrollmentRecord,
    identity::{Digest, NodeId, SessionId},
    node::{NodeDirectory, SealedNodeLog, log_state::NodeLogPhase},
};
use std::{future::Future, sync::Arc};
use tokio::time::{Instant, timeout_at};

mod publication;
mod records;

/// Exact original enrollment requests bound to canonical recovered retirement.
///
/// Capture after the existing recovery/member-retirement protocol commits its
/// Retired tombstone. No native retirement or recovery effect runs here. The
/// application authenticates the claimant and accounts the bounded copied rows
/// (at most sixteen records). Failed-boot/process closure and replacement policy
/// remain separate obligations; this value never grants node stop permission.
pub struct FleetRecoveredFollowerRetirement {
    snapshot: FleetJournalSnapshot,
    leader_node: NodeId,
    retired: SealedNodeLog,
    members: Vec<EnrollmentRecord>,
    evidence: Vec<Digest>,
    started_at_ms: i64,
    finished_at_ms: i64,
}

impl FleetRecoveredFollowerRetirement {
    /// Confirms the complete original roster around fresh canonical retirement.
    /// Missing/duplicate/foreign member requests and unretired epochs refuse
    /// capture. Terminal rows retain their original acceptance/evidence times.
    pub async fn capture(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        sealed: &SealedNodeLog,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started_at_ms = clock()?;
        if roster.snapshot().registry().bootstrap_revision().is_none()
            || directory.fleet() != roster.snapshot().head().scope().fleet
        {
            return Err(Error::Fenced);
        }
        roster.confirm(journal, deadline).await?;
        let (leader_node, retired) =
            canonical(directory, sealed, claimant, started_at_ms, deadline).await?;
        let members = records::select(roster, leader_node, &retired)?;
        let evidence = members
            .iter()
            .map(|row| records::evidence(leader_node, &retired, row))
            .collect::<Result<Vec<_>>>()?;
        for (row, digest) in members.iter().zip(&evidence) {
            records::validate_terminal(row, *digest)?;
        }
        roster.confirm(journal, deadline).await?;
        let finished_at_ms = clock()?;
        interval(started_at_ms, finished_at_ms)?;
        Ok(Self {
            snapshot: roster.snapshot().clone(),
            leader_node,
            retired,
            members,
            evidence,
            started_at_ms,
            finished_at_ms,
        })
    }

    /// Original full journal barrier; publication rechecks it before effects.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original physical leader, verified through its canonical tombstone.
    #[must_use]
    pub const fn leader_node(&self) -> NodeId {
        self.leader_node
    }
    /// Canonical Retired epoch, complete ensemble and pinned manifest.
    #[must_use]
    pub const fn retired(&self) -> &SealedNodeLog {
        &self.retired
    }
    /// All original requests in canonical member order, including Pending rows.
    #[must_use]
    pub fn members(&self) -> &[EnrollmentRecord] {
        &self.members
    }
    /// Capture times; publication cannot renew or restamp these observations.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

/// One joined durable publication, retaining its original source error.
pub struct FleetRecoveredFollowerMember {
    original: EnrollmentRecord,
    result: std::result::Result<EnrollmentRecord, Arc<Error>>,
}
impl FleetRecoveredFollowerMember {
    /// Immutable original request and first acceptance.
    #[must_use]
    pub const fn original(&self) -> &EnrollmentRecord {
        &self.original
    }
    /// Returned original retirement record or independently retained failure.
    pub fn result(&self) -> std::result::Result<&EnrollmentRecord, Arc<Error>> {
        self.result.as_ref().map_err(Arc::clone)
    }
}

/// Joined member publications plus the final complete-roster/canonical recheck.
/// An error in either stage prevents closure without discarding sibling results.
pub struct FleetRecoveredFollowerPublication {
    members: Vec<FleetRecoveredFollowerMember>,
    closure: std::result::Result<FleetRecoveredFollowerClosure, Arc<Error>>,
}
impl FleetRecoveredFollowerPublication {
    /// Every original member's response; lost replies never imply settlement.
    #[must_use]
    pub fn members(&self) -> &[FleetRecoveredFollowerMember] {
        &self.members
    }
    /// Confirms publication against a fresh complete roster and canonical epoch.
    /// Boot/process joining, replacement policy and node finalization are separate.
    pub fn confirmed(&self) -> Result<&FleetRecoveredFollowerClosure> {
        for member in &self.members {
            if let Err(source) = &member.result {
                return Err(retained(Arc::clone(source)));
            }
        }
        self.closure
            .as_ref()
            .map_err(|source| retained(Arc::clone(source)))
    }
    /// Original final-check error, including one caused by a failed member reply.
    #[must_use]
    pub fn closure_error(&self) -> Option<Arc<Error>> {
        self.closure.as_ref().err().cloned()
    }
}

/// Complete original ensemble enrollment closure at one checked journal barrier.
/// This settles only these follower rows. The failed leader's boot, reader roles,
/// affected Cells and native process lifetimes still require their own proofs.
pub struct FleetRecoveredFollowerClosure {
    snapshot: FleetJournalSnapshot,
    leader_node: NodeId,
    retired: SealedNodeLog,
    members: Vec<EnrollmentRecord>,
    digest: Digest,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetRecoveredFollowerClosure {
    /// Full post-publication head and registry confirmed around canonical authority.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original canonical physical leader.
    #[must_use]
    pub const fn leader_node(&self) -> NodeId {
        self.leader_node
    }
    /// Canonical terminal epoch and retained manifest.
    #[must_use]
    pub const fn retired(&self) -> &SealedNodeLog {
        &self.retired
    }
    /// Original retired member rows, preserving accepted and establishment history.
    #[must_use]
    pub fn members(&self) -> &[EnrollmentRecord] {
        &self.members
    }
    /// Identifies terminal authority and full original member records, without a clock.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original capture through final confirmation; this is interval evidence.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

async fn canonical(
    directory: &NodeDirectory,
    sealed: &SealedNodeLog,
    claimant: SessionId,
    now: i64,
    deadline: Instant,
) -> Result<(NodeId, SealedNodeLog)> {
    let retired = bounded(
        deadline,
        directory.retired_recovered_log(sealed, claimant, now),
    )
    .await?
    .ok_or(Error::Control(
        "recovered ensemble is not canonically retired",
    ))?;
    let member = *retired.log().members().first().ok_or(Error::Fenced)?;
    let authorization = bounded(
        deadline,
        directory.authorize_recovered_log_retire(
            claimant,
            member,
            retired.session(),
            retired.log().epoch(),
            retired.log().recovery_manifest(),
            now,
        ),
    )
    .await?;
    if authorization.sealed() != &retired || retired.log().phase() != NodeLogPhase::Retired {
        return Err(Error::Fenced);
    }
    Ok((authorization.leader_node(), retired))
}

fn interval(start: i64, end: i64) -> Result<()> {
    if start < 0 || end < start || end - start > 30_000 {
        return Err(Error::Deadline);
    }
    Ok(())
}

async fn bounded<T>(deadline: Instant, future: impl Future<Output = Result<T>>) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(Error::Deadline);
    }
    timeout_at(deadline, future)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-recovered-follower-deadline",
            source: Box::new(source),
        })?
}

fn journal_error(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-recovered-follower-journal",
        source,
    }
}

#[derive(Debug)]
struct RetainedError(Arc<Error>);
impl std::fmt::Display for RetainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: Arc<Error>) -> Error {
    Error::Facility {
        name: "fleet-recovered-follower-publication",
        source: Box::new(RetainedError(source)),
    }
}
