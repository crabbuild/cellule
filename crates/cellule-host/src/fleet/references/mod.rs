//! Complete current directory references beneath the application's observer.
use super::{FleetJournalSnapshot, FleetRoster};
use cellule_runtime::{
    Error, Result,
    fleet::operations::{EnrollmentRole, EnrollmentStatus},
    identity::{Digest, NodeId},
    node::{FollowerLogObservation, NodeDirectory, log_state::NodeLogPhase},
};
use tokio::time::{Instant, timeout_at};

mod traversal;

/// All authoritative log references to one physical follower, including failed
/// leader sessions. Native lanes, original producer work, replacement policy
/// and canonical recovery/retirement remain separate obligations.
///
/// Applications account the bounded copied buffer (at most 10,000 entries).
/// A complete listing is interval evidence, not an atomic directory snapshot or
/// permission to shut down. Recheck it after native collection and reconfirm
/// the complete roster before using it in a fleet observation.
pub struct FleetFollowerReferences {
    member: NodeId,
    roster: Digest,
    snapshot: FleetJournalSnapshot,
    topology: Digest,
    started_at_ms: i64,
    finished_at_ms: i64,
    collected_at_ms: i64,
    rechecked: Option<(i64, i64)>,
    entries: Vec<FollowerLogObservation>,
}

impl FleetFollowerReferences {
    /// Traverses native continuation pages against the original full roster.
    /// The supplied clock records actual capture times; it must not renew or
    /// restamp original observations. Source errors survive deadline wrapping.
    /// This performs no enrollment, rotation, recovery or retirement effect.
    pub async fn collect(
        directory: &NodeDirectory,
        roster: &FleetRoster,
        member: NodeId,
        page_limit: usize,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        if roster.snapshot().registry().bootstrap_revision().is_none()
            || directory.fleet() != roster.snapshot().head().scope().fleet
            || !roster
                .intents()
                .iter()
                .any(|intent| intent.node() == member)
            || !(1..=128).contains(&page_limit)
        {
            return Err(Error::Fenced);
        }
        let mut scan = traversal::Scan::default();
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            let now = clock()?;
            let page = timeout_at(
                deadline,
                directory.follower_logs_page(member, scan.next, page_limit, now),
            )
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-log-inventory-deadline",
                source: Box::new(source),
            })??;
            let done = scan.accept(member, page, now, clock()?)?;
            if done {
                break;
            }
        }
        Ok(Self {
            member,
            roster: roster.digest()?,
            snapshot: roster.snapshot().clone(),
            topology: scan.topology.ok_or(Error::Fenced)?,
            started_at_ms: scan.started_at_ms.ok_or(Error::Fenced)?,
            finished_at_ms: scan.finished_at_ms,
            collected_at_ms: scan.finished_at_ms,
            rechecked: None,
            entries: scan.entries,
        })
    }

    /// Physical follower node; no current boot substitutes an earlier lane.
    #[must_use]
    pub const fn member(&self) -> NodeId {
        self.member
    }

    /// Original-to-latest checked capture times, never refreshed row timestamps.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
    pub(crate) fn coverage_checkpoint(&self) -> (i64, Option<(i64, i64)>) {
        (self.collected_at_ms, self.rechecked)
    }
    pub(crate) fn coverage_digest(&self) -> Digest {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-foreign-coverage.v1\0");
        hash.update(self.member.as_bytes());
        hash.update(self.roster.as_bytes());
        hash.update(self.topology.as_bytes());
        for row in &self.entries {
            hash.update(row.leader.as_bytes());
            hash.update(row.leader_node.as_bytes());
            hash.update(&[
                row.leader_state as u8,
                row.log.phase() as u8,
                u8::from(row.log.active()),
            ]);
            hash.update(&row.log.epoch().to_be_bytes());
            hash.update(&row.log.tiered_through().to_be_bytes());
            hash.update(&(row.log.members().len() as u64).to_be_bytes());
            for member in row.log.members() {
                hash.update(member.as_bytes());
            }
            hash.update(&[u8::from(row.log.recovery().is_some())]);
            if let Some(claim) = row.log.recovery() {
                hash.update(claim.claimant().as_bytes());
                hash.update(&claim.generation().to_be_bytes());
                hash.update(&claim.expires_at_ms().to_be_bytes());
            }
            hash.update(&[u8::from(row.log.recovery_manifest().is_some())]);
            if let Some(manifest) = row.log.recovery_manifest() {
                hash.update(manifest.as_bytes());
            }
        }
        Digest::from_bytes(*hash.finalize().as_bytes())
    }

    /// Exact current authority observations in strict leader-session order.
    #[must_use]
    pub fn entries(&self) -> &[FollowerLogObservation] {
        &self.entries
    }

    /// Matches discovered references to retained original enrollment requests.
    /// Pending requests without a reference remain obligations in the roster;
    /// absence here cannot establish their nonexecution or failed-process join.
    pub fn validate_enrollments(&self, roster: &FleetRoster) -> Result<()> {
        if roster.snapshot() != &self.snapshot || roster.digest()? != self.roster {
            return Err(Error::Fenced);
        }
        for reference in &self.entries {
            if !roster.enrollments().iter().any(|row| {
                let spec = row.spec();
                spec.target.node == self.member
                    && (row.unresolved()
                        || (row.status() == EnrollmentStatus::Retired
                            && reference.log.phase() == NodeLogPhase::Retired))
                    && spec.source.is_some_and(|source| {
                        source.node == reference.leader_node && source.session == reference.leader
                    })
                    && matches!(spec.role, EnrollmentRole::Follower { log_epoch }
                        if log_epoch == reference.log.epoch())
            }) {
                return Err(Error::Control(
                    "authoritative follower reference is unregistered",
                ));
            }
        }
        Ok(())
    }

    /// Fully traverses again and compares exact rows, including volatile
    /// coverage and leader liveness which native topology cursors omit. A first
    /// page fingerprint alone cannot prove those authority fields unchanged.
    /// On failure the original observations and interval remain intact.
    pub async fn recheck(
        &mut self,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        page_limit: usize,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<()> {
        self.rechecked = None;
        if roster.snapshot() != &self.snapshot || roster.digest()? != self.roster {
            return Err(Error::Fenced);
        }
        let fresh =
            Self::collect(directory, roster, self.member, page_limit, deadline, clock).await?;
        if fresh.started_at_ms < self.finished_at_ms
            || fresh.finished_at_ms - self.started_at_ms > 30_000
            || fresh.topology != self.topology
            || fresh.entries != self.entries
        {
            return Err(Error::Node("authoritative follower inventory changed"));
        }
        self.finished_at_ms = fresh.finished_at_ms;
        self.rechecked = Some((fresh.started_at_ms, fresh.finished_at_ms));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
