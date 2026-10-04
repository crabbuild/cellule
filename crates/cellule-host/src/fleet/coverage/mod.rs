//! Cross-node original producer, persisted lane and current authority matching.
use super::{FleetFollowerReferences, FleetJournalSnapshot, FleetNodeInventory, FleetRoster};
use cellule_runtime::fleet::operations::EnrollmentStatus;
use cellule_runtime::{Error, Result, identity::Digest};
use std::collections::{HashMap, HashSet};

mod followers;

/// Checked native/foreign role graph at a complete retained roster barrier.
///
/// Collect every required original boot and every retained physical node's
/// foreign references, then recheck all native categories, every exact foreign
/// row and the full journal. Authentication, unexpected advertisement discovery,
/// current Cell authority, replacement policy and failed-process closure remain
/// separate adapter duties. This interval proves coverage, not settled roles,
/// atomicity, nonexecution of Pending work or permission to stop a node.
pub struct FleetRoleCoverage {
    snapshot: FleetJournalSnapshot,
    roster: Digest,
    digest: Digest,
    started_at_ms: i64,
    finished_at_ms: i64,
    native_boots: usize,
    physical_nodes: usize,
    pending_enrollments: usize,
}

impl FleetRoleCoverage {
    /// Checks a complete original graph after the ordered global rechecks.
    ///
    /// An Established but not-yet-appended lane is observed only through its
    /// exact delivered managed producer, current Open authority and installed
    /// source binding. It remains an obligation. Missing/duplicate boots or
    /// reference scans, incomplete/dropped rechecks and failed owners block this
    /// proof. Buffers stay with the supplied collectors; applications account
    /// the bounded temporary indexes (at most 10,000 entries per collection).
    pub fn check(
        roster: &FleetRoster,
        native: &[&FleetNodeInventory],
        foreign: &[&FleetFollowerReferences],
        now_ms: i64,
    ) -> Result<Self> {
        if roster.snapshot().registry().bootstrap_revision().is_none()
            || native.len() > 10_000
            || foreign.len() > 10_000
        {
            return Err(Error::Fenced);
        }
        let roster_digest = roster.digest()?;
        let mut boots = HashMap::new();
        for inventory in native {
            if !inventory.bindings().managed_startup
                || inventory.roster_digest() != roster_digest
                || boots
                    .insert((inventory.node(), inventory.session()), *inventory)
                    .is_some()
            {
                return Err(Error::Fenced);
            }
            roster.boot(inventory.node(), inventory.session())?;
        }
        let required = roster.required_boots();
        if boots.len() != required.len()
            || required
                .iter()
                .any(|boot| !boots.contains_key(&(boot.node, boot.session)))
        {
            return Err(Error::Control("required fleet boot inventory is missing"));
        }
        let mut members = HashMap::new();
        for references in foreign {
            references.validate_enrollments(roster)?;
            if members.insert(references.member(), *references).is_some() {
                return Err(Error::Fenced);
            }
        }
        if members.len() != roster.intents().len()
            || roster
                .intents()
                .iter()
                .any(|intent| !members.contains_key(&intent.node()))
        {
            return Err(Error::Control(
                "physical follower reference inventory is missing",
            ));
        }
        let started_at_ms = native
            .iter()
            .map(|i| i.interval().0)
            .chain(foreign.iter().map(|i| i.interval().0))
            .min()
            .ok_or(Error::Fenced)?;
        let collected = native
            .iter()
            .map(|i| i.coverage_checkpoint().0)
            .chain(foreign.iter().map(|i| i.coverage_checkpoint().0))
            .max()
            .ok_or(Error::Fenced)?;
        let mut native_finished = collected;
        for inventory in native {
            let (started, finished) = inventory
                .coverage_checkpoint()
                .1
                .ok_or(Error::Control("global native recheck is incomplete"))?;
            if started < collected || finished < started {
                return Err(Error::Fenced);
            }
            native_finished = native_finished.max(finished);
        }
        let mut finished_at_ms = native_finished;
        for references in foreign {
            let (started, finished) = references
                .coverage_checkpoint()
                .1
                .ok_or(Error::Control("global follower recheck is incomplete"))?;
            if started < native_finished || finished < started {
                return Err(Error::Fenced);
            }
            finished_at_ms = finished_at_ms.max(finished);
        }
        if started_at_ms < 0
            || finished_at_ms > now_ms
            || now_ms < started_at_ms
            || now_ms - started_at_ms > 30_000
        {
            return Err(Error::Fenced);
        }
        let empty_lanes = followers::match_authority(roster, &boots, &members)?;
        for inventory in native {
            inventory.validate_enrollments_with(roster, |row| {
                row.spec().key().is_ok_and(|key| empty_lanes.contains(&key))
            })?;
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-role-coverage.v1\0");
        hash.update(roster_digest.as_bytes());
        hash.update(&started_at_ms.to_be_bytes());
        hash.update(&finished_at_ms.to_be_bytes());
        let mut ordered_native = native.to_vec();
        ordered_native.sort_by_key(|i| (*i.node().as_bytes(), *i.session().as_bytes()));
        hash.update(&(native.len() as u64).to_be_bytes());
        for inventory in ordered_native {
            hash.update(inventory.coverage_digest()?.as_bytes());
            let (collected, checked) = inventory.coverage_checkpoint();
            let (begin, end) = checked.ok_or(Error::Fenced)?;
            for time in [inventory.interval().0, collected, begin, end] {
                hash.update(&time.to_be_bytes());
            }
        }
        let mut ordered_foreign = foreign.to_vec();
        ordered_foreign.sort_by_key(|i| *i.member().as_bytes());
        hash.update(&(foreign.len() as u64).to_be_bytes());
        for references in ordered_foreign {
            hash.update(references.coverage_digest().as_bytes());
            let (collected, checked) = references.coverage_checkpoint();
            let (begin, end) = checked.ok_or(Error::Fenced)?;
            for time in [references.interval().0, collected, begin, end] {
                hash.update(&time.to_be_bytes());
            }
        }
        Ok(Self {
            snapshot: roster.snapshot().clone(),
            roster: roster_digest,
            digest: Digest::from_bytes(*hash.finalize().as_bytes()),
            started_at_ms,
            finished_at_ms,
            native_boots: native.len(),
            physical_nodes: foreign.len(),
            pending_enrollments: roster
                .enrollments()
                .iter()
                .filter(|row| row.status() == EnrollmentStatus::Pending)
                .count(),
        })
    }
    /// Exact head/registry bound by every original native request and roster page.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Identifies original roster inputs, including terminal rows and timestamps.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// Identifies this interval's graph; it supplies no authentication or authority.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original collection through the final exact foreign rechecks, without restamping.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
    /// Every unresolved original boot, including required earlier process sessions.
    #[must_use]
    pub const fn native_boots(&self) -> usize {
        self.native_boots
    }
    /// Every retained physical intent, including nodes with no local writers.
    #[must_use]
    pub const fn physical_nodes(&self) -> usize {
        self.physical_nodes
    }
    /// Pending registry rows remain obligations despite complete inventory coverage.
    #[must_use]
    pub const fn pending_enrollments(&self) -> usize {
        self.pending_enrollments
    }
}
