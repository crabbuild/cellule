//! Complete durable roster traversal in the journal's shared transaction domain.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
};

use cellule_runtime::fleet::operations::{
    EnrollmentRecord, EnrollmentRole, EnrollmentStatus, NodeIntent,
};
use cellule_runtime::identity::{Digest, NodeId, SessionId};
use cellule_runtime::node::NodeAdvertisement;
use cellule_runtime::{Error, Result};
use tokio::time::{Instant, timeout_at};

use super::{FleetAdapterFuture, FleetJournal, FleetJournalSnapshot, operation};

mod scan;
use scan::Scan;

const MAX_ROSTER_ENTRIES: usize = 10_000;
const PAGE_ENTRIES: usize = 128;

/// Exact physical boot required by an unresolved registry responsibility.
/// Multiple boots on the same physical node remain distinct after replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetRosterBoot {
    /// Physical node, including an earlier failed boot's identity.
    pub node: NodeId,
    /// Exact participating process session; never substituted with its successor.
    pub session: SessionId,
}

/// Fully traversed retained intents and enrollments at one journal snapshot.
///
/// Construction rechecks the entire head and registry, not merely a live node
/// listing. Pages use the canonical 128-row/one-MiB codecs, and each collection
/// is capped at 10,000 rows. Applications account their retained collector
/// buffers. This value proves traversal under the adapter's consistent-page
/// contract; bootstrap, authentication, native-role matching and fresh authority
/// are separate requirements. In particular, it is not a finalization proof.
pub struct FleetRoster {
    snapshot: FleetJournalSnapshot,
    intents: Vec<NodeIntent>,
    enrollments: Vec<EnrollmentRecord>,
}

impl FleetRoster {
    /// Reads every page, including Pending, failed-boot and terminal records.
    /// An expired deadline fails before issuing another journal request. A lost
    /// read reply preserves its source error and never returns a partial roster.
    pub async fn collect(
        journal: &dyn FleetJournal,
        expected: &FleetJournalSnapshot,
        deadline: Instant,
    ) -> Result<Self> {
        confirm(journal, expected, deadline).await?;
        let version = expected.registry();
        let mut scan = Scan::new(version);
        let mut after = None;
        loop {
            let page = call(deadline, || {
                journal.intents_page(version, after, PAGE_ENTRIES)
            })
            .await?;
            after = scan.intents(page, after)?;
            if after.is_none() {
                break;
            }
        }
        let mut after = None;
        loop {
            let page = call(deadline, || {
                journal.enrollments_page(version, after, PAGE_ENTRIES)
            })
            .await?;
            after = scan.enrollments(page, after)?;
            if after.is_none() {
                break;
            }
        }
        confirm(journal, expected, deadline).await?;
        Ok(Self {
            snapshot: expected.clone(),
            intents: scan.intents,
            enrollments: scan.enrollments,
        })
    }

    /// Rechecks the original full snapshot after collecting native observations.
    /// A later finalization transaction must compare these versions again.
    pub async fn confirm(&self, journal: &dyn FleetJournal, deadline: Instant) -> Result<()> {
        confirm(journal, &self.snapshot, deadline).await
    }

    /// Returns the immutable head and registry barrier used by every page.
    #[must_use]
    pub const fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }

    /// Returns all physical intents, in strictly ascending node order.
    #[must_use]
    pub fn intents(&self) -> &[NodeIntent] {
        &self.intents
    }

    /// Returns all enrollment records in strictly ascending stable-key order.
    /// Terminal exclusion and retirement records are retained without renewal.
    #[must_use]
    pub fn enrollments(&self) -> &[EnrollmentRecord] {
        &self.enrollments
    }

    /// Returns both endpoints of every unresolved responsibility, without a
    /// liveness filter. Pending enrollment still requires observation even when
    /// its native effect or acceptance reply is unknown. Terminal rows do not
    /// introduce required boots, but remain part of the roster and its digest.
    #[must_use]
    pub fn required_boots(&self) -> Vec<FleetRosterBoot> {
        let mut boots = Vec::new();
        for record in self.enrollments.iter().filter(|record| record.unresolved()) {
            let spec = record.spec();
            for endpoint in spec.source.into_iter().chain(std::iter::once(spec.target)) {
                boots.push(FleetRosterBoot {
                    node: endpoint.node,
                    session: endpoint.session,
                });
            }
        }
        boots.sort_unstable_by_key(|boot| (*boot.node.as_bytes(), *boot.session.as_bytes()));
        boots.dedup();
        boots
    }

    /// Checks signed advertisement coverage against established durable boots.
    /// Missing/unknown/duplicate boots, unresolved boot acceptance, old boots
    /// after physical-node replacement, or absent bootstrap yield incomplete
    /// coverage. Both endpoints of unresolved reader/follower responsibilities
    /// remain required. Applications additionally pin signing keys, prove that
    /// unexpected live records were discovered, and match every native role;
    /// success here supplies only the registry-to-advertisement part of that
    /// proof. It never establishes finalization or replacement redundancy.
    pub fn covers_advertisements(&self, nodes: &[NodeAdvertisement], now_ms: i64) -> Result<bool> {
        if self.snapshot.registry().bootstrap_revision().is_none()
            || nodes.len() > MAX_ROSTER_ENTRIES
        {
            return Ok(false);
        }
        let mut established = HashSet::new();
        let intents = self
            .intents
            .iter()
            .map(|intent| (intent.node(), intent))
            .collect::<HashMap<_, _>>();
        for record in &self.enrollments {
            if !matches!(record.spec().role, EnrollmentRole::Node { .. }) || !record.unresolved() {
                continue;
            }
            let endpoint = record.spec().target;
            if record.status() != EnrollmentStatus::Established
                || !established.insert((endpoint.node, endpoint.session))
                || intents.get(&endpoint.node).is_none_or(|intent| {
                    super::FleetBootObservation::new((**intent).clone(), record.clone()).is_err()
                })
            {
                return Ok(false);
            }
        }
        let mut advertised = HashSet::new();
        let mut physical = HashSet::new();
        for node in nodes {
            cellule_runtime::fleet::placement::PlacementObservation::from_signed_advertisement(
                node, now_ms, false,
            )?;
            let boot = (node.node(), node.session());
            if node.fleet() != self.snapshot.head().scope().fleet
                || !advertised.insert(boot)
                || !physical.insert(node.node())
                || !established.contains(&boot)
                || intents
                    .get(&node.node())
                    .is_none_or(|intent| intent.session() != node.session())
            {
                return Ok(false);
            }
        }
        Ok(advertised == established
            && self
                .required_boots()
                .iter()
                .all(|boot| advertised.contains(&(boot.node, boot.session))))
    }

    /// Identifies every original row, including evidence, status and timestamps.
    /// This does not prove that native observations are atomic or authenticated.
    pub fn digest(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-retained-roster.v1\0");
        let mut field = |bytes: &[u8]| {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        };
        field(&self.snapshot.head().to_bytes().map_err(operation)?);
        field(&self.snapshot.registry().to_bytes().map_err(operation)?);
        field(&(self.intents.len() as u64).to_be_bytes());
        for intent in &self.intents {
            field(&intent.to_bytes().map_err(operation)?);
        }
        field(&(self.enrollments.len() as u64).to_be_bytes());
        for record in &self.enrollments {
            field(&record.to_bytes().map_err(operation)?);
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}

async fn confirm(
    journal: &dyn FleetJournal,
    expected: &FleetJournalSnapshot,
    deadline: Instant,
) -> Result<()> {
    let current = call(deadline, || journal.load_snapshot(expected.head().scope())).await?;
    if current != *expected {
        return Err(operation(
            cellule_runtime::fleet::operations::OperationError::Conflict,
        ));
    }
    Ok(())
}

async fn call<'a, T, F>(deadline: Instant, request: F) -> Result<T>
where
    F: FnOnce() -> FleetAdapterFuture<'a, T>,
{
    // Construct the adapter future only after checking admission. An adapter may
    // retain native read work when a dispatched waiter times out; it still joins
    // that work through its ordinary shutdown path.
    if Instant::now() >= deadline {
        return Err(Error::Node("fleet roster collection deadline elapsed"));
    }
    wait(deadline, request()).await
}

async fn wait<T>(
    deadline: Instant,
    request: impl Future<Output = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>>,
) -> Result<T> {
    timeout_at(deadline, request)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-roster-deadline",
            source: Box::new(source),
        })?
        .map_err(|source| Error::Facility {
            name: "fleet-roster-journal",
            source,
        })
}

#[cfg(test)]
mod tests;
