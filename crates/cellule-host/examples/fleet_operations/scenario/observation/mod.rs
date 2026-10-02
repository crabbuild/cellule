//! Full observation of this example's closed, writer-only construction profile.
//! Role-enabled applications need their producer/native/policy collectors.

use super::*;
use cellule_host::fleet::{
    FleetNodeSnapshot, FleetObservation, FleetOwnedCell, FleetRoster, FleetSnapshotNativePage,
    FleetSnapshotRequest, FleetSnapshotSubject,
};
use cellule_runtime::control::{Control, ControlState};
use cellule_runtime::fleet::operations::{EnrollmentRole, EnrollmentStatus, PublishedPosition};
use cellule_runtime::node::NodeAdvertisement;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

struct Capture {
    started: i64,
    finished: i64,
    complete: bool,
    nodes: Vec<NodeAdvertisement>,
    cells: Vec<FleetOwnedCell>,
}

pub(super) async fn complete_counts(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    deadline: Instant,
) -> JournalResult<Option<[usize; 3]>> {
    let capture = collect(fleet, roster, deadline).await?;
    if !capture.complete {
        return Ok(None);
    }
    let mut counts = [0; 3];
    for owned in capture.cells {
        let index = (0..3)
            .find(|n| node_id(*n) == owned.node && session(*n) == owned.session)
            .ok_or_else(|| invalid("example count endpoint differs"))?;
        counts[index] += 1;
    }
    Ok(Some(counts))
}

pub(super) async fn observe(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    deadline: Instant,
) -> JournalResult<FleetObservation> {
    let capture = collect(fleet, roster, deadline).await?;
    Ok(FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        capture.started,
        capture.finished,
        capture.complete,
        capture.nodes,
        capture.cells,
    )?)
}

async fn page(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    index: usize,
    subject: FleetSnapshotSubject,
    deadline: Instant,
) -> JournalResult<Arc<FleetNodeSnapshot>> {
    let issued = clock()?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let interval = i64::try_from(remaining.as_millis())?.min(30_000);
    let lease = roster
        .snapshot()
        .head()
        .controller()
        .ok_or_else(|| invalid("example snapshot has no controller"))?;
    let expires = issued
        .checked_add(interval)
        .ok_or_else(|| invalid("example snapshot time overflow"))?
        .min(lease.expires_at_ms);
    let sequence = fleet
        .capture_sequence
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| old.checked_add(1))
        .map_err(|_| invalid("example capture nonce exhausted"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.example-native-capture.v1\0");
    hash.update(&sequence.to_be_bytes());
    hash.update(session(index).as_bytes());
    let request = FleetSnapshotRequest::new(
        roster.snapshot().clone(),
        Digest::from_bytes(*hash.finalize().as_bytes()),
        node_id(index),
        session(index),
        subject,
        32,
        issued,
        expires,
    )?;
    let node = fleet
        .nodes
        .get(index)
        .ok_or_else(|| invalid("example snapshot node missing"))?;
    let response = node
        .fleet_snapshot(request.clone())
        .await
        .map_err(|source| Box::new(source) as JournalError)?;
    response.validate(&request, clock()?)?;
    Ok(response)
}

async fn collect(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    deadline: Instant,
) -> JournalResult<Capture> {
    let started = clock()?;
    if fleet.nodes.len() != 3 || fleet.boots.len() != 3 || fleet.records.len() != CELL_COUNT {
        return Err(invalid("example construction profile differs"));
    }
    let directory = &fleet.boots[0].directory;
    let expected_sessions = (0..3).map(session).collect::<Vec<_>>();
    let mut advertised = directory.advertised_sessions(started, 128).await?;
    advertised.sort_by_key(|boot| *boot.as_bytes());
    let mut complete = advertised == expected_sessions
        && roster.enrollments().iter().all(|record| {
            matches!(record.spec().role, EnrollmentRole::Node { .. })
                && record.status() != EnrollmentStatus::Pending
        });
    let mut nodes = Vec::new();
    let mut cells = Vec::new();
    let mut seen = HashSet::new();
    let mut topologies = Vec::new();
    for index in 0..3 {
        let host = page(fleet, roster, index, FleetSnapshotSubject::Host, deadline).await?;
        let bindings = host.bindings();
        // Absence of an owner is not proof of absent roles. Here construction
        // creates only catalog-backed writers, before exposing this private
        // adapter; verify that no producer/role installation escaped that profile.
        complete &= bindings.managed_startup
            && !bindings.readers
            && !bindings.follower_store
            && !bindings.follower_producer
            && !bindings.durability_supervisor
            && host.node_log().is_none()
            && host.state_before() == host.state_after();
        drop(host);
        for subject in [
            FleetSnapshotSubject::Readers(None),
            FleetSnapshotSubject::ReaderEnrollments(None),
            FleetSnapshotSubject::FollowerLanes(None),
            FleetSnapshotSubject::FollowerEnrollments(None),
            FleetSnapshotSubject::DurabilitySupervisor,
        ] {
            let response = page(fleet, roster, index, subject, deadline).await?;
            // Role-enabled coverage is unsupported here, including apparently
            // empty pages. Do not upgrade it to this writer-only profile.
            complete &= matches!(response.page(), FleetSnapshotNativePage::Unbound)
                && response.bindings() == bindings;
        }
        let response = page(
            fleet,
            roster,
            index,
            FleetSnapshotSubject::Cells(None),
            deadline,
        )
        .await?;
        let FleetSnapshotNativePage::Cells(actors) = response.page() else {
            return Err(invalid("example actor page category differs"));
        };
        if actors.next().is_some() {
            return Err(invalid("example actor inventory exceeds fixed bound"));
        }
        complete &= actors.transitioning_cells() == 0;
        topologies.push(actors.topology());
        for entry in actors.entries() {
            if !seen.insert(entry.cell()) || !fleet.records.contains_key(&entry.cell()) {
                return Err(invalid("example duplicate or unregistered native Cell"));
            }
            if let CellInventoryEntry::Owned(row) = entry {
                cells.push(FleetOwnedCell {
                    node: node_id(index),
                    session: session(index),
                    observation: (**row).clone(),
                });
            }
        }
        drop(response);
        // Include expired and fenced leader obligations; live discovery alone
        // could hide a follower role left by a failed boot.
        let logs = directory
            .follower_logs_page(node_id(index), None, 128, clock()?)
            .await?;
        complete &= logs.total_logs() == 0 && logs.next().is_none();
        nodes.push(fleet.boots[index].refresh_capacity(index, deadline).await?);
    }
    let mut authority = HashMap::new();
    for (cell, record) in fleet.records.iter() {
        let current = record
            .authority
            .load(*cell)
            .await?
            .ok_or_else(|| invalid("example current Cell authority missing"))?;
        authority.insert(*cell, current.value().clone());
    }
    cells.retain(|owned| {
        let matches = authority
            .get(&owned.observation.target.cell_id())
            .is_some_and(|current| matches_authority(owned, current));
        complete &= matches;
        matches
    });
    for (cell, current) in &authority {
        complete &= match current.state {
            ControlState::Serving => cells
                .iter()
                .any(|row| row.observation.target.cell_id() == *cell),
            ControlState::Idle | ControlState::Tombstoned => !seen.contains(cell),
            ControlState::Recovering => false,
        };
    }
    // Recheck exact authority after the full scan. A concurrent publication or
    // takeover invalidates that Cell's planning row, not just count completeness.
    for (cell, original) in &authority {
        let current = fleet
            .records
            .get(cell)
            .ok_or_else(|| invalid("example authority input disappeared"))?
            .authority
            .load(*cell)
            .await?
            .ok_or_else(|| invalid("example authority disappeared during capture"))?;
        if current.value() != original {
            complete = false;
            cells.retain(|row| row.observation.target.cell_id() != *cell);
        }
    }
    for (index, topology) in topologies.iter().enumerate() {
        let response = page(
            fleet,
            roster,
            index,
            FleetSnapshotSubject::Cells(None),
            deadline,
        )
        .await?;
        let FleetSnapshotNativePage::Cells(actors) = response.page() else {
            return Err(invalid("example repeated actor page category differs"));
        };
        if actors.topology() != *topology || actors.next().is_some() {
            complete = false;
            // A changed topology disables count planning. Unchanged writers
            // can independently remain pressure-relief candidates.
            retain_unchanged_writers(&mut cells, index, actors.entries());
        }
    }
    let mut after = directory.advertised_sessions(clock()?, 128).await?;
    after.sort_by_key(|boot| *boot.as_bytes());
    complete &= after == advertised && roster.covers_advertisements(&nodes, clock()?)?;
    roster.confirm(fleet.journal.as_ref(), deadline).await?;
    Ok(Capture {
        started,
        finished: clock()?,
        complete,
        nodes,
        cells,
    })
}

fn retain_unchanged_writers(
    cells: &mut Vec<FleetOwnedCell>,
    index: usize,
    entries: &[CellInventoryEntry],
) {
    cells.retain(|owned| {
        owned.node != node_id(index)
            || entries.iter().any(|entry| {
                let CellInventoryEntry::Owned(current) = entry else {
                    return false;
                };
                let original = &owned.observation;
                current.target == original.target
                    && current.generation == original.generation
                    && current.incarnation == original.incarnation
                    && current.code == original.code
                    && current.schema == original.schema
                    && current.position == original.position
                    && current.cost == original.cost
                    && current.blockers == original.blockers
            })
    });
}

fn matches_authority(owned: &FleetOwnedCell, current: &Control) -> bool {
    let row = &owned.observation;
    current.cell == row.target.cell_id()
        && current.incarnation == row.incarnation
        && current.code == row.code
        && current.schema == row.schema
        && current.state == ControlState::Serving
        && current.owner.as_ref().is_some_and(|owner| {
            owner.session == owned.session
                && (0..3).any(|n| node_id(n) == owned.node && owner == &super::owner(n))
        })
        && current.recovery.is_none()
        && current.root.as_ref().is_some_and(|root| {
            row.position
                == Some(PublishedPosition {
                    incarnation: current.incarnation,
                    epoch: current.epoch,
                    root: root.clone(),
                })
        })
}

#[cfg(test)]
mod tests;
