//! Full observation of this example's closed, writer-only construction profile.
//! Role-enabled applications need their producer/native/policy collectors.

use super::*;
use cellule_host::fleet::{
    FleetFollowerReferences, FleetMaintenanceEnrollments, FleetNodeInventory,
    FleetNodeInventoryScan, FleetNodeSnapshot, FleetObservation, FleetOwnedCell, FleetRoleCoverage,
    FleetRoster, FleetSnapshotNativePage, FleetSnapshotRequest, FleetSnapshotSubject,
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
    role_coverage: Option<FleetRoleCoverage>,
    maintenance_enrollments: Option<FleetMaintenanceEnrollments>,
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
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        capture.started,
        capture.finished,
        capture.complete,
        capture.nodes,
        capture.cells,
    )?;
    let observation = match capture.role_coverage {
        Some(coverage) => observation.with_role_coverage(coverage)?,
        None => observation,
    };
    Ok(match capture.maintenance_enrollments {
        Some(original) => observation
            .with_maintenance_enrollments(original)?
            .check_maintenance_policies(roster, capture.finished)?,
        None => observation,
    })
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
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |old| old.checked_add(1))
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
    let maintenance_enrollments =
        if roster
            .snapshot()
            .head()
            .maintenance()
            .is_some_and(|operation| {
                matches!(
                    operation.phase(),
                    cellule_runtime::fleet::operations::MaintenancePhase::Evacuating
                        | cellule_runtime::fleet::operations::MaintenancePhase::Closing
                )
            })
        {
            Some(
                FleetMaintenanceEnrollments::collect(
                    fleet.journal.as_ref(),
                    roster,
                    deadline,
                    clock,
                )
                .await?,
            )
        } else {
            None
        };
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
    let mut inventories: Vec<Option<FleetNodeInventory>> = Vec::new();
    let mut references = Vec::new();
    for index in 0..3 {
        let mut scan = FleetNodeInventoryScan::new(roster, node_id(index), session(index))?;
        let mut stable = true;
        while let Some(subject) = scan.next_subject()? {
            let response = page(fleet, roster, index, subject, deadline).await?;
            match scan.accept(response.request(), &response, clock()?) {
                Ok(()) => {}
                Err(cellule_runtime::Error::Node("native inventory category changed")) => {
                    stable = false;
                    break;
                }
                Err(source) => return Err(source.into()),
            }
        }
        // Movement can change topology during the scan. Keep accepted rows for
        // independent authority/actor revalidation, with completeness disabled.
        complete &= stable;
        for cell in scan
            .cells()
            .iter()
            .map(|row| row.observation.target.cell_id())
            .chain(scan.transitioning_cells().iter().copied())
        {
            if !fleet.records.contains_key(&cell) {
                return Err(invalid("example unregistered native Cell"));
            }
            // A release/activation can appear on both sides of this interval.
            // The fresh exact authority scan below retains only its actual owner.
            complete &= seen.insert(cell);
        }
        cells.extend(scan.cells().iter().cloned());
        let inventory = if stable {
            let inventory = scan.finish()?;
            let bindings = inventory.bindings();
            // Absence requires this bootstrapped closed writer composition,
            // native traversal and unexpected directory discovery together.
            complete &= bindings.managed_startup
                && !bindings.readers
                && !bindings.follower_store
                && !bindings.follower_producer
                && !bindings.durability_supervisor
                && inventory.node_log().is_none()
                && inventory.transitioning_cells().is_empty()
                && inventory.validate_enrollments(roster).is_ok();
            Some(inventory)
        } else {
            None
        };
        inventories.push(inventory);
        // Include expired and fenced leader obligations; live discovery alone
        // could hide a follower role left by a failed boot.
        let logs = FleetFollowerReferences::collect(
            directory,
            roster,
            node_id(index),
            128,
            deadline,
            clock,
        )
        .await?;
        complete &= logs.entries().is_empty() && logs.validate_enrollments(roster).is_ok();
        references.push(logs);
        nodes.push(
            fleet.boots[index]
                .refresh_capacity(index, fleet.journal.as_ref(), deadline)
                .await?,
        );
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
    complete &= recheck_authority(fleet, &authority, &mut cells).await?;
    // Recheck every role category after *all* authority and membership reads.
    // Stable local-only traversals cannot supply this fleet-wide interval.
    for (index, inventory) in inventories.iter_mut().enumerate() {
        let Some(inventory) = inventory else {
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
            retain_unchanged_writers(&mut cells, index, actors.entries());
            continue;
        };
        let mut recheck = inventory.recheck();
        while let Some(subject) = recheck.next_subject()? {
            let response = page(fleet, roster, index, subject.clone(), deadline).await?;
            if recheck
                .accept(response.request(), &response, clock()?)
                .is_err()
            {
                complete = false;
                match response.page() {
                    FleetSnapshotNativePage::Cells(actors) => {
                        // Count planning stops on changed topology. Keep only
                        // independently unchanged writer rows for pressure relief.
                        retain_unchanged_writers(&mut cells, index, actors.entries());
                    }
                    FleetSnapshotNativePage::Host => {
                        cells.retain(|owned| owned.node != node_id(index));
                    }
                    _ => {}
                }
                break;
            }
        }
        complete &= recheck.finish().is_ok();
    }
    for logs in &mut references {
        match logs.recheck(directory, roster, 128, deadline, clock).await {
            Ok(()) => {}
            Err(cellule_runtime::Error::Node("authoritative follower inventory changed")) => {
                complete = false;
            }
            Err(source) => return Err(source.into()),
        }
    }
    let mut after = directory.advertised_sessions(clock()?, 128).await?;
    after.sort_by_key(|boot| *boot.as_bytes());
    complete &= after == advertised && roster.covers_advertisements(&nodes, clock()?)?;
    let role_coverage = if complete {
        let native = inventories
            .iter()
            .filter_map(Option::as_ref)
            .collect::<Vec<_>>();
        let foreign = references.iter().collect::<Vec<_>>();
        match FleetRoleCoverage::check(roster, &native, &foreign, clock()?) {
            Ok(coverage) => Some(coverage),
            Err(_) => {
                complete = false;
                None
            }
        }
    } else {
        None
    };
    roster.confirm(fleet.journal.as_ref(), deadline).await?;
    Ok(Capture {
        started,
        finished: clock()?,
        complete,
        nodes,
        cells,
        role_coverage,
        maintenance_enrollments,
    })
}

async fn recheck_authority(
    fleet: &adapters::LocalFleet,
    authority: &HashMap<CellId, Control>,
    cells: &mut Vec<FleetOwnedCell>,
) -> JournalResult<bool> {
    let mut unchanged = true;
    for (cell, original) in authority {
        let current = fleet
            .records
            .get(cell)
            .ok_or_else(|| invalid("example authority input disappeared"))?
            .authority
            .load(*cell)
            .await?
            .ok_or_else(|| invalid("example authority disappeared during capture"))?;
        if current.value() != original {
            let value = current.value();
            let protected_same = value.cell == original.cell
                && value.incarnation == original.incarnation
                && value.epoch == original.epoch
                && value.state == original.state
                && value.owner == original.owner
                && value.root == original.root
                && value.recovery == original.recovery
                && value.code == original.code
                && value.schema == original.schema
                && value.next_due_ms == original.next_due_ms;
            eprintln!(
                "FLEET_CAPTURE authority_changed cell={cell:?} revision={}..{} progress={}..{} protected_fields_unchanged={protected_same}",
                original.revision, value.revision, original.progress, value.progress,
            );
            unchanged = false;
            cells.retain(|row| row.observation.target.cell_id() != *cell);
        }
    }
    Ok(unchanged)
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
