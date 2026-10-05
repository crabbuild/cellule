//! Full bounded observation of the reference fleet's native role graph.
//! Policy checks remain separate evidence and missing checks stay blocking.

use super::*;
use cellule_host::fleet::{
    FleetFailedBootProcessRequest, FleetFailedBootProcesses, FleetFailedBootRetirement,
    FleetFollowerEvacuationVerifier, FleetFollowerReferences, FleetMaintenanceEnrollments,
    FleetNodeInventory, FleetNodeInventoryScan, FleetNodeSnapshot, FleetObservation,
    FleetOwnedCell, FleetRoleCoverage, FleetRoster, FleetSnapshotNativePage, FleetSnapshotRequest,
    FleetSnapshotSubject,
};
use cellule_runtime::control::{Control, ControlState};
use cellule_runtime::fleet::operations::{
    DrainBlocker, EnrollmentRole, EnrollmentStatus, PublishedPosition,
};
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
) -> JournalResult<Option<Vec<usize>>> {
    let capture = collect(fleet, roster, deadline).await?;
    if !capture.complete {
        return Ok(None);
    }
    let mut counts = vec![0; fleet.nodes.len()];
    for owned in capture.cells {
        let index = (0..fleet.nodes.len())
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
    observe_inner(fleet, roster, deadline, None).await
}

pub(super) async fn observe_with_failed_boot_closure(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    deadline: Instant,
    request: &FleetFailedBootProcessRequest,
    processes: &dyn FleetFailedBootProcesses,
    claimant: SessionId,
) -> JournalResult<FleetObservation> {
    observe_inner(
        fleet,
        roster,
        deadline,
        Some((request, processes, claimant)),
    )
    .await
}

async fn observe_inner(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
    deadline: Instant,
    failed_boot: Option<(
        &FleetFailedBootProcessRequest,
        &dyn FleetFailedBootProcesses,
        SessionId,
    )>,
) -> JournalResult<FleetObservation> {
    let capture = collect(fleet, roster, deadline).await?;
    let mut reader_checks = Vec::new();
    let mut follower_checks = Vec::new();
    let mut finished = capture.finished;
    if let Some(original) = capture.maintenance_enrollments.as_ref() {
        if let Some(verifier) = &fleet.reader_verifier {
            reader_checks = verifier
                .collect_maintenance(fleet.journal.as_ref(), original, roster, deadline, clock)
                .await?;
        }
        let follower_verifier = FleetFollowerEvacuationVerifier::new(
            fleet.boots[0].directory.clone(),
            Arc::new(adapters::LocalSnapshots::new(fleet.nodes.clone())),
        );
        follower_checks = follower_verifier
            .collect_maintenance(fleet.journal.as_ref(), original, roster, deadline, clock)
            .await?;
        roster.confirm(fleet.journal.as_ref(), deadline).await?;
        finished = clock()?;
    }
    let failed_boot_closures = if let Some((request, processes, claimant)) = failed_boot {
        let closure = FleetFailedBootRetirement::capture_retained(
            fleet.journal.as_ref(),
            &fleet.boots[0].directory,
            roster,
            request,
            claimant,
            deadline,
            clock,
        )
        .await?
        .confirm(
            fleet.journal.as_ref(),
            &fleet.boots[0].directory,
            processes,
            claimant,
            deadline,
            clock,
        )
        .await?;
        finished = clock()?;
        Some(vec![closure])
    } else {
        None
    };
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        capture.started,
        finished,
        capture.complete,
        capture.nodes,
        capture.cells,
    )?;
    let observation = match capture.role_coverage {
        Some(coverage) => observation.with_role_coverage(coverage)?,
        None => observation,
    };
    let observation = match failed_boot_closures {
        Some(closures) => observation.with_failed_boot_closures(closures)?,
        None => observation,
    };
    let observation = match capture.maintenance_enrollments {
        Some(original) => observation
            .with_role_evacuations(reader_checks, follower_checks)?
            .with_maintenance_enrollments(original)?
            .check_maintenance_policies(roster, finished)?,
        None => observation,
    };
    Ok(observation)
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
    if fleet.nodes.len() < 3 || fleet.boots.len() != fleet.nodes.len() || fleet.records.is_empty() {
        return Err(invalid("example construction profile differs"));
    }
    let directory = &fleet.boots[0].directory;
    let mut expected_sessions = roster
        .enrollments()
        .iter()
        .filter(|record| {
            record.unresolved()
                && record.status() == EnrollmentStatus::Established
                && matches!(record.spec().role, EnrollmentRole::Node { .. })
        })
        .map(|record| record.spec().target.session)
        .collect::<Vec<_>>();
    expected_sessions.sort_by_key(|boot| *boot.as_bytes());
    let node_enrollments_complete = roster.enrollments().iter().all(|record| {
        !matches!(record.spec().role, EnrollmentRole::Node { .. })
            || record.status() != EnrollmentStatus::Pending
    });
    let mut active_indices = Vec::with_capacity(expected_sessions.len());
    for record in roster.enrollments().iter().filter(|record| {
        record.unresolved()
            && record.status() == EnrollmentStatus::Established
            && matches!(record.spec().role, EnrollmentRole::Node { .. })
    }) {
        let endpoint = record.spec().target;
        if let Some(index) = (0..fleet.nodes.len())
            .find(|index| node_id(*index) == endpoint.node && session(*index) == endpoint.session)
        {
            active_indices.push(index);
        }
    }
    active_indices.sort_unstable();
    let mut advertised = directory.advertised_sessions(started, 128).await?;
    advertised.sort_by_key(|boot| *boot.as_bytes());
    let mut complete = advertised == expected_sessions
        && active_indices.len() == expected_sessions.len()
        && node_enrollments_complete;
    let mut nodes = Vec::new();
    let mut cells = Vec::new();
    let mut seen = HashSet::new();
    let mut inventories: Vec<Option<FleetNodeInventory>> = Vec::new();
    for index in active_indices.iter().copied() {
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
            // Global role coverage below applies enrollment validation with
            // the exact cross-node proof for Established follower lanes that
            // have not received their first append yet.
            complete &=
                inventory.bindings().managed_startup && inventory.transitioning_cells().is_empty();
            Some(inventory)
        } else {
            None
        };
        inventories.push(inventory);
        // Include expired and fenced leader obligations; live discovery alone
        // could hide a follower role left by a failed boot. The graph may be
        // nonempty; its exact match is checked after every native recheck.
        nodes.push(
            fleet.boots[index]
                .refresh_capacity(index, fleet.journal.as_ref(), deadline)
                .await?,
        );
    }
    let members = (0..fleet.nodes.len()).map(node_id).collect::<Vec<_>>();
    let mut references =
        FleetFollowerReferences::collect_all(directory, roster, &members, 128, deadline, clock)
            .await?;
    for logs in &references {
        complete &= logs.validate_enrollments(roster).is_ok();
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
        let Some(current) = authority.get(&owned.observation.target.cell_id()) else {
            complete = false;
            return false;
        };
        let exact = matches_authority(owned, current, fleet.nodes.len());
        complete &= exact;
        exact
            || (maintenance_candidate(roster, owned, started)
                && matches_writer_authority(owned, current, fleet.nodes.len()))
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
    // Root changes invalidate complete counts and ordinary movement demand.
    // Explicit maintenance may retain this exact writer's peak envelope; the
    // prepared action still joins publication and obtains its final root proof.
    complete &= recheck_authority(fleet, roster, &authority, &mut cells).await?;
    // Recheck every role category after *all* authority and membership reads.
    // Stable local-only traversals cannot supply this fleet-wide interval.
    for (index, inventory) in active_indices.iter().copied().zip(inventories.iter_mut()) {
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
            retain_unchanged_writers(&mut cells, roster, index, actors.entries(), clock()?);
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
                        retain_unchanged_writers(
                            &mut cells,
                            roster,
                            index,
                            actors.entries(),
                            clock()?,
                        );
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
    match FleetFollowerReferences::recheck_all(
        &mut references,
        directory,
        roster,
        128,
        deadline,
        clock,
    )
    .await
    {
        Ok(()) => {}
        Err(cellule_runtime::Error::Node("authoritative follower inventory changed")) => {
            complete = false;
        }
        Err(source) => return Err(source.into()),
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
    let capture = Capture {
        started,
        finished: clock()?,
        complete,
        nodes,
        cells,
        role_coverage,
        maintenance_enrollments,
    };
    Ok(capture)
}

async fn recheck_authority(
    fleet: &adapters::LocalFleet,
    roster: &FleetRoster,
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
            unchanged = false;
            let now = clock()?;
            cells.retain(|row| {
                row.observation.target.cell_id() != *cell
                    || (maintenance_candidate(roster, row, now)
                        && matches_writer_authority(row, original, fleet.nodes.len())
                        && matches_writer_authority(row, current.value(), fleet.nodes.len()))
            });
        }
    }
    Ok(unchanged)
}

fn retain_unchanged_writers(
    cells: &mut Vec<FleetOwnedCell>,
    roster: &FleetRoster,
    index: usize,
    entries: &[CellInventoryEntry],
    now: i64,
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
                    && current.role == original.role
                    && current.owner_fence == original.owner_fence
                    && ((current.position == original.position
                        && current.cost == original.cost
                        && current.blockers == original.blockers)
                        || (maintenance_candidate(roster, owned, now)
                            && current.maintenance_cost == original.maintenance_cost
                            && current.blockers.iter().all(|blocker| {
                                matches!(
                                    blocker,
                                    DrainBlocker::BusyExecution
                                        | DrainBlocker::ExternalLease
                                        | DrainBlocker::PendingPublication
                                        | DrainBlocker::UnknownInventory
                                )
                            })))
            })
    });
}

fn maintenance_candidate(roster: &FleetRoster, owned: &FleetOwnedCell, now: i64) -> bool {
    roster
        .snapshot()
        .head()
        .maintenance()
        .is_some_and(|operation| {
            operation.phase() == cellule_runtime::fleet::operations::MaintenancePhase::Evacuating
                && operation.node() == owned.node
                && operation.session() == owned.session
                && now < operation.deadline_ms()
        })
}

fn matches_writer_authority(owned: &FleetOwnedCell, current: &Control, node_count: usize) -> bool {
    let row = &owned.observation;
    current.cell == row.target.cell_id()
        && current.incarnation == row.incarnation
        && current.owner_fence() == row.owner_fence
        && current.code == row.code
        && current.schema == row.schema
        && current.state == ControlState::Serving
        && current.owner.as_ref().is_some_and(|owner| {
            owner.session == owned.session
                && (0..node_count).any(|n| node_id(n) == owned.node && owner == &super::owner(n))
        })
        && current.recovery.is_none()
        && current.root.is_some()
}

fn matches_authority(owned: &FleetOwnedCell, current: &Control, node_count: usize) -> bool {
    let row = &owned.observation;
    matches_writer_authority(owned, current, node_count)
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
