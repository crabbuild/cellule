use super::*;
use cellule_runtime::cell::actor::CellInventoryEntry;

pub(super) fn header(
    page: &FleetSnapshotNativePage,
) -> Result<(Header, Option<FleetSnapshotSubject>, usize)> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-native-category.v1\0");
    let mut field = |n: usize| {
        hash.update(&(n as u64).to_be_bytes());
    };
    let (topology, total, next, count, bound) = match page {
        FleetSnapshotNativePage::Host => (None, 0, None, 0, true),
        FleetSnapshotNativePage::Unbound => (None, 0, None, 0, false),
        FleetSnapshotNativePage::Cells(p) => {
            field(p.owned_cells());
            field(p.transitioning_cells());
            (
                Some(p.topology()),
                p.owned_cells()
                    .checked_add(p.transitioning_cells())
                    .ok_or(Error::Capacity("native actor count overflow"))?,
                p.next().map(|c| FleetSnapshotSubject::Cells(Some(c))),
                p.entries().len(),
                true,
            )
        }
        FleetSnapshotNativePage::Readers(p) => {
            field(p.mode() as usize);
            field(usize::from(p.closed()));
            (
                Some(p.topology()),
                p.total_views(),
                p.next().map(|c| FleetSnapshotSubject::Readers(Some(c))),
                p.entries().len(),
                true,
            )
        }
        FleetSnapshotNativePage::ReaderEnrollments(p) => {
            field(p.mode() as usize);
            let jobs = p.jobs();
            for n in [
                jobs.retained(),
                jobs.running(),
                jobs.unobserved(),
                jobs.joining(),
                usize::from(jobs.draining()),
                usize::from(jobs.task_failure().is_some()),
                usize::from(jobs.protocol_failure().is_some()),
            ] {
                field(n);
            }
            (
                Some(p.topology()),
                p.total_enrollments(),
                p.next()
                    .map(|c| FleetSnapshotSubject::ReaderEnrollments(Some(c))),
                p.entries().len(),
                true,
            )
        }
        FleetSnapshotNativePage::FollowerLanes(p) => {
            field(p.mode() as usize);
            field(p.unretired_lanes());
            field(p.quarantined_entries());
            (
                Some(p.topology()),
                p.total_lanes(),
                p.next()
                    .map(|c| FleetSnapshotSubject::FollowerLanes(Some(c))),
                p.entries().len(),
                true,
            )
        }
        FleetSnapshotNativePage::FollowerEnrollments(p) => {
            field(p.mode() as usize);
            field(usize::from(p.protocol_busy()));
            field(usize::from(p.draining()));
            hash.update(&p.pending_epoch().unwrap_or(0).to_be_bytes());
            (
                Some(p.topology()),
                p.total_epochs(),
                p.next()
                    .map(|c| FleetSnapshotSubject::FollowerEnrollments(Some(c))),
                p.entries().len(),
                true,
            )
        }
        FleetSnapshotNativePage::DurabilitySupervisor(p) => {
            field(p.state as usize);
            field(usize::from(p.cancellation_requested));
            field(usize::from(p.supervisor_error.is_some()));
            field(usize::from(p.requests_error.is_some()));
            match &p.rotations {
                Err(_) => {
                    hash.update(&[0]);
                }
                Ok(rotations) => {
                    hash.update(&[1, u8::from(rotations.stopped)]);
                    hash.update(&rotations.running_epoch.unwrap_or(0).to_be_bytes());
                    for entry in [&rotations.pending, &rotations.completed] {
                        hash.update(&[u8::from(entry.is_some())]);
                        if let Some(entry) = entry {
                            let progress = &entry.progress;
                            hash.update(&entry.epoch.to_be_bytes());
                            hash.update(&[
                                progress.phase() as u8,
                                u8::from(progress.first_failure().is_some()),
                                u8::from(progress.latest_failure().is_some()),
                            ]);
                            hash.update(&[u8::from(progress.retirement().is_some())]);
                            if let Some(proof) = progress.retirement() {
                                retirement(&mut hash, proof);
                            }
                            hash.update(&[u8::from(progress.completion().is_some())]);
                            if let Some(completion) = progress.completion() {
                                retirement(&mut hash, completion.retirement());
                                hash.update(&completion.replacement_epoch().to_be_bytes());
                            }
                        }
                    }
                }
            }
            (None, 1, None, 1, true)
        }
    };
    if total > MAX_ENTRIES {
        return Err(Error::Capacity("native inventory category bound"));
    }
    Ok((
        Header {
            topology,
            total,
            minimum: match page {
                FleetSnapshotNativePage::Cells(p) => p.owned_cells().max(p.transitioning_cells()),
                _ => total,
            },
            extra: Digest::from_bytes(*hash.finalize().as_bytes()),
            bound,
        },
        next,
        count,
    ))
}

impl FleetNodeInventoryScan<'_> {
    pub(super) fn append(&mut self, page: &FleetSnapshotNativePage) -> Result<()> {
        match page {
            FleetSnapshotNativePage::Host | FleetSnapshotNativePage::Unbound => {}
            FleetSnapshotNativePage::Cells(p) => {
                self.role_state.actor_counts = Some((p.owned_cells(), p.transitioning_cells()));
                for entry in p.entries() {
                    ordered(
                        self.last_cell.map(|cell| *cell.as_bytes()),
                        *entry.cell().as_bytes(),
                    )?;
                    self.last_cell = Some(entry.cell());
                    match entry {
                        CellInventoryEntry::Owned(row) => {
                            if row.target.application()
                                != self.roster.snapshot().head().scope().application
                            {
                                return Err(Error::Fenced);
                            }
                            self.cells.push(FleetOwnedCell {
                                node: self.node,
                                session: self.session,
                                observation: (**row).clone(),
                            });
                        }
                        CellInventoryEntry::Transitioning { cell } => {
                            self.transitioning.push(*cell)
                        }
                    }
                }
            }
            FleetSnapshotNativePage::Readers(p) => {
                self.role_state.readers_closed = Some(p.closed());
                for row in p.entries() {
                    ordered(
                        self.readers.last().map(|r| *r.receipt().cell.as_bytes()),
                        *row.receipt().cell.as_bytes(),
                    )?;
                    self.readers.push(*row);
                }
            }
            FleetSnapshotNativePage::ReaderEnrollments(p) => {
                self.role_state.reader_jobs = Some(p.jobs().clone());
                for row in p.entries() {
                    ordered(
                        self.reader_enrollments
                            .last()
                            .map(|r| *r.source.description().cell.as_bytes()),
                        *row.source.description().cell.as_bytes(),
                    )?;
                    self.reader_enrollments.push(row.clone());
                }
            }
            FleetSnapshotNativePage::FollowerLanes(p) => {
                self.role_state.follower_store =
                    Some((p.unretired_lanes(), p.quarantined_entries()));
                for row in p.entries() {
                    let key = |r: &FollowerLaneObservation| (*r.leader.as_bytes(), r.epoch);
                    ordered(self.follower_lanes.last().map(key), key(row))?;
                    self.follower_lanes.push(*row);
                }
            }
            FleetSnapshotNativePage::FollowerEnrollments(p) => {
                self.role_state.follower_producer =
                    Some((p.protocol_busy(), p.draining(), p.pending_epoch()));
                for row in p.entries() {
                    ordered(self.follower_enrollments.last().map(|r| r.epoch), row.epoch)?;
                    self.follower_enrollments
                        .push(crate::FollowerEnrollmentProgress {
                            epoch: row.epoch,
                            attempt: row.attempt,
                            members: row.members.clone(),
                            native_started: row.native_started,
                            no_effect: row.no_effect,
                            delivered: row.delivered,
                            enrollment: row.enrollment,
                            refusal: row.refusal,
                            retirement: row.retirement.clone(),
                            native_closed: row.native_closed,
                            execution_error: row.execution_error.clone(),
                            journal_error: row.journal_error.clone(),
                        });
                }
            }
            FleetSnapshotNativePage::DurabilitySupervisor(p) => self.supervisor = Some(p.clone()),
        }
        Ok(())
    }
}

fn ordered<K: Ord>(previous: Option<K>, next: K) -> Result<()> {
    if previous.is_some_and(|previous| previous >= next) {
        return Err(Error::Node("native inventory rows repeat or regress"));
    }
    Ok(())
}

fn retirement(
    hash: &mut blake3::Hasher,
    proof: &cellule_runtime::node::log::NodeLogRetirementProof,
) {
    let barrier = proof.barrier();
    hash.update(barrier.leader_session().as_bytes());
    hash.update(&barrier.log_epoch().to_be_bytes());
    hash.update(&barrier.covered_through().to_be_bytes());
    hash.update(&(barrier.members().len() as u64).to_be_bytes());
    for member in barrier.members() {
        hash.update(member.as_bytes());
    }
}
