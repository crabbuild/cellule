use super::*;

impl<'a> FleetNodeInventoryScan<'a> {
    /// Already accepted generation-bound writers, including after a failed scan.
    /// These are partial advisory rows. Independently recheck current authority
    /// and actor identity before pressure relief; they supply no complete counts.
    #[must_use]
    pub fn cells(&self) -> &[FleetOwnedCell] {
        &self.cells
    }
    /// Already accepted transitional keys, never interpreted as absent owners.
    #[must_use]
    pub fn transitioning_cells(&self) -> &[CellId] {
        &self.transitioning
    }
    /// Pins an exact current Established managed boot in the supplied full roster.
    pub fn new(roster: &'a FleetRoster, node: NodeId, session: SessionId) -> Result<Self> {
        roster.boot(node, session)?;
        if roster.snapshot().registry().bootstrap_revision().is_none() {
            return Err(Error::Control(
                "native inventory requires bootstrapped roster",
            ));
        }
        Ok(Self {
            roster,
            node,
            session,
            stage: 0,
            next: None,
            headers: [None; CATEGORIES],
            counts: [0; CATEGORIES],
            nonces: HashSet::new(),
            host: None,
            started_at_ms: None,
            finished_at_ms: 0,
            poisoned: false,
            cells: Vec::new(),
            transitioning: Vec::new(),
            last_cell: None,
            role_state: RoleState::default(),
            readers: Vec::new(),
            reader_enrollments: Vec::new(),
            follower_lanes: Vec::new(),
            follower_enrollments: Vec::new(),
            supervisor: None,
        })
    }
    /// Returns the exact next category/cursor. None means every recheck completed.
    pub fn next_subject(&self) -> Result<Option<FleetSnapshotSubject>> {
        if self.poisoned {
            return Err(Error::Control("native inventory scan failed"));
        }
        Ok(if self.stage == 2 * CATEGORIES {
            None
        } else {
            Some(
                self.next
                    .clone()
                    .unwrap_or_else(|| subject(self.stage % CATEGORIES)),
            )
        })
    }
    /// Accepts one original request-bound response. This performs no I/O/effect.
    pub fn accept(
        &mut self,
        request: &FleetSnapshotRequest,
        response: &FleetNodeSnapshot,
        now_ms: i64,
    ) -> Result<()> {
        let expected = self
            .next_subject()?
            .ok_or(Error::Control("native inventory already complete"))?;
        self.poisoned = true;
        response.validate(request, now_ms)?;
        if request.expected() != self.roster.snapshot()
            || request.node() != self.node
            || request.session() != self.session
            || request.subject() != &expected
            || !self.nonces.insert(request.nonce())
            || self.nonces.len() > MAX_CAPTURES
        {
            return Err(Error::Fenced);
        }
        let host = Host {
            state: response.state_before(),
            mode: response.mode(),
            bindings: response.bindings(),
            node_log: response.node_log(),
        };
        if response.state_after() != host.state
            || !host.bindings.managed_startup
            || !matches!(host.state, NodeState::Ready | NodeState::Maintenance)
            || self.host.is_some_and(|old| old != host)
            || self.roster.boot(self.node, self.session)?.intent().mode() != host.mode
            || response.started_at_ms() < self.finished_at_ms
        {
            return Err(Error::Node("native inventory host binding changed"));
        }
        let started = *self.started_at_ms.get_or_insert(response.started_at_ms());
        if response.finished_at_ms() - started > 30_000 {
            return Err(Error::Node("native inventory capture interval exceeded"));
        }
        self.host = Some(host);
        self.finished_at_ms = response.finished_at_ms();
        let index = self.stage % CATEGORIES;
        let (header, next, count) = pages::header(response.page())?;
        if self.headers[index].is_some_and(|old| old != header) {
            return Err(Error::Node("native inventory category changed"));
        }
        if self.stage < CATEGORIES {
            self.headers[index] = Some(header);
            let total = self.counts[index]
                .checked_add(count)
                .filter(|total| *total <= header.total && *total <= MAX_ENTRIES)
                .ok_or(Error::Capacity("native inventory row bound"))?;
            if (next.is_none() && total < header.minimum) || (next.is_some() && count == 0) {
                return Err(Error::Node("native inventory continuation is incomplete"));
            }
            self.append(response.page())?;
            if index == 1
                && next.is_none()
                && self
                    .role_state
                    .actor_counts
                    .is_none_or(|(_, transitions)| transitions != self.transitioning.len())
            {
                return Err(Error::Node("native actor transition count differs"));
            }
            self.counts[index] = total;
            self.next = next;
            if self.next.is_none() {
                self.stage += 1;
            }
        } else {
            // The first page fingerprints the complete category, including rows
            // outside it. It is a fresh recheck, never a cached initial response.
            self.stage += 1;
            self.next = None;
        }
        self.poisoned = false;
        Ok(())
    }
    /// Produces a bounded traversal after every category and recheck.
    /// Call `validate_enrollments` to match the durable responsibilities. Then
    /// recheck after remote authority/policy discovery and reconfirm the roster.
    /// No combination of these local calls alone proves fleet settlement.
    pub fn finish(self) -> Result<FleetNodeInventory> {
        if self.poisoned || self.stage != 2 * CATEGORIES {
            return Err(Error::Control("native inventory traversal incomplete"));
        }
        Ok(FleetNodeInventory {
            node: self.node,
            session: self.session,
            roster: self.roster.digest()?,
            snapshot: self.roster.snapshot().clone(),
            nonces: self.nonces,
            started_at_ms: self
                .started_at_ms
                .ok_or(Error::Control("native inventory interval missing"))?,
            finished_at_ms: self.finished_at_ms,
            host: self
                .host
                .ok_or(Error::Control("native inventory host missing"))?,
            headers: self.headers,
            cells: self.cells,
            transitioning: self.transitioning,
            role_state: self.role_state,
            readers: self.readers,
            reader_enrollments: self.reader_enrollments,
            follower_lanes: self.follower_lanes,
            follower_enrollments: self.follower_enrollments,
            supervisor: self.supervisor,
        })
    }
}

pub(super) fn subject(index: usize) -> FleetSnapshotSubject {
    match index {
        0 => FleetSnapshotSubject::Host,
        1 => FleetSnapshotSubject::Cells(None),
        2 => FleetSnapshotSubject::Readers(None),
        3 => FleetSnapshotSubject::ReaderEnrollments(None),
        4 => FleetSnapshotSubject::FollowerLanes(None),
        5 => FleetSnapshotSubject::FollowerEnrollments(None),
        _ => FleetSnapshotSubject::DurabilitySupervisor,
    }
}
