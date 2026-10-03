use super::traversal::subject;
use super::*;

/// Rechecks all complete category fingerprints after fleet-wide discovery.
///
/// Success supplies interval evidence only. Reconfirm the journal and exact
/// current Cell/log authority and replacement policy before committing any
/// maintenance proof. A failed or dropped recheck supplies no confirmation.
pub struct FleetNodeInventoryRecheck<'a> {
    inventory: &'a mut FleetNodeInventory,
    stage: usize,
    poisoned: bool,
    started_at_ms: Option<i64>,
}

impl<'a> FleetNodeInventoryRecheck<'a> {
    pub(super) fn new(inventory: &'a mut FleetNodeInventory) -> Self {
        inventory.rechecked = None;
        Self {
            inventory,
            stage: 0,
            poisoned: false,
            started_at_ms: None,
        }
    }

    /// Requests fresh first pages: their fingerprints cover the whole category.
    pub fn next_subject(&self) -> Result<Option<FleetSnapshotSubject>> {
        if self.poisoned {
            return Err(Error::Control("native inventory recheck failed"));
        }
        Ok((self.stage < CATEGORIES).then(|| subject(self.stage)))
    }

    /// Accepts a fresh exact request-bound page without replacing original rows.
    pub fn accept(
        &mut self,
        request: &FleetSnapshotRequest,
        response: &FleetNodeSnapshot,
        now_ms: i64,
    ) -> Result<()> {
        let expected = self
            .next_subject()?
            .ok_or(Error::Control("native inventory recheck already complete"))?;
        self.poisoned = true;
        response.validate(request, now_ms)?;
        let original = &mut self.inventory;
        if request.expected() != &original.snapshot
            || request.node() != original.node
            || request.session() != original.session
            || request.subject() != &expected
            || !original.nonces.insert(request.nonce())
            || original.nonces.len() > MAX_CAPTURES
        {
            return Err(Error::Fenced);
        }
        let host = Host {
            state: response.state_before(),
            mode: response.mode(),
            bindings: response.bindings(),
            node_log: response.node_log(),
        };
        if response.state_after() != host.state || host != original.host {
            return Err(Error::Node("native inventory host binding changed"));
        }
        if response.started_at_ms() < original.finished_at_ms
            || response.finished_at_ms() - original.started_at_ms > 30_000
        {
            return Err(Error::Node("native inventory recheck interval differs"));
        }
        let (header, _, _) = pages::header(response.page())?;
        if original.headers[self.stage] != Some(header) {
            return Err(Error::Node("native inventory category changed"));
        }
        original.finished_at_ms = response.finished_at_ms();
        self.started_at_ms.get_or_insert(response.started_at_ms());
        self.stage += 1;
        self.poisoned = false;
        Ok(())
    }

    /// Returns the actual original-to-recheck interval after all seven categories.
    /// Individual accepted pages never grant a partial confirmation.
    pub fn finish(self) -> Result<(i64, i64)> {
        if self.poisoned || self.stage != CATEGORIES {
            return Err(Error::Control("native inventory recheck incomplete"));
        }
        self.inventory.rechecked = Some((
            self.started_at_ms.ok_or(Error::Fenced)?,
            self.inventory.finished_at_ms,
        ));
        Ok(self.inventory.interval())
    }
}
