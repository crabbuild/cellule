//! Original prefix evidence is retained inside the same observation barrier.
use super::*;

impl FleetObservation {
    /// Retains the complete original boot's native successor collection inside
    /// this capture. Every application remains in its original inventory even
    /// though planner rows are scoped to one application. Matching writer rows,
    /// signed physical destinations and the full retained journal must agree.
    /// This cannot upgrade `complete` or establish role/work settlement.
    pub fn with_original_writer_successors(
        mut self,
        inventory: FleetOriginalWriterSuccessorInventory,
    ) -> Result<Self> {
        if self.original_writer_successors.is_some() {
            return Err(Error::Control(
                "original writer successors already retained",
            ));
        }
        self.original_writer_successors = Some(inventory);
        self.validate_original_writer_successors()?;
        self.validate_failed_boot_closures()?;
        Ok(self)
    }

    /// Complete original prefix/current-serving input retained without filtering
    /// other applications or refreshing its original collection times.
    #[must_use]
    pub fn original_writer_successors(&self) -> Option<&FleetOriginalWriterSuccessorInventory> {
        self.original_writer_successors.as_ref()
    }

    pub(super) fn validate_original_writer_successors(&self) -> Result<()> {
        let Some(inventory) = &self.original_writer_successors else {
            return Ok(());
        };
        let original = inventory.original();
        let (started, finished) = inventory.interval();
        if original.snapshot().head().scope() != self.scope
            || original.snapshot().registry() != self.registry
            || started < self.capture_started_at_ms
            || finished > self.capture_finished_at_ms
            || self
                .roster
                .as_ref()
                .is_some_and(|roster| roster.snapshot() != original.snapshot())
            || self
                .role_coverage
                .as_ref()
                .is_some_and(|coverage| coverage.snapshot() != original.snapshot())
        {
            return Err(Error::Node("original writer observation barrier differs"));
        }
        for proof in inventory.proofs() {
            let serving = proof.serving();
            if !self.nodes.iter().any(|node| {
                node.node() == proof.node()
                    && node.session() == serving.owner().session
                    && node.endpoint() == serving.owner().endpoint
                    && node.release() == proof.release()
            }) {
                return Err(Error::Node("original writer successor boot differs"));
            }
            let observed = self.cells.iter().find(|owned| {
                owned.observation.target.cell_id() == proof.original().target.cell_id()
            });
            // A partial planner scan can omit a scoped row; the complete
            // original proof remains retained and cannot certify that scan.
            if observed.is_none()
                && self.complete
                && proof.original().target.application() == self.scope.application
            {
                return Err(Error::Node("original writer successor row is missing"));
            }
            if let Some(owned) = observed {
                let row = &owned.observation;
                let native = serving.native();
                if owned.node != proof.node()
                    || owned.session != serving.owner().session
                    || row.target != native.target
                    || row.incarnation != native.incarnation
                    || row.generation != native.generation
                    || row.code != native.code
                    || row.schema != native.schema
                    || row.role != native.role
                    || row.position.as_ref() != Some(serving.position())
                {
                    return Err(Error::Node("original writer successor row differs"));
                }
            }
        }
        Ok(())
    }
}
