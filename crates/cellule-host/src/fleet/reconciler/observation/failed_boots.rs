//! Original stopped-process and terminal role evidence in the planner barrier.
use super::*;

impl FleetObservation {
    /// Retains freshly reconfirmed committed original boot retirements.
    ///
    /// Use [`crate::fleet::FleetFailedBootRetirement::confirm`] after closing
    /// original related roles and work. Every capsule must share this exact
    /// head/registry and capture interval. A replacement boot may be advertised;
    /// the retired session cannot supply a current native row. This never
    /// upgrades `complete`, settles replacement policy or proves writer recovery.
    pub fn with_failed_boot_closures(
        mut self,
        closures: Vec<FleetFailedBootClosure>,
    ) -> Result<Self> {
        if self.failed_boot_closures.is_some() {
            return Err(Error::Control("failed boot closures already retained"));
        }
        self.failed_boot_closures = Some(closures);
        self.validate_role_coverage()?;
        Ok(self)
    }

    /// Original confirmations retained without refreshing collection timestamps.
    #[must_use]
    pub fn failed_boot_closures(&self) -> Option<&[FleetFailedBootClosure]> {
        self.failed_boot_closures.as_deref()
    }

    pub(super) fn validate_failed_boot_closures(&self) -> Result<()> {
        let Some(closures) = &self.failed_boot_closures else {
            return Ok(());
        };
        if closures.len() > 10_000 {
            return Err(Error::Capacity("failed boot closure bound exceeded"));
        }
        let mut boots = HashSet::new();
        let expected = self
            .roster
            .as_ref()
            .map(FleetRoster::snapshot)
            .or_else(|| self.role_coverage.as_ref().map(FleetRoleCoverage::snapshot))
            .or_else(|| {
                self.original_writer_successors
                    .as_ref()
                    .map(|inventory| inventory.original().snapshot())
            })
            .or_else(|| closures.first().map(FleetFailedBootClosure::snapshot));
        for closure in closures {
            let snapshot = closure.snapshot();
            let endpoint = closure.boot().spec().target;
            let (started, finished) = closure.interval();
            if snapshot.head().scope() != self.scope
                || snapshot.registry() != self.registry
                || expected != Some(snapshot)
                || started < self.capture_started_at_ms
                || finished > self.capture_finished_at_ms
                || !boots.insert((endpoint.node, endpoint.session))
                || self
                    .nodes
                    .iter()
                    .any(|node| node.session() == endpoint.session)
                || self
                    .cells
                    .iter()
                    .any(|owned| owned.session == endpoint.session)
            {
                return Err(Error::Node("failed boot observation barrier differs"));
            }
            if self
                .roster
                .as_ref()
                .is_some_and(|roster| !roster.enrollments().iter().any(|row| row == closure.boot()))
            {
                return Err(Error::Node("failed boot observation roster differs"));
            }
            if let Some(inventory) = &self.original_writer_successors {
                let original = inventory.original();
                let fence = original.recovered().fence();
                if (fence.node(), fence.session()) == (endpoint.node, endpoint.session)
                    && (closure.process() != original.process()
                        || closure.canonical().fence() != *fence
                        || closure.canonical().log() != original.recovered().log())
                {
                    return Err(Error::Node("failed boot original writer evidence differs"));
                }
            }
        }
        Ok(())
    }
}
