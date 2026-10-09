use super::*;

impl FleetObservation {
    /// Retains the complete original role set at this exact fresh full barrier.
    /// The original journal capture survives current retirement and boot changes.
    /// Attaching it cannot upgrade completeness or certify policy/work settlement.
    pub fn with_maintenance_enrollments(
        mut self,
        original: FleetMaintenanceEnrollments,
    ) -> Result<Self> {
        if self.maintenance_enrollments.is_some() {
            return Err(Error::Control(
                "original maintenance enrollments already retained",
            ));
        }
        self.maintenance_enrollments = Some(original);
        self.validate_role_coverage()?;
        Ok(self)
    }
    /// Every original role acceptance, retained even after current retirement.
    #[must_use]
    pub fn maintenance_enrollments(&self) -> Option<&FleetMaintenanceEnrollments> {
        self.maintenance_enrollments.as_ref()
    }
    pub(super) fn validate_maintenance_enrollments(&self) -> Result<()> {
        let Some(original) = &self.maintenance_enrollments else {
            return Ok(());
        };
        let snapshot = original.snapshot();
        let (started, finished) = original.interval();
        if snapshot.head().scope() != self.scope
            || snapshot.registry() != self.registry
            || started < self.capture_started_at_ms
            || finished > self.capture_finished_at_ms
            || self
                .roster
                .as_ref()
                .is_some_and(|roster| roster.snapshot() != snapshot)
            || self
                .role_coverage
                .as_ref()
                .is_some_and(|coverage| coverage.snapshot() != snapshot)
            || self
                .original_writer_successors
                .as_ref()
                .is_some_and(|inventory| inventory.original().snapshot() != snapshot)
            || self.failed_boot_closures.as_ref().is_some_and(|closures| {
                closures
                    .iter()
                    .any(|closure| closure.snapshot() != snapshot)
            })
            || self
                .recovered_follower_closures
                .as_ref()
                .is_some_and(|closures| {
                    closures
                        .iter()
                        .any(|closure| closure.snapshot() != snapshot)
                })
            || self
                .reader_evacuations()
                .is_some_and(|checks| checks.iter().any(|check| check.snapshot() != snapshot))
            || self
                .follower_evacuations()
                .is_some_and(|checks| checks.iter().any(|check| check.snapshot() != snapshot))
        {
            return Err(Error::Node(
                "original maintenance enrollment observation barrier differs",
            ));
        }
        let expected_roster = original.roster_digest();
        if self
            .roster
            .as_ref()
            .map(FleetRoster::digest)
            .transpose()?
            .is_some_and(|digest| digest != expected_roster)
            || self
                .role_coverage
                .as_ref()
                .is_some_and(|coverage| coverage.roster_digest() != expected_roster)
            || self.reader_evacuations().is_some_and(|checks| {
                checks
                    .iter()
                    .any(|check| check.roster_digest() != expected_roster)
            })
            || self.follower_evacuations().is_some_and(|checks| {
                checks
                    .iter()
                    .any(|check| check.roster_digest() != expected_roster)
            })
        {
            return Err(Error::Node(
                "original maintenance enrollment roster differs",
            ));
        }
        Ok(())
    }
}
