//! Original independently joined nonexecution inside the full capture barrier.
use super::*;

impl FleetObservation {
    /// Retains independently confirmed original nonexecution. Attach this and
    /// the exact original capture before matching maintenance policies. Attachment
    /// starts no effect and cannot upgrade complete observation or grant finalization.
    pub fn with_maintenance_nonexecution(
        mut self,
        checks: FleetMaintenanceNonexecution,
    ) -> Result<Self> {
        if self.maintenance_nonexecution.is_some() {
            return Err(Error::Control("maintenance nonexecution already retained"));
        }
        self.maintenance_nonexecution = Some(checks);
        self.validate_role_coverage()?;
        Ok(self)
    }
    /// Original provider confirmations, never restamped on attachment.
    #[must_use]
    pub fn maintenance_nonexecution(&self) -> Option<&FleetMaintenanceNonexecution> {
        self.maintenance_nonexecution.as_ref()
    }
    pub(super) fn validate_maintenance_nonexecution(&self) -> Result<()> {
        let Some(checks) = &self.maintenance_nonexecution else {
            return Ok(());
        };
        let snapshot = checks.snapshot();
        let interval = checks.interval();
        if snapshot.head().scope() != self.scope
            || snapshot.registry() != self.registry
            || interval.0 < self.capture_started_at_ms
            || interval.1 > self.capture_finished_at_ms
        {
            return Err(Error::Node(
                "maintenance nonexecution observation barrier differs",
            ));
        }
        if let Some(original) = &self.maintenance_enrollments
            && (snapshot != original.snapshot()
                || checks.roster_digest() != original.roster_digest()
                || checks.original_digest() != original.digest()?)
        {
            return Err(Error::Node(
                "maintenance nonexecution original capture differs",
            ));
        }
        if let Some(roster) = &self.roster
            && (snapshot != roster.snapshot() || checks.roster_digest() != roster.digest()?)
        {
            return Err(Error::Node("maintenance nonexecution roster differs"));
        }
        if self.role_coverage.as_ref().is_some_and(|coverage| {
            snapshot != coverage.snapshot() || checks.roster_digest() != coverage.roster_digest()
        }) || self
            .original_writer_successors
            .as_ref()
            .is_some_and(|inventory| snapshot != inventory.original().snapshot())
            || self.failed_boot_closures.as_ref().is_some_and(|closures| {
                closures
                    .iter()
                    .any(|closure| closure.snapshot() != snapshot)
            })
            || self.reader_evacuations().is_some_and(|roles| {
                roles.iter().any(|check| {
                    check.snapshot() != snapshot || check.roster_digest() != checks.roster_digest()
                })
            })
            || self.follower_evacuations().is_some_and(|roles| {
                roles.iter().any(|check| {
                    check.snapshot() != snapshot || check.roster_digest() != checks.roster_digest()
                })
            })
        {
            return Err(Error::Node(
                "maintenance nonexecution evidence barrier differs",
            ));
        }
        Ok(())
    }
}
