use super::*;

impl FleetObservation {
    /// Matches every original role and currently unresolved related request to
    /// the supplied fresh policy checks. Attach the original capture and all
    /// available reader/follower checks first. Missing checks become explicit
    /// blockers, including source-side succession and unknown nonexecution.
    /// This cannot upgrade `complete` or grant SettleRoles/Finalize rights.
    pub fn check_maintenance_policies(mut self, roster: &FleetRoster, now_ms: i64) -> Result<Self> {
        if self.maintenance_policies.is_some() {
            return Err(Error::Control(
                "maintenance policy coverage already retained",
            ));
        }
        self.placements(now_ms)?;
        let original = self.maintenance_enrollments.as_ref().ok_or(Error::Control(
            "original maintenance enrollments are required for policy coverage",
        ))?;
        let coverage = FleetMaintenancePolicyCoverage::check(
            original,
            roster,
            self.reader_evacuations().unwrap_or(&[]),
            self.follower_evacuations().unwrap_or(&[]),
            self.maintenance_policy_inputs()?,
        )?;
        self.maintenance_policies = Some(coverage);
        self.validate_role_coverage()?;
        Ok(self)
    }
    /// Complete request-policy matching, including every explicit remaining gap.
    #[must_use]
    pub fn maintenance_policy_coverage(&self) -> Option<&FleetMaintenancePolicyCoverage> {
        self.maintenance_policies.as_ref()
    }
    pub(super) fn validate_maintenance_policies(&self) -> Result<()> {
        let Some(coverage) = &self.maintenance_policies else {
            return Ok(());
        };
        let original = self.maintenance_enrollments.as_ref().ok_or(Error::Fenced)?;
        if coverage.snapshot() != original.snapshot()
            || coverage.roster_digest() != original.roster_digest()
            || coverage.inputs() != self.maintenance_policy_inputs()?
            || coverage.interval().0 < self.capture_started_at_ms
            || coverage.interval().1 > self.capture_finished_at_ms
        {
            return Err(Error::Node("maintenance policy coverage inputs differ"));
        }
        Ok(())
    }
    fn maintenance_policy_inputs(&self) -> Result<Digest> {
        let original = self.maintenance_enrollments.as_ref().ok_or(Error::Fenced)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-maintenance-policy-inputs.v1\0");
        hash.update(original.digest()?.as_bytes());
        self.hash_role_evacuations(&mut hash)?;
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}
