//! Exact source-reader policy capture inside the full original observation.
use super::*;

impl FleetObservation {
    /// Retains exact source native joining, final-root derivation and current
    /// successor/reader policy before complete request matching. Attachment
    /// starts no effect and cannot upgrade `complete` or grant finalization.
    pub fn with_source_reader_policies(
        mut self,
        policies: FleetSourceReaderPolicies,
    ) -> Result<Self> {
        if self.source_reader_policies.is_some() {
            return Err(Error::Control("source reader policies already retained"));
        }
        self.source_reader_policies = Some(policies);
        self.validate_role_coverage()?;
        Ok(self)
    }
    /// Original source checks, with their retained charged native capsules.
    #[must_use]
    pub fn source_reader_policies(&self) -> Option<&FleetSourceReaderPolicies> {
        self.source_reader_policies.as_ref()
    }
    pub(super) fn validate_source_reader_policies(&self) -> Result<()> {
        let Some(checks) = &self.source_reader_policies else {
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
                "source reader policy observation barrier differs",
            ));
        }
        if let Some(original) = &self.maintenance_enrollments
            && (snapshot != original.snapshot()
                || checks.roster_digest() != original.roster_digest()
                || checks.original_digest() != original.digest()?)
        {
            return Err(Error::Node("source reader policy original capture differs"));
        }
        if let Some(roster) = &self.roster
            && (snapshot != roster.snapshot() || checks.roster_digest() != roster.digest()?)
        {
            return Err(Error::Node("source reader policy roster differs"));
        }
        if self.role_coverage.as_ref().is_some_and(|coverage| {
            snapshot != coverage.snapshot() || checks.roster_digest() != coverage.roster_digest()
        }) || self
            .original_writer_successors
            .as_ref()
            .is_some_and(|inventory| snapshot != inventory.original().snapshot())
            || self
                .failed_boot_closures
                .as_ref()
                .is_some_and(|rows| rows.iter().any(|row| row.snapshot() != snapshot))
            || self
                .recovered_follower_closures
                .as_ref()
                .is_some_and(|rows| rows.iter().any(|row| row.snapshot() != snapshot))
            || self.maintenance_nonexecution.as_ref().is_some_and(|proof| {
                snapshot != proof.snapshot() || checks.roster_digest() != proof.roster_digest()
            })
            || self.reader_evacuations().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row.snapshot() != snapshot || checks.roster_digest() != row.roster_digest()
                })
            })
            || self.follower_evacuations().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row.snapshot() != snapshot || checks.roster_digest() != row.roster_digest()
                })
            })
        {
            return Err(Error::Node("source reader policy evidence barrier differs"));
        }
        for check in checks.checks() {
            self.current_writer(check.node(), check.release(), check.serving())?;
            self.role_boot(
                check.node(),
                check.serving().owner().session,
                check.boot_identity(),
                crate::read_replicas::maintenance::boot_identity,
            )?;
            for replacement in check.replacements() {
                self.role_boot(
                    replacement.node,
                    replacement.session,
                    replacement.boot_identity,
                    crate::read_replicas::maintenance::boot_identity,
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn hash_source_reader_policies(&self, hash: &mut blake3::Hasher) -> Result<()> {
        hash.update(&[u8::from(self.source_reader_policies.is_some())]);
        if let Some(policies) = &self.source_reader_policies {
            hash.update(policies.digest()?.as_bytes());
        }
        Ok(())
    }
}
