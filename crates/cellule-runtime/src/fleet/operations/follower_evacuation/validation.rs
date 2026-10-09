use super::*;
impl FollowerReplacementPolicy {
    pub(in crate::fleet::operations) fn validate(self) -> Result<()> {
        self.scope.validate()?;
        if self.revision == 0 || !(1..=2).contains(&self.minimum_members) {
            return Err(OperationError::Invalid(
                "invalid follower replacement policy",
            ));
        }
        Ok(())
    }
}
impl FollowerEvacuationRecord {
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        self.registry.confirm(self.registry)?;
        self.policy.validate()?;
        if !matches!(
            self.operation.phase(),
            MaintenancePhase::Evacuating | MaintenancePhase::Closing
        ) || self.registry.scope() != self.policy.scope
            || self.retired.is_empty()
            || self.retired.len() > 2
            || self.replacements.len() < usize::from(self.policy.minimum_members)
            || self.replacements.len() > 2
            || [
                self.head_digest,
                self.original_key,
                self.original_digest,
                self.source_boot,
                self.replacement_evidence,
            ]
            .iter()
            .any(|digest| !nonzero(digest.as_bytes()))
            || self.replacement_epoch <= self.original_epoch()?
            || self.covered_through == u64::MAX
            || self.started_at_ms < self.operation.created_at_ms
            || self.finished_at_ms < self.started_at_ms
            || self.finished_at_ms - self.started_at_ms > 30_000
            || self.finished_at_ms >= self.operation.deadline_ms()
        {
            return Err(OperationError::Invalid(
                "invalid follower evacuation capture",
            ));
        }
        let source = self.source()?;
        let epoch = self.original_epoch()?;
        if source.node == self.operation.node() {
            return Err(OperationError::Invalid(
                "follower source is maintenance donor",
            ));
        }
        let mut originals = HashSet::new();
        let mut sessions = HashSet::new();
        let mut after = None;
        let mut donor = false;
        for row in &self.retired {
            row.validate_replay(row.spec())?;
            let endpoint = row.spec().target;
            let node = *endpoint.node.as_bytes();
            if row.status() != EnrollmentStatus::Retired
                || row.established_evidence().is_none()
                || row.spec().scope != self.policy.scope
                || row.spec().source != Some(source)
                || row.spec().role != (EnrollmentRole::Follower { log_epoch: epoch })
                || row.updated_at_ms() > self.finished_at_ms
                || !originals.insert(row.spec().key()?)
                || !sessions.insert(endpoint.session)
                || after.is_some_and(|previous| node <= previous)
            {
                return Err(OperationError::Invalid(
                    "invalid original follower ensemble",
                ));
            }
            if row.spec().key()? == self.original_key {
                if endpoint.node != self.operation.node() {
                    return Err(OperationError::Invalid("original follower donor differs"));
                }
                donor = true;
            }
            after = Some(node);
        }
        if !donor {
            return Err(OperationError::Invalid("original donor request is absent"));
        }
        let mut keys = HashSet::new();
        let mut sessions = HashSet::new();
        let mut after = None;
        for entry in &self.replacements {
            let row = &entry.enrollment;
            row.validate_replay(row.spec())?;
            let endpoint = row.spec().target;
            let node = *endpoint.node.as_bytes();
            if row.status() != EnrollmentStatus::Established
                || row.spec().scope != self.policy.scope
                || row.spec().source.is_none_or(|current| {
                    current.node != source.node || current.session != source.session
                })
                || row.spec().role
                    != (EnrollmentRole::Follower {
                        log_epoch: self.replacement_epoch,
                    })
                || endpoint.node == self.operation.node()
                || endpoint.session == self.operation.session()
                || row.updated_at_ms() > self.finished_at_ms
                || !nonzero(entry.boot_identity.as_bytes())
                || !keys.insert(row.spec().key()?)
                || !sessions.insert(endpoint.session)
                || after.is_some_and(|previous| node <= previous)
            {
                return Err(OperationError::Invalid(
                    "invalid follower replacement ensemble",
                ));
            }
            after = Some(node);
        }
        Ok(())
    }
}
