use super::*;
impl ReaderEvacuationRecord {
    pub(in crate::fleet::operations) fn validate_basis(&self) -> Result<()> {
        self.operation.validate()?;
        self.registry.confirm(self.registry)?;
        self.retired.validate_replay(self.retired.spec())?;
        self.authority
            .encode()
            .map_err(|error| OperationError::Control(Box::new(error)))?;
        let EnrollmentRole::Reader { target, position } = &self.retired.spec().role else {
            return Err(OperationError::Invalid("reader evacuation role differs"));
        };
        if !matches!(
            self.operation.phase(),
            MaintenancePhase::Evacuating | MaintenancePhase::Closing
        ) || self.retired.status() != EnrollmentStatus::Retired
            || self.retired.established_evidence().is_none()
            || self.retired.spec().target.node != self.operation.node()
            || self.registry.scope() != self.retired.spec().scope
            || !nonzero(self.head_digest.as_bytes())
            || !nonzero(self.original_digest.as_bytes())
            || self.started_at_ms < 0
            || self.finished_at_ms < self.started_at_ms
            || self.finished_at_ms - self.started_at_ms > 30_000
            || self.started_at_ms < self.operation.created_at_ms
            || self.finished_at_ms >= self.operation.deadline_ms()
            || self.retired.updated_at_ms() > self.finished_at_ms
            || self.authority.cell != target.cell_id()
            || self.authority.incarnation != position.incarnation
            || self.authority.state != ControlState::Serving
            || self.authority.recovery.is_some()
            || self.authority.owner.is_none()
            || self
                .authority
                .root
                .as_ref()
                .is_none_or(|root| root.commit_sequence < self.minimum_sequence)
            || self.minimum_sequence < position.root.commit_sequence
            || self.minimum_sequence > i64::MAX as u64
            || self.desired_readers > MAX_READERS
            || self.policy_revision == Some(0)
            || (self.policy_revision.is_none() && self.desired_readers != 0)
        {
            return Err(OperationError::Invalid("invalid reader evacuation capture"));
        }
        Ok(())
    }
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        self.validate_basis()?;
        let expected = usize::from(self.desired_readers).div_ceil(MAX_PAGE_ENTRIES);
        if self.pages.len() != expected
            || self.pages.len() > MAX_READER_PAGES
            || self.pages.iter().any(|digest| !nonzero(digest.as_bytes()))
        {
            return Err(OperationError::Invalid("invalid reader evacuation pages"));
        }
        Ok(())
    }
}
impl ReaderEvacuationPage {
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        if !nonzero(self.basis.as_bytes())
            || self.ordinal as usize >= MAX_READER_PAGES
            || self.entries.is_empty()
            || self.entries.len() > MAX_PAGE_ENTRIES
        {
            return Err(OperationError::Invalid("invalid reader evacuation page"));
        }
        let mut after = None;
        for entry in &self.entries {
            let node = *entry.node.as_bytes();
            if !nonzero(&node)
                || !nonzero(entry.session.as_bytes())
                || !nonzero(entry.boot_identity.as_bytes())
                || !nonzero(entry.enrollment_key.as_bytes())
                || !nonzero(entry.enrollment_digest.as_bytes())
                || entry.commit_sequence > i64::MAX as u64
                || after.is_some_and(|previous| node <= previous)
            {
                return Err(OperationError::Invalid(
                    "invalid reader replacement witness",
                ));
            }
            after = Some(node);
        }
        Ok(())
    }
}
