use super::*;

impl MaintenanceEnrollmentInventory {
    pub(in crate::fleet::operations) fn validate_basis(&self) -> Result<()> {
        self.operation.validate()?;
        self.registry.confirm(self.registry)?;
        if self.operation.phase() != MaintenancePhase::Cordoned
            || self.registry.bootstrap_revision().is_none()
            || !nonzero(self.head_digest.as_bytes())
            || self.captured_at_ms < self.operation.created_at_ms()
            || self.count > MAX_MAINTENANCE_ENROLLMENTS
        {
            return Err(OperationError::Invalid(
                "invalid original maintenance enrollment basis",
            ));
        }
        Ok(())
    }
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        self.validate_basis()?;
        if self.pages.len() != self.count.div_ceil(MAX_PAGE_ENTRIES)
            || self.pages.len() > MAX_ENROLLMENT_INVENTORY_PAGES
            || self.pages.iter().any(|digest| !nonzero(digest.as_bytes()))
        {
            return Err(OperationError::Invalid(
                "invalid maintenance enrollment page list",
            ));
        }
        Ok(())
    }
}
impl MaintenanceEnrollmentPage {
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        if !nonzero(self.basis.as_bytes())
            || self.ordinal as usize >= MAX_ENROLLMENT_INVENTORY_PAGES
            || self.entries.is_empty()
            || self.entries.len() > MAX_PAGE_ENTRIES
        {
            return Err(OperationError::Invalid(
                "invalid maintenance enrollment page",
            ));
        }
        let mut after = None;
        for row in &self.entries {
            row.validate_replay(row.spec())?;
            let key = row.spec().key()?;
            if !row.unresolved()
                || matches!(row.spec().role, EnrollmentRole::Node { .. })
                || after.is_some_and(|previous| previous >= *key.as_bytes())
            {
                return Err(OperationError::Invalid(
                    "invalid original maintenance enrollment",
                ));
            }
            after = Some(*key.as_bytes());
        }
        Ok(())
    }
}
