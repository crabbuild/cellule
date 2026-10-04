use super::*;

pub(super) fn key(
    row: &OriginalWriterObservation,
) -> ([u8; 16], [u8; 16], [u8; 32], [u8; 16], u64) {
    (
        *row.target.application().as_bytes(),
        *row.target.tenant().as_bytes(),
        *row.control.cell.as_bytes(),
        *row.control.incarnation.as_bytes(),
        row.control.epoch,
    )
}
impl OriginalWriterInventoryRecord {
    pub(in crate::fleet::operations) fn validate_basis(&self) -> Result<()> {
        let basis = &self.basis;
        basis.operation.validate()?;
        basis.registry.confirm(basis.registry)?;
        basis.boot.validate_replay(basis.boot.spec())?;
        if basis.registry.bootstrap_revision().is_none()
            || basis.registry.scope() != basis.boot.spec().scope
            || !matches!(basis.boot.spec().role, EnrollmentRole::Node { .. })
            || basis.boot.spec().source.is_some()
            || basis.boot.established_evidence().is_none()
            || !matches!(
                basis.boot.status(),
                EnrollmentStatus::Established | EnrollmentStatus::Retired
            )
            || basis.boot.spec().target.node != basis.operation.node()
            || basis.boot.spec().target.session != basis.operation.session()
            || basis.operation.phase() == MaintenancePhase::Completed
            || basis.interval.0 < basis.operation.created_at_ms
            || basis.interval.0 < basis.boot.updated_at_ms()
            || basis.interval.1 < basis.interval.0
            || basis.interval.1 - basis.interval.0 > 30_000
            || basis.interval.1 >= basis.operation.deadline_ms()
            || [
                basis.head_digest,
                basis.process_request,
                basis.process_witness,
                basis.catalog_witness,
            ]
            .iter()
            .any(|digest| !nonzero(digest.as_bytes()))
            || self.count > MAX_ORIGINAL_WRITERS
            || self.catalogs.len() > MAX_ORIGINAL_CATALOGS
        {
            return Err(OperationError::Invalid(
                "invalid original writer inventory basis",
            ));
        }
        let mut after = None;
        let mut owners = 0_u64;
        let mut cells = 0_u64;
        for row in &self.catalogs {
            let key = (*row.application.as_bytes(), *row.tenant.as_bytes());
            if [row.source, row.heads, row.histories]
                .iter()
                .any(|digest| !nonzero(digest.as_bytes()))
                || row.cells > 10_000
                || row.owners > MAX_ORIGINAL_WRITERS as u64
                || (row.cells == 0 && row.owners != 0)
                || after.is_some_and(|previous| previous >= key)
            {
                return Err(OperationError::Invalid("invalid original catalog witness"));
            }
            owners += row.owners;
            cells += row.cells;
            after = Some(key);
        }
        if cells > MAX_ORIGINAL_WRITERS as u64 || owners != self.count as u64 {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        self.validate_basis()?;
        if self.pages.len() != self.count.div_ceil(WRITERS_PER_PAGE)
            || self.pages.len() > MAX_WRITER_PAGES
            || self.pages.iter().any(|digest| !nonzero(digest.as_bytes()))
        {
            return Err(OperationError::Invalid(
                "invalid original writer inventory pages",
            ));
        }
        Ok(())
    }
}
impl OriginalWriterInventoryPage {
    pub(in crate::fleet::operations) fn validate(&self) -> Result<()> {
        if !nonzero(self.basis.as_bytes())
            || self.ordinal as usize >= MAX_WRITER_PAGES
            || self.entries.is_empty()
            || self.entries.len() > WRITERS_PER_PAGE
        {
            return Err(OperationError::Invalid(
                "invalid original writer inventory page",
            ));
        }
        let mut after = None;
        for row in &self.entries {
            let body = row
                .control
                .encode()
                .map_err(|source| OperationError::Control(Box::new(source)))?;
            let target = CellTarget::new(
                row.target.tenant(),
                row.target.application(),
                row.target.namespace(),
                row.target.partition(),
            )
            .map_err(|source| OperationError::Identity(Box::new(source)))?;
            let key = key(row);
            if target.cell_id() != row.control.cell
                || row.control.owner.is_none()
                || body.len() > 8 * 1024
                || after.is_some_and(|previous| previous >= key)
            {
                return Err(OperationError::Invalid(
                    "invalid original writer observation",
                ));
            }
            after = Some(key);
        }
        Ok(())
    }
}
