use super::*;
use cellule_runtime::fleet::operations::{
    EnrollmentPage, IntentPage, OperationError, RegistryVersion,
};

pub(super) struct Scan {
    version: RegistryVersion,
    pub(super) intents: Vec<NodeIntent>,
    pub(super) enrollments: Vec<EnrollmentRecord>,
}

impl Scan {
    pub(super) fn new(version: RegistryVersion) -> Self {
        Self {
            version,
            intents: Vec::new(),
            enrollments: Vec::new(),
        }
    }

    pub(super) fn intents(
        &mut self,
        page: IntentPage,
        after: Option<NodeId>,
    ) -> Result<Option<NodeId>> {
        // Encode through the canonical page validator: in-process adapters obey
        // the same shape and byte bound as transported pages.
        page.to_bytes().map_err(operation)?;
        if page.version() != self.version
            || page.after() != after
            || self.intents.last().map(NodeIntent::node) != after
        {
            return Err(operation(OperationError::Conflict));
        }
        if page.entries().len() > MAX_ROSTER_ENTRIES.saturating_sub(self.intents.len()) {
            return Err(Error::Capacity("fleet intent roster bound"));
        }
        self.intents.extend_from_slice(page.entries());
        Ok(page.next())
    }

    pub(super) fn enrollments(
        &mut self,
        page: EnrollmentPage,
        after: Option<Digest>,
    ) -> Result<Option<Digest>> {
        page.to_bytes().map_err(operation)?;
        let previous = self
            .enrollments
            .last()
            .map(|record| record.spec().key())
            .transpose()
            .map_err(operation)?;
        if page.version() != self.version || page.after() != after || previous != after {
            return Err(operation(OperationError::Conflict));
        }
        if page.entries().len() > MAX_ROSTER_ENTRIES.saturating_sub(self.enrollments.len()) {
            return Err(Error::Capacity("fleet enrollment roster bound"));
        }
        self.enrollments.extend_from_slice(page.entries());
        Ok(page.next())
    }
}
