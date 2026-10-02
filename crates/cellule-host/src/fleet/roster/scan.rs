use super::*;
use cellule_runtime::fleet::operations::{
    EnrollmentPage, IntentPage, OperationError, RegistryVersion,
};

pub(super) struct Scan<'a> {
    version: RegistryVersion,
    pub(super) intents: Vec<NodeIntent>,
    pub(super) enrollments: Vec<EnrollmentRecord>,
    pub(super) runtime: Option<&'a CellRuntime>,
    pub(super) memory: Vec<NodeByteReservation>,
}

impl Scan<'_> {
    pub(super) fn new(version: RegistryVersion) -> Self {
        Self {
            version,
            intents: Vec::new(),
            enrollments: Vec::new(),
            runtime: None,
            memory: Vec::new(),
        }
    }

    pub(super) fn intents(
        &mut self,
        page: IntentPage,
        after: Option<NodeId>,
    ) -> Result<Option<NodeId>> {
        // Encode through the canonical page validator: in-process adapters obey
        // the same shape and byte bound as transported pages.
        let bytes = page.to_bytes().map_err(operation)?;
        if page.version() != self.version
            || page.after() != after
            || self.intents.last().map(NodeIntent::node) != after
        {
            return Err(operation(OperationError::Conflict));
        }
        if page.entries().len() > MAX_ROSTER_ENTRIES.saturating_sub(self.intents.len()) {
            return Err(Error::Capacity("fleet intent roster bound"));
        }
        self.retain::<NodeIntent>(page.entries().len(), bytes.len())?;
        self.intents.extend_from_slice(page.entries());
        Ok(page.next())
    }

    pub(super) fn enrollments(
        &mut self,
        page: EnrollmentPage,
        after: Option<Digest>,
    ) -> Result<Option<Digest>> {
        let bytes = page.to_bytes().map_err(operation)?;
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
        self.retain::<EnrollmentRecord>(page.entries().len(), bytes.len())?;
        self.enrollments.extend_from_slice(page.entries());
        Ok(page.next())
    }

    fn retain<T>(&mut self, entries: usize, encoded_bytes: usize) -> Result<()> {
        if let Some(runtime) = self.runtime
            && entries != 0
        {
            // Three times fixed row/token storage covers spare Vec capacity and
            // old/new allocations during growth. Encoded dynamic fields retain
            // a separate conservative charge. Reserve before cloning any row.
            let memory = runtime.try_reserve_node_metadata_bytes(
                3 * entries * std::mem::size_of::<T>()
                    + 2 * encoded_bytes
                    + 3 * std::mem::size_of::<NodeByteReservation>(),
            )?;
            self.memory.push(memory);
        }
        Ok(())
    }
}
