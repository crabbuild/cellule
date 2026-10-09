//! Complete reconstruction of immutable original inputs after controller restart.
use super::*;
use cellule_runtime::fleet::operations::OriginalWriterObservation;

/// Complete committed original writer set reconstructed through its canonical
/// journal pointer and every exact page. Missing commitment is distinct from an
/// explicitly authenticated empty set; missing pages never yield a partial set.
///
/// This retains original scopes, epochs, controls and times. It is historical
/// input, not current successor serving, a root pin or maintenance settlement.
/// The application authenticates the journal, accounts bounded collector memory
/// and owns accepted adapter work through the enclosing finite deadline.
pub struct FleetOriginalWriterInventory {
    record: OriginalWriterInventoryRecord,
    pages: Vec<OriginalWriterInventoryPage>,
}

impl FleetOriginalWriterInventory {
    /// Loads the immutable original set at the supplied complete journal read
    /// barrier. The canonical record bounds every page and owner before this
    /// value escapes; wrong scope, key, page identity/order or counts refuse.
    /// `None` means no committed set at that read, never zero original writers
    /// or proof that an accepted capture cannot still publish.
    pub async fn load(
        journal: &dyn FleetOriginalWriterJournal,
        expected: &FleetJournalSnapshot,
        operation: OperationId,
        process_request: Digest,
        deadline: Instant,
    ) -> Result<Option<Self>> {
        if operation.as_bytes() == &[0; 16] || process_request.as_bytes() == &[0; 32] {
            return Err(Error::Control("invalid original writer inventory key"));
        }
        bounded(deadline, async {
            let Some(record) = journal
                .original_writers(expected, operation, process_request)
                .await
                .map_err(adapter_error)?
            else {
                return Ok(None);
            };
            if record.basis().registry.scope() != expected.head().scope()
                || record.basis().operation.id() != operation
                || record.basis().process_request != process_request
            {
                return Err(Error::Fenced);
            }
            // Validate the bounded header before reserving the page vector.
            // Pages are immutable history; they cannot refresh this read's
            // barrier or supply current native-role evidence.
            record.digest().map_err(operation_error)?;
            let mut pages = Vec::with_capacity(record.pages().len());
            for digest in record.pages() {
                pages.push(
                    journal
                        .original_writer_page(expected.head().scope(), *digest)
                        .await
                        .map_err(adapter_error)?
                        .ok_or(Error::Fenced)?,
                );
            }
            record.validate_pages(&pages).map_err(operation_error)?;
            Ok(Some(Self { record, pages }))
        })
        .await
    }

    /// Original immutable manifest, including complete source witnesses.
    #[must_use]
    pub const fn record(&self) -> &OriginalWriterInventoryRecord {
        &self.record
    }

    /// Every exact original page in canonical order.
    #[must_use]
    pub fn pages(&self) -> &[OriginalWriterInventoryPage] {
        &self.pages
    }

    /// All original owner epochs across the fully validated page set.
    pub fn writers(&self) -> impl Iterator<Item = &OriginalWriterObservation> {
        self.pages.iter().flat_map(|page| page.entries().iter())
    }
}
