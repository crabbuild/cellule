//! Original Cell capture and persistence through the existing fleet journal.
use super::*;
use cellule_runtime::{
    cell::catalog::{CatalogScanReceipt, CellCatalog},
    control::authority::CellAuthority,
    fleet::operations::{
        FleetScope, MaintenanceOperation, OperationId, OriginalWriterInventoryPage,
        OriginalWriterInventoryRecord,
    },
    identity::{ApplicationId, TenantId},
    ltx::CellStorageLayout,
};

mod capture;
mod inventory;
pub use inventory::FleetOriginalWriterInventory;

/// One application-authenticated canonical catalog source. Construct from the
/// same storage layout for catalog and authority; never accept remote credentials.
#[derive(Clone)]
pub struct FleetOriginalCatalogSource {
    identity: Digest,
    tenant: TenantId,
    layout: CellStorageLayout,
}
impl FleetOriginalCatalogSource {
    /// Shape validation only. The provider attests canonical source identity.
    pub fn new(identity: Digest, tenant: TenantId, layout: CellStorageLayout) -> Result<Self> {
        if identity.as_bytes() == &[0; 32] {
            return Err(Error::Control("invalid original catalog source"));
        }
        Ok(Self {
            identity,
            tenant,
            layout,
        })
    }
    /// Original trusted canonical source identity.
    #[must_use]
    pub const fn identity(&self) -> Digest {
        self.identity
    }
    /// Original tenant scope.
    #[must_use]
    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }
    /// Original application scope.
    #[must_use]
    pub fn application(&self) -> ApplicationId {
        ApplicationId::from_bytes(*self.layout.application_id())
    }
}

/// Application attestation of every canonical catalog the original physical
/// boot could write, across every application/tenant. Empty is an explicit
/// authenticated no-writer configuration, never inferred from missing storage.
pub struct FleetOriginalCatalogSet {
    request: Digest,
    operation: MaintenanceOperation,
    witness: Digest,
    sources: Vec<FleetOriginalCatalogSource>,
}
impl FleetOriginalCatalogSet {
    /// Binds a bounded complete source set to the exact original request and
    /// operation. Providers retain the source-set witness across reconstruction.
    pub fn new(
        request: &FleetFailedBootProcessRequest,
        operation: MaintenanceOperation,
        witness: Digest,
        mut sources: Vec<FleetOriginalCatalogSource>,
    ) -> Result<Self> {
        if witness.as_bytes() == &[0; 32]
            || sources.len() > cellule_runtime::fleet::operations::MAX_ORIGINAL_CATALOGS
            || operation.node() != request.boot().spec().target.node
            || operation.session() != request.boot().spec().target.session
        {
            return Err(Error::Fenced);
        }
        operation.to_bytes().map_err(operation_error)?;
        sources.sort_by_key(|source| (*source.application().as_bytes(), *source.tenant.as_bytes()));
        if sources.windows(2).any(|pair| {
            (pair[0].application(), pair[0].tenant) == (pair[1].application(), pair[1].tenant)
        }) {
            return Err(Error::Control("original catalog scopes are duplicated"));
        }
        Ok(Self {
            request: request.digest(),
            operation,
            witness,
            sources,
        })
    }
    fn matches(&self, other: &Self) -> bool {
        self.request == other.request
            && self.operation == other.operation
            && self.witness == other.witness
            && self.sources.len() == other.sources.len()
            && self.sources.iter().zip(&other.sources).all(|(a, b)| {
                a.identity == b.identity
                    && a.tenant == b.tenant
                    && a.application() == b.application()
                    && a.layout.application_prefix() == b.layout.application_prefix()
            })
    }
}

/// Read-only application provider for original boot's complete authorized
/// catalog set. Authenticate every original physical/session scope and canonical
/// storage mapping; exclude omitted applications/tenants and session reuse. A
/// filtered resident list, current-owner scan or successful recovery is insufficient.
/// Reads must return the same durable witness and canonical mappings. Accepted
/// provider work uses the application's existing finite owner; no effect starts here.
pub trait FleetOriginalCatalogs: Send + Sync {
    /// Confirms complete original configuration under the full operation barrier.
    fn catalogs<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
        operation: &'a MaintenanceOperation,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, FleetOriginalCatalogSet>;
}

/// Immutable original set in the same registry transaction domain as all other
/// fleet work. First publication advances the registry atomically. Identical
/// replay returns original bytes/times; a second different original set conflicts.
pub trait FleetOriginalWriterJournal: FleetJournal {
    /// Checks full barrier, bootstrap, live controller/current operation/intent
    /// and original boot row, then commits every page plus its one original-set
    /// pointer atomically. Orphan pages cannot establish a retained inventory.
    fn persist_original_writers<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a OriginalWriterInventoryRecord,
        pages: &'a [OriginalWriterInventoryPage],
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, OriginalWriterInventoryRecord>;
    /// Reads the committed original set at a complete current barrier.
    fn original_writers<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        process_request: Digest,
    ) -> FleetAdapterFuture<'a, Option<OriginalWriterInventoryRecord>>;
    /// Reads one digest-verified historical page; validate all manifest pages.
    fn original_writer_page(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<OriginalWriterInventoryPage>>;
}

/// Opaque fully collected original set. Persist it before dependent effects.
/// Collection alone grants no relocation, successor readiness or role settlement.
pub struct FleetOriginalWriterCapture {
    snapshot: FleetJournalSnapshot,
    request: FleetFailedBootProcessRequest,
    process: FleetFailedBootProcessEvidence,
    sources: FleetOriginalCatalogSet,
    catalogs: Vec<CatalogScanReceipt>,
    record: OriginalWriterInventoryRecord,
    pages: Vec<OriginalWriterInventoryPage>,
}
impl FleetOriginalWriterCapture {
    /// Complete immutable original manifest, without publication rights.
    #[must_use]
    pub const fn record(&self) -> &OriginalWriterInventoryRecord {
        &self.record
    }
    /// Complete immutable original pages.
    #[must_use]
    pub fn pages(&self) -> &[OriginalWriterInventoryPage] {
        &self.pages
    }
}
fn operation_error(source: cellule_runtime::fleet::operations::OperationError) -> Error {
    operation(source)
}
