//! Original responsibility set retained atomically before evacuation starts.
use super::*;
use crate::identity::Digest;

mod validation;

/// Hard bound shared with the complete retained enrollment roster.
pub const MAX_MAINTENANCE_ENROLLMENTS: usize = 10_000;
pub(super) const MAX_ENROLLMENT_INVENTORY_PAGES: usize =
    MAX_MAINTENANCE_ENROLLMENTS.div_ceil(MAX_PAGE_ENTRIES);

/// Immutable original reader/follower acceptance page, including unknown results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceEnrollmentPage {
    pub(super) basis: Digest,
    pub(super) ordinal: u32,
    pub(super) entries: Vec<EnrollmentRecord>,
}
impl MaintenanceEnrollmentPage {
    /// Shared original manifest basis, independent of later role progress.
    #[must_use]
    pub const fn basis(&self) -> Digest {
        self.basis
    }
    /// Zero-based ordinal; the full manifest requires every page in order.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    /// Full original unresolved acceptances, in request-key order.
    #[must_use]
    pub fn entries(&self) -> &[EnrollmentRecord] {
        &self.entries
    }
    /// Canonical immutable content identity.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
}

/// Complete original role set at the first Cordoned-to-Evacuating transaction.
///
/// Pending and Established responsibilities at either endpoint of the physical
/// node remain original obligations, including earlier boots. Retiring rows,
/// replacing a boot, extending a deadline or starting a later operation cannot
/// replace this set. Absence is unknown; an empty committed manifest is explicit.
/// This metadata grants no closure, policy, authority or finalization rights.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceEnrollmentInventory {
    pub(super) operation: MaintenanceOperation,
    pub(super) head_digest: Digest,
    pub(super) registry: RegistryVersion,
    pub(super) captured_at_ms: i64,
    pub(super) count: usize,
    pub(super) pages: Vec<Digest>,
}
impl MaintenanceEnrollmentInventory {
    /// Constructs bounded canonical pages from the complete unresolved role set.
    /// The adapter must select all related rows in the same transaction as the
    /// first BeginEvacuation CAS. A separate read followed by publication cannot
    /// establish completeness. This constructor checks shape, not provenance.
    pub fn new(
        operation: MaintenanceOperation,
        barrier: (Digest, RegistryVersion),
        captured_at_ms: i64,
        entries: Vec<EnrollmentRecord>,
    ) -> Result<(Self, Vec<MaintenanceEnrollmentPage>)> {
        if entries.len() > MAX_MAINTENANCE_ENROLLMENTS {
            return Err(OperationError::Budget);
        }
        let mut entries = entries
            .into_iter()
            .map(|row| Ok((*row.spec().key()?.as_bytes(), row)))
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|(key, _)| *key);
        let mut record = Self {
            operation,
            head_digest: barrier.0,
            registry: barrier.1,
            captured_at_ms,
            count: entries.len(),
            pages: Vec::new(),
        };
        record.validate_basis()?;
        let basis = record.basis_digest()?;
        let pages = entries
            .chunks(MAX_PAGE_ENTRIES)
            .enumerate()
            .map(|(ordinal, entries)| MaintenanceEnrollmentPage {
                basis,
                ordinal: ordinal as u32,
                entries: entries.iter().map(|(_, row)| row.clone()).collect(),
            })
            .collect::<Vec<_>>();
        record.pages = pages
            .iter()
            .map(MaintenanceEnrollmentPage::digest)
            .collect::<Result<_>>()?;
        record.validate_pages(&pages)?;
        Ok((record, pages))
    }
    /// Original operation before evacuation, never substituted with a later boot.
    #[must_use]
    pub const fn operation(&self) -> &MaintenanceOperation {
        &self.operation
    }
    /// Original full head identity checked in the accepting transaction.
    #[must_use]
    pub const fn head_digest(&self) -> Digest {
        self.head_digest
    }
    /// Original registry before evacuation effects, independently of fresh reads.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
    /// Original logical acceptance time, retained on replay and reconstruction.
    #[must_use]
    pub const fn captured_at_ms(&self) -> i64 {
        self.captured_at_ms
    }
    /// Complete original reader/follower count; zero requires a committed record.
    #[must_use]
    pub const fn enrollment_count(&self) -> usize {
        self.count
    }
    /// Every immutable page identity in ordinal order.
    #[must_use]
    pub fn pages(&self) -> &[Digest] {
        &self.pages
    }
    /// Canonical manifest identity, including the complete page list.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
    /// Verifies the entire original set, exact page sizes, scope, physical node,
    /// accepted status/time and unique request identities across page boundaries.
    pub fn validate_pages(&self, pages: &[MaintenanceEnrollmentPage]) -> Result<()> {
        self.validate()?;
        if pages.len() != self.pages.len() {
            return Err(OperationError::Conflict);
        }
        let basis = self.basis_digest()?;
        let mut after = None;
        let mut total = 0;
        for (ordinal, (page, digest)) in pages.iter().zip(&self.pages).enumerate() {
            page.validate()?;
            if page.basis != basis
                || page.ordinal as usize != ordinal
                || page.digest()? != *digest
                || page.entries.len() != (self.count - total).min(MAX_PAGE_ENTRIES)
            {
                return Err(OperationError::Conflict);
            }
            for row in &page.entries {
                let key = row.spec().key()?;
                if row.spec().scope != self.registry.scope()
                    || !Self::includes(self.operation.node(), row)
                    || row.updated_at_ms() > self.captured_at_ms
                    || after.is_some_and(|previous| previous >= *key.as_bytes())
                {
                    return Err(OperationError::Conflict);
                }
                after = Some(*key.as_bytes());
                total += 1;
            }
        }
        if total != self.count {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
    /// Selects original unresolved role obligations at either physical endpoint.
    /// Boot retirement is a separate authoritative withdrawal/process protocol.
    #[must_use]
    pub fn includes(node: crate::identity::NodeId, row: &EnrollmentRecord) -> bool {
        row.unresolved()
            && !matches!(row.spec().role, EnrollmentRole::Node { .. })
            && (row.spec().target.node == node
                || row.spec().source.is_some_and(|source| source.node == node))
    }
}

#[cfg(test)]
mod tests;
