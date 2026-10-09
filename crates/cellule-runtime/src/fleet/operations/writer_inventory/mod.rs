//! Original failed-boot writer metadata, retained before dependent effects.
use super::*;
use crate::{
    control::Control,
    identity::{ApplicationId, CellTarget, Digest, TenantId},
};

mod validation;

/// Hard bounds for complete original observations and authenticated catalog scopes.
pub const MAX_ORIGINAL_WRITERS: usize = 10_000;
/// Maximum canonical application/tenant sources in one complete capture.
pub const MAX_ORIGINAL_CATALOGS: usize = 128;
// A full control is bounded to 8 KiB and a target partition to 1 KiB. Sixty-four
// rows fit the ordinary one-MiB page even at those bounds, without truncation.
pub(super) const WRITERS_PER_PAGE: usize = 64;
pub(super) const MAX_WRITER_PAGES: usize = MAX_ORIGINAL_WRITERS.div_ceil(WRITERS_PER_PAGE);

/// Complete original publication basis. Digests identify provider attestations;
/// constructing or decoding this metadata grants no authentication or authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalWriterInventoryBasis {
    /// Operation as captured, without restamping or later session substitution.
    pub operation: MaintenanceOperation,
    /// Digest of the complete original controller head.
    pub head_digest: Digest,
    /// Original registry barrier.
    pub registry: RegistryVersion,
    /// Original enrolled physical boot, including first establishment history.
    pub boot: EnrollmentRecord,
    /// Immutable original process/fence request identity.
    pub process_request: Digest,
    /// Durable original process and accepted-work joining evidence.
    pub process_witness: Digest,
    /// Application-authenticated complete catalog source-set identity.
    pub catalog_witness: Digest,
    /// Original capture interval.
    pub interval: (i64, i64),
}

/// One fully traversed application/tenant scope. The history digest covers every
/// catalog target, including absence and owners outside the failed boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalCatalogWitness {
    /// Canonical application identity.
    pub application: ApplicationId,
    /// Canonical tenant identity.
    pub tenant: TenantId,
    /// Application's durable identity of this canonical storage source.
    pub source: Digest,
    /// Digest of all 256 original revisions and ordered page identities.
    pub heads: Digest,
    /// Digest of all original per-Cell authority/history observations.
    pub histories: Digest,
    /// Complete catalog entry count, including unused bootstrap entries.
    pub cells: u64,
    /// Original boot's ownership observations retained from this scope.
    pub owners: u64,
}

/// Exact original owner observation. Rootless, recovering and object-covered
/// writers remain obligations; the complete Control includes every overlay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalWriterObservation {
    /// Complete tenant/application/namespace/partition binding.
    pub target: CellTarget,
    /// Original control, not reconstructed from a successor counter.
    pub control: Control,
}

/// Immutable bounded page of the complete original ownership set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalWriterInventoryPage {
    pub(super) basis: Digest,
    pub(super) ordinal: u32,
    pub(super) entries: Vec<OriginalWriterObservation>,
}
impl OriginalWriterInventoryPage {
    /// Manifest basis shared by all original pages.
    #[must_use]
    pub const fn basis(&self) -> Digest {
        self.basis
    }
    /// Zero-based complete-page ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    /// Original owner observations in canonical scope/Cell/epoch order.
    #[must_use]
    pub fn entries(&self) -> &[OriginalWriterObservation] {
        &self.entries
    }
    /// Canonical immutable page identity.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
}

/// Durable original set; historical capture cannot prove current successor
/// serving, availability, acknowledged-prefix inclusion or maintenance completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalWriterInventoryRecord {
    pub(super) basis: OriginalWriterInventoryBasis,
    pub(super) catalogs: Vec<OriginalCatalogWitness>,
    pub(super) count: usize,
    pub(super) pages: Vec<Digest>,
}
impl OriginalWriterInventoryRecord {
    /// Builds canonical bounded pages after validating complete metadata shape.
    /// Providers still authenticate/collect the complete set before publication.
    pub fn new(
        basis: OriginalWriterInventoryBasis,
        mut catalogs: Vec<OriginalCatalogWitness>,
        mut owners: Vec<OriginalWriterObservation>,
    ) -> Result<(Self, Vec<OriginalWriterInventoryPage>)> {
        if owners.len() > MAX_ORIGINAL_WRITERS || catalogs.len() > MAX_ORIGINAL_CATALOGS {
            return Err(OperationError::Budget);
        }
        catalogs.sort_by_key(|row| (*row.application.as_bytes(), *row.tenant.as_bytes()));
        owners.sort_by_key(validation::key);
        let mut record = Self {
            basis,
            catalogs,
            count: owners.len(),
            pages: Vec::new(),
        };
        record.validate_basis()?;
        let digest = record.basis_digest()?;
        let pages: Vec<_> = owners
            .chunks(WRITERS_PER_PAGE)
            .enumerate()
            .map(|(ordinal, entries)| OriginalWriterInventoryPage {
                basis: digest,
                ordinal: ordinal as u32,
                entries: entries.to_vec(),
            })
            .collect();
        record.pages = pages
            .iter()
            .map(OriginalWriterInventoryPage::digest)
            .collect::<Result<_>>()?;
        record.validate_pages(&pages)?;
        Ok((record, pages))
    }
    /// Original immutable basis; never a fresh settlement assertion.
    #[must_use]
    pub const fn basis(&self) -> &OriginalWriterInventoryBasis {
        &self.basis
    }
    /// Complete original catalog scope observations.
    #[must_use]
    pub fn catalogs(&self) -> &[OriginalCatalogWitness] {
        &self.catalogs
    }
    /// Complete original ownership count, including repeated epochs of one Cell.
    #[must_use]
    pub const fn owner_count(&self) -> usize {
        self.count
    }
    /// Complete ordered original page identities.
    #[must_use]
    pub fn pages(&self) -> &[Digest] {
        &self.pages
    }
    /// Immutable complete record identity.
    pub fn digest(&self) -> Result<Digest> {
        Ok(Digest::from_bytes(
            *blake3::hash(&self.to_bytes()?).as_bytes(),
        ))
    }
    /// Checks every exact page, original boot, scope, ordinal and unique epoch.
    pub fn validate_pages(&self, pages: &[OriginalWriterInventoryPage]) -> Result<()> {
        self.validate()?;
        if pages.len() != self.pages.len() {
            return Err(OperationError::Conflict);
        }
        let basis = self.basis_digest()?;
        let mut after = None;
        let mut previous: Option<&OriginalWriterObservation> = None;
        let mut counts = vec![0_u64; self.catalogs.len()];
        let mut total = 0;
        for (ordinal, (page, expected)) in pages.iter().zip(&self.pages).enumerate() {
            page.validate()?;
            if page.basis != basis
                || page.ordinal as usize != ordinal
                || page.digest()? != *expected
                || page.entries.len() != (self.count - total).min(WRITERS_PER_PAGE)
            {
                return Err(OperationError::Conflict);
            }
            for row in &page.entries {
                let key = validation::key(row);
                if after.is_some_and(|previous| previous >= key)
                    || row
                        .control
                        .owner
                        .as_ref()
                        .is_none_or(|owner| owner.session != self.basis.boot.spec().target.session)
                {
                    return Err(OperationError::Conflict);
                }
                // One traversal observes one incarnation per Cell. Repeated
                // original epochs, including across page boundaries, retain the
                // same target and strictly increasing canonical history.
                if previous.is_some_and(|prior| {
                    prior.target.application() == row.target.application()
                        && prior.target.tenant() == row.target.tenant()
                        && prior.control.cell == row.control.cell
                        && (prior.target != row.target
                            || prior.control.incarnation != row.control.incarnation
                            || prior.control.revision >= row.control.revision
                            || prior.control.progress >= row.control.progress)
                }) {
                    return Err(OperationError::Conflict);
                }
                let index = self
                    .catalogs
                    .binary_search_by_key(
                        &(
                            *row.target.application().as_bytes(),
                            *row.target.tenant().as_bytes(),
                        ),
                        |scope| (*scope.application.as_bytes(), *scope.tenant.as_bytes()),
                    )
                    .map_err(|_| OperationError::Conflict)?;
                counts[index] += 1;
                after = Some(key);
                previous = Some(row);
                total += 1;
            }
        }
        if total != self.count
            || counts
                .iter()
                .zip(&self.catalogs)
                .any(|(count, scope)| *count != scope.owners)
        {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
