//! Current native writer observation shared by movement and fleet collection.
use super::*;
use crate::{control::ControlState, fleet::operations::PublishedPosition, identity::IncarnationId};

/// Exact authority position observed through the current native actor's FIFO
/// admission and generation-bound inventory. This is a point observation, not a
/// root pin, origin/prefix proof, physical-boot proof or maintenance permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellServingObservation {
    owner: Owner,
    position: PublishedPosition,
    native: OwnedCellObservation,
}
impl CellServingObservation {
    /// Current canonical writer, including its endpoint.
    #[must_use]
    pub fn owner(&self) -> &Owner {
        &self.owner
    }
    /// Exact selected root and ownership epoch.
    #[must_use]
    pub fn position(&self) -> &PublishedPosition {
        &self.position
    }
    /// Original native generation/contract observed at that position. Advisory
    /// demand fields remain advisory and can change between observations.
    #[must_use]
    pub fn native(&self) -> &OwnedCellObservation {
        &self.native
    }
    /// Compares exact writer/position/generation/contract without interpreting
    /// changing advisory demand or last-use samples as ownership changes.
    #[must_use]
    pub fn same_writer(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.position == other.position
            && self.native.target == other.native.target
            && self.native.generation == other.native.generation
            && self.native.code == other.native.code
            && self.native.schema == other.native.schema
            && self.native.role == other.native.role
    }
}
impl CellRuntime {
    /// Observes an admitted current writer strictly after an original epoch.
    /// Uses the existing FIFO query, authority and actor inventory paths; starts
    /// no acquisition or command. Repeat after origin/prefix I/O and compare
    /// [`CellServingObservation::same_writer`]. The caller authenticates canonical
    /// backend/physical scope and owns the enclosing absolute deadline.
    pub async fn observe_serving(
        &self,
        catalog: &CatalogProof,
        authority: &CellAuthority,
        incarnation: IncarnationId,
        after_epoch: u64,
    ) -> crate::Result<CellServingObservation> {
        self.ensure_running()?;
        let cell = catalog.entry().cell();
        let current = authority.load(cell).await?.ok_or(Error::Fenced)?;
        let value = current.value();
        if value.incarnation != incarnation
            || value.epoch <= after_epoch
            || value.state != ControlState::Serving
            || value
                .owner
                .as_ref()
                .is_none_or(|o| o.session != self.inner.session)
        {
            return Err(Error::Fenced);
        }
        let handle = self
            .local_handle(catalog.clone(), &current)
            .await?
            .ok_or(Error::CellDraining)?;
        handle.query(1, 1, |_| Ok(Vec::new())).await?;
        let selected = authority.load(cell).await?.ok_or(Error::Fenced)?;
        let value = selected.value();
        if value.incarnation != incarnation
            || value.epoch != current.value().epoch
            || value.owner != current.value().owner
            || value.code != current.value().code
            || value.schema != current.value().schema
            || value.state != ControlState::Serving
        {
            return Err(Error::Fenced);
        }
        let position = PublishedPosition {
            incarnation,
            epoch: value.epoch,
            root: value.root.clone().ok_or(Error::Fenced)?,
        };
        let mut cursor = None;
        let native = loop {
            let page = self.fleet_cells_page(cursor, 128).await?;
            if let Some(entry) = page.entries().iter().find(|entry| entry.cell() == cell) {
                break match entry {
                    CellInventoryEntry::Owned(owner)
                        if owner.incarnation == incarnation
                            && owner.code == value.code
                            && owner.schema == value.schema
                            && owner.position.as_ref() == Some(&position) =>
                    {
                        (**owner).clone()
                    }
                    _ => return Err(Error::CellDraining),
                };
            }
            cursor = match page.next() {
                Some(next) => Some(next),
                None => return Err(Error::Fenced),
            };
        };
        // Inventory can suspend too. The original admitted handle must still
        // serve the exact selected writer/contract/root after that read.
        handle.query(1, 1, |_| Ok(Vec::new())).await?;
        let confirmed = authority.load(cell).await?.ok_or(Error::Fenced)?;
        if confirmed.value().incarnation != incarnation
            || confirmed.value().owner != value.owner
            || confirmed.value().epoch != value.epoch
            || confirmed.value().code != value.code
            || confirmed.value().schema != value.schema
            || confirmed.value().state != ControlState::Serving
            || confirmed.value().root.as_ref() != Some(&position.root)
        {
            return Err(Error::Fenced);
        }
        self.ensure_running()?;
        Ok(CellServingObservation {
            owner: value.owner.clone().ok_or(Error::Fenced)?,
            position,
            native,
        })
    }
}
