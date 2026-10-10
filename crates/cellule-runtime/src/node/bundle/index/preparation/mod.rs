//! Admitted immutable shard bytes used only to construct publication proposals.
use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger, ResourceReservation};

/// Runtime-owned scratch retained between ordered publication preparations.
///
/// This caches encoded catalog shards, never authority or recovery proofs. The
/// canonical header is read on every preparation; selection and recovery keep
/// their own origin checks. The runtime supplies this value to its publication
/// authority after reserving its complete bounded lifetime on the node ledger.
pub struct BundlePreparation {
    scope: Option<Scope>,
    entries: Box<[Option<Entry>]>,
    bytes: usize,
    evict: usize,
    _reservation: ResourceReservation,
}

#[derive(PartialEq, Eq)]
struct Scope {
    store: u64,
    path: blake3::Hash,
    session: SessionId,
    epoch: u64,
}

struct Entry {
    extent: Locator,
    body: Bytes,
}

pub(crate) const PREPARATION_BYTES: usize = 2 << 20;
// Include the fixed index and conservative per-allocation bookkeeping within
// the reservation. Copies own exactly one shard, never a slice of a full bundle.
const BODY_BYTES: usize = PREPARATION_BYTES
    - std::mem::size_of::<BundlePreparation>()
    - SHARDS * (std::mem::size_of::<Option<Entry>>() + 64);

impl BundlePreparation {
    pub(crate) fn reserve(resources: &ResourceLedger) -> Result<Self> {
        let reservation =
            resources.try_reserve(ResourceCost::zero().with_retained_bytes(PREPARATION_BYTES))?;
        Ok(Self {
            scope: None,
            entries: std::iter::repeat_with(|| None)
                .take(SHARDS)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bytes: 0,
            evict: 0,
            _reservation: reservation,
        })
    }

    pub(in crate::node::bundle) fn bind(
        &mut self,
        layout: &cellule_ltx::CellStorageLayout,
        session: SessionId,
        epoch: u64,
    ) {
        let scope = Scope {
            store: layout.immutable_cache_identity(),
            path: blake3::hash(layout.node_path(session.as_bytes()).as_ref().as_bytes()),
            session,
            epoch,
        };
        if self.scope.as_ref() != Some(&scope) {
            for entry in &mut self.entries {
                *entry = None;
            }
            self.bytes = 0;
            self.evict = 0;
            self.scope = Some(scope);
        }
    }

    pub(super) fn get(&self, id: u8, extent: &Locator) -> Option<Bytes> {
        self.entries[usize::from(id)]
            .as_ref()
            .filter(|entry| &entry.extent == extent)
            .map(|entry| entry.body.clone())
    }

    pub(super) fn insert(&mut self, id: u8, extent: &Locator, body: &Bytes) {
        if body.len() > BODY_BYTES || self.scope.is_none() {
            return;
        }
        if let Some(previous) = self.entries[usize::from(id)].take() {
            self.bytes -= previous.body.len();
        }
        while self.bytes + body.len() > BODY_BYTES {
            if let Some(previous) = self.entries[self.evict].take() {
                self.bytes -= previous.body.len();
            }
            self.evict = (self.evict + 1) % SHARDS;
        }
        self.bytes += body.len();
        self.entries[usize::from(id)] = Some(Entry {
            extent: extent.clone(),
            body: Bytes::copy_from_slice(body),
        });
    }

    pub(in crate::node::bundle) fn remember(
        &mut self,
        layout: &cellule_ltx::CellStorageLayout,
        prepared: &PreparedNodeBundle,
    ) -> Result<()> {
        self.bind(layout, prepared.catalog.session, prepared.catalog.epoch);
        let root = codec::decode(&prepared.body[..HEADER_BYTES])?;
        for (id, shard) in root.shards.iter().enumerate() {
            let Some(shard) = shard.as_ref().filter(|shard| shard.extent.object.is_none()) else {
                continue;
            };
            // The encoder already self-verified the complete proposal. Retain
            // only exact authenticated local extents under their final address.
            let body = extent_bytes(&prepared.body, &shard.extent)?;
            let mut extent = shard.extent.clone();
            extent.object = Some(prepared.head.digest);
            self.insert(id as u8, &extent, &body);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
