//! Copy-on-write authenticated catalog shards inside the selected bundle object.
//!
//! The node head authenticates a fixed header. That header authenticates each
//! shard extent and every new native extent. Unchanged shards keep their exact
//! object/range/digest, so neither publication nor a point lookup walks a chain.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

mod codec;
mod dense;
mod history;
mod io;
#[cfg(test)]
mod tests;
pub(super) use history::validate_deferred;
#[cfg(test)]
pub(super) use io::load;
#[cfg(test)]
pub(super) use tests::encode_inline;

#[cfg(test)]
pub(super) fn history_extent(catalog: &Catalog, pin: Digest) -> Option<Locator> {
    catalog
        .index
        .as_ref()?
        .histories
        .get(pin.as_bytes())
        .map(|history| history.extent.clone())
}
pub(super) use io::{ensure_drained, load_cells};

pub(super) const HEADER_BYTES: usize = 32 << 10;
const SHARDS: usize = 256;
const MAGIC: &[u8; 8] = b"\0\0\0\x04CNB2";
const DENSE_MAGIC: &[u8; 8] = b"\0\0\0\x04CNB3";
pub(super) type CellKey = ([u8; 16], [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Shard {
    extent: Locator,
    bindings: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Root {
    detached: bool,
    session: SessionId,
    epoch: u64,
    predecessor: Option<Digest>,
    selected_through: u64,
    object_bytes: u64,
    shards: Vec<Option<Shard>>,
    frames: Vec<Locator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LoadedIndex {
    root: Root,
    loaded: BTreeMap<u8, Vec<Binding>>,
    histories: BTreeMap<[u8; 32], history::History>,
}

/// The shard key includes the application, so equal Cell IDs in applications
/// never alias. Epoch/incarnation remain inside the authenticated binding rows.
pub(super) fn shard(application: &[u8; 16], cell: &[u8; 32]) -> u8 {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.bundle-catalog-shard.v2\0");
    hash.update(application);
    hash.update(cell);
    hash.finalize().as_bytes()[0]
}

fn binding_shard(binding: &Binding) -> u8 {
    shard(
        binding.application.as_bytes(),
        binding.control.cell.as_bytes(),
    )
}

fn grouped(catalog: &Catalog) -> BTreeMap<u8, Vec<Binding>> {
    let mut groups = BTreeMap::<u8, Vec<Binding>>::new();
    for binding in &catalog.bindings {
        groups
            .entry(binding_shard(binding))
            .or_default()
            .push(binding.clone());
    }
    groups
}

fn leaf(catalog: &Catalog, bindings: Vec<Binding>) -> Catalog {
    Catalog {
        session: catalog.session,
        epoch: catalog.epoch,
        predecessor: None,
        selected_through: catalog.selected_through,
        bindings,
        index: None,
    }
}

pub(super) fn encode(
    catalog: &mut Catalog,
    frames: &[cellule_ltx::VerifiedNodeFrame],
) -> Result<(Bytes, Digest)> {
    dense::encode(catalog, frames)
}

fn extent_bytes(body: &Bytes, locator: &Locator) -> Result<Bytes> {
    let end = locator
        .offset
        .checked_add(locator.bytes)
        .ok_or(Error::Node("bundle extent overflow"))?;
    let bytes = body
        .get(locator.offset as usize..end as usize)
        .ok_or(Error::Node("bundle extent is truncated"))?;
    if *blake3::hash(bytes).as_bytes() != *locator.frame_digest.as_bytes() {
        return Err(Error::Node("bundle indexed extent digest differs"));
    }
    Ok(Bytes::copy_from_slice(bytes))
}

#[cfg(test)]
fn decode_leaf(body: Bytes, shard: &Shard, id: u8, root: &Root) -> Result<Catalog> {
    decode_leaf_with_histories(body, shard, id, root).map(|(catalog, _)| catalog)
}

fn decode_leaf_with_histories(
    body: Bytes,
    shard: &Shard,
    id: u8,
    root: &Root,
) -> Result<(Catalog, BTreeMap<[u8; 32], history::History>)> {
    if body.len() as u64 != shard.extent.bytes
        || *blake3::hash(&body).as_bytes() != *shard.extent.frame_digest.as_bytes()
    {
        return Err(Error::Node("bundle catalog shard digest differs"));
    }
    let (catalog, histories) = history::decode_leaf(&body)?;
    if !root.detached && body.get(..8) == Some(b"\0\0\0\x04CBL3".as_slice()) {
        return Err(Error::Node("inline bundle index contains detached history"));
    }
    if catalog.session != root.session
        || catalog.epoch != root.epoch
        || catalog.selected_through > root.selected_through
        || catalog.predecessor.is_some()
        || catalog.bindings.len() != shard.bindings
        || catalog
            .bindings
            .iter()
            .any(|binding| binding_shard(binding) != id)
    {
        return Err(Error::Node("bundle catalog shard scope differs"));
    }
    Ok((catalog, histories))
}

pub(super) fn body_digest(body: &Bytes) -> Result<Digest> {
    if matches!(body.get(..8), Some(magic) if magic == MAGIC || magic == DENSE_MAGIC) {
        let header = body
            .get(..HEADER_BYTES)
            .ok_or(Error::Node("truncated bundle index header"))?;
        let root = codec::decode(header)?;
        if root.object_bytes != body.len() as u64 {
            return Err(Error::Node("bundle proposal object length differs"));
        }
        Ok(Digest::from_bytes(*blake3::hash(header).as_bytes()))
    } else {
        Ok(Digest::from_bytes(*blake3::hash(body).as_bytes()))
    }
}
