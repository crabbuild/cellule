use super::*;
use crate::node::bundle::index as catalog_index;

/// The other entries deliberately have no available origin roots. Selection
/// and lookup of this Cell must not open unrelated reconstruction dependencies.
async fn inventory(f: &mut Fixture, cell: &Cell, count: usize) {
    let head = f.node.advertisement().bundle_head().unwrap();
    let mut catalog = load_catalog(&f.layout, head_session(f), head)
        .await
        .unwrap();
    let seed = catalog.bindings[0].clone();
    for number in 1..count {
        let mut row = seed.clone();
        let mut id = [0_u8; 32];
        id[..8].copy_from_slice(&(number as u64).to_le_bytes());
        row.control.cell = CellId::from_bytes(id);
        row.control.bundle_binding.as_mut().unwrap().digest =
            Digest::from_bytes(*blake3::hash(&id).as_bytes());
        catalog.bindings.push(row);
    }
    catalog
        .bindings
        .sort_unstable_by_key(|row| *row.control.bundle_binding.unwrap().digest.as_bytes());
    let prepared = f
        .directory
        .upload_catalog(Some(head), catalog, &[])
        .await
        .unwrap();
    f.node = f
        .directory
        .select_catalog(&f.node, &prepared, NOW)
        .await
        .unwrap();
    assert_eq!(cell.control.value().cell, CellId::from_bytes([4; 32]));
}

fn head_session(f: &Fixture) -> SessionId {
    f.node.advertisement().session()
}

mod bootstrap;
mod checkpoint;
mod cohort;
mod compatibility;
mod copy_on_write;
mod density;
mod inventory;
