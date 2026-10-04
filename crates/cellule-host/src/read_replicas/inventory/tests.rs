use super::*;

#[test]
fn reader_inventory_fingerprint_binds_every_exact_root_field() {
    let root = cellule_runtime::control::RootRef {
        digest: Digest::from_bytes([3; 32]),
        txid: 4,
        checksum: 5,
        commit_sequence: 6,
    }
    .to_ltx(
        CellId::from_bytes([1; 32]),
        cellule_runtime::identity::IncarnationId::from_bytes([2; 16]),
    );
    let fingerprint = |root| {
        let mut hash = blake3::Hasher::new();
        hash_root(&mut hash, root);
        hash.finalize()
    };
    for field in 0..6 {
        let mut changed = root;
        match field {
            0 => changed.cell = [7; 32],
            1 => changed.incarnation = [8; 16],
            2 => changed.digest = [9; 32],
            3 => changed.position.txid += 1,
            4 => changed.position.checksum += 1,
            _ => changed.commit_sequence += 1,
        }
        assert_ne!(fingerprint(root), fingerprint(changed), "field {field}");
    }
}
