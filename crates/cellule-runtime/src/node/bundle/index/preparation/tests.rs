use super::*;
use cellule_ltx::CellStorageLayout;
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

fn layout() -> CellStorageLayout {
    CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("test"),
        [9; 16],
    )
}

fn extent(body: &[u8]) -> Locator {
    Locator {
        object: Some(Digest::from_bytes([1; 32])),
        offset: HEADER_BYTES as u64,
        bytes: body.len() as u64,
        frame_digest: Digest::from_bytes(*blake3::hash(body).as_bytes()),
    }
}

#[test]
fn preparation_retention_is_admitted_bounded_copied_and_released() {
    let ledger = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(PREPARATION_BYTES));
    let mut cache = BundlePreparation::reserve(&ledger).unwrap();
    assert!(BundlePreparation::reserve(&ledger).is_err());
    cache.bind(&layout(), SessionId::from_bytes([1; 16]), 1);
    let source = Bytes::from(vec![7; MAX_BUNDLE_BYTES as usize]);
    let shard = source.slice(..(32 << 10));
    for id in 0..SHARDS {
        cache.insert(id as u8, &extent(&shard), &shard);
        assert!(cache.bytes <= BODY_BYTES);
    }
    assert!(
        cache.entries.iter().any(Option::is_none),
        "the byte limit must evict"
    );
    let retained = cache.get(255, &extent(&shard)).unwrap();
    assert_eq!(retained, shard);
    assert_ne!(
        retained.as_ptr(),
        shard.as_ptr(),
        "a shard must not retain a 4 MiB parent"
    );
    drop(retained);
    assert_eq!(
        ledger.snapshot().unwrap().used.retained_bytes(),
        PREPARATION_BYTES
    );
    drop(cache);
    assert_eq!(ledger.snapshot().unwrap().used.retained_bytes(), 0);
}

#[test]
fn preparation_reuse_requires_exact_extent_and_origin_scope() {
    let ledger = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(PREPARATION_BYTES));
    let mut cache = BundlePreparation::reserve(&ledger).unwrap();
    let original = layout();
    let session = SessionId::from_bytes([1; 16]);
    let body = Bytes::from_static(b"immutable shard");
    let reference = extent(&body);
    cache.bind(&original, session, 1);
    cache.insert(7, &reference, &body);
    cache.bind(&original.clone(), session, 1);
    assert_eq!(cache.get(7, &reference), Some(body.clone()));
    for field in 0..4 {
        let mut changed = reference.clone();
        match field {
            0 => changed.object = Some(Digest::from_bytes([2; 32])),
            1 => changed.offset += 1,
            2 => changed.bytes += 1,
            _ => changed.frame_digest = Digest::from_bytes([2; 32]),
        }
        assert!(cache.get(7, &changed).is_none());
    }
    assert!(cache.get(8, &reference).is_none());
    for (layout, session, epoch) in [
        (layout(), session, 1),
        (original.clone(), SessionId::from_bytes([2; 16]), 1),
        (original.clone(), session, 2),
        (
            CellStorageLayout::new(original.store().clone(), Path::from("other"), [9; 16]),
            session,
            1,
        ),
    ] {
        cache.bind(&original, SessionId::from_bytes([1; 16]), 1);
        cache.insert(7, &reference, &body);
        cache.bind(&layout, session, epoch);
        assert!(cache.get(7, &reference).is_none());
        assert_eq!(cache.bytes, 0);
    }
}
