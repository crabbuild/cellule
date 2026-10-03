use super::super::tests::FaultStore;
use super::*;
use cellule_store::Store;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::{Arc, atomic::Ordering};

fn root(digest: u8, sequence: u64) -> RootRef {
    RootRef {
        cell: [1; 32],
        incarnation: [2; 16],
        digest: [digest; 32],
        position: cellule_ltx::Position {
            txid: sequence + 1,
            checksum: cellule_ltx::types::CHECKSUM_FLAG | sequence,
        },
        commit_sequence: sequence,
    }
}
fn authority(store: Arc<dyn object_store::ObjectStore>) -> CellAuthority {
    CellAuthority::new(CellStorageLayout::new(
        Store::new(store),
        Path::from("root-lineage"),
        [3; 16],
    ))
}
#[test]
fn codec_bounds_canonical_links_and_all_corruption() {
    let record = CellRootLineage {
        root: root(80, 80),
        predecessors: (1..=64).map(|byte| root(byte, u64::from(byte))).collect(),
    };
    let body = record.encode().unwrap();
    assert!(body.len() < MAX_LINEAGE_BYTES as usize);
    assert_eq!(CellRootLineage::decode(&body).unwrap(), record);
    for length in 0..body.len() {
        assert!(CellRootLineage::decode(&body[..length]).is_err());
    }
    for index in 0..body.len() {
        let mut corrupt = body.clone();
        corrupt[index] ^= 1;
        assert!(CellRootLineage::decode(&corrupt).is_err());
    }
    let mut extra = body;
    extra.push(0);
    assert!(CellRootLineage::decode(&extra).is_err());
    let mut record = record;
    record.predecessors.push(root(65, 65));
    assert!(record.encode().is_err());
}
#[test]
fn codec_refuses_scope_position_duplicates_and_order_changes() {
    for change in 0..6 {
        let mut record = CellRootLineage {
            root: root(8, 8),
            predecessors: vec![root(1, 1), root(2, 2)],
        };
        match change {
            0 => record.predecessors[0].cell = [9; 32],
            1 => record.predecessors[0].incarnation = [9; 16],
            2 => record.predecessors[0] = record.root,
            3 => record.predecessors[0] = root(1, 9),
            4 => record.predecessors[1] = record.predecessors[0],
            _ => record.predecessors.reverse(),
        }
        assert!(record.encode().is_err());
    }
    let mut parent = root(1, 8);
    parent.digest = [1; 32];
    let record = CellRootLineage {
        root: root(8, 8),
        predecessors: vec![parent],
    };
    record.encode().unwrap(); // Representation-only compaction preserves position.
    let mut changed = record;
    changed.predecessors[0].position.checksum ^= 1;
    assert!(changed.encode().is_err());
}
#[tokio::test]
async fn links_accumulate_and_reconstruct_without_authority() {
    let backend = Arc::new(InMemory::new());
    let first = authority(backend.clone());
    let child = root(8, 8);
    first
        .retain_verified_link(child, Some(root(2, 2)))
        .await
        .unwrap();
    first
        .retain_verified_link(child, Some(root(1, 1)))
        .await
        .unwrap();
    first
        .retain_verified_link(child, Some(root(2, 2)))
        .await
        .unwrap();
    let second = authority(backend);
    let record = second.root_lineage(child).await.unwrap().unwrap();
    assert_eq!(record.predecessors(), &[root(1, 1), root(2, 2)]);
    assert!(
        second
            .load(CellId::from_bytes(child.cell))
            .await
            .unwrap()
            .is_none()
    );
    let mut different = root(2, 2);
    different.commit_sequence += 1;
    assert!(
        second
            .retain_verified_link(child, Some(different))
            .await
            .is_err()
    );
    assert_eq!(second.root_lineage(child).await.unwrap().unwrap(), record);
}
#[tokio::test]
async fn failed_and_lost_link_replies_do_not_invent_success() {
    for fault in [1, 2] {
        let store = Arc::new(FaultStore::default());
        let authority = authority(store.clone());
        store.fault.store(fault, Ordering::SeqCst);
        let result = authority
            .retain_verified_link(root(8, 8), Some(root(1, 1)))
            .await;
        if fault == 1 {
            assert!(matches!(result, Err(Error::Storage(_))));
            assert!(authority.root_lineage(root(8, 8)).await.unwrap().is_none());
        } else {
            result.unwrap();
        }
    }
}
#[tokio::test]
async fn competing_distinct_links_cannot_erase_the_original() {
    let store = Arc::new(FaultStore::default());
    let first = authority(store.clone());
    let second = authority(store.clone());
    store.fault.store(3, Ordering::SeqCst);
    let task = tokio::spawn(async move {
        first
            .retain_verified_link(root(8, 8), Some(root(1, 1)))
            .await
    });
    store.entered.notified().await;
    second
        .retain_verified_link(root(8, 8), Some(root(2, 2)))
        .await
        .unwrap();
    store.resume.notify_one();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::Storage(StorageError::StateConflict { .. }))
    ));
    second
        .retain_verified_link(root(8, 8), Some(root(1, 1)))
        .await
        .unwrap();
    assert_eq!(
        second
            .root_lineage(root(8, 8))
            .await
            .unwrap()
            .unwrap()
            .predecessors(),
        &[root(1, 1), root(2, 2)]
    );
}
#[tokio::test]
async fn publication_confirms_native_links_before_its_root_cas() {
    for fault in [1, 2] {
        let store = Arc::new(FaultStore::default());
        let authority = authority(store.clone());
        let directory = tempfile::tempdir().unwrap();
        let mut database = cellule_ltx::Db::open(
            &directory.path().join("publication.sqlite"),
            cellule_ltx::Limits::default(),
        )
        .unwrap();
        database
            .transaction(|transaction| {
                transaction.execute_batch(
                    "CREATE TABLE values_seen(value INTEGER); INSERT INTO values_seen VALUES(7)",
                )?;
                Ok(())
            })
            .unwrap();
        let cell = CellId::from_bytes([1; 32]);
        let incarnation = IncarnationId::from_bytes([2; 16]);
        let initial = Control::initial(
            cell,
            incarnation,
            crate::control::Owner {
                session: crate::identity::SessionId::from_bytes([4; 16]),
                endpoint: "https://publisher.internal".into(),
            },
            crate::identity::Digest::from_bytes([5; 32]),
            1,
        )
        .unwrap();
        authority
            .layout
            .store()
            .create_strict(
                &authority.layout.control_path(cell.as_bytes()),
                Bytes::from(initial.encode().unwrap()),
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            authority.layout.clone(),
            *cell.as_bytes(),
            *incarnation.as_bytes(),
            cellule_ltx::Limits::default(),
        )
        .unwrap();
        let prepared = replica
            .prepare(None, &database.capture().unwrap(), 1, 1)
            .await
            .unwrap();
        let observed = authority.load(cell).await.unwrap().unwrap();
        let mut publisher = crate::publication::CellPublisher::new(
            replica,
            authority.clone(),
            observed,
            directory.path().to_owned(),
        );
        store.fault.store(fault, Ordering::SeqCst);
        let result = publisher.publish_prepared(&prepared, None).await;
        if fault == 1 {
            assert!(matches!(result, Err(Error::Storage(_))));
            assert_eq!(
                authority.load(cell).await.unwrap().unwrap().value(),
                &initial
            );
            assert!(
                authority
                    .root_lineage(prepared.root())
                    .await
                    .unwrap()
                    .is_none()
            );
            publisher.publish_prepared(&prepared, None).await.unwrap();
        } else {
            result.unwrap();
        }
        assert_eq!(
            authority
                .load(cell)
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root(),
            Some(prepared.root())
        );
        assert!(
            authority
                .root_lineage(prepared.root())
                .await
                .unwrap()
                .unwrap()
                .predecessors()
                .is_empty()
        );
        database.close().unwrap();
    }
}

#[tokio::test]
async fn corrupt_or_foreign_metadata_is_not_absence() {
    let backend = Arc::new(InMemory::new());
    let authority = authority(backend.clone());
    let child = root(8, 8);
    let path = authority
        .layout
        .root_lineage_path(&child.cell, &child.incarnation, &child.digest);
    assert!(authority.root_lineage(child).await.unwrap().is_none());
    backend
        .put(&path, Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();
    assert!(authority.root_lineage(child).await.is_err());
    let foreign = CellRootLineage {
        root: root(9, 9),
        predecessors: vec![],
    };
    backend
        .put(&path, Bytes::from(foreign.encode().unwrap()).into())
        .await
        .unwrap();
    assert!(authority.root_lineage(child).await.is_err());
}
