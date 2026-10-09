use super::super::tests::FaultStore;
use super::*;
use crate::{
    control::{Owner, RootRef},
    identity::{Digest, SessionId},
};
use cellule_store::Store;
use object_store::ObjectStoreExt;
use object_store::{memory::InMemory, path::Path};
use std::sync::{Arc, atomic::Ordering};

fn input() -> Control {
    Control::initial(
        CellId::from_bytes([1; 32]),
        IncarnationId::from_bytes([2; 16]),
        Owner {
            session: SessionId::from_bytes([3; 16]),
            endpoint: "https://original.example".into(),
        },
        Digest::from_bytes([4; 32]),
        1,
    )
    .unwrap()
}
fn successor(input: &Control) -> Control {
    input
        .takeover(Owner {
            session: SessionId::from_bytes([5; 16]),
            endpoint: "https://successor.example".into(),
        })
        .unwrap()
}
fn authority(store: Arc<dyn object_store::ObjectStore>) -> CellAuthority {
    CellAuthority::new(CellStorageLayout::new(
        Store::new(store),
        Path::from("acquisition-test"),
        [7; 16],
    ))
}
fn record(input: Control) -> CellAcquisitionRecord {
    CellAcquisitionRecord {
        materialized: successor(&input),
        input,
    }
}
#[test]
fn codec_preserves_exact_rootless_and_idle_inputs() {
    let mut published = input();
    published.state = ControlState::Serving;
    published.root = Some(RootRef {
        digest: Digest::from_bytes([6; 32]),
        txid: 1,
        checksum: cellule_ltx::types::CHECKSUM_FLAG | 7,
        commit_sequence: 8,
    });
    let idle = published.release().unwrap();
    for input in [input(), published, idle] {
        let original = record(input);
        let body = original.encode().unwrap();
        assert_eq!(CellAcquisitionRecord::decode(&body).unwrap(), original);
        for length in 0..body.len() {
            assert!(CellAcquisitionRecord::decode(&body[..length]).is_err());
        }
        let mut changed = body.clone();
        changed.push(0);
        assert!(CellAcquisitionRecord::decode(&changed).is_err());
        let mut changed = body;
        changed[0] ^= 1;
        assert!(CellAcquisitionRecord::decode(&changed).is_err());
    }
    assert!(CellAcquisitionRecord::decode(&vec![0; MAX_ACQUISITION_BYTES as usize + 1]).is_err());
}
#[test]
fn codec_refuses_changed_root_scope_owner_and_claim_position() {
    let original = record(input());
    for change in 0..5 {
        let mut changed = original.clone();
        match change {
            0 => changed.materialized.cell = CellId::from_bytes([8; 32]),
            1 => changed.materialized.epoch += 1,
            2 => {
                changed.materialized.owner.as_mut().unwrap().session =
                    original.input.owner.as_ref().unwrap().session
            }
            3 => changed.materialized.code = Digest::from_bytes([9; 32]),
            _ => {
                changed.materialized.root = Some(RootRef {
                    digest: Digest::from_bytes([6; 32]),
                    txid: 1,
                    checksum: cellule_ltx::types::CHECKSUM_FLAG | 7,
                    commit_sequence: 8,
                })
            }
        }
        assert!(changed.encode().is_err());
    }
}
#[tokio::test]
async fn immutable_metadata_reconstructs_and_never_invents_serving() {
    let store = Arc::new(InMemory::new());
    let authority = authority(store.clone());
    let input = input();
    let materialized = successor(&input);
    authority
        .retain_acquisition(&input, &materialized)
        .await
        .unwrap();
    authority
        .retain_acquisition(&input, &materialized)
        .await
        .unwrap();
    let independent = super::tests::authority(store);
    let retained = independent
        .acquisition_record(input.cell, input.incarnation, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.input(), &input);
    assert_eq!(retained.materialized(), &materialized);
    assert_eq!(retained.materialized().state, ControlState::Recovering);
    assert!(independent.load(input.cell).await.unwrap().is_none());
    let mut different = input.clone();
    different.revision += 1;
    different.progress += 1;
    assert!(
        authority
            .retain_acquisition(&different, &successor(&different))
            .await
            .is_err()
    );
    assert_eq!(
        independent
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap(),
        Some(retained)
    );
    assert!(
        independent
            .acquisition_record(input.cell, input.incarnation, 3)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        independent
            .acquisition_record(input.cell, input.incarnation, 1)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn original_publication_failure_and_lost_reply_preserve_exact_history() {
    for lost in [false, true] {
        let store = Arc::new(FaultStore::default());
        let authority = authority(store.clone());
        let input = input();
        let materialized = successor(&input);
        store
            .fault
            .store(if lost { 2 } else { 1 }, Ordering::SeqCst);
        let result = authority.retain_acquisition(&input, &materialized).await;
        if lost {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::Storage(_))));
            assert!(
                authority
                    .acquisition_record(input.cell, input.incarnation, 2)
                    .await
                    .unwrap()
                    .is_none()
            );
            authority
                .retain_acquisition(&input, &materialized)
                .await
                .unwrap();
        }
        assert_eq!(
            authority
                .acquisition_record(input.cell, input.incarnation, 2)
                .await
                .unwrap(),
            Some(record(input))
        );
    }
}
#[tokio::test]
async fn failed_original_read_is_not_absence() {
    let store = Arc::new(FaultStore::default());
    let authority = authority(store.clone());
    let input = input();
    let materialized = successor(&input);
    authority
        .retain_acquisition(&input, &materialized)
        .await
        .unwrap();
    store.fault.store(6, Ordering::SeqCst);
    assert!(matches!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await,
        Err(Error::Storage(_)),
    ));
    assert_eq!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap(),
        Some(record(input)),
    );
}

#[tokio::test]
async fn cancellation_before_publication_cannot_invent_a_record() {
    let store = Arc::new(FaultStore::default());
    let authority = authority(store.clone());
    let input = input();
    let materialized = successor(&input);
    store.fault.store(3, Ordering::SeqCst);
    let original = input.clone();
    let next = materialized.clone();
    let publisher = authority.clone();
    let blocked = tokio::spawn(async move { publisher.retain_acquisition(&original, &next).await });
    store.entered.notified().await;
    blocked.abort();
    assert!(blocked.await.unwrap_err().is_cancelled());
    assert!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap()
            .is_none()
    );
    authority
        .retain_acquisition(&input, &materialized)
        .await
        .unwrap();
    assert_eq!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap(),
        Some(record(input)),
    );
}

#[tokio::test]
async fn competing_identical_publications_adopt_one_original_record() {
    let store = Arc::new(FaultStore::default());
    let first = authority(store.clone());
    let second = authority(store.clone());
    let input = input();
    let materialized = successor(&input);
    store.fault.store(3, Ordering::SeqCst);
    let original = input.clone();
    let next = materialized.clone();
    let original_authority = first.clone();
    let blocked = tokio::spawn(async move {
        original_authority
            .retain_acquisition(&original, &next)
            .await
    });
    store.entered.notified().await;
    second
        .retain_acquisition(&input, &materialized)
        .await
        .unwrap();
    store.resume.notify_one();
    blocked.await.unwrap().unwrap();
    assert_eq!(
        first
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap(),
        Some(record(input))
    );
}
#[tokio::test]
async fn missing_foreign_and_corrupt_history_is_not_successful_acquisition() {
    let backend = Arc::new(InMemory::new());
    let authority = authority(backend.clone());
    let input = input();
    let original = record(input.clone());
    let path = authority.layout().acquisition_record_path(
        input.cell.as_bytes(),
        input.incarnation.as_bytes(),
        2,
    );
    assert!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .unwrap()
            .is_none()
    );
    let mut foreign = input.clone();
    foreign.cell = CellId::from_bytes([9; 32]);
    backend
        .put(&path, Bytes::from(record(foreign).encode().unwrap()).into())
        .await
        .unwrap();
    assert!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .is_err()
    );
    let mut corrupt = original.encode().unwrap();
    corrupt.push(0);
    backend
        .put(&path, Bytes::from(corrupt).into())
        .await
        .unwrap();
    assert!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .is_err()
    );
    backend
        .put(
            &path,
            Bytes::from(vec![0; MAX_ACQUISITION_BYTES as usize + 1]).into(),
        )
        .await
        .unwrap();
    assert!(
        authority
            .acquisition_record(input.cell, input.incarnation, 2)
            .await
            .is_err()
    );
}

fn suffix() -> crate::recovery::manifest::PinnedRecoveryCell {
    crate::recovery::manifest::PinnedRecoveryCell {
        application: crate::identity::ApplicationId::from_bytes([7; 16]),
        cell: CellId::from_bytes([1; 32]),
        incarnation: IncarnationId::from_bytes([2; 16]),
        cell_epoch: 1,
        recovery: crate::control::RecoveryOverlayRef {
            leader_session: SessionId::from_bytes([3; 16]),
            log_epoch: 1,
            manifest_digest: Digest::from_bytes([9; 32]),
            first_node_sequence: 1,
            last_node_sequence: 2,
            predecessor: RootRef {
                digest: Digest::from_bytes([6; 32]),
                txid: 1,
                checksum: cellule_ltx::types::CHECKSUM_FLAG,
                commit_sequence: 1,
            },
            final_txid: 2,
            final_checksum: cellule_ltx::types::CHECKSUM_FLAG | 1,
            final_commit_sequence: 2,
        },
    }
}
// These shape fixtures exercise refusal before any origin graph can succeed.
async fn original_suffix_scope(
    authority: &CellAuthority,
    required: &crate::recovery::manifest::PinnedRecoveryCell,
    selected: &Control,
) {
    let mut original = input();
    original.state = ControlState::Serving;
    original.root = Some(required.recovery.predecessor.clone());
    let original = original.attach_recovery(required.recovery.clone()).unwrap();
    authority.retain_owner(&original).await.unwrap();
    authority
        .layout
        .store()
        .create_strict(
            &authority.layout.control_path(required.cell.as_bytes()),
            Bytes::from(selected.encode().unwrap()),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn recovered_prefix_requires_original_acquisition_and_preserves_read_errors() {
    let store = Arc::new(FaultStore::default());
    let authority = authority(store.clone());
    let required = suffix();
    let root = cellule_ltx::RootRef {
        cell: *required.cell.as_bytes(),
        incarnation: *required.incarnation.as_bytes(),
        digest: [8; 32],
        position: cellule_ltx::Position {
            txid: 2,
            checksum: required.recovery.final_checksum,
        },
        commit_sequence: 2,
    };
    let replica = cellule_ltx::CellReplica::new(
        authority.layout.clone(),
        root.cell,
        root.incarnation,
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    let mut selected = successor(&input());
    selected.state = ControlState::Serving;
    selected.root = Some(RootRef::from_ltx(required.cell, required.incarnation, root).unwrap());
    original_suffix_scope(&authority, &required, &selected).await;
    store.fault.store(6, Ordering::SeqCst);
    assert!(matches!(
        authority
            .verify_recovered_prefix(&required, root, &replica, 0)
            .await,
        Err(Error::Capacity(_))
    ));
    assert_eq!(store.fault.load(Ordering::SeqCst), 6);
    assert!(matches!(
        authority
            .verify_recovered_prefix(&required, root, &replica, 8)
            .await,
        Err(Error::Storage(_))
    ));
    assert!(
        matches!(authority.verify_recovered_prefix(&required, root, &replica, 8).await,
        Err(Error::AcquisitionHistoryIncomplete { cell, incarnation, epoch })
        if cell==required.cell && incarnation==required.incarnation && epoch==2)
    );
}
#[tokio::test]
async fn matching_endpoint_cannot_replace_a_sealed_recovery_input() {
    let authority = authority(Arc::new(InMemory::new()));
    let required = suffix();
    let mut original = input();
    original.state = ControlState::Serving;
    original.root = Some(RootRef {
        digest: Digest::from_bytes([8; 32]),
        txid: required.recovery.final_txid,
        checksum: required.recovery.final_checksum,
        commit_sequence: required.recovery.final_commit_sequence,
    });
    let claimed = successor(&original);
    authority
        .retain_acquisition(&original, &claimed)
        .await
        .unwrap();
    let root = claimed.ltx_root().unwrap();
    let mut selected = claimed.clone();
    selected.state = ControlState::Serving;
    original_suffix_scope(&authority, &required, &selected).await;
    let replica = cellule_ltx::CellReplica::new(
        authority.layout.clone(),
        root.cell,
        root.incarnation,
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        authority
            .verify_recovered_prefix(&required, root, &replica, 8)
            .await,
        Err(Error::Control(
            "canonical acquisition recovery input differs"
        ))
    ));
}

#[tokio::test]
async fn idle_suffix_verification_requires_exact_control_and_does_not_certify_serving() {
    let authority = authority(Arc::new(InMemory::new()));
    let required = suffix();
    let mut selected = successor(&input());
    selected.state = ControlState::Idle;
    selected.owner = None;
    selected.recovery = None;
    selected.root = Some(RootRef {
        digest: Digest::from_bytes([8; 32]),
        txid: required.recovery.final_txid,
        checksum: required.recovery.final_checksum,
        commit_sequence: required.recovery.final_commit_sequence,
    });
    original_suffix_scope(&authority, &required, &selected).await;
    let observed = authority.load(required.cell).await.unwrap().unwrap();
    let root = observed.value().ltx_root().unwrap();
    let replica = cellule_ltx::CellReplica::new(
        authority.layout.clone(),
        root.cell,
        root.incarnation,
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        authority
            .verify_recovered_prefix(&required, root, &replica, 8)
            .await,
        Err(Error::Fenced)
    ));
    assert!(matches!(
        authority
            .verify_recovered_idle_prefix(&required, &observed, &replica, 0)
            .await,
        Err(Error::Capacity(_))
    ));
    assert!(matches!(
        authority
            .verify_recovered_idle_prefix(&required, &observed, &replica, 8)
            .await,
        Err(Error::AcquisitionHistoryIncomplete { epoch: 2, .. })
    ));
    // Even an unchanged root at a newer revision cannot replace the complete
    // originally selected Idle control. No origin read or acquisition follows.
    selected.revision += 1;
    let path = authority.layout.control_path(required.cell.as_bytes());
    authority.layout.store().delete(&path).await.unwrap();
    authority
        .layout
        .store()
        .create_strict(&path, Bytes::from(selected.encode().unwrap()))
        .await
        .unwrap();
    assert!(matches!(
        authority
            .verify_recovered_idle_prefix(&required, &observed, &replica, 8)
            .await,
        Err(Error::Fenced)
    ));
    let latest = authority.load(required.cell).await.unwrap().unwrap();
    assert_eq!(latest.value(), &selected);
}
