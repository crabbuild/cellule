//! Public persisted-tail inventory and expired-owner authority discovery.

use std::sync::Arc;

use bytes::Bytes;
use cellule_ltx::{Db, NodeFrameScope, encode_node_frame};
use cellule_runtime::{
    Error,
    fleet::admission::NodeAdmission,
    follower::{FollowerLaneState, FollowerStore},
    identity::{Digest, NodeId, SessionId},
    ltx::{CellStorageLayout, DiskBudget, Limits},
    node::{
        LogLeaderState, NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain, NodeMode,
    },
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

const NOW: i64 = 1_000_000;

fn advertisement(id: u8, at: i64) -> NodeAdvertisement {
    NodeAdvertisement::sign(
        NodeId::from_bytes([id; 16]),
        SessionId::from_bytes([id; 16]),
        "https://node.internal:8789".into(),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([3; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
        &SigningKey::from_bytes(&[7; 32]),
        1,
        at,
        at + 10_000,
        vec![Digest::from_bytes([6; 32])],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1 << 30,
            free_disk_bytes: 1 << 30,
            follower_free_bytes: if id == 1 { 0 } else { 1 << 30 },
            follower_retained_bytes: 0,
            job_credits: 3,
            log_protocol: 1,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn cold_foreign_tail_and_dead_owner_remain_visible_through_cordon_and_recovery_claim() {
    let limits = Limits::default();
    let source = tempfile::tempdir().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction
                .execute_batch("CREATE TABLE events(value INTEGER); INSERT INTO events VALUES (17)")
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let frame = encode_node_frame(
        NodeFrameScope {
            leader_session: [1; 16],
            log_epoch: 4,
            node_sequence: 1,
            application: [3; 16],
            cell: [4; 32],
            incarnation: [5; 16],
            cell_epoch: 6,
            commit_sequence: 1,
        },
        segment.info().clone(),
        Bytes::from(std::fs::read(segment.path()).unwrap()),
        limits,
    )
    .unwrap()
    .encoded()
    .clone();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("root"),
        [9; 16],
    );
    let directory = NodeDirectory::new(
        layout,
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    );
    let leader = SessionId::from_bytes([1; 16]);
    let member = NodeId::from_bytes([2; 16]);
    let created = directory.create(advertisement(1, NOW), NOW).await.unwrap();
    directory.create(advertisement(2, NOW), NOW).await.unwrap();
    directory
        .recruit_log(&created, 4, 1, 10, NOW + 1)
        .await
        .unwrap();
    let local_root = tempfile::tempdir().unwrap();
    let disk = DiskBudget::new(1 << 30);
    let gate = NodeAdmission::default();
    let store = FollowerStore::open(local_root.path().to_owned(), limits, disk.clone())
        .unwrap()
        .with_node_admission(gate.clone());
    assert_eq!(
        store
            .append(leader, 4, vec![frame.clone()], 0)
            .await
            .unwrap()
            .durable_through,
        1
    );
    gate.cordon().unwrap();
    assert!(matches!(
        store.append(leader, 5, vec![frame.clone()], 0).await,
        Err(Error::CellDraining)
    ));
    drop(store);
    let reopened = FollowerStore::open(local_root.path().to_owned(), limits, disk)
        .unwrap()
        .with_node_admission(gate);
    let local = reopened
        .fleet_lanes_page(None, 128, NOW + 20_000)
        .await
        .unwrap();
    assert_eq!(local.mode(), NodeMode::Cordoned);
    assert_eq!(local.total_lanes(), 1);
    assert_eq!(local.unretired_lanes(), 1);
    assert_eq!(local.entries()[0].leader, leader);
    assert_eq!(local.entries()[0].epoch, 4);
    assert_eq!(local.entries()[0].state, FollowerLaneState::Open);
    let remote = directory
        .follower_logs_page(member, None, 128, NOW + 20_000)
        .await
        .unwrap();
    assert_eq!(remote.total_logs(), 1);
    assert_eq!(remote.entries()[0].leader_state, LogLeaderState::Expired);
    directory
        .create(advertisement(3, NOW + 20_000), NOW + 20_000)
        .await
        .unwrap();
    directory
        .claim_expired(leader, SessionId::from_bytes([3; 16]), NOW + 20_001)
        .await
        .unwrap();
    let recovering = directory
        .follower_logs_page(member, None, 128, NOW + 20_002)
        .await
        .unwrap();
    assert_eq!(recovering.total_logs(), 1);
    assert_eq!(recovering.entries()[0].leader_state, LogLeaderState::Fenced);
    assert!(recovering.entries()[0].log.recovery().is_some());
    // Discovery does not seal, retire, or delete the tail. Ordinary recovery
    // still obtains the same acknowledged frame under its own authority.
    assert_eq!(reopened.seal(leader, 4).await.unwrap().durable_through, 1);
    let retained = reopened.read_tail_page(leader, 4, 1).await.unwrap();
    assert_eq!(retained.frames, vec![frame]);
    assert!(retained.next_sequence.is_none());
    assert!(reopened.retained_bytes() > 0);
    database.close().unwrap();
}
