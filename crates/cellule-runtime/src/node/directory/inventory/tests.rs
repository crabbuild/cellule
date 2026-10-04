use super::*;

use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

const NOW: i64 = 1_000_000;

fn directory() -> NodeDirectory {
    NodeDirectory::new(
        CellStorageLayout::new(
            cellule_store::Store::new(Arc::new(InMemory::new())),
            Path::from("root"),
            [9; 16],
        ),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    )
}

fn member() -> NodeId {
    NodeId::from_bytes([9; 16])
}

fn advertisement(id: u8, at: i64) -> NodeAdvertisement {
    let session = SessionId::from_bytes([id; 16]);
    NodeAdvertisement::sign(
        NodeId::from_bytes([id; 16]),
        session,
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
            free_memory_bytes: 1_000,
            free_disk_bytes: 2_000,
            follower_free_bytes: if id == 9 { 2_000 } else { 0 },
            follower_retained_bytes: 0,
            job_credits: 3,
            log_protocol: NODE_LOG_PROTOCOL_VERSION,
        },
    )
    .unwrap()
}

async fn enroll(directory: &NodeDirectory, id: u8, at: i64) -> VersionedNodeAdvertisement {
    let created = directory.create(advertisement(id, at), at).await.unwrap();
    // Inventory fixtures pin one retained ensemble independently of how live
    // membership subsequently changes. Recruitment has its own public tests.
    let mut next = created.advertisement().clone();
    next.generation += 1;
    next.log = Some(NodeLogStatus::open(next.node, 4, vec![member()]).unwrap());
    directory
        .update_advertisement(&created, next, at + 1)
        .await
        .unwrap()
}

#[tokio::test]
async fn discovery_pages_include_inactive_expired_and_fenced_epochs() {
    let directory = directory();
    directory.create(advertisement(9, NOW), NOW).await.unwrap();
    for id in [3, 1, 2] {
        enroll(&directory, id, NOW).await;
    }
    let first = directory
        .follower_logs_page(member(), None, 1, NOW + 2)
        .await
        .unwrap();
    assert_eq!(first.total_logs(), 3);
    assert_eq!(first.entries()[0].leader, SessionId::from_bytes([1; 16]));
    assert_eq!(first.entries()[0].leader_state, LogLeaderState::Live);
    assert!(!first.entries()[0].log.active());
    let cursor = LogInventoryCursor::from_bytes(&first.next().unwrap().to_bytes()).unwrap();
    let expired = directory
        .follower_logs_page(member(), Some(cursor), 128, NOW + 20_000)
        .await
        .unwrap();
    assert_eq!(expired.topology(), first.topology());
    assert_eq!(expired.entries().len(), 2);
    assert!(
        expired
            .entries()
            .iter()
            .all(|row| row.leader_state == LogLeaderState::Expired)
    );
    assert!(expired.next().is_none());
    // Expiry alone never removes obligations. Stale fencing retains their logs.
    assert_eq!(directory.collect_stale(NOW + 340_001, 16).await.unwrap(), 4);
    let fenced = directory
        .follower_logs_page(member(), None, 128, NOW + 340_002)
        .await
        .unwrap();
    assert_eq!(fenced.total_logs(), 3);
    assert!(
        fenced
            .entries()
            .iter()
            .all(|row| row.leader_state == LogLeaderState::Fenced)
    );
    assert!(
        fenced
            .entries()
            .iter()
            .all(|row| row.log.members().contains(&member()))
    );
}

#[tokio::test]
async fn epoch_change_member_change_or_directory_reconstruction_restarts_pagination() {
    let directory = directory();
    directory.create(advertisement(9, NOW), NOW).await.unwrap();
    let first_owner = enroll(&directory, 1, NOW).await;
    enroll(&directory, 2, NOW).await;
    let first = directory
        .follower_logs_page(member(), None, 1, NOW + 2)
        .await
        .unwrap();
    let cursor = first.next().unwrap();
    assert!(
        directory
            .follower_logs_page(NodeId::from_bytes([8; 16]), Some(cursor), 1, NOW + 3)
            .await
            .is_err()
    );
    let reconstructed = NodeDirectory::new(
        directory.layout.clone(),
        directory.fleet,
        directory.image,
        directory.release,
    );
    assert!(
        reconstructed
            .follower_logs_page(member(), Some(cursor), 1, NOW + 3)
            .await
            .is_err()
    );
    let mut next = first_owner.advertisement().clone();
    next.generation += 1;
    next.log = Some(NodeLogStatus::open(next.node, 5, vec![member()]).unwrap());
    directory
        .update_advertisement(&first_owner, next, NOW + 3)
        .await
        .unwrap();
    assert!(
        directory
            .follower_logs_page(member(), Some(cursor), 1, NOW + 4)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn coverage_updates_preserve_topology_but_are_returned_fresh() {
    let directory = directory();
    directory.create(advertisement(9, NOW), NOW).await.unwrap();
    enroll(&directory, 1, NOW).await;
    let second = enroll(&directory, 2, NOW).await;
    let page = directory
        .follower_logs_page(member(), None, 1, NOW + 2)
        .await
        .unwrap();
    directory
        .advance_log_coverage(&second, 27, NOW + 3)
        .await
        .unwrap();
    let next = directory
        .follower_logs_page(member(), page.next(), 1, NOW + 4)
        .await
        .unwrap();
    assert_eq!(next.topology(), page.topology());
    assert_eq!(next.entries()[0].log.tiered_through(), 27);
}

#[tokio::test]
async fn malformed_records_or_page_bounds_never_return_a_partial_success() {
    let directory = directory();
    for limit in [0, 129, usize::MAX] {
        assert!(
            directory
                .follower_logs_page(member(), None, limit, NOW)
                .await
                .is_err()
        );
    }
    assert!(
        directory
            .follower_logs_page(NodeId::from_bytes([0; 16]), None, 1, NOW)
            .await
            .is_err()
    );
    assert!(
        directory
            .follower_logs_page(member(), None, 1, -1)
            .await
            .is_err()
    );
    assert!(LogInventoryCursor::from_bytes(&[0; 48]).is_err());
    assert!(LogInventoryCursor::from_bytes(&[1; 49]).is_err());
    directory
        .layout
        .store()
        .create_strict_with_etag(
            &directory.layout.node_path(&[1; 16]),
            Bytes::from_static(b"invalid record"),
        )
        .await
        .unwrap();
    assert!(
        directory
            .follower_logs_page(member(), None, 128, NOW)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ordinary_recovery_claim_remains_visible_as_a_follower_obligation() {
    let directory = directory();
    directory.create(advertisement(9, NOW), NOW).await.unwrap();
    enroll(&directory, 1, NOW).await;
    let expired_at = NOW + 20_000;
    directory
        .create(advertisement(8, expired_at), expired_at)
        .await
        .unwrap();
    directory
        .claim_expired(
            SessionId::from_bytes([1; 16]),
            SessionId::from_bytes([8; 16]),
            expired_at,
        )
        .await
        .unwrap();
    let page = directory
        .follower_logs_page(member(), None, 128, expired_at + 1)
        .await
        .unwrap();
    assert_eq!(page.total_logs(), 1);
    assert_eq!(page.entries()[0].leader_state, LogLeaderState::Fenced);
    assert_eq!(page.entries()[0].log.phase(), NodeLogPhase::Recovering);
    assert_eq!(
        page.entries()[0].log.recovery().unwrap().claimant(),
        SessionId::from_bytes([8; 16])
    );
}
