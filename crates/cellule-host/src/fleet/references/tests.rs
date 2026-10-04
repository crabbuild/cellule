//! Directory pagination contracts; managed native roles use the public example.
use super::traversal::Scan;
use super::*;
use cellule_runtime::{
    identity::SessionId,
    ltx::CellStorageLayout,
    node::{NodeAdvertisement, NodeCapacity, NodeFailureDomain},
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

const NOW: i64 = 1_000_000;
fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}
fn session(index: u8) -> SessionId {
    SessionId::from_bytes([index; 16])
}

async fn directory() -> NodeDirectory {
    let directory = NodeDirectory::new(
        CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("references"),
            [3; 16],
        ),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    );
    for index in 1..=3 {
        directory
            .create(
                NodeAdvertisement::sign(
                    node(index),
                    session(index),
                    format!("https://node-{index}.example"),
                    directory.fleet(),
                    Digest::from_bytes([3; 32]),
                    Digest::from_bytes([4; 32]),
                    Digest::from_bytes([5; 32]),
                    &SigningKey::from_bytes(&[index; 32]),
                    1,
                    NOW,
                    NOW + 30_000,
                    vec![Digest::from_bytes([6; 32])],
                    vec![1],
                    NodeFailureDomain::default(),
                    NodeCapacity {
                        follower_free_bytes: 1 << 20,
                        free_memory_bytes: 1 << 20,
                        free_disk_bytes: 1 << 20,
                        job_credits: 4,
                        log_protocol: 1,
                        ..Default::default()
                    },
                )
                .unwrap(),
                NOW,
            )
            .await
            .unwrap();
    }
    for index in [2, 1] {
        let owner = directory.load(session(index), NOW).await.unwrap().unwrap();
        let prepared = directory
            .prepare_log_enrollment(&owner, 1, 1, 3, NOW)
            .await
            .unwrap()
            .unwrap();
        let attempt = directory
            .prepare_log_enrollment_attempt(&prepared, NOW)
            .await
            .unwrap();
        directory
            .commit_log_enrollment(&attempt, NOW)
            .await
            .unwrap();
    }
    directory
}

#[tokio::test]
async fn canonical_continuations_collect_every_leader_in_order() {
    let directory = directory().await;
    let mut scan = Scan::default();
    let first = directory
        .follower_logs_page(node(3), None, 1, NOW)
        .await
        .unwrap();
    assert_eq!(first.total_logs(), 2);
    assert_eq!(first.entries()[0].leader, session(1));
    assert!(!scan.accept(node(3), first, NOW, NOW + 1).unwrap());
    let second = directory
        .follower_logs_page(node(3), scan.next, 1, NOW + 2)
        .await
        .unwrap();
    assert!(scan.accept(node(3), second, NOW + 2, NOW + 3).unwrap());
    assert_eq!(
        scan.entries
            .iter()
            .map(|row| row.leader)
            .collect::<Vec<_>>(),
        vec![session(1), session(2)]
    );
    assert_eq!(scan.started_at_ms, Some(NOW));
    assert_eq!(scan.finished_at_ms, NOW + 3);
}

#[tokio::test]
async fn foreign_member_and_regressed_capture_cannot_complete_a_traversal() {
    let directory = directory().await;
    let first = directory
        .follower_logs_page(node(3), None, 1, NOW)
        .await
        .unwrap();
    assert!(Scan::default().accept(node(2), first, NOW, NOW).is_err());
    let mut scan = Scan::default();
    let first = directory
        .follower_logs_page(node(3), None, 1, NOW)
        .await
        .unwrap();
    assert!(!scan.accept(node(3), first, NOW, NOW + 2).unwrap());
    let next = directory
        .follower_logs_page(node(3), scan.next, 1, NOW + 1)
        .await
        .unwrap();
    assert!(scan.accept(node(3), next, NOW + 1, NOW + 3).is_err());
    assert_eq!(scan.entries.len(), 1);
    assert!(scan.next.is_some());
}
