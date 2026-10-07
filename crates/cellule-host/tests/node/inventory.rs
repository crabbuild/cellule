//! Public reader inventory with real restored views and shared host admission.

use super::*;
use cellule_host::read_replicas::ReaderInventoryCursor;
use cellule_runtime::peer::PeerReplicaResolver;
use cellule_runtime::{
    CellRuntime,
    cell::catalog::{CatalogEntry, CellCatalog},
    control::{Owner, authority::CellAuthority},
    identity::{CellTarget, IncarnationId, NamespaceId, NodeId, TenantId},
    ltx::{CellReplica, CellStorageLayout},
    node::{NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain, NodeMode},
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

pub(super) fn clock() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

pub(super) fn advertisement(id: u8, code: Digest, now: i64) -> NodeAdvertisement {
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
        now,
        now + 30_000,
        vec![code],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1 << 30,
            free_disk_bytes: 1 << 30,
            follower_free_bytes: 1 << 30,
            follower_retained_bytes: 0,
            job_credits: 3,
            log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_inventory_pages_track_real_views_cordon_and_canonical_shutdown() {
    let application = application();
    let code = application.registry().module_digests()[0];
    let namespace = NamespaceId::from_bytes([2; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("reader-inventory"),
        [3; 16],
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    );
    let now = clock();
    for id in [1, 2] {
        directory
            .create(advertisement(id, code, now), now)
            .await
            .unwrap();
    }
    let limits = ReplicaLimits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..ReplicaLimits::default()
    };
    let reader_root = tempfile::tempdir().unwrap();
    let node = CellNodeBuilder::new(application)
        .with_runtime(
            SqlWorkerPool::new(1, 8)
                .unwrap()
                .with_native_memory_limit(128 << 20)
                .unwrap(),
            16 << 20,
        )
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([2; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let manager = node
        .install_read_replicas(
            layout.clone(),
            directory,
            reader_root.path().to_owned(),
            limits,
        )
        .unwrap();
    node.install_node_lease(NodeLeaseGuard::new(now, now + 60_000).unwrap())
        .unwrap();
    let retained = node.stats().retained_bytes();
    assert!(
        manager
            .fleet_reader_enrollments_page(None, 128, now)
            .unwrap()
            .is_none()
    );
    assert_eq!(node.stats().retained_bytes(), retained);
    for limit in [0, 129, usize::MAX] {
        assert!(manager.fleet_readers_page(None, limit, now).await.is_err());
        assert!(
            manager
                .fleet_reader_enrollments_page(None, limit, now)
                .is_err()
        );
    }
    assert!(manager.fleet_reader_enrollments_page(None, 1, -1).is_err());
    assert!(manager.fleet_readers_page(None, 1, -1).await.is_err());
    assert!(ReaderInventoryCursor::from_bytes(&[0; 63]).is_err());
    let empty = manager.fleet_readers_page(None, 128, now).await.unwrap();
    assert_eq!(empty.total_views(), 0);
    assert!(empty.entries().is_empty());
    assert!(empty.next().is_none());
    drop(empty);
    let source_root = tempfile::tempdir().unwrap();
    let source = CellRuntime::new(
        SqlWorkerPool::new(1, 3).unwrap(),
        16 << 20,
        SessionId::from_bytes([1; 16]),
    )
    .unwrap();
    let catalog = CellCatalog::new(layout.clone(), TenantId::from_bytes([1; 16]));
    let authority = CellAuthority::new(layout.clone());
    let mut handles = Vec::new();
    let mut targets = Vec::new();
    for id in 1..=3_u8 {
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            ApplicationId::from_bytes([3; 16]),
            namespace,
            &[id],
        )
        .unwrap();
        let incarnation = IncarnationId::from_bytes([id; 16]);
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let observed = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session: SessionId::from_bytes([1; 16]),
                    endpoint: "https://node.internal:8789".into(),
                },
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits,
        )
        .unwrap();
        handles.push(
            source
                .bootstrap(
                    proof,
                    replica,
                    authority.clone(),
                    observed,
                    source_root.path().join(format!("{id}.sqlite")),
                    |transaction| {
                        transaction.execute_batch(
                            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                        )?;
                        Ok(())
                    },
                )
                .await
                .unwrap(),
        );
        manager.set_target(&target, 0, 1).await.unwrap().unwrap();
        let receipt = manager
            .activate(target.clone(), SessionId::from_bytes([1; 16]))
            .await
            .unwrap();
        assert_eq!(receipt.cell, target.cell_id());
        assert_eq!(receipt.incarnation, incarnation);
        targets.push(target);
    }
    let mut retained_peer_views = Vec::new();
    for target in &targets {
        retained_peer_views.push(manager.resolve(target.clone()).await.unwrap());
    }
    let resident_before = node.stats().resident_bytes();
    let before = node.stats().retained_bytes();
    let first = manager.fleet_readers_page(None, 1, clock()).await.unwrap();
    assert_eq!(first.session(), SessionId::from_bytes([2; 16]));
    assert_eq!(first.total_views(), 3);
    assert_eq!(first.entries().len(), 1);
    assert_eq!(node.stats().retained_bytes(), before + (1 << 20));
    let cursor = ReaderInventoryCursor::from_bytes(&first.next().unwrap().to_bytes()).unwrap();
    let next = manager
        .fleet_readers_page(Some(cursor), 128, clock())
        .await
        .unwrap();
    assert_eq!(next.topology(), first.topology());
    assert_eq!(next.entries().len(), 2);
    assert!(next.next().is_none());
    assert!(
        next.entries()[0].receipt().cell.as_bytes() > first.entries()[0].receipt().cell.as_bytes()
    );
    let removed = first.entries()[0].receipt().cell;
    drop(first);
    drop(next);
    assert_eq!(node.stats().retained_bytes(), before);
    manager.remove(removed).await.unwrap();
    assert!(node.stats().resident_bytes() < resident_before);
    for peer in &retained_peer_views {
        if peer.receipt().await.cell == removed {
            assert!(matches!(peer.readiness().await, Err(Error::Fenced)));
        }
    }
    assert!(
        manager
            .fleet_readers_page(Some(cursor), 1, clock())
            .await
            .is_err()
    );
    assert_eq!(node.stats().retained_bytes(), before);
    let before_cordon = manager.fleet_readers_page(None, 1, clock()).await.unwrap();
    let cursor = before_cordon.next().unwrap();
    let topology = before_cordon.topology();
    drop(before_cordon);
    node.runtime().stop_acquiring().unwrap();
    assert!(
        manager
            .fleet_readers_page(Some(cursor), 1, clock())
            .await
            .is_err()
    );
    let cordoned = manager
        .fleet_readers_page(None, 128, clock())
        .await
        .unwrap();
    assert_eq!(cordoned.mode(), NodeMode::Cordoned);
    assert_ne!(cordoned.topology(), topology);
    assert_eq!(cordoned.total_views(), 2);
    drop(cordoned);
    let removed_target = targets
        .into_iter()
        .find(|target| target.cell_id() == removed)
        .unwrap();
    assert!(matches!(
        manager
            .activate(removed_target, SessionId::from_bytes([1; 16]))
            .await,
        Err(Error::CellDraining)
    ));
    manager.shutdown().await.unwrap();
    let closed = manager
        .fleet_readers_page(None, 128, clock())
        .await
        .unwrap();
    assert!(closed.closed());
    assert_eq!(closed.total_views(), 0);
    drop(closed);
    node.shutdown().await.unwrap();
    assert_eq!(node.state(), NodeState::Stopped);
    assert_eq!(node.stats().retained_bytes(), 0);
    assert_eq!(node.stats().resident_bytes(), 0);
    for peer in &retained_peer_views {
        assert!(peer.readiness().await.is_err());
    }
    drop(retained_peer_views);
    for handle in handles {
        handle.drain().await.unwrap();
    }
    source.shutdown().await.unwrap();
}
