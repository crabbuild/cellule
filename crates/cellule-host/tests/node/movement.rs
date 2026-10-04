//! Canonical source evidence consumed by a leased receiver's prepared resources.

use super::*;
use cellule_runtime::cell::actor::CellInventoryEntry;
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::cell::executor::{HandlerOutcome, MutationIdentity, Resolution};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::fleet::operations::{AttemptId, MoveAttemptSpec, OperationId};
use cellule_runtime::identity::{
    CellTarget, IncarnationId, NamespaceId, NodeId, RequestId, TenantId,
};
use cellule_runtime::ltx::{CellReplica, CellStorageLayout};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

fn clock() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn node(session: SessionId) -> CellNode {
    let node = CellNodeBuilder::new(application())
        .with_runtime(
            SqlWorkerPool::new(1, 8)
                .unwrap()
                .with_native_memory_limit(128 << 20)
                .unwrap(),
            64 << 20,
        )
        .with_replica_host(ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
        .with_session(session)
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let now = clock();
    node.install_node_lease(NodeLeaseGuard::new(now, now + 60_000).unwrap())
        .unwrap();
    node
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leased_host_release_returns_exact_evidence_for_prepared_receiver_activation() {
    let source_session = SessionId::from_bytes([176; 16]);
    let receiver_session = SessionId::from_bytes([177; 16]);
    let source = node(source_session);
    let receiver = node(receiver_session);
    let root = tempfile::tempdir().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("host-movement"),
        [3; 16],
    );
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([2; 16]),
        b"host-movement",
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([178; 16]);
    let limits = ReplicaLimits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..ReplicaLimits::default()
    };
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        limits,
    )
    .unwrap();
    let code = source.application().registry().module_digests()[0];
    let catalog = CellCatalog::new(layout.clone(), target.tenant());
    let proof = catalog
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let initial = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session: source_session,
                endpoint: "https://source.internal:8789".into(),
            },
        )
        .await
        .unwrap();
    let handle = source
        .runtime()
        .bootstrap(
            proof.clone(),
            replica.clone(),
            authority.clone(),
            initial,
            root.path().join("source.sqlite"),
            |transaction| {
                transaction.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([179; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let digest = Digest::from_bytes([179; 32]);
    let acknowledged = handle
        .execute(identity, digest, now, 64, 64, |transaction| {
            transaction.execute("UPDATE counter SET value = 37", [])?;
            Ok(HandlerOutcome::Success(vec![37]))
        })
        .await
        .unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = source.runtime().fleet_cells_page(None, 128).await.unwrap();
            if let Some(CellInventoryEntry::Owned(owner)) = page.entries().first()
                && owner.cost.is_some()
                && owner.stable_observations == 2
            {
                return (**owner).clone();
            }
            drop(page);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let spec = MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([180; 16]).unwrap(),
            sequence: 1,
        },
        target: target.clone(),
        incarnation,
        source_node: NodeId::from_bytes([176; 16]),
        source: source_session,
        generation: observation.generation,
        source_epoch: observation.position.unwrap().epoch,
        destination_node: NodeId::from_bytes([177; 16]),
        destination: receiver_session,
        cost: observation.cost.unwrap(),
        snapshot_digest: Digest::from_bytes([181; 32]),
        deadline_ms: clock() + 60_000,
    };
    let prepared = receiver
        .runtime()
        .prepare_receiver(
            spec.clone(),
            proof.clone(),
            replica,
            root.path().join("receiver.sqlite"),
            spec.deadline_ms,
            clock(),
        )
        .unwrap();
    assert_eq!(
        receiver.stats().local_disk_reserved_bytes(),
        spec.cost.disk_bytes
    );
    // Cordon retains source release eligibility and the leased canonical path.
    source.begin_scale_down().unwrap();
    let released = source
        .release_idle_cell_at(
            target.cell_id(),
            source_session,
            spec.generation,
            incarnation,
            spec.source_epoch,
        )
        .await
        .unwrap();
    assert_eq!(released.epoch, spec.source_epoch);
    assert_eq!(released.incarnation, incarnation);
    assert!(released.root.commit_sequence >= acknowledged.commit_sequence());
    assert_eq!(source.stats().active_cells(), 0);
    let idle = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(Some(&released.root), idle.value().root.as_ref());
    let activated = receiver
        .runtime()
        .activate_prepared_receiver(
            &prepared,
            authority.clone(),
            idle,
            Owner {
                session: receiver_session,
                endpoint: "https://receiver.internal:8789".into(),
            },
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(
        activated
            .resolve(identity, digest, clock(), 64)
            .await
            .unwrap(),
        Resolution::Committed(acknowledged)
    );
    assert_eq!(
        activated
            .query(64, 64, |connection| {
                Ok(connection
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            })
            .await
            .unwrap(),
        37_i64.to_be_bytes()
    );
    let serving = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(
        serving.value().owner.as_ref().unwrap().session,
        receiver_session
    );
    assert_eq!(Some(&released.root), serving.value().root.as_ref());
    source.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
    assert_eq!(receiver.stats().active_cells(), 0);
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
}
