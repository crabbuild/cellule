//! Real receiver credit, canonical acquisition, cancellation and shutdown.

use super::*;
use cellule_runtime::cell::actor::{CellHandle, PreparedCellReceiver, ReceiverState};
use cellule_runtime::fleet::operations::{
    AttemptId, MAX_RECORD_BYTES, MoveAttemptSpec, OperationError, OperationId,
};

const RECEIVER: SessionId = SessionId::from_bytes([121; 16]);

fn receiver_runtime() -> CellRuntime {
    runtime_with_capacity(64 << 20, 8, 8 << 30)
}

fn runtime_with_capacity(memory: usize, cells: usize, disk: u64) -> CellRuntime {
    let pool = SqlWorkerPool::new(1, cells)
        .unwrap()
        .with_native_memory_limit(memory)
        .unwrap();
    CellRuntime::new_with_replica_host(
        pool,
        64 << 20,
        RECEIVER,
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(disk)),
    )
    .unwrap()
}

async fn attempt(source: &CellRuntime, fixture: &Fixture, sequence: u64) -> MoveAttemptSpec {
    let owner = super::inventory::stable_owner(source).await;
    assert_eq!(owner.target, fixture.target);
    MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([122; 16]).unwrap(),
            sequence,
        },
        target: owner.target,
        incarnation: owner.incarnation,
        source_node: NodeId::from_bytes([123; 16]),
        source: source.fleet_cells_page(None, 1).await.unwrap().session(),
        generation: owner.generation,
        source_epoch: owner.position.unwrap().epoch,
        destination_node: NodeId::from_bytes([124; 16]),
        destination: RECEIVER,
        cost: owner.cost.unwrap(),
        snapshot_digest: Digest::from_bytes([125; 32]),
        deadline_ms: now_ms() + 60_000,
    }
}

fn prepare(
    runtime: &CellRuntime,
    spec: &MoveAttemptSpec,
    handle: &CellHandle,
    fixture: &Fixture,
) -> cellule_runtime::Result<PreparedCellReceiver> {
    runtime.prepare_receiver(
        spec.clone(),
        handle.catalog().clone(),
        fixture.replica.clone(),
        fixture._directory.path().join("receiver.sqlite"),
        spec.deadline_ms,
        now_ms(),
    )
}

fn owner() -> Owner {
    Owner {
        session: RECEIVER,
        endpoint: "https://receiver.internal:8081".into(),
    }
}

async fn counter(handle: &CellHandle) -> i64 {
    let bytes = handle
        .query(64, 64, |connection| {
            Ok(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_be_bytes()
                .to_vec())
        })
        .await
        .unwrap();
    i64::from_be_bytes(bytes.try_into().unwrap())
}

async fn wait_state(prepared: &PreparedCellReceiver, expected: ReceiverState) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while prepared.state().unwrap() != expected {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn retire(runtime: &CellRuntime, prepared: &PreparedCellReceiver) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match runtime.retire_prepared_receiver(prepared).await {
                Ok(()) => return,
                Err(cellule_runtime::Error::FleetOperation(error))
                    if matches!(*error, OperationError::Busy) =>
                {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("retirement failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_credit_transfers_into_exact_root_sql_and_one_fenced_writer() {
    let fixture = fixture_for(b"prepared-receiver-root");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let clock = now_ms();
    let acknowledged = handle
        .execute(
            mutation_identity_window(126, clock, clock + 60_000),
            Digest::from_bytes([126; 32]),
            clock,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 77", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    let reserved = receiver.stats();
    assert_eq!(reserved.active_cells(), 1);
    assert_eq!(reserved.resident_bytes(), spec.cost.memory_bytes as usize);
    assert_eq!(
        reserved.file_descriptors(),
        spec.cost.file_descriptors as usize
    );
    assert_eq!(reserved.worker_jobs(), 1);
    assert_eq!(reserved.local_disk_reserved_bytes(), spec.cost.disk_bytes);
    assert_eq!(reserved.retained_bytes(), MAX_RECORD_BYTES as usize);
    let duplicate = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    assert_eq!(receiver.stats(), reserved);
    assert_eq!(duplicate.spec().unwrap(), spec);
    drop(prepared);
    let prepared = receiver.prepared_receiver(spec.id).unwrap().unwrap();
    assert_eq!(counter(&handle).await, 77);
    let released = source
        .release_idle_cell_at(
            spec.target.cell_id(),
            spec.source,
            spec.generation,
            spec.incarnation,
            spec.source_epoch,
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(idle.value().root.as_ref().unwrap().commit_sequence >= acknowledged.commit_sequence());
    assert_eq!(Some(&released.root), idle.value().root.as_ref());
    assert_eq!(released.epoch, spec.source_epoch);
    let root = idle.value().root.clone();
    let activated = receiver
        .activate_prepared_receiver(&prepared, authority.clone(), idle, owner(), now_ms())
        .await
        .unwrap();
    assert_eq!(prepared.state().unwrap(), ReceiverState::Activated);
    assert_eq!(counter(&activated).await, 77);
    assert_eq!(
        activated
            .resolve(
                mutation_identity_window(126, clock, clock + 60_000),
                Digest::from_bytes([126; 32]),
                now_ms(),
                64,
            )
            .await
            .unwrap(),
        Resolution::Committed(acknowledged)
    );
    let serving = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serving.value().state, ControlState::Serving);
    assert_eq!(serving.value().owner.as_ref().unwrap().session, RECEIVER);
    assert_eq!(serving.value().epoch, spec.source_epoch + 1);
    assert_eq!(serving.value().root, root);
    let after = receiver.stats();
    assert_eq!(after.active_cells(), 1);
    assert_eq!(after.worker_jobs(), 0);
    assert_eq!(after.resident_bytes(), reserved.resident_bytes());
    assert_eq!(after.file_descriptors(), reserved.file_descriptors());
    assert!(
        after.local_disk_reserved_bytes() > 0
            && after.local_disk_reserved_bytes() < reserved.local_disk_reserved_bytes()
    );
    assert!(
        source
            .local_handle(handle.catalog().clone(), &serving)
            .await
            .unwrap()
            .is_none()
    );
    retire(&receiver, &prepared).await;
    assert_eq!(receiver.stats().retained_bytes(), 0);
    assert_eq!(receiver.stats().active_cells(), 1);
    activated.drain().await.unwrap();
    receiver.shutdown().await.unwrap();
    assert_eq!(receiver.stats().active_cells(), 0);
    assert_eq!(receiver.stats().resident_bytes(), 0);
    assert_eq!(receiver.stats().file_descriptors(), 0);
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_does_not_free_credit_and_cancellation_never_reprepares_same_attempt() {
    let fixture = fixture_for(b"prepared-receiver-cancel");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    let retained = prepared.clone();
    drop(prepared);
    assert_eq!(
        receiver.stats().local_disk_reserved_bytes(),
        spec.cost.disk_bytes
    );
    handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(receiver.activate_prepared_receiver(&retained, authority.clone(), idle.clone(), owner(), spec.deadline_ms).await,
        Err(cellule_runtime::Error::FleetOperation(error)) if matches!(*error, OperationError::Deadline))
    );
    assert_eq!(retained.state().unwrap(), ReceiverState::Prepared);
    assert_eq!(
        receiver.stats().local_disk_reserved_bytes(),
        spec.cost.disk_bytes
    );
    receiver.cancel_prepared_receiver(&retained).unwrap();
    receiver.cancel_prepared_receiver(&retained).unwrap();
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
    assert_eq!(receiver.stats().worker_jobs(), 0);
    assert_eq!(receiver.stats().active_cells(), 0);
    assert_eq!(receiver.stats().retained_bytes(), MAX_RECORD_BYTES as usize);
    let duplicate = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    assert_eq!(duplicate.state().unwrap(), ReceiverState::Cancelled);
    let mut changed = spec.clone();
    changed.snapshot_digest = Digest::from_bytes([127; 32]);
    assert!(
        matches!(prepare(&receiver, &changed, &handle, &fixture), Err(cellule_runtime::Error::FleetOperation(error)) if matches!(*error, OperationError::Conflict))
    );
    assert_eq!(
        authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        idle.value()
    );
    retire(&receiver, &retained).await;
    assert!(matches!(
        retained.state(),
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
    receiver.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_refusal_leaves_source_serving_and_returns_partial_charges() {
    let fixture = fixture_for(b"prepared-receiver-refusal");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    // Fail resident memory, file descriptors, then disk after the earlier tokens
    // were admitted. Every refusal must undo its own partial preparation.
    for receiver in [
        runtime_with_capacity(64 << 10, 8, 8 << 30),
        runtime_with_capacity(64 << 20, 1, 8 << 30),
        runtime_with_capacity(64 << 20, 8, 1),
    ] {
        assert!(matches!(
            prepare(&receiver, &spec, &handle, &fixture),
            Err(cellule_runtime::Error::Capacity(_) | cellule_runtime::Error::Ltx(_))
        ));
        assert_eq!(receiver.stats().active_cells(), 0);
        assert_eq!(receiver.stats().worker_jobs(), 0);
        assert_eq!(receiver.stats().resident_bytes(), 0);
        assert_eq!(receiver.stats().file_descriptors(), 0);
        assert_eq!(receiver.stats().retained_bytes(), 0);
        assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
        assert!(receiver.prepared_receiver(spec.id).unwrap().is_none());
        assert_eq!(counter(&handle).await, 0);
        receiver.shutdown().await.unwrap();
    }
    let receiver = receiver_runtime();
    let mut wrong = spec.clone();
    wrong.destination = SessionId::from_bytes([128; 16]);
    assert!(matches!(
        prepare(&receiver, &wrong, &handle, &fixture),
        Err(cellule_runtime::Error::Fenced)
    ));
    let mut underdeclared = spec.clone();
    underdeclared.cost.disk_bytes -= 1;
    assert!(matches!(
        prepare(&receiver, &underdeclared, &handle, &fixture),
        Err(cellule_runtime::Error::Capacity(_))
    ));
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    // Another attempt cannot steal the exact worker while its prepared credit
    // is retained, even though node memory, disk and Cell slots remain free.
    let mut competing = spec.clone();
    competing.id.sequence = 2;
    assert!(matches!(
        prepare(&receiver, &competing, &handle, &fixture),
        Err(cellule_runtime::Error::Capacity("incoming Cell worker"))
    ));
    assert_eq!(
        receiver.stats().local_disk_reserved_bytes(),
        spec.cost.disk_bytes
    );
    assert_eq!(receiver.stats().active_cells(), 1);
    assert_eq!(receiver.stats().retained_bytes(), MAX_RECORD_BYTES as usize);
    receiver.cancel_prepared_receiver(&prepared).unwrap();
    retire(&receiver, &prepared).await;
    receiver.shutdown().await.unwrap();
    handle.drain().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_replica_scope_is_refused_before_any_receiver_admission() {
    let fixture = fixture_for(b"prepared-receiver-scope");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    for (cell, incarnation) in [
        ([129; 32], *spec.incarnation.as_bytes()),
        (*spec.target.cell_id().as_bytes(), [130; 16]),
        ([129; 32], [130; 16]),
    ] {
        let replica = CellReplica::new(
            fixture.layout.clone(),
            cell,
            incarnation,
            fixture.replica.limits(),
        )
        .unwrap();
        assert_eq!(replica.scope(), (cell, incarnation));
        assert!(matches!(
            receiver.prepare_receiver(
                spec.clone(),
                handle.catalog().clone(),
                replica,
                fixture._directory.path().join("receiver.sqlite"),
                spec.deadline_ms,
                now_ms(),
            ),
            Err(cellule_runtime::Error::Fenced)
        ));
        assert_eq!(receiver.stats().active_cells(), 0);
        assert_eq!(receiver.stats().worker_jobs(), 0);
        assert_eq!(receiver.stats().resident_bytes(), 0);
        assert_eq!(receiver.stats().file_descriptors(), 0);
        assert_eq!(receiver.stats().retained_bytes(), 0);
        assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
        assert!(receiver.prepared_receiver(spec.id).unwrap().is_none());
    }
    assert_eq!(counter(&handle).await, 0);
    receiver.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unused_preparation_closes_on_shutdown_despite_retained_caller_handles() {
    let fixture = fixture_for(b"prepared-receiver-shutdown");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    let retained = prepared.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.shutdown())
        .await
        .unwrap()
        .unwrap();
    let stats = receiver.stats();
    assert_eq!(stats.active_cells(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
    assert_eq!(stats.retained_bytes(), 0);
    assert!(matches!(
        retained.state(),
        Err(cellule_runtime::Error::RuntimeClosed)
    ));
    assert_eq!(counter(&handle).await, 0);
    handle.drain().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_restore_returns_credit_and_canonically_releases_its_acquired_epoch() {
    let fixture = fixture_for(b"prepared-receiver-restore-failure");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let destination = fixture
        ._directory
        .path()
        .join("directory-is-not-a-database");
    std::fs::create_dir(&destination).unwrap();
    let prepared = receiver
        .prepare_receiver(
            spec.clone(),
            handle.catalog().clone(),
            fixture.replica.clone(),
            destination,
            spec.deadline_ms,
            now_ms(),
        )
        .unwrap();
    handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = idle.value().root.clone();
    assert!(matches!(
        receiver
            .activate_prepared_receiver(&prepared, authority.clone(), idle, owner(), now_ms(),)
            .await,
        Err(cellule_runtime::Error::Ltx(_))
    ));
    assert_eq!(prepared.state().unwrap(), ReceiverState::Failed);
    let after = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.value().epoch, spec.source_epoch + 1);
    assert_eq!(after.value().state, ControlState::Idle);
    assert!(after.value().owner.is_none());
    assert_eq!(after.value().root, root);
    assert_eq!(receiver.stats().active_cells(), 0);
    assert_eq!(receiver.stats().resident_bytes(), 0);
    assert_eq!(receiver.stats().file_descriptors(), 0);
    assert_eq!(receiver.stats().worker_jobs(), 0);
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
    retire(&receiver, &prepared).await;
    receiver.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_receive_uses_current_root_after_an_intervening_canonical_owner() {
    let fixture = fixture_for(b"prepared-receiver-current-idle");
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let other_session = SessionId::from_bytes([129; 16]);
    let other = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 2).unwrap(),
        64 << 20,
        other_session,
        ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
    )
    .unwrap();
    let other_handle = other
        .acquire_idle_restored(
            handle.catalog().clone(),
            fixture.replica.clone(),
            authority.clone(),
            idle,
            fixture._directory.path().join("other.sqlite"),
            Owner {
                session: other_session,
                endpoint: "https://other.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let clock = now_ms();
    other_handle
        .execute(
            mutation_identity_window(130, clock, clock + 60_000),
            Digest::from_bytes([130; 32]),
            clock,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 98", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    other_handle.drain().await.unwrap();
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().epoch, spec.source_epoch + 1);
    let latest_root = current.value().root.clone();
    let activated = receiver
        .activate_prepared_receiver(&prepared, authority.clone(), current, owner(), now_ms())
        .await
        .unwrap();
    assert_eq!(counter(&activated).await, 98);
    let serving = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serving.value().root, latest_root);
    assert_eq!(serving.value().epoch, spec.source_epoch + 2);
    receiver.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_activation_waiter_and_cas_reply_leave_one_inspectable_serving_actor() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"prepared-receiver-lost-reply",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    store.arm_next_update();
    store.lose_next_update_response();
    let activation = {
        let receiver = receiver.clone();
        let prepared = prepared.clone();
        let authority = authority.clone();
        let idle = idle.clone();
        tokio::spawn(async move {
            receiver
                .activate_prepared_receiver(&prepared, authority, idle, owner(), now_ms())
                .await
        })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.wait_until_blocked(),
    )
    .await
    .unwrap();
    activation.abort();
    assert!(activation.await.err().unwrap().is_cancelled());
    assert_eq!(prepared.state().unwrap(), ReceiverState::Activating);
    assert!(
        matches!(receiver.cancel_prepared_receiver(&prepared), Err(cellule_runtime::Error::FleetOperation(error)) if matches!(*error, OperationError::Busy))
    );
    assert!(
        matches!(receiver.activate_prepared_receiver(&prepared, authority.clone(), idle, owner(), now_ms()).await, Err(cellule_runtime::Error::FleetOperation(error)) if matches!(*error, OperationError::Busy))
    );
    assert_eq!(
        receiver.stats().local_disk_reserved_bytes(),
        spec.cost.disk_bytes
    );
    store.release();
    wait_state(&prepared, ReceiverState::Activated).await;
    assert!(store.lost_update_response_consumed());
    let serving = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let activated = receiver
        .local_handle(handle.catalog().clone(), &serving)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&activated).await, 0);
    assert_eq!(serving.value().owner.as_ref().unwrap().session, RECEIVER);
    assert_eq!(receiver.stats().active_cells(), 1);
    receiver.shutdown().await.unwrap();
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_joins_accepted_claim_and_rolls_it_back_before_worker_close() {
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = fixture_with_limits_and_store(
        b"prepared-receiver-join",
        Limits::default(),
        Store::new(store.clone()),
    );
    let (source, handle, _) = activate_runtime(&fixture, 64 << 20).await;
    let spec = attempt(&source, &fixture, 1).await;
    let receiver = receiver_runtime();
    let prepared = prepare(&receiver, &spec, &handle, &fixture).unwrap();
    handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = idle.value().root.clone();
    store.arm_next_update();
    let activation = {
        let receiver = receiver.clone();
        let prepared = prepared.clone();
        let authority = authority.clone();
        tokio::spawn(async move {
            receiver
                .activate_prepared_receiver(&prepared, authority, idle, owner(), now_ms())
                .await
        })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.wait_until_blocked(),
    )
    .await
    .unwrap();
    let shutdown = {
        let receiver = receiver.clone();
        tokio::spawn(async move { receiver.shutdown().await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while receiver.is_acquiring() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    assert_eq!(receiver.stats().active_cells(), 1);
    store.release();
    assert!(matches!(
        activation.await.unwrap(),
        Err(cellule_runtime::Error::RuntimeClosed | cellule_runtime::Error::CellDraining)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(10), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().state, ControlState::Idle);
    assert!(current.value().owner.is_none());
    assert_eq!(current.value().root, root);
    assert_eq!(receiver.stats().active_cells(), 0);
    assert_eq!(receiver.stats().worker_jobs(), 0);
    assert_eq!(receiver.stats().local_disk_reserved_bytes(), 0);
    assert_eq!(receiver.stats().retained_bytes(), 0);
    source.shutdown().await.unwrap();
}
