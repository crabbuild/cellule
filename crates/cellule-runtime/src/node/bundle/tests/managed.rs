//! Ordinary actors driven by the runtime producer, without a manual selector.
use super::actor::{Authority, transport};
use super::*;
use crate::cell::actor::CellRuntime;
use crate::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use crate::cell::executor::{HandlerOutcome, MutationIdentity};
use crate::cell::worker::SqlWorkerPool;
use crate::fleet::telemetry::{CellTelemetry, CommandResponseSource};
use crate::identity::{CellTarget, NamespaceId, RequestId, TenantId};
use crate::node::durability::NodeDurability;
use crate::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority,
};
use crate::node::log_shipper::NodeLogShipper;
use crate::node::log_shipper::{AssignedCapture, NodeLogSubmission};
use futures_util::future::BoxFuture;
use std::sync::{Mutex, atomic::Ordering};

#[derive(Default)]
struct Responses(Mutex<Vec<CommandResponseSource>>);
impl CellTelemetry for Responses {
    fn command_response(
        &self,
        source: CommandResponseSource,
        _: std::time::Duration,
        _: std::time::Duration,
    ) {
        self.0.lock().unwrap().push(source);
    }
}

#[tokio::test]
async fn rejected_producer_working_credit_does_not_install_an_irreversible_feed() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let peers = transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        peers,
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(8 << 20).unwrap();
    durability
        .attach_selection_resources(pool.resource_ledger())
        .unwrap();
    assert!(matches!(
        durability.start_bundle_publication(authority),
        Err(Error::Capacity(_))
    ));
    let mut feed = durability.take_publication_feed().unwrap();
    durability.shutdown().await.unwrap();
    assert!(feed.recv().await.is_none());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    pool.shutdown().await.unwrap();
}

struct FailedSelection(Arc<Authority>);
impl NodeBundleAuthority for FailedSelection {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        NodeBundleAuthority::bind(self.0.as_ref(), authority, observed)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        NodeBundleAuthority::close(self.0.as_ref(), authority, observed, issued)
    }
}
impl NodeBundlePublicationAuthority for FailedSelection {
    fn select<'a>(
        &'a self,
        _: &'a [AssignedCapture],
        _: &'a [BundleCheckpoint],
        _: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async { Err(Error::Node("injected bundle selection failure")) })
    }
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        self.0.checkpoint(checkpoints)
    }
}

#[tokio::test]
async fn producer_failure_fences_new_work_and_join_preserves_its_cause() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let peers = transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        peers,
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(32 << 20).unwrap();
    durability
        .attach_selection_resources(pool.resource_ledger())
        .unwrap();
    durability
        .start_bundle_publication(Arc::new(FailedSelection(authority)))
        .unwrap();
    cell.db
        .transaction(|tx| tx.execute("INSERT INTO outcomes VALUES('failed','retained')", []))
        .unwrap();
    let cuts = cell.db.capture().unwrap();
    durability
        .submit_assigned(
            NodeLogSubmission::new(
                ApplicationId::from_bytes([9; 16]),
                cell.control.value().cell,
                cell.control.value().incarnation,
                cell.control.value().epoch,
                2,
                &cuts,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), f.lease.wait_fenced())
        .await
        .unwrap();
    let error = durability.shutdown().await.unwrap_err();
    assert!(
        matches!(error, Error::Shared(source) if matches!(source.as_ref(), Error::Node("injected bundle selection failure")))
    );
    assert_eq!(durability.progress().unwrap().tiered_through, 0);
    assert_eq!(durability.progress().unwrap().issued_through, 1);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_producer_selects_actor_prefixes_and_joins_checkpoints_complete_close_and_cold_results()
 {
    managed_actor_case(10, 32 << 20, false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_actor_retires_215_grouped_commands_per_cell_before_joined_root_materialization() {
    managed_actor_case(215, 64 << 20, true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_actor_retires_65_sequential_captures_before_joined_root_materialization() {
    managed_actor_case(65, 64 << 20, false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_actor_continues_selected_receipts_across_an_active_root_checkpoint() {
    managed_actor_case(215, 64 << 20, true, true).await;
}

async fn managed_actor_case(
    per_cell: u16,
    retained_bytes: usize,
    grouped: bool,
    checkpoint_continuation: bool,
) {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let peers = transport(&f, false);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        peers.clone(),
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(2, 4).unwrap();
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        pool.clone(),
        retained_bytes,
        SessionId::from_bytes([1; 16]),
        cellule_ltx::Host::default().with_dirty_slots(dirty.clone()),
    )
    .unwrap();
    runtime.install_node_lease(f.lease.clone()).unwrap();
    runtime
        .install_node_durability(ApplicationId::from_bytes([9; 16]), durability.clone())
        .unwrap();
    durability
        .start_bundle_publication(authority.clone())
        .unwrap();
    let responses = Arc::new(Responses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let mut cells = Vec::new();
    for byte in [4, 5] {
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            ApplicationId::from_bytes([9; 16]),
            NamespaceId::from_bytes([13; 16]),
            &[byte],
        )
        .unwrap();
        let catalog = CellCatalog::new(f.layout.clone(), target.tenant());
        let proof = catalog
            .provision(
                CatalogEntry::new(
                    &target,
                    CatalogRole::Application,
                    Digest::from_bytes([12; 32]),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let cell_authority = CellAuthority::new(f.layout.clone());
        let control = cell_authority
            .create_initial(
                &proof,
                IncarnationId::from_bytes([byte; 16]),
                Owner {
                    session: SessionId::from_bytes([1; 16]),
                    endpoint: "https://bundle.internal:8081".into(),
                },
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            f.layout.clone(),
            *target.cell_id().as_bytes(),
            [byte; 16],
            Limits::default(),
        )
        .unwrap();
        let handle = runtime
            .bootstrap(
                proof,
                replica.clone(),
                cell_authority.clone(),
                control,
                f.scratch.path().join(format!("managed-{byte}.sqlite")),
                |tx| {
                    tx.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)",
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        cells.push((target, cell_authority, replica, handle));
    }
    // Ordinary per-Cell roots are unavailable. The producer must select exact
    // origin coverage to grant any command ACK, read or retry visibility.
    let mut preparation = Some(dirty.acquire_owned().await.unwrap());
    let digest = Digest::from_bytes([9; 32]);
    let mut commands = tokio::task::JoinSet::new();
    let mut results = Vec::new();
    for byte in 1_u16..=per_cell * 2 {
        if byte % 8 == 1 {
            renew_actor_lease(&authority, &f.lease).await;
        }
        let handle = cells[usize::from(byte % 2)].3.clone();
        let mut request = [0; 16];
        request[..2].copy_from_slice(&byte.to_le_bytes());
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes(request),
            issued_at_ms: 10,
            expires_at_ms: 10_000,
        };
        let command = async move {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                handle.execute(identity, digest, 20, 1024, 1024, |tx| {
                    tx.execute("UPDATE counter SET value=value+1", [])?;
                    Ok(HandlerOutcome::Success(b"managed".to_vec()))
                }),
            )
            .await
            .unwrap()
            .unwrap();
            (byte, identity, result)
        };
        if grouped {
            commands.spawn(command);
            // Exercise native grouping within the unchanged 64-request Cell
            // mailbox; this correctness test does not offer overload traffic.
            if commands.len() == 32 {
                while let Some(result) = commands.join_next().await {
                    results.push(result.unwrap());
                }
                renew_actor_lease(&authority, &f.lease).await;
            }
        } else {
            results.push(command.await);
        }
    }
    while let Some(result) = commands.join_next().await {
        results.push(result.unwrap());
        if results.len() % 8 == 0 {
            renew_actor_lease(&authority, &f.lease).await;
        }
    }
    for (byte, identity, result) in &results {
        let handle = &cells[usize::from(byte % 2)].3;
        if !grouped {
            assert_eq!(result.commit_sequence(), u64::from(*byte).div_ceil(2));
        }
        assert_eq!(
            handle
                .execute(*identity, digest, 21, 1024, 1024, |_| panic!(
                    "proved retry must not execute again"
                ))
                .await
                .unwrap(),
            *result
        );
    }
    for parity in [0, 1] {
        let mut sequences = results
            .iter()
            .filter(|(byte, _, _)| byte % 2 == parity)
            .map(|(_, _, result)| result.commit_sequence())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=u64::from(per_cell)).collect::<Vec<_>>());
    }
    for (_, _, _, handle) in &cells {
        assert_eq!(
            handle
                .query(1024, 1024, |connection| Ok(connection
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                    .to_le_bytes()
                    .to_vec()))
                .await
                .unwrap(),
            i64::from(per_cell).to_le_bytes()
        );
    }
    // Root preparation is still held: exact selected reads/retries did not
    // require any root, and covered physical captures have left worker RAM.
    let mut selected_frames = 0_u64;
    for (target, authority, _, _) in &cells {
        assert_eq!(
            authority
                .load(target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root()
                .unwrap()
                .commit_sequence,
            0
        );
        let proof = authority.load(target.cell_id()).await.unwrap().unwrap();
        let selected = f
            .directory
            .load_bundle_coverage(authority, &proof, Limits::default())
            .await
            .unwrap();
        assert_eq!(selected.commit_sequence(), u64::from(per_cell));
        assert!(selected.locator_count() <= 256);
        selected_frames += selected.locator_count() as u64;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.pending(target.cell_id()).await.unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let due = runtime.due_resident(i64::MAX, 2).await.unwrap();
    assert_eq!(due.len(), 2);
    assert!(
        due.iter()
            .all(|cell| cell.expected_commit_sequence() == u64::from(per_cell))
    );
    assert_eq!(
        responses
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|source| **source == CommandResponseSource::Bundle)
            .count(),
        usize::from(per_cell) * 2
    );
    if checkpoint_continuation {
        // These captures are selected against the old base while the actor's
        // first materializer owns the publisher and waits for preparation.
        for byte in per_cell * 2 + 1..=per_cell * 2 + 4 {
            renew_actor_lease(&authority, &f.lease).await;
            let handle = &cells[usize::from(byte % 2)].3;
            let mut request = [0; 16];
            request[..2].copy_from_slice(&byte.to_le_bytes());
            let identity = MutationIdentity {
                request_id: RequestId::from_bytes(request),
                issued_at_ms: 10,
                expires_at_ms: 10_000,
            };
            let result = handle
                .execute(identity, digest, 20, 1024, 1024, |tx| {
                    tx.execute("UPDATE counter SET value=value+1", [])?;
                    Ok(HandlerOutcome::Success(b"managed".to_vec()))
                })
                .await
                .unwrap();
            results.push((byte, identity, result));
        }
        drop(preparation.take());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for (target, cell_authority, _, _) in &cells {
                loop {
                    let control = cell_authority
                        .load(target.cell_id())
                        .await
                        .unwrap()
                        .unwrap();
                    if control.value().ltx_root().unwrap().commit_sequence >= u64::from(per_cell)
                        && pool.pending(target.cell_id()).await.unwrap().is_none()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        })
        .await
        .unwrap();
        // The old receipts are now retired. New proofs use the checkpointed
        // base and must preserve their exact retained suffix rather than fence.
        for byte in per_cell * 2 + 5..=per_cell * 2 + 6 {
            renew_actor_lease(&authority, &f.lease).await;
            let handle = &cells[usize::from(byte % 2)].3;
            let mut request = [0; 16];
            request[..2].copy_from_slice(&byte.to_le_bytes());
            let identity = MutationIdentity {
                request_id: RequestId::from_bytes(request),
                issued_at_ms: 10,
                expires_at_ms: 10_000,
            };
            let result = handle
                .execute(identity, digest, 20, 1024, 1024, |tx| {
                    tx.execute("UPDATE counter SET value=value+1", [])?;
                    Ok(HandlerOutcome::Success(b"managed".to_vec()))
                })
                .await
                .unwrap();
            results.push((byte, identity, result));
        }
        for (byte, identity, result) in &results {
            let handle = &cells[usize::from(byte % 2)].3;
            assert_eq!(
                handle
                    .execute(*identity, digest, 21, 1024, 1024, |_| panic!(
                        "checkpointed retry must not execute again"
                    ))
                    .await
                    .unwrap(),
                *result
            );
        }
        for (_, _, _, handle) in &cells {
            assert_eq!(
                handle
                    .query(1024, 1024, |connection| Ok(connection
                        .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                        .to_le_bytes()
                        .to_vec()))
                    .await
                    .unwrap(),
                i64::from(per_cell + 3).to_le_bytes()
            );
        }
        selected_frames += 6;
    }
    let expected_per_cell = per_cell + if checkpoint_continuation { 3 } else { 0 };
    let shutdown = runtime.shutdown();
    tokio::pin!(shutdown);
    if preparation.is_some() {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );
    }
    drop(preparation);
    peers.released.store(true, Ordering::Release);
    peers.changed.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut shutdown)
        .await
        .unwrap()
        .unwrap();
    for (target, cell_authority, replica, _) in cells {
        let control = cell_authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(control.value().state, ControlState::Idle);
        assert!(control.value().bundle_binding.is_none());
        let root = control.value().ltx_root().unwrap();
        assert_eq!(root.commit_sequence, u64::from(expected_per_cell));
        let path = f
            .scratch
            .path()
            .join(format!("cold-{}.sqlite", root.incarnation[0]));
        replica
            .open_root(&root)
            .await
            .unwrap()
            .restore(&path)
            .await
            .unwrap();
        let cold = cellule_ltx::rusqlite::Connection::open(path).unwrap();
        assert_eq!(
            cold.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            i64::from(expected_per_cell)
        );
        assert_eq!(
            cold.query_row(
                "SELECT COUNT(*) FROM sys_requests WHERE result=?1",
                [b"managed".as_slice()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            i64::from(expected_per_cell)
        );
    }
    assert_eq!(
        durability.progress().unwrap().issued_through,
        selected_frames
    );
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
}

pub(super) async fn renew_actor_lease(authority: &Authority, lease: &NodeLeaseGuard) {
    // Advance the original guard only after a signed authoritative heartbeat.
    let mut node = authority.observed.lock().await;
    let mut next = node.advertisement().clone();
    next.issued_at_ms += 1;
    next.expires_at_ms += 1;
    next.progress += 1;
    next.signature = SigningKey::from_bytes(&[10; 32])
        .sign(&next.signing_bytes().unwrap())
        .to_bytes();
    let now = next.issued_at_ms;
    let refreshed = authority.directory.refresh(&node, next, now).await.unwrap();
    lease
        .renew(now, refreshed.advertisement().expires_at_ms())
        .unwrap();
    *node = refreshed;
}
