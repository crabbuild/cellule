//! The real actor ACK/read/retry path, with original authority and durable peers.
use super::*;
use crate::cell::actor::CellRuntime;
use crate::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use crate::cell::executor::{HandlerOutcome, MutationIdentity, Resolution};
use crate::cell::worker::SqlWorkerPool;
use crate::fleet::telemetry::{CellTelemetry, CommandResponseSource};
use crate::identity::{CellTarget, NamespaceId, RequestId, TenantId};
use crate::node::durability::{NodeBundleAuthority, NodeDurability, NodeLogAuthority};
use crate::node::log_shipper::NodeLogShipper;
use crate::node::log_transport::{
    AppendRequest, LocalFollowerTransport, NodeLogTransport, RetireRequest, SealRequest,
    TailRequest,
};
use futures_util::future::BoxFuture;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(super) struct Authority {
    pub(super) directory: NodeDirectory,
    pub(super) observed: tokio::sync::Mutex<VersionedNodeAdvertisement>,
}

impl NodeBundleAuthority for Authority {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        Box::pin(async move {
            let mut node = self.observed.lock().await;
            let (next, pinned) = self
                .directory
                .bind_bundle_cell(&node, authority, observed, NOW)
                .await?;
            *node = next;
            Ok(pinned)
        })
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut node = self.observed.lock().await;
            let proof = self
                .directory
                .load_bundle_coverage(authority, observed, Limits::default())
                .await?;
            *node = self
                .directory
                .checkpoint_bundle_cell(&node, authority, &proof, Limits::default(), NOW)
                .await?;
            *node = self
                .directory
                .begin_bundle_close(&node, proof.binding(), issued, NOW)
                .await?;
            *node = self
                .directory
                .finish_bundle_close(&node, proof.binding(), issued, NOW)
                .await?;
            Ok(())
        })
    }
}

impl NodeLogAuthority for Authority {
    fn activate<'a>(&'a self, _: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut node = self.observed.lock().await;
            *node = self.directory.activate_log(&node, NOW).await?;
            Ok(())
        })
    }
    fn advance_coverage<'a>(&'a self, _: u64, through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut node = self.observed.lock().await;
            *node = self
                .directory
                .advance_log_coverage(&node, through, NOW)
                .await?;
            Ok(())
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            retirement.confirmed()?;
            let mut node = self.observed.lock().await;
            *node = self
                .directory
                .close_log(&node, retirement.barrier(), NOW)
                .await?;
            Ok(())
        })
    }
}

pub(super) struct PausedFollowers {
    peers: [LocalFollowerTransport; 2],
    released: AtomicBool,
    changed: tokio::sync::Notify,
}

pub(super) fn transport(f: &Fixture, released: bool) -> Arc<PausedFollowers> {
    let peers = [2, 3].map(|byte| {
        let store = crate::follower::FollowerStore::open(
            f.scratch.path().join(format!("follower-{byte}")),
            Limits::default(),
            cellule_ltx::DiskBudget::new(32 << 20),
        )
        .unwrap();
        LocalFollowerTransport::new(NodeId::from_bytes([byte; 16]), store)
    });
    Arc::new(PausedFollowers {
        peers,
        released: AtomicBool::new(released),
        changed: tokio::sync::Notify::new(),
    })
}

impl NodeLogTransport for PausedFollowers {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        Box::pin(async move {
            while !self.released.load(Ordering::Acquire) {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if !self.released.load(Ordering::Acquire) {
                    changed.await;
                }
            }
            let index = usize::from(member == NodeId::from_bytes([3; 16]));
            self.peers[index].append(member, request).await
        })
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        self.peers[usize::from(member == NodeId::from_bytes([3; 16]))].seal(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        self.peers[usize::from(member == NodeId::from_bytes([3; 16]))].retire(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        self.peers[usize::from(member == NodeId::from_bytes([3; 16]))].tail(member, request)
    }
}

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

#[tokio::test(flavor = "multi_thread")]
async fn selected_complete_capture_acks_and_reads_before_root_cas_then_joins_drain_and_restores() {
    actor_case(false, false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prior_fleet_ack_retains_complete_issued_binding_until_joined_root_and_catalog_closure() {
    actor_case(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_bundle_caller_retains_mutation_and_retry_until_joined_drain() {
    actor_case(false, true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn selected_capture_releases_files_before_root_cas_and_joins_origin_materialization() {
    actor_case(false, false, true).await;
}

async fn actor_case(prior_fleet: bool, cancel_caller: bool, early_selection: bool) {
    let backend = Arc::new(super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(backend.clone()).await;
    super::coverage::enroll(&mut f).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let transport = transport(&f, false);
    let shipper =
        NodeLogShipper::new(f.gate.clone(), transport.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        transport.clone(),
        f.lease.clone(),
    ));
    let mut feed = durability
        .enable_bundle_publication(authority.clone())
        .unwrap();
    let pool = SqlWorkerPool::new(2, 4).unwrap();
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        pool.clone(),
        32 << 20,
        SessionId::from_bytes([1; 16]),
        cellule_ltx::Host::default().with_dirty_slots(dirty.clone()),
    )
    .unwrap();
    runtime.install_node_lease(f.lease.clone()).unwrap();
    runtime
        .install_node_durability(ApplicationId::from_bytes([9; 16]), durability.clone())
        .unwrap();
    let responses = Arc::new(Responses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([9; 16]),
        NamespaceId::from_bytes([13; 16]),
        b"bundle-actor",
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
            IncarnationId::from_bytes([4; 16]),
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
        [4; 16],
        Limits::default(),
    )
    .unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            replica.clone(),
            cell_authority.clone(),
            control,
            f.scratch.path().join("actor.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    runtime.active_catalog_entries().await.unwrap();
    let before = cell_authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = before.value().ltx_root().unwrap();
    assert!(before.value().bundle_binding.is_some());
    let preparation_hold = if early_selection {
        Some(dirty.clone().acquire_owned().await.unwrap())
    } else {
        None
    };
    backend.mode.store(9, Ordering::SeqCst);
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([8; 16]),
        issued_at_ms: 10,
        expires_at_ms: 10_000,
    };
    let digest = Digest::from_bytes([9; 32]);
    let client = handle.clone();
    let command = tokio::spawn(async move {
        client
            .execute(identity, digest, 20, 1024, 1024, |tx| {
                tx.execute("UPDATE counter SET value=value+1", [])?;
                Ok(HandlerOutcome::Success(b"selected".to_vec()))
            })
            .await
    });
    let mut command = Some(command);
    let capture = tokio::time::timeout(std::time::Duration::from_secs(5), feed.recv())
        .await
        .unwrap()
        .unwrap();
    if !early_selection {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            backend.pin_started.notified(),
        )
        .await
        .unwrap();
    }
    let captured_paths = pool
        .pending(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .cuts()
        .segments
        .iter()
        .map(|segment| segment.path().to_owned())
        .collect::<Vec<_>>();
    assert!(!captured_paths.is_empty());
    assert!(!command.as_ref().unwrap().is_finished());
    let prior_outcome = if prior_fleet {
        transport.released.store(true, Ordering::Release);
        transport.changed.notify_waiters();
        Some(
            tokio::time::timeout(std::time::Duration::from_secs(5), command.take().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        )
    } else {
        if cancel_caller {
            command.as_ref().unwrap().abort();
        }
        None
    };
    let publication = {
        let mut node = authority.observed.lock().await;
        let prepared = authority
            .directory
            .prepare_node_bundle(&node, capture.frames(), &[capture.assignment()], NOW)
            .await
            .unwrap();
        let (next, proofs) = authority
            .directory
            .select_node_bundle(&node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .unwrap();
        *node = next;
        durability
            .confirm_selected_captures(std::slice::from_ref(&capture), proofs)
            .unwrap()
    };
    drop(preparation_hold);
    if early_selection {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            backend.pin_started.notified(),
        )
        .await
        .unwrap();
        assert!(
            pool.pending(target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .cuts()
                .segments
                .is_empty()
        );
        assert!(
            captured_paths.iter().all(|path| !path.exists()),
            "exact selected capture must release disk before root CAS"
        );
    }
    let outcome = if let Some(outcome) = prior_outcome {
        outcome
    } else if cancel_caller {
        assert!(command.take().unwrap().await.unwrap_err().is_cancelled());
        handle
            .execute(identity, digest, 21, 1024, 1024, |_| {
                panic!("cancelled retry must not execute again")
            })
            .await
            .unwrap()
    } else {
        tokio::time::timeout(std::time::Duration::from_secs(5), command.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    };
    assert_eq!(outcome.commit_sequence(), 1);
    let mut suffix_publication = None;
    let mut suffix_capture = None;
    if early_selection {
        let second_handle = handle.clone();
        let second = tokio::spawn(async move {
            second_handle
                .execute(
                    MutationIdentity {
                        request_id: RequestId::from_bytes([9; 16]),
                        ..identity
                    },
                    digest,
                    22,
                    1024,
                    1024,
                    |tx| {
                        tx.execute("UPDATE counter SET value=2", [])?;
                        Ok(HandlerOutcome::Success(b"later".to_vec()))
                    },
                )
                .await
        });
        let capture = tokio::time::timeout(std::time::Duration::from_secs(5), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!second.is_finished());
        let unproven = pool
            .query(
                target.cell_id(),
                1024,
                crate::cell::worker::SqlDeadline::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                ),
                Box::new(|_| panic!("unproven suffix must not reach a query handler")),
            )
            .await;
        assert!(matches!(unproven, Err(Error::PendingPublication)));
        let selected = {
            let mut node = authority.observed.lock().await;
            let prepared = authority
                .directory
                .prepare_node_bundle(&node, capture.frames(), &[capture.assignment()], NOW)
                .await
                .unwrap();
            let (next, proofs) = authority
                .directory
                .select_node_bundle(&node, &prepared, &f.lease, Limits::default(), NOW)
                .await
                .unwrap();
            *node = next;
            durability
                .confirm_selected_captures(std::slice::from_ref(&capture), proofs)
                .unwrap()
        };
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), second)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .commit_sequence(),
            2
        );
        suffix_publication = Some(selected);
        suffix_capture = Some(capture);
    }
    runtime.active_catalog_entries().await.unwrap();
    let expected_responses = if early_selection {
        vec![CommandResponseSource::Bundle, CommandResponseSource::Bundle]
    } else {
        vec![if prior_fleet {
            CommandResponseSource::Fleet
        } else if cancel_caller {
            CommandResponseSource::Recorded
        } else {
            CommandResponseSource::Bundle
        }]
    };
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        expected_responses.as_slice()
    );
    assert_eq!(
        cell_authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root(),
        Some(root)
    );
    assert_eq!(
        handle
            .query(1024, 1024, |connection| Ok(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_le_bytes()
                .to_vec()))
            .await
            .unwrap(),
        (if early_selection { 2_i64 } else { 1_i64 }).to_le_bytes()
    );
    assert_eq!(
        handle.resolve(identity, digest, 21, 1024).await.unwrap(),
        Resolution::Committed(outcome.clone())
    );
    assert_eq!(
        handle
            .execute(identity, digest, 21, 1024, 1024, |_| panic!(
                "retry must use the selected outcome"
            ))
            .await
            .unwrap(),
        outcome
    );
    let drain = handle.drain();
    tokio::pin!(drain);
    let early = tokio::time::timeout(std::time::Duration::from_millis(50), &mut drain).await;
    backend.pin_resume.notify_one();
    transport.released.store(true, Ordering::Release);
    transport.changed.notify_waiters();
    assert!(
        early.is_err(),
        "selected ACK cannot discharge root/drain obligations"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut drain)
        .await
        .unwrap()
        .unwrap();
    let control = cell_authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(control.value().bundle_binding.is_none());
    assert_eq!(control.value().state, ControlState::Idle);
    let selected_root = control.value().ltx_root().unwrap();
    assert_eq!(
        selected_root.commit_sequence,
        if early_selection { 2 } else { 1 }
    );
    let restored = f.scratch.path().join("cold.sqlite");
    replica
        .open_root(&selected_root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        if early_selection { 2 } else { 1 }
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id=?1",
                [identity.request_id.as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
        b"selected"
    );
    drop(connection);
    drop(publication);
    drop(capture);
    drop(suffix_publication);
    drop(suffix_capture);
    runtime.shutdown().await.unwrap();
    assert!(feed.recv().await.is_none());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
}
