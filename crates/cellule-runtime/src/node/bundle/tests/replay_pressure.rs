//! Original Fleet outcomes remain replayable when mutation debt is full.
use super::actor::{Authority, transport};
use super::readiness::DelayedSelection;
use super::*;
use crate::cell::actor::CellRuntime;
use crate::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use crate::cell::executor::{HandlerOutcome, MAX_PENDING_PUBLICATIONS, MutationIdentity};
use crate::cell::worker::SqlWorkerPool;
use crate::identity::{CellTarget, NamespaceId, RequestId, TenantId};
use crate::node::durability::NodeDurability;
use crate::node::log_shipper::NodeLogShipper;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[tokio::test]
async fn replay_admission_retains_fleet_outcomes_while_new_writes_are_refused() {
    pressure_case(true).await;
}

#[tokio::test]
async fn replay_admission_retains_fleet_outcomes_at_the_cell_debt_limit() {
    pressure_case(false).await;
}

async fn pressure_case(node_pressure: bool) {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let authority = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let delayed = Arc::new(DelayedSelection {
        authority: authority.clone(),
        held: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
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
    let pool = SqlWorkerPool::new(2, 4).unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        pool.clone(),
        64 << 20,
        SessionId::from_bytes([1; 16]),
        cellule_ltx::Host::default(),
    )
    .unwrap();
    runtime.install_node_lease(f.lease.clone()).unwrap();
    runtime
        .install_node_durability(ApplicationId::from_bytes([9; 16]), durability.clone())
        .unwrap();
    durability
        .start_bundle_publication(delayed.clone())
        .unwrap();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([9; 16]),
        NamespaceId::from_bytes([13; 16]),
        b"replay-pressure",
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
            f.scratch.path().join("pressure.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    delayed.held.store(true, Ordering::Release);
    let identity = |byte| MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: 10,
        expires_at_ms: 10_000,
    };
    let digest = Digest::from_bytes([9; 32]);
    let first = handle
        .execute(identity(1), digest, 20, 1024, 1024, |tx| {
            tx.execute("UPDATE counter SET value=value+1", [])?;
            Ok(HandlerOutcome::Success(b"ready".to_vec()))
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), delayed.entered.notified())
        .await
        .unwrap();
    // A real follower proof released this response; selection is still held.
    assert_eq!(
        f.gate.progress().unwrap().follower_proven_through,
        f.gate.progress().unwrap().issued_through
    );
    let resources = pool.resource_ledger();
    if node_pressure {
        let snapshot = resources.snapshot().unwrap();
        let pressure = runtime
            .try_reserve_node_metadata_bytes(
                snapshot.limit.retained_bytes() * 3 / 4 - snapshot.used.retained_bytes(),
            )
            .unwrap();
        let replay = tokio::time::timeout(
            Duration::from_secs(3),
            handle.execute(identity(1), digest, 21, 1024, 1024, |_| {
                panic!("retry executed under node pressure")
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(replay, first);
        assert!(matches!(
            handle
                .execute(identity(200), digest, 21, 1024, 1024, |_| {
                    panic!("new mutation executed under node pressure")
                })
                .await,
            Err(Error::Capacity("publication backlog"))
        ));
        assert!(matches!(
            handle
                .execute(
                    identity(1),
                    Digest::from_bytes([8; 32]),
                    21,
                    1024,
                    1024,
                    |_| { panic!("conflicting retry executed") }
                )
                .await,
            Err(Error::RequestConflict)
        ));
        assert!(matches!(
            handle
                .execute(identity(1), digest, 21, 1024, 1, |_| {
                    panic!("oversized retry executed")
                })
                .await,
            Err(Error::Command("stored result exceeds command limit"))
        ));
        assert!(matches!(
            handle
                .execute(identity(1), digest, 10_000, 1024, 1024, |_| {
                    panic!("expired retry executed")
                })
                .await,
            Err(Error::Command("invalid mutation identity lifetime"))
        ));
        drop(pressure);
    }
    // Independently fill the unchanged per-Cell physical debt limit. A FIFO
    // retry at its head must not wait for root publication or block a query.
    for byte in 2..=MAX_PENDING_PUBLICATIONS as u8 {
        handle
            .execute(identity(byte), digest, 21, 1024, 1024, |tx| {
                tx.execute("UPDATE counter SET value=value+1", [])?;
                Ok(HandlerOutcome::Success(b"ready".to_vec()))
            })
            .await
            .unwrap();
    }
    assert_eq!(
        runtime
            .publication_progress()
            .await
            .unwrap()
            .pending_publications,
        MAX_PENDING_PUBLICATIONS
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            handle.execute(identity(1), digest, 22, 1024, 1024, |_| panic!(
                "retry executed under Cell pressure"
            ),)
        )
        .await
        .unwrap()
        .unwrap(),
        first
    );
    assert!(matches!(
        handle
            .execute(identity(1), digest, 10_000, 1024, 1024, |_| {
                panic!("expired retry executed at the Cell debt limit")
            })
            .await,
        Err(Error::Command("invalid mutation identity lifetime"))
    ));
    assert_eq!(
        handle
            .query(1024, 1024, |db| Ok(db
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0),)?
                .to_le_bytes()
                .to_vec()))
            .await
            .unwrap(),
        (MAX_PENDING_PUBLICATIONS as i64).to_le_bytes()
    );
    let ran = Arc::new(AtomicBool::new(false));
    let executed = ran.clone();
    let fresh = handle.execute(identity(201), digest, 22, 1024, 1024, move |tx| {
        assert!(!executed.swap(true, Ordering::SeqCst));
        tx.execute("UPDATE counter SET value=value+1", [])?;
        Ok(HandlerOutcome::Success(b"accepted".to_vec()))
    });
    tokio::pin!(fresh);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fresh.as_mut())
            .await
            .is_err()
    );
    assert!(!ran.load(Ordering::SeqCst));
    delayed.held.store(false, Ordering::Release);
    delayed.changed.notify_waiters();
    let accepted = tokio::time::timeout(Duration::from_secs(5), fresh.as_mut())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        accepted.commit_sequence(),
        MAX_PENDING_PUBLICATIONS as u64 + 1
    );
    assert!(ran.load(Ordering::SeqCst));
    super::managed::renew_actor_lease(&authority, &f.lease).await;
    tokio::time::timeout(Duration::from_secs(15), runtime.shutdown())
        .await
        .unwrap()
        .unwrap();
    let control = cell_authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().state, ControlState::Idle);
    assert!(control.value().bundle_binding.is_none());
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, MAX_PENDING_PUBLICATIONS as u64 + 1);
    let path = f.scratch.path().join("cold-pressure.sqlite");
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
        MAX_PENDING_PUBLICATIONS as i64 + 1
    );
    assert_eq!(
        cold.query_row("SELECT COUNT(*) FROM sys_requests", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        MAX_PENDING_PUBLICATIONS as i64 + 1
    );
    assert_eq!(resources.snapshot().unwrap().used.retained_bytes(), 0);
}
