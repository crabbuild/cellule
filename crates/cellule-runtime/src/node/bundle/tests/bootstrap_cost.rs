//! Full fresh-Cell catalog/owner/SQLite/root/enrollment/first-read diagnostic.
use super::*;
use crate::cell::{
    actor::CellRuntime,
    catalog::{CatalogEntry, CatalogRole, CellCatalog},
    worker::SqlWorkerPool,
};
use crate::identity::{CellTarget, NamespaceId, TenantId};
use crate::node::{
    durability::{NodeBundleAuthority, NodeDurability},
    log_shipper::NodeLogShipper,
};
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "manual fresh SQLite/bootstrap/first-read diagnostic, not production capacity"]
async fn bootstrap_first_read_comparison() {
    for delay in [5, 20] {
        for count in [1, 8] {
            for sample in 0..3 {
                for coalesce in if sample % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    measure(delay, count, sample, coalesce).await;
                }
            }
        }
    }
}

async fn measure(delay: u64, count: u8, sample: usize, coalesce: bool) {
    let backend = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(delay),
            wait_put_per_call: Duration::from_millis(delay),
            ..Default::default()
        },
    ));
    let mut f = Fixture::with_store(backend).await;
    super::coverage::enroll(&mut f).await;
    let original = Arc::new(super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let authority: Arc<dyn NodeBundleAuthority> = if coalesce {
        original.clone()
    } else {
        Arc::new(super::enrollment_cost::Serial(original.clone()))
    };
    let peers = super::actor::transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        original,
        peers,
        f.lease.clone(),
    ));
    let feed = durability.enable_bundle_publication(authority).unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(2, count as usize).unwrap(),
        32 << 20,
        SessionId::from_bytes([1; 16]),
        cellule_ltx::Host::default(),
    )
    .unwrap();
    runtime.install_node_lease(f.lease.clone()).unwrap();
    runtime
        .install_node_durability(ApplicationId::from_bytes([9; 16]), durability.clone())
        .unwrap();
    let catalog = CellCatalog::new(f.layout.clone(), TenantId::from_bytes([1; 16]));
    let cell_authority = CellAuthority::new(f.layout.clone());
    f.count.reset();
    let started = Instant::now();
    let results = futures_util::future::join_all((0..count).map(|number| {
        let runtime = &runtime;
        let catalog = &catalog;
        let cell_authority = &cell_authority;
        let f = &f;
        async move {
            let target = CellTarget::new(TenantId::from_bytes([1;16]), ApplicationId::from_bytes([9;16]), NamespaceId::from_bytes([13;16]), &[number]).unwrap();
            let proof = catalog.provision(CatalogEntry::new(&target, CatalogRole::Application, Digest::from_bytes([12;32]), 1).unwrap()).await.unwrap();
            assert!(cell_authority.load(target.cell_id()).await.unwrap().is_none());
            let control = cell_authority.create_initial(&proof, IncarnationId::from_bytes([number+10;16]), Owner {
                session: SessionId::from_bytes([1;16]), endpoint: "https://bundle.internal:8081".into(),
            }).await.unwrap();
            let replica = CellReplica::new(f.layout.clone(), *target.cell_id().as_bytes(), *control.value().incarnation.as_bytes(), Limits::default()).unwrap();
            let handle = runtime.bootstrap(proof, replica.clone(), cell_authority.clone(), control, f.scratch.path().join(format!("first-{number}.sqlite")), |tx| {
                tx.execute_batch("CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)")?;
                Ok(())
            }).await.unwrap();
            let value = handle.query(0, 8, |db| {
                let value: u64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_le_bytes().to_vec())
            }).await.unwrap();
            assert_eq!(value, 0_u64.to_le_bytes());
            (handle, replica, started.elapsed().as_secs_f64()*1000.0)
        }
    })).await;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let puts = f.count.put_requests();
    let reads = f.count.counts();
    let latencies = results.iter().map(|(_, _, ms)| *ms).collect::<Vec<_>>();
    f.count.reset();
    for (handle, _, _) in &results {
        assert_eq!(handle.query(0, 1, |_| Ok(vec![0])).await.unwrap(), vec![0]);
    }
    assert_eq!(
        f.count.put_requests() + f.count.counts().body_requests() + f.count.counts().heads,
        0,
        "warm leased reads must add no object-store work"
    );
    eprintln!(
        "bootstrap_measurement {}",
        serde_json::json!({
            "mode": if coalesce { "cohort" } else { "serial" }, "delay_ms": delay,
            "cells": count, "sample": sample, "elapsed_ms": elapsed,
            "latencies_ms": latencies, "puts": puts, "gets": reads.body_requests(),
            "warm_read_object_operations": 0,
        })
    );
    for (handle, replica, _) in results {
        handle.drain().await.unwrap();
        let control = cell_authority
            .load(handle.cell_id())
            .await
            .unwrap()
            .unwrap();
        let path = f.scratch.path().join(format!(
            "restored-{}.sqlite",
            crate::identity::encode_hex(handle.cell_id().as_bytes())
        ));
        replica
            .open_root(&control.value().ltx_root().unwrap())
            .await
            .unwrap()
            .restore(&path)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(path).unwrap();
        let value: u64 = db
            .query_row("SELECT value FROM counter", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, 0);
    }
    runtime.shutdown().await.unwrap();
    drop(feed);
    durability.shutdown().await.unwrap();
}
