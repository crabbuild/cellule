//! Manual measured comparison of the original serial adapter and bounded cohorts.
use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger};
use crate::node::durability::{NodeBundleAuthority, NodeDurability};
use crate::node::log_shipper::NodeLogShipper;
use futures_util::future::BoxFuture;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::{Duration, Instant};

pub(super) struct Serial(pub(super) Arc<super::actor::Authority>);

impl NodeBundleAuthority for Serial {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        control: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        self.0.bind(authority, control)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        control: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        self.0.close(authority, control, issued)
    }
}

#[tokio::test]
#[ignore = "manual controlled-latency enrollment diagnostic, not production capacity"]
async fn enrollment_latency_comparison() {
    for delay in [0, 5, 20] {
        for cells in [1, 8] {
            for sample in 0..3 {
                // Alternate ordering to keep host drift from favoring a mode.
                for coalesce in if sample % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    measure(delay, cells, sample, coalesce).await;
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
    let mut cells = Vec::new();
    for number in 4..4 + count {
        cells.push(f.unbound_cell_at_commit(number, [9; 16], 0).await);
    }
    let original = Arc::new(super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let authority: Arc<dyn NodeBundleAuthority> = if coalesce {
        original.clone()
    } else {
        Arc::new(Serial(original.clone()))
    };
    let peers = super::actor::transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        original.clone(),
        peers,
        f.lease.clone(),
    ));
    let resources = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(64 << 20));
    durability
        .attach_selection_resources(resources.clone())
        .unwrap();
    let feed = durability.enable_bundle_publication(authority).unwrap();
    f.count.reset();
    let started = Instant::now();
    let results = futures_util::future::join_all(cells.iter().map(|cell| {
        let durability = &durability;
        async move {
            let result = durability
                .bind_bundle_cell(&cell.authority, &cell.control)
                .await;
            (result.unwrap(), started.elapsed().as_secs_f64() * 1000.0)
        }
    }))
    .await;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let puts = f.count.put_requests();
    let reads = f.count.counts();
    let latencies = results
        .iter()
        .map(|(_, latency)| *latency)
        .collect::<Vec<_>>();
    assert_eq!(
        puts,
        if coalesce {
            count as usize + 4
        } else {
            5 * count as usize
        }
    );
    eprintln!(
        "enrollment_measurement {}",
        serde_json::json!({
            "mode": if coalesce { "cohort" } else { "serial" },
            "delay_ms": delay, "cells": count, "sample": sample,
            "elapsed_ms": elapsed, "latencies_ms": latencies,
            "puts": puts, "gets": reads.body_requests(), "heads": reads.heads,
        })
    );
    for (cell, (control, _)) in cells.iter_mut().zip(results) {
        cell.control = control;
        let issued = f
            .gate
            .close_cell_issuance(
                Fixture::scope(cell),
                cell.control.value().ltx_root().unwrap(),
            )
            .unwrap();
        original
            .close(&cell.authority, &cell.control, issued)
            .await
            .unwrap();
    }
    drop(feed);
    durability.shutdown().await.unwrap();
    assert_eq!(resources.snapshot().unwrap().used.retained_bytes(), 0);
}
