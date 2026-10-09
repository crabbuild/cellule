use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger};
use crate::fleet::telemetry::{CellTelemetry, CellTelemetryHandle, SharedPublicationTiming};
use crate::publication::SharedPublication;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct SharedEvidence {
    cells: AtomicU64,
    pressure: AtomicU64,
}

impl CellTelemetry for SharedEvidence {
    fn shared_publication(&self, timing: SharedPublicationTiming) {
        assert!(timing.succeeded);
        self.cells.fetch_add(timing.cells, Ordering::SeqCst);
    }

    fn shared_publication_singleton(&self, _: std::time::Duration) {
        self.cells.fetch_add(1, Ordering::SeqCst);
    }

    fn shared_publication_fallback(&self, pressure: bool) {
        assert!(
            pressure,
            "these small verified overlays fit the original byte bound"
        );
        self.pressure.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn selected_bundle_materialization_enters_shared_producer_and_recovers_exactly() {
    materialize(8 << 20, 8, 0).await;
}

#[tokio::test]
async fn selected_bundle_materialization_falls_back_under_original_memory_pressure() {
    materialize(1, 0, 8).await;
}

async fn materialize(memory: usize, shared_cells: u64, pressure: u64) {
    let mut f = Fixture::new().await;
    let host = cellule_ltx::Host::default()
        .with_local_disk_budget(cellule_ltx::DiskBudget::new(8 << 20))
        .with_io_slots(Arc::new(tokio::sync::Semaphore::new(1)))
        .with_job_slots(Arc::new(tokio::sync::Semaphore::new(1)))
        .with_dirty_slots(Arc::new(tokio::sync::Semaphore::new(1)))
        .with_recovery_slots(Arc::new(tokio::sync::Semaphore::new(1)))
        .with_scratch_slots(Arc::new(tokio::sync::Semaphore::new(1)));
    let disk = host.local_disk_budget();
    let ledger = ResourceLedger::new(
        ResourceCost::zero()
            .with_retained_bytes(memory)
            .with_publication_file_descriptors(64),
    );
    let evidence = Arc::new(SharedEvidence::default());
    let coordinator = SharedPublication::new(
        ledger.clone(),
        CellTelemetryHandle::from_sink(evidence.clone()),
    );
    let mut cells = Vec::new();
    let mut frames = Vec::new();
    let mut assignments = Vec::new();
    for byte in 4..12 {
        let mut cell = f.cell(byte).await;
        cell.replica = cell.replica.with_host(host.clone());
        let (_, next, assigned) = f.append(&mut cell, 2);
        frames.extend(next);
        assignments.push(assigned);
        cells.push(cell);
    }
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assignments, NOW)
        .await
        .unwrap();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    let mut tasks = FuturesUnordered::new();
    for cell in &cells {
        let proof = proofs
            .iter()
            .find(|proof| proof.binding.control.cell == cell.control.value().cell)
            .unwrap();
        let mut publisher = f
            .publisher(cell)
            .with_shared_publication(coordinator.clone());
        let expected_cell = cell.control.value().cell;
        tasks.push(async move {
            let root = publisher.materialize_bundle(proof).await.unwrap();
            (expected_cell, root)
        });
    }
    let mut roots = Vec::new();
    while let Some(result) = tokio::time::timeout(std::time::Duration::from_secs(5), tasks.next())
        .await
        .unwrap()
    {
        roots.push(result);
    }
    assert_eq!(roots.len(), 8);
    coordinator.shutdown().await.unwrap();
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    assert_eq!(disk.used(), 0);
    for (cell, root) in roots {
        assert_eq!(root.commit_sequence, 2);
        let original = cells
            .iter()
            .find(|candidate| candidate.control.value().cell == cell)
            .unwrap();
        let proof = proofs
            .iter()
            .find(|proof| proof.binding.control.cell == cell)
            .unwrap();
        let overlay = proof
            .recovery_overlay(original.authority.layout(), Limits::default())
            .await
            .unwrap();
        let direct = original
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let cold = CellReplica::new(
            original.authority.layout().clone(),
            root.cell,
            root.incarnation,
            Limits::default(),
        )
        .unwrap();
        cold.reachable_objects(&root).await.unwrap();
        let destination = f
            .scratch
            .path()
            .join(format!("shared-{}", cell.as_bytes()[0]));
        let canonical = f
            .scratch
            .path()
            .join(format!("direct-{}", cell.as_bytes()[0]));
        cold.open_root(&root)
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        cold.open_root(&direct.root())
            .await
            .unwrap()
            .restore(&canonical)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            std::fs::read(canonical).unwrap()
        );
        let db = rusqlite::Connection::open(destination).unwrap();
        let outcomes: Vec<(String, String)> = db
            .prepare("SELECT request,result FROM outcomes ORDER BY request")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            outcomes,
            [
                ("request-2".into(), "result-2".into()),
                ("seed".into(), "original".into())
            ]
        );
    }
    assert_eq!(evidence.cells.load(Ordering::SeqCst), shared_cells);
    assert_eq!(evidence.pressure.load(Ordering::SeqCst), pressure);
}
