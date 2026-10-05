//! Controlled provider cost for coalesced cuts and compaction-plus-append.

use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "manual fixed-latency batch/compaction diagnostic; not node capacity qualification"]
async fn captured_batch_provider_cost() {
    for cuts in [1, 4, 16] {
        for compact in [false, true] {
            for repetition in 0..3 {
                measure(cuts, compact, repetition).await;
            }
        }
    }
}

async fn measure(cut_count: u64, compact: bool, repetition: u64) {
    let source = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut writer = Db::open(&source.path().join("writer.sqlite"), Limits::default()).unwrap();
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let provider = Arc::new(ThrottledStore::new(
        counted.clone(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(7),
            wait_put_per_call: Duration::from_millis(40),
            ..ThrottleConfig::default()
        },
    ));
    let cell = replica(Store::new(provider), [235; 32], [236; 16]);
    let mut root = None;
    let mut all = Vec::new();
    for sequence in 1..=8 {
        writer
            .transaction(|tx| {
                if sequence == 1 {
                    tx.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(0)")?;
                }
                tx.execute("UPDATE counter SET v=?1", [sequence])?;
                Ok(())
            })
            .unwrap();
        let captured = writer.capture().unwrap();
        all.extend(captured.segments.clone());
        root = Some(
            cell.prepare(root.as_ref(), &captured, sequence, 1)
                .await
                .unwrap()
                .root(),
        );
    }
    let root = root.unwrap();
    let mut batch = None;
    for sequence in 9..=8 + cut_count {
        writer
            .transaction(|tx| {
                tx.execute("UPDATE counter SET v=?1", [sequence])?;
                Ok(())
            })
            .unwrap();
        let captured = writer.capture().unwrap();
        all.extend(captured.segments.clone());
        let current = batch.get_or_insert_with(|| CaptureBatch {
            segments: Vec::new(),
            position: captured.position,
            timing: CaptureTiming::default(),
        });
        current.position = captured.position;
        current.segments.extend(captured.segments);
    }
    let batch = batch.unwrap();
    assert_eq!(batch.segments.len() as u64, cut_count);
    counted.reset();
    let _ = cell.take_publication_cost();
    let started = Instant::now();
    let prepared = if compact {
        cell.prepare_scheduled_compaction_append(
            &root,
            &batch,
            8 + cut_count,
            1,
            32,
            scratch.path(),
        )
        .await
        .unwrap()
        .unwrap()
    } else {
        cell.prepare(Some(&root), &batch, 8 + cut_count, 1)
            .await
            .unwrap()
    };
    let elapsed = started.elapsed();
    let reads = counted.counts();
    let cost = cell.take_publication_cost();
    let result = serde_json::json!({
        "cuts": cut_count, "compaction": compact, "repetition": repetition,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "puts": counted.put_requests(), "heads": reads.heads,
        "ranges": reads.ranges, "full_gets": reads.full,
        "objects": cost.objects, "bytes": cost.bytes,
        "final_segments": prepared.verified().segment_count(),
        "synthetic_get_ms": 7, "synthetic_put_ms": 40,
    });
    writer.close().unwrap();
    let expected = source.path().join("expected.sqlite");
    restore_exact(
        &VerifiedPlan::new(&all, batch.position, Limits::default()).unwrap(),
        &expected,
    )
    .unwrap();
    let restored = source.path().join("restored.sqlite");
    cell.open_root(&prepared.root())
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(restored).unwrap(),
        std::fs::read(expected).unwrap()
    );
    assert_eq!(prepared.root().position, batch.position);
    assert_eq!(prepared.root().commit_sequence, 8 + cut_count);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    println!("BATCH_COST {result}");
}
