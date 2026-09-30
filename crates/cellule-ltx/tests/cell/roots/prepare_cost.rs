//! Local provider-cost model for one incremental Cell publish.
//!
//! The recorded capacity runs measure publication end to end but not the split
//! inside root preparation. This model counts the provider operations each
//! publish needs and prices them with the p95 latencies those runs recorded, so
//! a change to predecessor verification can be judged before it is written.

use std::time::{Duration, Instant};

use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};

/// Provider p95 latencies recorded by the 2026-09-29 write-capacity run:
/// PUT 6.7-7.5 ms, GET 1.9-2.1 ms, HEAD 1.4-1.6 ms.
const GET_MS: u64 = 2;
const PUT_MS: u64 = 7;

/// Reports provider operations and latency for three chained publishes.
///
/// Publish 1 is cold. Publishes 2 and 3 continue the previous root, which is
/// where a preparation-time predecessor verification is paid on every command.
#[tokio::test]
#[ignore = "manual provider-cost model for one incremental publish"]
async fn incremental_publish_provider_cost_model() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut writer = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    writer
        .transaction(|transaction| {
            transaction.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL);\
                 INSERT INTO counter VALUES(0);\
                 CREATE TABLE payload(value BLOB NOT NULL);\
                 INSERT INTO payload VALUES(randomblob(4000000))",
            )
        })
        .unwrap();

    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let config = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(GET_MS),
        wait_put_per_call: Duration::from_millis(PUT_MS),
        ..ThrottleConfig::default()
    };
    let provider = Arc::new(ThrottledStore::new(Arc::clone(&counted), config));
    let replica = replica(Store::new(provider), [51; 32], [52; 16]);

    let mut root = replica
        .prepare(None, &writer.capture().unwrap(), 1, 1)
        .await
        .unwrap()
        .root();
    println!("publish 1 (cold): {counted:?}");

    for sequence in 2..=3 {
        writer
            .transaction(|transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(())
            })
            .unwrap();
        let cuts = writer.capture().unwrap();
        counted.reset();
        let started = Instant::now();
        let prepared = replica
            .prepare(Some(&root), &cuts, sequence, 1)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let counts = counted.counts();
        let puts = counted.put_requests();
        let modeled = Duration::from_millis(
            (counts.heads as u64 + counts.ranges as u64 + counts.full as u64) * GET_MS
                + puts as u64 * PUT_MS,
        );
        println!(
            "publish {sequence}: {elapsed:?} (modeled {modeled:?}) \
             heads={} ranges={} full={} puts={}",
            counts.heads, counts.ranges, counts.full, puts
        );
        root = prepared.root();
    }
    writer.close().unwrap();
}
