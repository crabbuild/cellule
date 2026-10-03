//! Local provider-cost model for one incremental Cell publish.
//!
//! The recorded capacity runs measure publication end to end but not the split
//! inside root preparation. This model counts the provider operations each
//! publish needs and prices them with the p95 latencies those runs recorded, so
//! a change to predecessor verification can be judged before it is written.
//!
//! Recorded result (2026-09-30, in-memory provider plus the throttle below):
//! an incremental publish needs **2 HEADs, 0 GETs, and 6 PUTs**. Root and
//! directory bodies come from the process metadata cache, so predecessor
//! verification costs two HEADs — roughly 3 ms of the recorded 33-38 ms
//! preparation p95 — while the six mandatory immutable uploads dominate it.
//! A predecessor-graph cache would therefore buy about a tenth of preparation
//! for a new trust assumption in the durable path, which is why it is not
//! implemented. Revisit this measurement before proposing one: it only becomes
//! worthwhile if the HEAD count grows with root shape (for example a root with
//! many segments whose pages are not cached).

use std::time::{Duration, Instant};

use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};

/// Provider p95 latencies recorded by the 2026-09-29 write-capacity run:
/// PUT 6.7-7.5 ms, GET 1.9-2.1 ms, HEAD 1.4-1.6 ms.
const GET_MS: u64 = 2;
const PUT_MS: u64 = 7;

#[tokio::test]
async fn append_reuses_verified_descriptor_pages_and_rejects_missing_origin() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut writer = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    writer
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(0)")
        })
        .unwrap();
    let mut cuts = writer.capture_deferred().unwrap();
    // Fill two complete descriptor pages. The next append changes neither.
    for _ in 1..192 {
        writer
            .transaction(|transaction| transaction.execute_batch("UPDATE counter SET v = v + 1"))
            .unwrap();
        let next = writer.capture_deferred().unwrap();
        cuts.position = next.position;
        cuts.segments.extend(next.segments);
    }
    let backend = Arc::new(InMemory::new());
    let counted = Arc::new(CountingObjectStore::new(backend.clone()));
    let layout = CellStorageLayout::new(Store::new(counted.clone()), Path::from("reuse"), [3; 16]);
    let cell = [151; 32];
    let incarnation = [152; 16];
    let replica = CellReplica::new(layout.clone(), cell, incarnation, Limits::default()).unwrap();
    let root = replica.prepare(None, &cuts, 1, 1).await.unwrap().root();
    writer.prune_captured(&cuts).unwrap();
    let _ = replica.take_publication_cost();

    writer
        .transaction(|transaction| transaction.execute_batch("UPDATE counter SET v = v + 1"))
        .unwrap();
    let next = writer.capture_deferred().unwrap();
    counted.reset();
    let successor = replica
        .prepare(Some(&root), &next, 2, 1)
        .await
        .unwrap()
        .root();
    assert_eq!(
        counted.put_requests(),
        5,
        "only new body, index, directory, descriptor page and root"
    );
    assert_eq!(
        counted.counts().full,
        0,
        "no duplicate PUT conflict verification reads"
    );
    assert_eq!(
        counted.counts().heads,
        3,
        "predecessor root and both descriptor pages still checked"
    );
    assert_eq!(replica.take_publication_cost().objects, 5);
    writer.prune_captured(&next).unwrap();

    let restored = directory.path().join("restored.sqlite");
    replica
        .open_root(&successor)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(&restored).unwrap();
    let value: u64 = connection
        .query_row("SELECT v FROM counter", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 192);
    drop(connection);

    let objects = replica.reachable_objects(&root).await.unwrap();
    let descriptor_page = objects
        .iter()
        .find(|object| object.kind == CellObjectKind::Root && object.digest != root.digest)
        .unwrap();
    let path = layout.incarnation_object_path(
        &cell,
        &incarnation,
        &descriptor_page.digest,
        descriptor_page.kind,
    );
    backend.delete(&path).await.unwrap();
    writer
        .transaction(|transaction| transaction.execute_batch("UPDATE counter SET v = v + 1"))
        .unwrap();
    let next = writer.capture_deferred().unwrap();
    counted.reset();
    assert!(
        replica
            .prepare(Some(&successor), &next, 3, 1)
            .await
            .is_err()
    );
    assert_eq!(
        counted.put_requests(),
        0,
        "missing inherited metadata cannot publish a successor"
    );
    writer.durability_barrier().unwrap();
    writer.close().unwrap();
}

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
    let cold = counted.counts();
    println!(
        "publish 1 (cold): heads={} ranges={} full={} puts={} multipart={}",
        cold.heads,
        cold.ranges,
        cold.full,
        counted.put_requests(),
        counted.multipart_starts()
    );

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
