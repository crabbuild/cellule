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
async fn small_roots_keep_descriptor_metadata_inline_and_check_origin() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut writer = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    writer
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(0)")
        })
        .unwrap();
    let backend = Arc::new(InMemory::new());
    let counted = Arc::new(CountingObjectStore::new(backend.clone()));
    let layout = CellStorageLayout::new(Store::new(counted.clone()), Path::from("inline"), [5; 16]);
    let cell = [153; 32];
    let incarnation = [154; 16];
    let replica = CellReplica::new(layout.clone(), cell, incarnation, Limits::default()).unwrap();
    let mut root = None;
    for sequence in 1..=32 {
        if sequence > 1 {
            writer
                .transaction(|transaction| {
                    transaction.execute_batch("UPDATE counter SET v = v + 1")
                })
                .unwrap();
        }
        let cuts = writer.capture_deferred().unwrap();
        counted.reset();
        let next = replica
            .prepare(root.as_ref(), &cuts, sequence, 1)
            .await
            .unwrap()
            .root();
        assert_eq!(
            counted.put_requests(),
            4,
            "small roots upload body, index, directory and root without a separate descriptor page"
        );
        assert_eq!(counted.counts().full, 0);
        assert_eq!(
            counted.counts().heads,
            usize::from(root.is_some()),
            "the authenticated predecessor root must still exist at origin"
        );
        assert_eq!(replica.take_publication_cost().objects, 4);
        writer.prune_captured(&cuts).unwrap();
        root = Some(next);
    }
    let root = root.unwrap();
    let objects = replica.reachable_objects(&root).await.unwrap();
    assert_eq!(
        objects
            .iter()
            .filter(|object| object.kind == CellObjectKind::Root)
            .count(),
        1,
        "the exact origin inventory has no external descriptor page"
    );
    let restored = directory.path().join("restored.sqlite");
    replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(&restored).unwrap();
    let value: u64 = connection
        .query_row("SELECT v FROM counter", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 31);
    drop(connection);

    let path =
        layout.incarnation_object_path(&cell, &incarnation, &root.digest, CellObjectKind::Root);
    backend.delete(&path).await.unwrap();
    writer
        .transaction(|transaction| transaction.execute_batch("UPDATE counter SET v = v + 1"))
        .unwrap();
    let cuts = writer.capture_deferred().unwrap();
    counted.reset();
    assert!(replica.prepare(Some(&root), &cuts, 33, 1).await.is_err());
    assert_eq!(
        counted.put_requests(),
        0,
        "missing origin root cannot justify an append"
    );
    writer.durability_barrier().unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn append_reuses_verified_descriptor_pages_and_rejects_missing_origin() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut writer = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    writer
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(0); CREATE TABLE padding(v); INSERT INTO padding VALUES(zeroblob(300000))")
        })
        .unwrap();
    // Keep the decoded working set beyond the small-delta merge bound so
    // this fixture still exercises external descriptor origin verification.
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
        4,
        "only new body, index, directory and root with an inline tail"
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
    assert_eq!(replica.take_publication_cost().objects, 4);
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

#[tokio::test]
async fn inline_tail_spills_and_returns_at_stable_page_boundaries() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut writer = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    let backend = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(backend.clone()),
        Path::from("boundaries"),
        [6; 16],
    );
    let cell = [155; 32];
    let incarnation = [156; 16];
    let replica = CellReplica::new(layout.clone(), cell, incarnation, Limits::default()).unwrap();
    let mut root = None;
    let mut segments = Vec::new();
    for sequence in 1..=129 {
        writer
            .transaction(|transaction| {
                if sequence == 1 {
                    transaction
                        .execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(0)")?;
                } else {
                    transaction.execute_batch("UPDATE counter SET v = v + 1")?;
                }
                Ok(())
            })
            .unwrap();
        let cuts = writer.capture_deferred().unwrap();
        segments.extend(cuts.segments.clone());
        let next = replica
            .prepare(root.as_ref(), &cuts, sequence, 1)
            .await
            .unwrap()
            .root();
        root = Some(next);
        let shape = match sequence {
            32 => Some((0, 32)),
            33 => Some((1, 0)),
            96 => Some((1, 0)),
            97 => Some((1, 1)),
            128 => Some((1, 32)),
            129 => Some((2, 0)),
            _ => None,
        };
        if let Some((pages, inline)) = shape {
            let path = layout.incarnation_object_path(
                &cell,
                &incarnation,
                &next.digest,
                CellObjectKind::Root,
            );
            let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wire["segment_pages"].as_array().unwrap().len(), pages);
            assert_eq!(wire["segments"].as_array().unwrap().len(), inline);
            assert!(bytes.len() <= 32 << 10);

            // A new Store identity excludes the metadata cache. The complete
            // origin inventory and restored image must agree with local LTX.
            let cold = CellReplica::new(
                CellStorageLayout::new(
                    Store::new(backend.clone()),
                    Path::from("boundaries"),
                    [6; 16],
                ),
                cell,
                incarnation,
                Limits::default(),
            )
            .unwrap();
            let objects = cold.reachable_objects(&next).await.unwrap();
            assert_eq!(
                objects
                    .iter()
                    .filter(|object| object.kind == CellObjectKind::Root)
                    .count(),
                pages + 1
            );
            let restored = directory.path().join(format!("restored-{sequence}.sqlite"));
            cold.open_root(&next)
                .await
                .unwrap()
                .restore(&restored)
                .await
                .unwrap();
            let expected = directory.path().join(format!("expected-{sequence}.sqlite"));
            let plan = VerifiedPlan::new(&segments, cuts.position, Limits::default()).unwrap();
            restore_exact(&plan, &expected).unwrap();
            assert_eq!(
                std::fs::read(restored).unwrap(),
                std::fs::read(expected).unwrap()
            );
        }
    }
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
