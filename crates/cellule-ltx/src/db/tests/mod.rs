use super::*;
use crate::{VerifiedPlan, restore_exact};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
#[error("inventory rejected the command")]
struct Rejected;

struct TimingClock {
    origin: Instant,
    ticks: AtomicU64,
}

impl TimingClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            ticks: AtomicU64::new(0),
        }
    }
}

impl crate::environment::Clock for TimingClock {
    fn unix_millis(&self) -> i64 {
        123456789
    }

    fn file_age(&self, _: &Path) -> std::io::Result<Duration> {
        Ok(Duration::ZERO)
    }

    fn monotonic(&self) -> Instant {
        self.origin + Duration::from_micros(self.ticks.fetch_add(1, Ordering::Relaxed))
    }
}

#[test]
fn managed_connections_set_the_budgeted_page_cache() {
    let temp = tempfile::TempDir::new().unwrap();
    let connection = open_connection(&temp.path().join("cache.sqlite"), None).unwrap();
    let cache_kib: i64 = connection
        .query_row("PRAGMA cache_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cache_kib, -MANAGED_CONNECTION_PAGE_CACHE_KIB);
}

#[test]
fn external_durability_is_explicit_and_refuses_an_uncaptured_commit() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&temp.path().join("durability.sqlite"), Limits::default()).unwrap();
    let synchronous = |db: &mut Db| {
        db.query_with(|connection| {
            connection.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
        })
        .unwrap()
    };
    assert_eq!(synchronous(&mut db), 2);
    db.transaction(|tx| tx.execute_batch("CREATE TABLE witness(value INTEGER)"))
        .unwrap();
    assert!(matches!(
        db.use_external_durability(),
        Err(LtxError::InvalidState(_))
    ));
    assert_eq!(synchronous(&mut db), 2);
    db.capture().unwrap();
    assert!(db.use_external_durability().is_err());
    db.close().unwrap();

    let mut external = Db::open(&temp.path().join("external.sqlite"), Limits::default()).unwrap();
    external.use_external_durability().unwrap();
    assert_eq!(synchronous(&mut external), 1);
    external
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE witness(value INTEGER); INSERT INTO witness VALUES(7)")
        })
        .unwrap();
    let cuts = external.capture().unwrap();
    let restored = temp.path().join("restored.sqlite");
    let plan = VerifiedPlan::new(&cuts.segments, cuts.position, Limits::default()).unwrap();
    restore_exact(&plan, &restored).unwrap();
    let connection = Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM witness", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        7
    );
    external.close().unwrap();
}

#[test]
fn external_durability_configuration_failure_fences_the_session() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&temp.path().join("configuration.sqlite"), Limits::default()).unwrap();
    // SQLite refuses a safety-level change inside a transaction. The public
    // transaction callback cannot leave one behind; inject it at this seam to
    // check the configuration error path, including refusal of subsequent SQL.
    db.writer.execute_batch("BEGIN").unwrap();
    assert!(matches!(
        db.use_external_durability(),
        Err(LtxError::Sqlite(_))
    ));
    assert!(matches!(db.transaction(|_| Ok(())), Err(LtxError::Fenced)));
    assert!(matches!(
        db.use_external_durability(),
        Err(LtxError::Fenced)
    ));
}

#[test]
fn capture_reports_deterministic_bounded_timing_for_real_ltx_work() {
    let temp = tempfile::TempDir::new().unwrap();
    let host = crate::Host::default().with_clock(Arc::new(TimingClock::new()));
    let mut db =
        Db::open_with_host(&temp.path().join("timed.sqlite"), Limits::default(), host).unwrap();
    db.transaction(|tx| {
        tx.execute_batch("CREATE TABLE events(value TEXT); INSERT INTO events VALUES ('ok')")
    })
    .unwrap();

    let batch = db.capture().unwrap();
    let phase_nanos = batch.timing.preparation_nanos
        + batch.timing.schema_check_nanos
        + batch.timing.wal_existence_nanos
        + batch.timing.position_resolution_nanos
        + batch.timing.wal_read_nanos
        + batch.timing.page_collection_nanos
        + batch.timing.verification_nanos
        + batch.timing.encode_nanos
        + batch.timing.local_write_nanos
        + batch.timing.fsync_nanos
        + batch.timing.parent_sync_nanos
        + batch.timing.checkpoint_nanos;
    let ltx_bytes = batch
        .segments
        .iter()
        .map(|segment| segment.info().size_bytes)
        .sum::<u64>();
    let segment = &batch.segments[0];
    let file = crate::LtxHost {
        facilities: crate::Host::default(),
        max_database_bytes: Limits::default().max_database_bytes,
        max_file_bytes: Limits::default().max_file_bytes,
    }
    .open(segment.path())
    .unwrap();
    let (decoded, size, digest) = crate::ltx::inspect_reader(file).unwrap();
    assert_eq!(
        segment.info(),
        &crate::SegmentInfo::from_inspected(&decoded, size, digest)
    );
    assert!(batch.timing.total_nanos > 0);
    assert!(phase_nanos <= batch.timing.total_nanos);
    assert_eq!(batch.timing.segment_count as usize, batch.segments.len());
    assert_eq!(batch.timing.ltx_bytes, ltx_bytes);
    assert!(batch.timing.wal_bytes > 0);
    assert!(batch.timing.database_bytes > 0);
    assert!(batch.timing.schema_check_nanos > 0);
    assert!(batch.timing.wal_existence_nanos > 0);
    assert!(batch.timing.position_resolution_nanos > 0);
    assert!(batch.timing.page_collection_nanos > 0);
    assert!(batch.timing.local_write_nanos > 0);
    assert!(batch.timing.fsync_nanos > 0);
    assert!(batch.timing.parent_sync_nanos > 0);
    assert_eq!(
        batch.timing.wal_sparse_reads + batch.timing.wal_full_reads,
        1
    );
    assert!(batch.timing.wal_image_bytes > 0);
    assert!(batch.timing.wal_file_bytes >= batch.timing.wal_read_bytes);
    assert!(batch.timing.wal_read_bytes > 0);
    assert_eq!(batch.timing.wal_snapshot_reads, 1);
}

#[cfg(feature = "replica")]
#[test]
fn failed_capture_emits_its_bounded_ledger() {
    #[derive(Default)]
    struct CaptureTelemetry(std::sync::Mutex<Vec<(crate::CaptureTiming, bool)>>);

    impl crate::LtxTelemetry for CaptureTelemetry {
        fn capture(&self, timing: &crate::CaptureTiming, succeeded: bool) {
            self.0.lock().unwrap().push((*timing, succeeded));
        }
    }

    let temp = tempfile::TempDir::new().unwrap();
    let telemetry = Arc::new(CaptureTelemetry::default());
    let host = crate::Host::default().with_ltx_telemetry(telemetry.clone());
    // One retained cut fills the session plan budget, so the next capture is
    // refused before the writer starts and still reports its bounded ledger.
    let limits = Limits {
        max_segments: 1,
        ..Limits::default()
    };
    let mut db = Db::open_with_host(&temp.path().join("failed.sqlite"), limits, host).unwrap();
    db.transaction(|tx| {
        tx.execute_batch(
            "CREATE TABLE events(value BLOB); INSERT INTO events VALUES(randomblob(4096))",
        )
    })
    .unwrap();
    db.capture().unwrap();

    // A checkpoint captures first, so its refusal is raised before the writer
    // starts: the failed attempt still emits its bounded ledger.
    assert!(matches!(
        db.checkpoint(crate::CheckpointMode::Passive),
        Err(LtxError::Limit(crate::LimitKind::RetainedCaptureArtifacts))
    ));
    let attempts = telemetry.0.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts[0].1);
    assert!(!attempts[1].1);
    assert!(attempts[1].0.total_nanos > 0);
    assert_eq!(attempts[1].0.wal_read_bytes, 0);
    assert!(attempts[0].0.wal_read_bytes > 0);
}

#[cfg(feature = "replica")]
#[test]
fn captured_indexes_match_independent_ltx_inspection() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&temp.path().join("indexed.sqlite"), Limits::default()).unwrap();
    db.transaction(|transaction| {
        transaction.execute_batch(
            "CREATE TABLE payload(value BLOB); \
             INSERT INTO payload VALUES(randomblob(200000))",
        )
    })
    .unwrap();

    let batch = db.capture().unwrap();

    let index_bytes: usize = batch
        .segments
        .iter()
        .map(|segment| segment.captured_index().unwrap().len())
        .sum();
    assert!(batch.retained_memory_bytes() >= index_bytes as u64);
    assert!(batch.retained_memory_bytes() < 32 * 1024);
    assert!(
        batch
            .segments
            .iter()
            .map(|segment| segment.info().size_bytes)
            .sum::<u64>()
            > 200_000
    );
    assert!(batch.clone().retained_memory_bytes() <= batch.retained_memory_bytes());
    let mut padded = Vec::with_capacity(64 * 1024);
    let segment = &batch.segments[0];
    padded.extend_from_slice(&segment.captured_index().unwrap());
    let capacity = padded.capacity();
    let mut padded_batch = batch.clone();
    padded_batch.segments = vec![
        crate::LocalSegment::new(segment.path().to_owned(), segment.info().clone())
            .with_captured_index(padded),
    ];
    assert!(padded_batch.retained_memory_bytes() >= capacity as u64);

    for segment in &batch.segments {
        let file = std::fs::File::open(segment.path()).unwrap();
        let (decoded, size, digest, pages) = crate::ltx::inspect_reader_with_index(file).unwrap();
        assert_eq!(
            crate::SegmentInfo::from_inspected(&decoded, size, digest),
            *segment.info()
        );
        assert_eq!(
            segment.captured_index().unwrap().as_ref(),
            crate::paged::encode_index_from_pages(&pages)
                .unwrap()
                .as_slice()
        );
        let cloned = segment.clone();
        assert_eq!(
            segment.captured_index().unwrap().as_ptr(),
            cloned.captured_index().unwrap().as_ptr()
        );
    }
    db.close().unwrap();
}

#[test]
fn managed_connections_use_bounded_sqlite_lookaside() {
    use rusqlite::ffi;

    let temp = tempfile::TempDir::new().unwrap();
    for index in 0..MANAGED_SQLITE_CONNECTIONS {
        let connection =
            open_connection(&temp.path().join(format!("lookaside-{index}.sqlite")), None).unwrap();
        let _statement = connection.prepare("SELECT 1").unwrap();
        let mut current = 0;
        let mut highwater = 0;
        let result = unsafe {
            // SAFETY: the connection remains alive and is exclusively
            // borrowed for the duration of this status query.
            ffi::sqlite3_db_status(
                connection.handle(),
                ffi::SQLITE_DBSTATUS_LOOKASIDE_USED,
                &mut current,
                &mut highwater,
                0,
            )
        };
        assert_eq!(result, ffi::SQLITE_OK);
        assert!(
            current > 0,
            "small statements should use the connection arena"
        );
        assert!(highwater >= current);
        // SQLite may split the arena into 512-byte and 128-byte slots.
        assert!(u64::try_from(highwater).unwrap() <= MANAGED_CONNECTION_LOOKASIDE_BYTES / 128);
    }
}

#[test]
fn oversized_commit_captures_as_a_full_image_instead_of_fencing() {
    let temp = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        max_capture_bytes: 8 * 1024,
        ..Limits::default()
    };
    let mut db = Db::open(&temp.path().join("oversized.sqlite"), limits).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE payload(value BLOB)"))
        .unwrap();
    let first = db.capture().unwrap();

    // A commit whose delta cannot fit the incremental bound must still be
    // captured: the writer escalates it to a full database image bounded by
    // the file bound instead of fencing the session.
    db.transaction(|tx| tx.execute_batch("INSERT INTO payload VALUES(randomblob(65536))"))
        .unwrap();
    let second = db.capture().unwrap();
    assert_eq!(second.segments.len(), 1);
    let escalated = &second.segments[0];
    assert!(
        escalated.info().size_bytes > limits.max_capture_bytes,
        "the escalated cut must exceed the incremental bound"
    );
    assert!(escalated.info().size_bytes <= limits.max_file_bytes);

    // The escalated cut stays a valid chain element: a plan over both cuts
    // restores the exact database.
    let mut segments = first.segments.clone();
    segments.extend(second.segments.iter().cloned());
    let plan = VerifiedPlan::new(&segments, second.position, limits).unwrap();
    let destination = temp.path().join("restored.sqlite");
    assert_eq!(restore_exact(&plan, &destination).unwrap(), second.position);
    let connection = Connection::open(&destination).unwrap();
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .unwrap();
    let bytes: i64 = connection
        .query_row("SELECT length(value) FROM payload", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(bytes, 65536);
}

#[test]
fn newly_written_pages_are_counted_once_for_incremental_admission() {
    let temp = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        max_capture_bytes: 40 * 1024,
        ..Limits::default()
    };
    let mut db = Db::open(&temp.path().join("growth.sqlite"), limits).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE payload(value BLOB)"))
        .unwrap();
    let schema = db.capture().unwrap();
    db.transaction(|tx| tx.execute_batch("INSERT INTO payload VALUES(randomblob(24576))"))
        .unwrap();
    let growth = db.capture().unwrap();
    assert!(growth.segments[0].info().size_bytes <= limits.max_capture_bytes);

    let mut segments = schema.segments;
    segments.extend(growth.segments);
    let plan = VerifiedPlan::new(&segments, growth.position, limits).unwrap();
    let restored = temp.path().join("growth-restored.sqlite");
    restore_exact(&plan, &restored).unwrap();
    let connection = Connection::open(restored).unwrap();
    let size: i64 = connection
        .query_row("SELECT length(value) FROM payload", [], |row| row.get(0))
        .unwrap();
    assert_eq!(size, 24576);
}

#[test]
fn truncate_boundary_image_may_exceed_the_incremental_bound() {
    let temp = tempfile::TempDir::new().unwrap();
    let limits = Limits {
        max_capture_bytes: 64 * 1024,
        ..Limits::default()
    };
    let mut db = Db::open(&temp.path().join("boundary.sqlite"), limits).unwrap();
    db.transaction(|tx| {
        tx.execute_batch(
            "CREATE TABLE payload(value BLOB); CREATE INDEX payload_len ON payload(length(value))",
        )
    })
    .unwrap();
    let mut segments = db.capture().unwrap().segments;
    for _ in 0..20 {
        db.transaction(|tx| tx.execute_batch("INSERT INTO payload VALUES(randomblob(8192))"))
            .unwrap();
        segments.extend(db.capture().unwrap().segments);
    }

    // A truncate checkpoint writes the whole database as one boundary image.
    // That image legitimately exceeds the incremental bound and is bounded by
    // the file bound instead of failing and fencing the session.
    let batch = db.checkpoint(crate::CheckpointMode::Truncate).unwrap();
    let position = batch.position;
    segments.extend(batch.segments);
    let largest = segments
        .iter()
        .map(|segment| segment.info().size_bytes)
        .max()
        .unwrap();
    assert!(
        largest > limits.max_capture_bytes,
        "the boundary image must exceed the incremental bound"
    );
    assert!(largest <= limits.max_file_bytes);

    let plan = VerifiedPlan::new(&segments, position, limits).unwrap();
    let destination = temp.path().join("boundary-restored.sqlite");
    assert_eq!(restore_exact(&plan, &destination).unwrap(), position);
    let connection = Connection::open(&destination).unwrap();
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 20);
}

#[test]
fn local_disk_admission_rejects_before_running_the_transaction() {
    let temp = tempfile::TempDir::new().unwrap();
    let budget = crate::DiskBudget::new(4 << 20);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let mut db =
        Db::open_with_host(&temp.path().join("disk.sqlite"), Limits::default(), host).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v)"))
        .unwrap();
    db.capture().unwrap();
    let settled = budget.used();
    let blocker = budget.try_reserve(budget.available()).unwrap();
    let ran = std::cell::Cell::new(false);
    let result = db.transaction(|_| {
        ran.set(true);
        Ok(())
    });
    assert!(matches!(
        result,
        Err(LtxError::Limit(crate::LimitKind::LocalDiskBytes))
    ));
    assert!(!ran.get());
    drop(blocker);
    assert_eq!(budget.used(), settled);
    db.close().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
fn existing_database_disk_admission_precedes_session_claim() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("existing.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE t(v)").unwrap();
    drop(connection);
    let bytes = std::fs::metadata(&path).unwrap().len();
    let host = crate::Host::default()
        .with_local_disk_budget(crate::DiskBudget::new(bytes.saturating_sub(1)));

    let result = Db::open_with_host(&path, Limits::default(), host);

    assert!(matches!(
        result,
        Err(LtxError::Limit(crate::LimitKind::LocalDiskBytes))
    ));
    assert!(!CaptureEngine::meta_path_for(&path).exists());
}

#[test]
fn resume_disk_admission_precedes_database_installation() {
    let temp = tempfile::TempDir::new().unwrap();
    let source_path = temp.path().join("source.sqlite");
    let limits = Limits::default();
    let mut source = Db::open(&source_path, limits).unwrap();
    source
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1)")
        })
        .unwrap();
    let batch = source.capture().unwrap();
    let plan = crate::VerifiedPlan::new(&batch.segments, batch.position, limits).unwrap();
    source.close().unwrap();
    let database_bytes = u64::from(batch.segments[0].info().database_pages)
        * u64::from(batch.segments[0].info().page_size);
    let host = crate::Host::default()
        .with_local_disk_budget(crate::DiskBudget::new(database_bytes.saturating_sub(1)));
    let destination = temp.path().join("destination.sqlite");

    let result = Db::resume_with_host(&plan, &destination, limits, host);

    assert!(matches!(
        result,
        Err(LtxError::Limit(crate::LimitKind::LocalDiskBytes))
    ));
    assert!(!destination.exists());
}

#[test]
fn pending_wal_and_captured_segments_reconcile_and_release_disk_admission() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("accounted.sqlite");
    let budget = crate::DiskBudget::new(4 * 1024 * 1024);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let limits = Limits {
        max_capture_bytes: 1024 * 1024,
        ..Limits::default()
    };
    let mut db = Db::open_with_host(&path, limits, host).unwrap();

    db.transaction(|transaction| {
        transaction.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1)")
    })
    .unwrap();
    let physical_before_capture = std::fs::metadata(&path).unwrap().len()
        + std::fs::metadata(format!("{}-wal", path.display()))
            .unwrap()
            .len()
        + std::fs::metadata(format!("{}-shm", path.display()))
            .unwrap()
            .len();
    assert!(
        budget.used() > physical_before_capture,
        "retain credit until the commit is captured"
    );
    assert!(
        budget.used() < limits.max_capture_bytes,
        "a tiny write must not reserve the incremental ceiling"
    );

    let batch = db.capture().unwrap();
    let retained = batch
        .segments
        .iter()
        .map(|segment| segment.info().size_bytes)
        .sum::<u64>();
    let wal = std::fs::metadata(format!("{}-wal", path.display()))
        .unwrap()
        .len();
    let database = std::fs::metadata(&path).unwrap().len();
    let shm = std::fs::metadata(format!("{}-shm", path.display()))
        .unwrap()
        .len();
    assert_eq!(budget.used(), database + retained + wal + shm);

    db.close().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
fn typed_operation_error_rolls_back_and_keeps_writer_usable() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = Db::open(&temp.path().join("typed.sqlite"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE inventory(value INTEGER NOT NULL)"))
        .unwrap();

    let rejected = db.transaction_with(|tx| {
        tx.execute("INSERT INTO inventory VALUES (1)", [])
            .map_err(|_| Rejected)?;
        Err::<(), _>(Rejected)
    });
    assert!(matches!(
        rejected,
        Err(crate::TransactionError::Operation(Rejected))
    ));

    db.transaction(|tx| {
        tx.execute("INSERT INTO inventory VALUES (2)", [])
            .map(|_| ())
    })
    .unwrap();
    let count = db
        .writer
        .query_row("SELECT count(*) FROM inventory", [], |row| {
            row.get::<_, u32>(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn truncate_checkpoint_and_auto_vacuum_preserve_every_cut() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("source.sqlite");
    let initial = Connection::open(&path).unwrap();
    initial
        .execute_batch("PRAGMA auto_vacuum=FULL; VACUUM;")
        .unwrap();
    drop(initial);
    let mut db = Db::open(&path, Limits::default()).unwrap();
    db.capture.truncate_page_n = 20;
    db.capture.min_checkpoint_page_n = 10;
    let mut segments = Vec::new();
    let mut last_pages = 0;
    let mut shrank = false;
    let mut multiple_cuts = false;
    for round in 0..6 {
        db.transaction(|tx| {
            tx.execute("CREATE TABLE IF NOT EXISTS t (data BLOB)", [])?;
            if round % 2 == 0 {
                for _ in 0..80 {
                    tx.execute("INSERT INTO t VALUES (randomblob(8000))", [])?;
                }
            } else {
                tx.execute("DELETE FROM t", [])?;
            }
            Ok(())
        })
        .unwrap();
        let batch = db.capture().unwrap();
        multiple_cuts |= batch.segments.len() > 1;
        for segment in &batch.segments {
            shrank |= last_pages > segment.info().database_pages;
            last_pages = segment.info().database_pages;
        }
        segments.extend(batch.segments);
        let plan = crate::VerifiedPlan::new(&segments, batch.position, Limits::default()).unwrap();
        let restored = temp.path().join(format!("restored-{round}.sqlite"));
        crate::restore_exact(&plan, &restored).unwrap();
        let conn = Connection::open(&restored).unwrap();
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(check, "ok");
        let count: u32 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, if round % 2 == 0 { 80 } else { 0 });
        crate::compact_exact(&plan, &temp.path().join(format!("compacted-{round}.ltx"))).unwrap();
    }
    assert!(shrank);
    assert!(multiple_cuts);
}

#[cfg(feature = "replica")]
mod continuation;
mod reader;

#[test]
fn small_independent_cells_share_disk_by_actual_growth() {
    const CELLS: usize = 16;
    let temp = tempfile::TempDir::new().unwrap();
    let budget = crate::DiskBudget::new(1 << 30);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let mut databases = Vec::new();
    for index in 0..CELLS {
        let mut db = Db::open_with_host(
            &temp.path().join(format!("cell-{index}.sqlite")),
            Limits::default(),
            host.clone(),
        )
        .unwrap();
        db.transaction(|tx| {
            tx.execute_batch("CREATE TABLE value(n INTEGER); INSERT INTO value VALUES (0)")
        })
        .unwrap();
        db.capture().unwrap();
        databases.push(db);
    }
    let settled_before = budget.used();
    let barrier = Arc::new(std::sync::Barrier::new(CELLS + 1));
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let (sender, receiver) = std::sync::mpsc::channel();
    let (entered, refused, peak) = std::thread::scope(|scope| {
        let mut jobs = Vec::new();
        for (index, db) in databases.iter_mut().enumerate() {
            let barrier = barrier.clone();
            let release = release.clone();
            let sender = sender.clone();
            jobs.push(scope.spawn(move || {
                barrier.wait();
                let result = db.transaction(|tx| {
                    tx.execute("UPDATE value SET n=n+1", [])?;
                    sender.send((index, None)).unwrap();
                    let (lock, changed) = &*release;
                    let mut ready = lock.lock().unwrap();
                    while !*ready {
                        ready = changed.wait(ready).unwrap();
                    }
                    Ok(())
                });
                match result {
                    Ok(()) => {
                        db.capture().unwrap();
                        true
                    }
                    Err(error) => {
                        sender.send((index, Some(error.to_string()))).unwrap();
                        false
                    }
                }
            }));
        }
        barrier.wait();
        let mut entered = 0;
        let mut refused = Vec::new();
        for _ in 0..CELLS {
            match receiver.recv_timeout(Duration::from_secs(10)) {
                Ok((_, None)) => entered += 1,
                Ok((index, Some(error))) => refused.push((index, error)),
                Err(error) => {
                    refused.push((CELLS, error.to_string()));
                    break;
                }
            }
        }
        let peak = budget.used();
        let (lock, changed) = &*release;
        *lock.lock().unwrap() = true;
        changed.notify_all();
        let committed = jobs
            .into_iter()
            .map(|job| job.join().unwrap())
            .filter(|committed| *committed)
            .count();
        assert_eq!(committed, entered);
        (entered, refused, peak)
    });
    let settled_after = budget.used();
    for db in databases {
        db.close().unwrap();
    }
    assert_eq!(budget.used(), 0);
    println!(
        "small-cell admission: entered={entered} refused={refused:?} settled_before={settled_before} peak_reserved={peak} settled_after={settled_after}"
    );
    assert_eq!(
        entered, CELLS,
        "independent tiny writes must fit the actual 1-GiB disk budget"
    );
}

#[test]
fn disk_growth_refusal_rolls_back_spilled_pages_and_keeps_their_charge() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("spilled.sqlite");
    let budget = crate::DiskBudget::new(1 << 20);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let mut db = Db::open_with_host(&path, Limits::default(), host).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE payload(v BLOB)"))
        .unwrap();
    db.capture().unwrap();
    let used_before = budget.used();
    let result = db.transaction_with(|tx| -> rusqlite::Result<()> {
        for _ in 0..1000 {
            tx.execute("INSERT INTO payload VALUES (zeroblob(8192))", [])?;
        }
        Ok(())
    });
    assert!(
        matches!(result, Err(crate::TransactionError::RolledBack { resource: LtxError::Limit(crate::LimitKind::LocalDiskBytes), operation: Some(ref error) }) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull)),
        "{result:?}"
    );
    assert!(
        !db.fenced,
        "SQLite proves rollback after FULL before the commit hook"
    );
    let count = db
        .query_with(|connection| {
            connection.query_row("SELECT count(*) FROM payload", [], |row| {
                row.get::<_, u64>(0)
            })
        })
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        budget.used() > used_before,
        "rollback does not delete spilled WAL bytes"
    );
    assert!(budget.used() <= budget.capacity());
    let physical = std::fs::metadata(&path).unwrap().len()
        + std::fs::metadata(format!("{}-wal", path.display()))
            .unwrap()
            .len()
        + std::fs::metadata(format!("{}-shm", path.display()))
            .unwrap()
            .len();
    assert!(
        budget.used() >= physical,
        "retained bytes and spill residue stay charged"
    );
    // The VFS error source remains available to the runtime, which must not
    // confuse a rolled-back storage refusal with an application-domain error.
    assert!(
        db.disk.take_error().is_none(),
        "the proved rollback owns its exact quota source"
    );
    db.transaction(|tx| {
        tx.execute("INSERT INTO payload VALUES ('retry')", [])
            .map(|_| ())
    })
    .unwrap();
    let batch = db.capture().unwrap();
    assert!(!batch.segments.is_empty());
    db.close().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
fn bounded_small_budget_captures_large_writes_as_full_images_for_each_page_size() {
    for page_size in [512_u32, 4096, 65536] {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("image.sqlite");
        let initial = Connection::open(&path).unwrap();
        initial.pragma_update(None, "page_size", page_size).unwrap();
        initial.execute_batch("VACUUM").unwrap();
        drop(initial);
        let budget = crate::DiskBudget::new(16 << 20);
        let host = crate::Host::default().with_local_disk_budget(budget.clone());
        let limits = Limits {
            max_capture_bytes: 128,
            max_database_bytes: 4 << 20,
            max_file_bytes: 8 << 20,
            ..Limits::default()
        };
        let mut db = Db::open_with_host(&path, limits, host).unwrap();
        db.transaction(|tx| {
            tx.execute_batch(
                "CREATE TABLE payload(v); INSERT INTO payload VALUES (randomblob(300000))",
            )
        })
        .unwrap();
        let batch = db.capture().unwrap();
        assert!(
            batch
                .segments
                .iter()
                .any(|segment| segment.info().size_bytes > limits.max_capture_bytes)
        );
        let plan = VerifiedPlan::new(&batch.segments, batch.position, limits).unwrap();
        let restored = temp.path().join("restored.sqlite");
        restore_exact(&plan, &restored).unwrap();
        let conn = Connection::open(&restored).unwrap();
        assert_eq!(
            conn.query_row("SELECT length(v) FROM payload", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            300000
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        db.close().unwrap();
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn committed_cut_and_truncate_capture_finish_using_their_reserved_credit() {
    let temp = tempfile::TempDir::new().unwrap();
    let budget = crate::DiskBudget::new(8 << 20);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let mut db = Db::open_with_host(
        &temp.path().join("reserved.sqlite"),
        Limits::default(),
        host,
    )
    .unwrap();
    db.capture.truncate_page_n = 1;
    db.transaction(|tx| {
        tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (randomblob(30000))")
    })
    .unwrap();
    let blocker = budget.try_reserve(budget.available()).unwrap();
    let batch = db.capture().unwrap();
    assert!(
        batch.segments.len() > 1,
        "exercise ordinary and boundary cuts"
    );
    assert!(!db.has_pending_capture());
    let plan = VerifiedPlan::new(&batch.segments, batch.position, Limits::default()).unwrap();
    let restored = temp.path().join("restored.sqlite");
    restore_exact(&plan, &restored).unwrap();
    assert_eq!(
        Connection::open(&restored)
            .unwrap()
            .query_row("SELECT length(v) FROM t", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        30000
    );
    drop(blocker);
    db.close().unwrap();
    assert_eq!(budget.used(), 0);
}

#[test]
#[cfg(feature = "replica")]
fn pruning_a_published_cut_preserves_newer_pending_capture_credit() {
    let temp = tempfile::TempDir::new().unwrap();
    let budget = crate::DiskBudget::new(8 << 20);
    let host = crate::Host::default().with_local_disk_budget(budget.clone());
    let mut db =
        Db::open_with_host(&temp.path().join("pruned.sqlite"), Limits::default(), host).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (0)"))
        .unwrap();
    let seed = db.capture().unwrap();
    db.transaction(|tx| tx.execute("UPDATE t SET v=1", []).map(|_| ()))
        .unwrap();
    assert!(db.has_pending_capture());
    let reserved_before = budget.used();
    let removed_bytes = seed
        .segments
        .iter()
        .map(|segment| segment.info().size_bytes)
        .sum::<u64>();
    assert_eq!(db.prune_captured(&seed).unwrap(), seed.segments.len());
    assert_eq!(budget.used(), reserved_before - removed_bytes);
    let blocker = budget.try_reserve(budget.available()).unwrap();
    let next = db.capture().unwrap();
    assert_eq!(next.position.txid, seed.position.txid + 1);
    drop(blocker);
    db.close().unwrap();
    assert_eq!(budget.used(), 0);
}
