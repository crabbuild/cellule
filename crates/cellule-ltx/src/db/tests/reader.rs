use super::*;
use rusqlite::{
    DatabaseName, ErrorCode,
    hooks::{AuthAction, AuthContext, Authorization},
};

fn database(temp: &tempfile::TempDir) -> Db {
    let mut db = Db::open(&temp.path().join("reader.sqlite"), Limits::default()).unwrap();
    db.transaction(|transaction| {
        transaction
            .execute_batch("CREATE TABLE events(value INTEGER); INSERT INTO events VALUES (1)")
    })
    .unwrap();
    db.capture().unwrap();
    db
}

#[test]
fn reader_is_protected_and_refreshes_its_snapshot_without_reprepare() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = database(&temp);
    assert!(db.reader.is_readonly(DatabaseName::Main).unwrap());
    let preparations = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&preparations);
    db.reader.authorizer(Some(move |context: AuthContext<'_>| {
        if let AuthAction::Read {
            table_name: "events",
            column_name: "value",
        } = context.action
        {
            observed.fetch_add(1, Ordering::Relaxed);
        }
        Authorization::Allow
    }));
    for expected in 1..=3 {
        let value = db
            .query_with(|connection| {
                assert!(
                    !connection.is_autocommit(),
                    "callback has one managed snapshot"
                );
                connection
                    .prepare_cached("SELECT value FROM events")?
                    .query_row([], |row| row.get::<_, i64>(0))
            })
            .unwrap();
        assert_eq!(value, expected);
        assert!(
            db.reader.is_autocommit(),
            "snapshot ends before owner reuse"
        );
        assert_eq!(preparations.load(Ordering::Relaxed), 1);
        assert_eq!(
            db.writer
                .query_row("PRAGMA query_only", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        db.transaction(|transaction| {
            transaction.execute("UPDATE events SET value = value + 1", [])
        })
        .unwrap();
        db.capture().unwrap();
    }
    // An idle owner reader must not prevent truncation after its last callback.
    let page_size: u64 = db
        .writer
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    let sealing_frame =
        crate::WAL_HEADER_SIZE as u64 + crate::WAL_FRAME_HEADER_SIZE as u64 + page_size;
    assert!(
        std::fs::metadata(temp.path().join("reader.sqlite-wal"))
            .unwrap()
            .len()
            > sealing_frame
    );
    db.checkpoint(crate::CheckpointMode::Truncate).unwrap();
    // Capture deliberately writes one sequence frame after restarting WAL.
    assert_eq!(
        std::fs::metadata(temp.path().join("reader.sqlite-wal"))
            .unwrap()
            .len(),
        sealing_frame
    );
    db.close().unwrap();
}

#[test]
fn reader_cannot_write_even_if_query_only_is_disabled() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = database(&temp);
    db.query_with(|connection| -> rusqlite::Result<()> {
        let denied = connection
            .execute("UPDATE events SET value = 99", [])
            .unwrap_err();
        assert_eq!(denied.sqlite_error_code(), Some(ErrorCode::ReadOnly));
        // Deliberately violate and restore the pragma contract to verify the
        // independent read-only connection boundary as well.
        connection.pragma_update(None, "query_only", false)?;
        let denied = connection
            .execute("UPDATE events SET value = 99", [])
            .unwrap_err();
        assert_eq!(denied.sqlite_error_code(), Some(ErrorCode::ReadOnly));
        connection.pragma_update(None, "query_only", true)?;
        let denied = connection
            .execute_batch("CREATE TEMP TABLE forbidden(value INTEGER)")
            .unwrap_err();
        assert_eq!(denied.sqlite_error_code(), Some(ErrorCode::ReadOnly));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        db.query_with(|connection| connection
            .query_row("SELECT value FROM events", [], |row| row.get::<_, i64>(0)))
            .unwrap(),
        1
    );
    db.close().unwrap();
}

#[test]
fn failed_read_ends_its_snapshot_and_preserves_the_application_error() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = database(&temp);
    let result = db.query_with(|connection| -> std::result::Result<(), Rejected> {
        assert_eq!(
            connection
                .query_row("SELECT value FROM events", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        Err(Rejected)
    });
    assert!(matches!(
        result,
        Err(crate::QueryError::Operation(Rejected))
    ));
    assert!(db.reader.is_autocommit());
    db.transaction(|transaction| transaction.execute("UPDATE events SET value = 2", []))
        .unwrap();
    assert_eq!(
        db.query_with(|connection| connection
            .query_row("SELECT value FROM events", [], |row| row.get::<_, i64>(0)))
            .unwrap(),
        2
    );
    db.close().unwrap();
}

#[test]
fn ending_the_managed_snapshot_inside_a_successful_callback_fences() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = database(&temp);
    let result = db.query_with(|connection| connection.execute_batch("COMMIT"));
    assert!(matches!(
        result,
        Err(crate::QueryError::State(LtxError::InvalidState(
            "query ended its managed read transaction"
        )))
    ));
    assert!(matches!(db.transaction(|_| Ok(())), Err(LtxError::Fenced)));
    db.close().unwrap();
}

#[test]
fn managed_interrupt_reaches_an_executing_read_and_is_inert_after_close() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut db = database(&temp);
    let interrupt = db.interrupt_handle();
    let (started, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut started = Some(started);
        db.reader.progress_handler(
            1_000,
            Some(move || {
                if let Some(started) = started.take() {
                    let _ = started.send(());
                }
                false
            }),
        );
        let result = db.query_with(|connection| connection.query_row(
            "WITH RECURSIVE counter(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM counter WHERE value < 1000000000) SELECT sum(value) FROM counter",
            [], |row| row.get::<_, i64>(0),
        ));
        assert!(
            matches!(result, Err(crate::QueryError::Operation(ref error)) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted))
        );
        assert!(db.reader.is_autocommit());
        db.transaction(|transaction| transaction.execute("UPDATE events SET value = 2", []))
            .unwrap();
        db.close().unwrap();
    });
    receive.recv_timeout(Duration::from_secs(5)).unwrap();
    interrupt.interrupt();
    worker.join().unwrap();
    interrupt.interrupt();
}
