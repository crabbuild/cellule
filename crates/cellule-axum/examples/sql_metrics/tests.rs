use super::*;

#[test]
fn real_capture_and_capacity_failure_reach_the_application_snapshot() {
    let directory = tempfile::TempDir::new().unwrap();
    let metrics = std::sync::Arc::new(QueryMetrics::default());
    let telemetry =
        cellule_runtime::fleet::telemetry::CellTelemetryHandle::from_sink(metrics.clone());
    let host = cellule_ltx::Host::default().with_ltx_telemetry(std::sync::Arc::new(telemetry));
    let mut database = cellule_ltx::Db::open_with_host(
        &directory.path().join("capture.sqlite"),
        cellule_ltx::Limits {
            max_segments: 1,
            ..cellule_ltx::Limits::default()
        },
        host,
    )
    .unwrap();
    database
        .transaction(|tx| {
            tx.execute_batch(
                "CREATE TABLE events(value BLOB); INSERT INTO events VALUES(randomblob(4096))",
            )
        })
        .unwrap();
    let batch = database.capture().unwrap();
    let before = metrics.snapshot();
    assert_eq!(before["capture"]["phases"]["total"]["count"], 1);
    assert_eq!(
        before["capture"]["wal_read_bytes"],
        batch.timing.wal_read_bytes
    );
    assert_eq!(before["capture"]["ltx_bytes"], batch.timing.ltx_bytes);
    assert!(batch.timing.wal_read_bytes > 0);
    assert!(matches!(
        database.checkpoint(cellule_ltx::CheckpointMode::Passive),
        Err(cellule_ltx::LtxError::Limit(
            cellule_ltx::LimitKind::RetainedCaptureArtifacts
        ))
    ));
    let after = metrics.snapshot();
    assert_eq!(after["capture"]["phases"]["total"]["count"], 2);
    assert_eq!(after["capture"]["failures"], 1);
    assert_eq!(
        after["capture"]["wal_read_bytes"],
        before["capture"]["wal_read_bytes"]
    );
}
