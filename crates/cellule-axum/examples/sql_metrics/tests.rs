use super::*;

#[test]
fn sparse_export_preserves_cumulative_histogram_indices_and_overflow() {
    let histogram = Histogram::default();
    histogram.observe(Duration::from_micros(201));
    histogram.observe(Duration::from_micros(201));
    histogram.observe(Duration::from_secs(3));
    let raw = histogram.raw();
    assert_eq!(raw["bucket_count"], WRITE_BUCKETS);
    assert_eq!(raw["resolution_us"], 100);
    assert_eq!(
        raw["nonzero_buckets"],
        serde_json::json!([[3, 2], [WRITE_BUCKETS - 1, 1]])
    );
    assert_eq!(raw["total_ns"], 3_000_402_000_u64);
    assert!(raw.get("buckets").is_none());
}

#[test]
fn response_and_proof_timings_keep_ack_latency_separate_from_materialization() {
    let metrics = QueryMetrics::default();
    metrics.command_response(
        CommandResponseSource::Fleet,
        Duration::from_millis(2),
        Duration::from_millis(1),
    );
    metrics.durability_proof(
        cellule_runtime::node::log::DurabilitySource::Fleet,
        Duration::from_millis(1),
    );
    metrics.durability_proof(
        cellule_runtime::node::log::DurabilitySource::Object,
        Duration::from_millis(50),
    );
    let sample = metrics.window_snapshot();
    assert_eq!(
        sample["histograms"]["response_fleet"]["nonzero_buckets"],
        serde_json::json!([[20, 1]])
    );
    assert_eq!(
        sample["histograms"]["proof_fleet"]["nonzero_buckets"],
        serde_json::json!([[10, 1]])
    );
    assert_eq!(
        sample["histograms"]["proof_object"]["nonzero_buckets"],
        serde_json::json!([[500, 1]])
    );
    assert_eq!(sample["response_sources"]["fleet"], 1);
    assert_eq!(sample["response_sources"]["object"], 0);
}

#[tokio::test]
async fn enrollment_roles_survive_stream_completion_without_leaking_into_other_tasks() {
    use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
    let metrics = std::sync::Arc::new(QueryMetrics::default());
    let store = cellule_store::Store::new(std::sync::Arc::new(InMemory::new()))
        .with_storage_observer(metrics.clone());
    let path = Path::from("fixture/nodes/session.json");
    store
        .inner()
        .put(&path, bytes::Bytes::from_static(b"enrollment").into())
        .await
        .unwrap();
    let (owner, receiver) = tokio::join!(
        metrics.enrollment(false, store.inner().get(&path)),
        metrics.enrollment(true, store.inner().get(&path)),
    );
    // Both task scopes have ended, but streamed accounting retains its role.
    owner.unwrap().bytes().await.unwrap();
    receiver.unwrap().bytes().await.unwrap();
    store
        .inner()
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let snapshot = metrics.snapshot();
    for role in ["owner_enrollment", "receiver_enrollment", "node_authority"] {
        assert_eq!(
            snapshot["storage_families"][role]["get"]["outcomes"]["success"],
            1
        );
    }
    assert_eq!(
        snapshot["storage_operations"]["get"]["outcomes"]["success"],
        3
    );
}

#[test]
fn commands_per_root_counts_only_confirmed_publications() {
    let metrics = QueryMetrics::default();
    let timing = PublicationTiming {
        queue_wait: Duration::ZERO,
        preparation: Duration::ZERO,
        authority: Duration::ZERO,
        total: Duration::ZERO,
        succeeded: true,
        commit_sequence: 12,
        covered_commits: 12,
    };
    metrics.publication_completed(cellule_runtime::CellId::from_bytes([1; 32]), timing);
    metrics.publication_completed(
        cellule_runtime::CellId::from_bytes([1; 32]),
        PublicationTiming {
            succeeded: false,
            covered_commits: 100,
            ..timing
        },
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot["writes"]["selected_roots"], 1);
    assert_eq!(snapshot["writes"]["materialized_commits"], 12);
    assert_eq!(snapshot["writes"]["publication_failures"], 1);
}

#[tokio::test]
async fn fixed_storage_families_reconcile_after_stream_completion() {
    use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
    let metrics = std::sync::Arc::new(QueryMetrics::default());
    let store = cellule_store::Store::new(std::sync::Arc::new(InMemory::new()))
        .with_storage_observer(metrics.clone());
    for path in [
        "fixture/cells/v1/app/cells/x/inc/y/object.root",
        "fixture/cells/v1/app/cells/x/control.json",
        "fixture/cells/v1/app/nodes/session.json",
        "fixture/unclassified",
    ] {
        let path = Path::from(path);
        store
            .inner()
            .put(&path, bytes::Bytes::from_static(b"verified").into())
            .await
            .unwrap();
        assert_eq!(
            store
                .inner()
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "verified"
        );
    }
    let snapshot = metrics.snapshot();
    for operation in ["put", "get"] {
        let sum: u64 = snapshot["storage_families"]
            .as_object()
            .unwrap()
            .values()
            .map(|family| family[operation]["outcomes"]["success"].as_u64().unwrap())
            .sum();
        assert_eq!(
            sum,
            snapshot["storage_operations"][operation]["outcomes"]["success"]
        );
        assert_eq!(sum, 4);
    }
    metrics.peer_phase(PeerPhase::RoundTrip, Duration::from_micros(350));
    let raw = metrics.window_snapshot();
    assert_eq!(
        raw["histograms"]["round_trip"]["nonzero_buckets"],
        serde_json::json!([[4, 1]])
    );
    assert_eq!(
        raw["histograms"]["round_trip"]["bucket_count"],
        WRITE_BUCKETS
    );
    assert_eq!(raw["histograms"]["round_trip"]["total_ns"], 350_000);
}

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
