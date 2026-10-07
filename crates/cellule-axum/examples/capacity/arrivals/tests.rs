use super::*;

#[tokio::test]
async fn delayed_producer_keeps_every_original_due_time_and_window_offer() {
    let start = Instant::now() - Duration::from_secs(5);
    let warm_end = start + Duration::from_secs(1);
    let end = warm_end + Duration::from_secs(1);
    let (sender, mut receiver) = mpsc::channel(20);
    let arrivals = produce(sender, start, warm_end, end, [2, 3]).await;
    assert_eq!(arrivals.offered, [2, 3]);
    assert_eq!(arrivals.dropped, [0, 0]);
    assert_eq!(arrivals.warmup_dropped, [0, 0]);
    let mut counts = [0; 2];
    let mut measured = [0; 2];
    while let Some(job) = receiver.recv().await {
        let kind = match job.kind {
            Kind::Write => 0,
            Kind::Read => 1,
        };
        let rate = [2, 3][kind];
        assert_eq!(job.index, counts[kind]);
        assert_eq!(
            job.due,
            start + Duration::from_nanos(job.index * 1_000_000_000 / rate)
        );
        assert!(job.due < end);
        assert_eq!(job.measured, job.due >= warm_end);
        counts[kind] += 1;
        measured[kind] += u64::from(job.measured);
    }
    assert_eq!(counts, [4, 6]);
    assert_eq!(measured, arrivals.offered);
}

#[tokio::test]
async fn late_full_queue_records_drops_instead_of_omitting_offers() {
    let start = Instant::now() - Duration::from_secs(5);
    let warm_end = start + Duration::from_secs(1);
    let end = warm_end + Duration::from_secs(1);
    let (sender, mut receiver) = mpsc::channel(1);
    let arrivals = produce(sender, start, warm_end, end, [0, 3]).await;
    assert_eq!(arrivals.offered, [0, 3]);
    assert_eq!(arrivals.dropped, [0, 3]);
    assert_eq!(arrivals.warmup_dropped, [0, 2]);
    assert!(!receiver.recv().await.unwrap().measured);
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn immediate_recovery_window_needs_no_new_warmup() {
    let start = Instant::now() - Duration::from_secs(5);
    let end = start + Duration::from_secs(1);
    let (sender, mut receiver) = mpsc::channel(4);
    let arrivals = produce(sender, start, start, end, [2, 0]).await;
    assert_eq!(arrivals.offered, [2, 0]);
    assert_eq!(arrivals.warmup_dropped, [0, 0]);
    while let Some(job) = receiver.recv().await {
        assert!(job.measured);
    }
}

#[test]
fn recovery_metadata_and_zero_warmup_are_supported_by_the_real_config_decoder() {
    let config = serde_json::json!({"address":"127.0.0.1:8080", "cells":1000,
        "concurrency":128, "queue_capacity":128, "write_rate":10, "read_rate":0,
        "warmup_seconds":0, "seconds":30, "evidence_directory":"/tmp/audit",
        "phase":"recovery"});
    let decoded: super::super::Config = serde_json::from_value(config.clone()).unwrap();
    decoded.validate().unwrap();
    let mut warmed = config.clone();
    warmed["warmup_seconds"] = serde_json::json!(30);
    let decoded: super::super::Config = serde_json::from_value(warmed).unwrap();
    assert!(decoded.validate().is_err());
    let mut unknown = config;
    unknown["phase"] = serde_json::json!("hide_steady_point");
    assert!(serde_json::from_value::<super::super::Config>(unknown).is_err());
}
