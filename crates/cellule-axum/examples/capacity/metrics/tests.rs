use super::*;

#[test]
fn delayed_completions_do_not_inflate_window_tps_and_queue_latency_is_visible() {
    let mut metrics = Metrics::new(1);
    metrics.observe(
        0,
        true,
        true,
        Duration::from_millis(12),
        Duration::from_millis(2),
        100,
    );
    metrics.observe(
        0,
        true,
        false,
        Duration::from_millis(300),
        Duration::from_millis(3),
        100,
    );
    metrics.observe(
        0,
        false,
        true,
        Duration::from_secs(11),
        Duration::from_millis(4),
        0,
    );
    let summary = metrics.summary(1, 5, 4, 1, 0);
    assert_eq!(summary["successful_requests_per_second"], 1.0);
    assert_eq!(summary["successes_including_drain"], 2);
    assert_eq!(summary["successes_after_deadline"], 1);
    assert_eq!(summary["errors"], 1);
    assert_eq!(summary["producer_unissued"], 1);
    assert_eq!(summary["scheduled_histogram_overflow"], 1);
    assert!(summary["scheduled_latency_ms_all_attempts"]["p99"].is_null());
    assert_eq!(summary["request_latency_ms_all_attempts"]["p99"], 4.0);
}
