#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_residence_and_post_batch_samples_converge_without_oscillation() {
    let started = tokio::time::Instant::now();
    let summary = super::super::count_balance().await.unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_secs(60));
    assert_eq!(summary.final_counts, [4, 4, 4]);
    assert_eq!(summary.released, 8);
    assert_eq!(summary.activated, 8);
    assert_eq!(summary.retired, 8);
    assert_eq!(summary.receipt_checks, 8);
    assert_eq!(summary.max_inflight, 2);
    assert!(summary.max_restore_bytes > 0 && summary.max_restore_bytes <= 8 << 30);
    assert_eq!(summary.joined_nodes, 3);
    assert_eq!(summary.boot_retirements, 3);
    assert_eq!(summary.receiver_nodes, 2);
    assert_eq!(summary.controller_epoch, 1);
    // Transitional batches can be incomplete. The scenario requires complete
    // final counts and two complete, scheduling-enabled equilibrium passes.
}
