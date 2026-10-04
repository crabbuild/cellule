#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn measured_overload_moves_real_cells_after_durable_controller_reconstruction() {
    let summary = super::overload().await.unwrap();
    assert_eq!(summary.released, 2);
    assert_eq!(summary.activated, 2);
    assert_eq!(summary.retired, 2);
    assert_eq!(summary.receipt_checks, 2);
    assert_eq!(summary.max_inflight, 2);
    assert!(summary.max_restore_bytes > 0 && summary.max_restore_bytes <= 8 << 30);
    assert_eq!(summary.joined_nodes, 3);
    assert_eq!(summary.boot_retirements, 3);
    assert_eq!(summary.receiver_nodes, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_controller_adopts_lost_releases_after_real_expiry_and_joins_receiver_credit() {
    let summary = super::controller_restart().await.unwrap();
    assert_eq!(summary.lost_release_replies, 2);
    assert_eq!(summary.controller_epoch, 2);
    assert_eq!(summary.expired_receiver_cleanups, 2);
    assert_eq!(summary.released, 2);
    assert_eq!(summary.activated, 2);
    assert_eq!(summary.retired, 2);
    assert_eq!(summary.receipt_checks, 2);
    assert_eq!(summary.max_inflight, 2);
    assert!(summary.max_restore_bytes > 0 && summary.max_restore_bytes <= 8 << 30);
    assert_eq!(summary.joined_nodes, 3);
    assert_eq!(summary.boot_retirements, 3);
    assert_eq!(summary.receiver_nodes, 2);
}
