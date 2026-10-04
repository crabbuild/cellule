use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_loss_command_preserves_every_receipt_and_joins_all_owners() {
    let summary = crate::scenario::receiver_loss().await.unwrap();
    assert_eq!(summary.released, 1);
    assert_eq!(summary.activated, 1);
    assert_eq!(summary.retired, 1);
    assert_eq!(summary.receipt_checks, CELL_COUNT);
    assert_eq!(summary.max_inflight, 1);
    assert!(summary.max_restore_bytes > 0);
    assert!(summary.max_restore_bytes <= FleetProfile::default().max_restore_bytes);
    assert_eq!(summary.controller_epoch, 2);
    assert_eq!(summary.receiver_process_closures, 1);
    assert_eq!(summary.lost_activation_replies, 1);
    assert_eq!(summary.routed_activation_replays, 1);
    assert_eq!(summary.joined_nodes, 3);
    assert_eq!(summary.boot_retirements, 3);
    assert_eq!(summary.final_counts, [CELL_COUNT - 1, 0, 1]);
    assert_eq!(summary.blockers, vec![DrainBlocker::OutcomeUnknown]);
}
