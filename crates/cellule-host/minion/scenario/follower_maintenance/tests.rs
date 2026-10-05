#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executable_follower_maintenance_covers_old_tail_replaces_both_members_and_joins_every_owner()
 {
    let summary = super::super::maintenance_follower().await.unwrap();
    assert!(summary.maintenance_completed);
    assert!(summary.maintenance_boot_withdrawn);
    assert_eq!(summary.final_counts, [1, 0, 0, 0]);
    assert_eq!(summary.receipt_checks, 5);
    assert_eq!(summary.joined_nodes, 4);
    assert_eq!(summary.boot_retirements, 4);
    assert_eq!(summary.receiver_nodes, 2);
    assert_eq!(
        (summary.released, summary.activated, summary.retired),
        (0, 0, 0)
    );
    assert_eq!(summary.max_inflight, 0);
    assert_eq!(summary.max_restore_bytes, 0);
    assert!(!summary.blockers.is_empty());
}
