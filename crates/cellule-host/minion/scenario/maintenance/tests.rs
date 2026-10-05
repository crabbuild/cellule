#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executable_busy_maintenance_closes_sustained_admission_preserves_every_outcome_and_joins()
{
    let summary = super::super::maintenance_busy().await.unwrap();
    assert_eq!(
        (summary.released, summary.activated, summary.retired),
        (12, 12, 12)
    );
    assert!(summary.receipt_checks >= 14);
    assert_eq!(summary.final_counts[0], 0);
    assert_eq!(summary.final_counts.iter().sum::<usize>(), 12);
    assert_eq!(summary.receiver_nodes, 2);
    assert!(summary.maintenance_completed);
    assert!(summary.maintenance_boot_withdrawn);
    assert_eq!(summary.joined_nodes, 3);
    assert_eq!(summary.boot_retirements, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_startup_retains_native_error_and_joins_every_client_before_node_cleanup() {
    use super::*;
    let root = tempfile::tempdir().unwrap();
    let journal = Arc::new(
        SqliteJournal::open(
            root.path().join("busy-startup.sqlite"),
            scope(),
            FleetProfile::default(),
            clock().unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut nodes = Vec::new();
    let mut boots = Vec::new();
    let (_, acknowledged) = initialize(&root, &journal, &mut nodes, &mut boots, 300_000)
        .await
        .unwrap();
    nodes[0].shutdown().await.unwrap();
    let mut traffic = traffic::Traffic::start(&acknowledged).unwrap();
    let error = traffic.started().await.unwrap_err();
    fn native(error: &(dyn std::error::Error + 'static)) -> bool {
        matches!(
            error.downcast_ref::<cellule_runtime::Error>(),
            Some(
                cellule_runtime::Error::RuntimeClosed
                    | cellule_runtime::Error::CellDraining
                    | cellule_runtime::Error::CellNotActive
                    | cellule_runtime::Error::Fenced
            )
        ) || error.source().is_some_and(native)
    }
    assert!(native(error.as_ref()), "lost native source: {error:?}");
    let joined = match traffic.finish().await {
        Ok(_) => panic!("closed original writer accepted sustained traffic"),
        Err(error) => error,
    };
    assert!(native(joined.as_ref()), "lost joined source: {joined:?}");
    for (node, boot) in nodes.iter().zip(&boots) {
        node.shutdown().await.unwrap();
        boot.withdraw(&journal).await.unwrap();
        let stats = node.stats();
        assert_eq!(stats.active_cells(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.file_descriptors(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
    }
    journal.close().await.unwrap();
}
