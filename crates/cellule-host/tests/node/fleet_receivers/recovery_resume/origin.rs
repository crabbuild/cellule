//! Recovery metadata cannot replace the actual origin bytes before reacquisition.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_idle_reacquisition_requires_complete_materialized_origin() {
    let (movement, action, idle, receipt) = failed(1, 42, false).await;
    let root = idle.ltx_root().unwrap();
    let layout = movement.inputs.authority.layout();
    let path = layout.incarnation_object_path(
        &root.cell,
        &root.incarnation,
        &root.digest,
        cellule_runtime::ltx::CellObjectKind::Root,
    );
    let (body, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    let refused = apply(&movement.receiver, action.clone()).await;
    assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
    assert!(matches!(
        refused.execution_error.as_ref().unwrap().as_ref(),
        Error::Ltx(_)
    ));
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        &idle
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert_eq!(movement.receiver.stats().file_descriptors(), 0);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    // Recording the actual native materialization is allowed; origin absence
    // still blocks the next ownership CAS and any serving/settlement result.
    let evidence = movement
        .source
        .journal
        .recovery_evidence(action.key().unwrap())
        .unwrap();
    assert_eq!(evidence.restored().ltx_root(), Some(root));
    layout.store().create_strict(&path, body).await.unwrap();
    finish(movement, action, &idle, 42, false, receipt).await;
}
