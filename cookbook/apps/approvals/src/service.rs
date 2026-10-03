use crate::{APPROVALS, ApprovalApplication, Approvals};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::LocalNode;
use cellule_runtime::{
    ApplicationId, CellTarget, TenantId, partition_for_shard,
    primitives::workflow::{ActivityRunOutcome, ActivitySupervisor},
};
use std::time::Duration;
/// Service assembly errors retain their originating framework or infrastructure source.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node, provider, task-installation, or lifecycle error.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Native capability or supervisor preparation error.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
}
/// Opens both declared Workflow Cells before ingress. Drain the node on startup failure.
pub async fn open(
    node: &LocalNode,
    tenant: TenantId,
    application: ApplicationId,
) -> Result<ApplicationHandle<ApprovalApplication>, ServiceError> {
    let handle = node.application_handle::<ApprovalApplication>(tenant)?;
    for shard in 0..2 {
        node.open_cell(
            &CellTarget::new(tenant, application, APPROVALS, &partition_for_shard(shard))?,
            &Approvals,
        )
        .await?;
    }
    Ok(handle)
}
/// Installs one owned reminder Activity runner per Workflow shard.
/// Native maintenance already owns timers. Accepted Activities finish completion before drain.
pub fn spawn_reminders(
    node: &LocalNode,
    handle: ApplicationHandle<ApprovalApplication>,
) -> Result<(), ServiceError> {
    for shard in 0..2 {
        let supervisor = ActivitySupervisor::new(handle.activities::<Approvals>()?, 15_000)?;
        node.spawn_worker(move |cancel| async move {
            while !cancel.is_cancelled() {
                if matches!(
                    supervisor.run_once(shard, None).await?,
                    ActivityRunOutcome::IdentityConflict { .. }
                ) {
                    return Err(
                        cellule_runtime::primitives::workflow::ActivitySupervisorError::Runtime(
                            cellule_runtime::Error::Command(
                                "reminder completion identity conflict",
                            ),
                        ),
                    );
                }
                tokio::select! {
                    () = cancel.cancelled() => {},
                    () = tokio::time::sleep(Duration::from_millis(100)) => {},
                }
            }
            Ok::<_, cellule_runtime::primitives::workflow::ActivitySupervisorError>(())
        })?;
    }
    Ok(())
}
