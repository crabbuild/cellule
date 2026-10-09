use crate::{DATA, Dataset, ExportApplication, FILES, Files, RUNS, Runs};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::LocalNode;
use cellule_runtime::primitives::workflow::{ActivityRunOutcome, ActivitySupervisor};
use std::time::Duration;
/// Opens the three declared transaction domains. The embedding drains on startup failure.
pub async fn open(
    node: &LocalNode,
    handle: &ApplicationHandle<ExportApplication>,
) -> Result<(), crate::BoxError> {
    for (namespace, module) in [(DATA, 0), (FILES, 1), (RUNS, 2)] {
        let target = handle.target_for_scope(namespace, b"fixed-export-shard")?;
        match module {
            0 => {
                node.open_cell(&target, &Dataset).await?;
            }
            1 => {
                node.open_cell(&target, &Files).await?;
            }
            _ => {
                node.open_cell(&target, &Runs).await?;
            }
        }
    }
    Ok(())
}
/// Owns the native Activity supervisor; accepted computation and completion finish before drain.
pub fn spawn_exports(
    node: &LocalNode,
    handle: ApplicationHandle<ExportApplication>,
) -> Result<(), crate::BoxError> {
    let supervisor = ActivitySupervisor::new(handle.activities::<Runs>()?, 15_000)?;
    node.spawn_worker(move|cancel|async move{
        while !cancel.is_cancelled(){
            if matches!(supervisor.run_once(0,None).await?,ActivityRunOutcome::IdentityConflict{..}){return Err(cellule_runtime::primitives::workflow::ActivitySupervisorError::Runtime(cellule_runtime::Error::Command("export completion identity conflict")));}
            tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(50))=>{}}
        }
        Ok::<_,cellule_runtime::primitives::workflow::ActivitySupervisorError>(())
    })?;
    Ok(())
}
