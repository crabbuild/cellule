use crate::{MediaApplication, RESULTS, RUNS, Results, Runs, SOURCES, Sources};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::LocalNode;
use cellule_runtime::primitives::workflow::{ActivityRunOutcome, ActivitySupervisor};
use std::time::Duration;
/// Opens the three declared transaction domains. The embedding drains on startup failure.
pub async fn open(
    node: &LocalNode,
    handle: &ApplicationHandle<MediaApplication>,
) -> Result<(), crate::BoxError> {
    for (namespace, module) in [(SOURCES, 0), (RESULTS, 1), (RUNS, 2)] {
        let target = handle.target_for_scope(namespace, b"fixed-media-shard")?;
        match module {
            0 => {
                node.open_cell(&target, &Sources).await?;
            }
            1 => {
                node.open_cell(&target, &Results).await?;
            }
            _ => {
                node.open_cell(&target, &Runs).await?;
            }
        }
    }
    Ok(())
}
/// Owns the native Activity supervisor; accepted computation and completion finish before drain.
pub fn spawn_processing(
    node: &LocalNode,
    handle: ApplicationHandle<MediaApplication>,
) -> Result<(), crate::BoxError> {
    let supervisor = ActivitySupervisor::new(handle.activities::<Runs>()?, 15_000)?;
    node.spawn_worker(move|cancel|async move{
        while !cancel.is_cancelled(){
            if matches!(supervisor.run_once(0,None).await?,ActivityRunOutcome::IdentityConflict{..}){return Err(cellule_runtime::primitives::workflow::ActivitySupervisorError::Runtime(cellule_runtime::Error::Command("media completion identity conflict")));}
            tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(50))=>{}}
        }
        Ok::<_,cellule_runtime::primitives::workflow::ActivitySupervisorError>(())
    })?;
    Ok(())
}
