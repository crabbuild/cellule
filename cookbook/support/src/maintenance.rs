use std::{sync::Arc, time::Duration};

use cellule_runtime::primitives::maintenance::{MaintenanceTickOutcome, MaintenanceTickRequest};
use cellule_runtime::{CellClient, CellRuntime, Registry};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Result, new_identity, now_ms};

pub(crate) async fn run(
    runtime: CellRuntime,
    client: CellClient,
    registry: Arc<Registry>,
    cancellation: CancellationToken,
    acquisition: Arc<Mutex<()>>,
) -> Result<()> {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        // Migration replaces a live handle. Keep the due scan and dispatch in
        // one activation boundary so maintenance cannot use a retired capability.
        let _acquisition = tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            guard = acquisition.lock() => guard,
        };
        let due = runtime.due_resident(now_ms()?, 32).await?;
        if due.is_empty() {
            continue;
        }
        let targets = runtime.active_cell_targets().await?;
        for resident in due {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            // A published sequence is a hint. The typed Tick checks it again
            // inside the transaction and safely does nothing if it is stale.
            // Complete dispatched work before honoring cancellation.
            let target = targets
                .iter()
                .find(|target| target.cell_id() == resident.handle().cell_id())
                .ok_or(cellule_runtime::Error::CellNotActive)?;
            let mut expected_commit_sequence = resident.expected_commit_sequence();
            // An idle native claim also publishes a command outcome. It can make
            // the scanner's hint stale while the Tick waits behind that claim.
            // Retry from the Tick's own published receipt, without bypassing its
            // sequence check. Bound retries so a busy Cell cannot monopolize the
            // scanner or prevent cancellation and maintenance of other Cells.
            for _ in 0..4 {
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                let observed = registry
                    .run_maintenance_once(
                        client.clone(),
                        target.clone(),
                        new_identity()?,
                        MaintenanceTickRequest {
                            expected_commit_sequence,
                        },
                    )
                    .await?;
                tracing::debug!(cell = ?target.cell_id(), expected_commit_sequence,
                    published_sequence = observed.receipt.commit_sequence,
                    outcome = ?observed.output, "cookbook maintenance tick");
                if matches!(observed.output, MaintenanceTickOutcome::Applied { .. }) {
                    break;
                }
                expected_commit_sequence = observed.receipt.commit_sequence;
            }
        }
    }
}
