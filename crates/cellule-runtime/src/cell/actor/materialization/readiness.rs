//! Observe original selection without owning the Cell publication token.
use super::*;

pub(super) fn wait(cell: CellId, active: &mut ActiveCell, tasks: &mut JoinSet<TaskResult>) {
    if active.selection_waiter.is_some() || active.coordination.is_fenced() {
        return;
    }
    let Some(durability) = active
        .publications
        .front()
        .and_then(|queued| queued.durability.as_ref())
        .cloned()
    else {
        return;
    };
    let generation = active.generation;
    let cancelled = tokio_util::sync::CancellationToken::new();
    active.selection_waiter = Some(cancelled.clone().drop_guard());
    // Resident Cell admission covers one bounded observer. It shares the
    // existing capture/selected metadata credits and creates no new proof.
    // Cancellation stops only observation; the original producer still owns
    // every issued cut and its checkpoint/drain obligations.
    tasks.spawn(async move {
        let result = tokio::select! {
            selected = durability.selected_capture() => selected.and_then(|capture| {
                capture.ok_or(Error::Control("managed selection lacks capture"))?;
                Ok(())
            }),
            () = cancelled.cancelled() => Ok(()),
        };
        TaskResult::BundleSelectionReady {
            cell,
            generation,
            result,
        }
    });
}
