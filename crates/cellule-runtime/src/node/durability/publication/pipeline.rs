//! One lazy successor: pre-admitted overlap, ordered proof, joined I/O.
use super::*;
use crate::fleet::resource::{ResourceCost, ResourceReservation};
use crate::fleet::telemetry::{
    NodePublicationPhase as Phase, PublicationPipelineFallback as Fallback,
};
use crate::node::bundle::PreparedNodeBundle;
use std::sync::OnceLock;

pub(super) struct Unconfirmed {
    pub(super) captures: Vec<AssignedCapture>,
    pub(super) proofs: Vec<BundleCoverageProof>,
}
struct Memory {
    first: usize,
    second: usize,
    reserved: ResourceReservation,
}
struct Successor {
    cohort: CaptureCohort,
    uploaded: PreparedNodeBundle,
    memory: ResourceReservation,
    _working: ResourceReservation,
}
fn reserve(
    original: &NodeDurability,
    authority: &dyn NodeBundlePublicationAuthority,
    captures: usize,
    staged: &crate::node::bundle::StagedNodeBundle,
) -> Result<Memory> {
    let first = authority.receipt_memory_bound(captures)?;
    let second = authority.receipt_memory_bound(MAX_CAPTURES)?;
    let working = staged.retained_work_bytes()?;
    let bytes = first
        .checked_add(second)
        .and_then(|bytes| bytes.checked_add(working))
        .ok_or(Error::Capacity("bundle pipeline memory"))?;
    let reserved = original
        .selection_resources
        .get()
        .ok_or(Error::PendingPublication)?
        .try_reserve_pipeline(ResourceCost::zero().with_retained_bytes(bytes))?;
    Ok(Memory {
        first,
        second,
        reserved,
    })
}
fn recorded<T>(
    result: Result<T>,
    failure: &OnceLock<Arc<Error>>,
    lease: &NodeLeaseGuard,
) -> Result<T> {
    result.map_err(|error| {
        let source = match error {
            Error::Shared(source) => source,
            other => Arc::new(other),
        };
        let first = Arc::clone(failure.get_or_init(|| source));
        lease.fence();
        Error::Shared(first)
    })
}
#[allow(
    clippy::too_many_arguments,
    reason = "one original feed, ordering, admission and callback lifetime"
)]
pub(super) async fn publish(
    original: &NodeDurability,
    authority: &Arc<dyn NodeBundlePublicationAuthority>,
    first: CaptureCohort,
    feed: &mut NodePublicationFeed,
    carry: &mut Option<AssignedCapture>,
    checkpoints: &mut mpsc::Receiver<CheckpointRequest>,
    prefixes: &mut prefixes::Prefixes,
    preparation: &mut crate::node::bundle::BundlePreparation,
    progress: &watch::Sender<Progress>,
    lease: &NodeLeaseGuard,
) -> Result<Option<Unconfirmed>> {
    let telemetry = original.shipper.telemetry();
    let first_capture_count = first.captures.len();
    let failure = OnceLock::new();
    let round = match telemetry
        .publication_phase(Phase::CatalogWait, authority.begin_round())
        .await
    {
        Ok(round) => round,
        Err(error) => {
            let error = Arc::new(error);
            first.ready_checkpoints.complete(Err(Arc::clone(&error)));
            return Err(Error::Shared(error));
        }
    };
    let staged = match telemetry
        .publication_phase(
            Phase::Preparation,
            round.stage(
                None,
                &first.captures,
                first.ready_checkpoints.values(),
                Some(preparation),
            ),
        )
        .await
    {
        Ok(staged) => staged,
        Err(error) => {
            let error = Arc::new(error);
            first.ready_checkpoints.complete(Err(Arc::clone(&error)));
            return Err(Error::Shared(error));
        }
    };
    let (finished, first_finished) = oneshot::channel();
    let (receipt, first_receipt) = oneshot::channel();
    // Preserve the original five-buffer working bound by serializing heavy
    // preparation and verification. Uploads still overlap. Extra admission
    // owns the first compact proposal while the successor uses that scratch.
    // During first verification, its three scratch buffers and the at-most
    // two-buffer successor fit the original five; the first proposal remains
    // covered by this exact additional reservation throughout both cohorts.
    let scratch = tokio::sync::Mutex::new(());
    let (first_result, successor) = tokio::join!(
        async {
            let result = async {
                let publication = async {
                    let uploaded = telemetry
                        .publication_phase(Phase::Upload, round.upload(Arc::clone(&staged)))
                        .await?;
                    let prior = prefixes.for_captures(&first.captures);
                    let borrowed = prior
                        .iter()
                        .map(|selected| &selected.proof)
                        .collect::<Vec<_>>();
                    telemetry
                        .publication_phase(Phase::Selection, async {
                            let _scratch = scratch.lock().await;
                            round.select(&uploaded, &borrowed, lease).await
                        })
                        .await
                }
                .await;
                let publication = recorded(publication, &failure, lease).map_err(Arc::new);
                first
                    .ready_checkpoints
                    .complete(publication.as_ref().map(|_| ()).map_err(Arc::clone));
                // Stop waiting for another capture once this selection completes.
                // An already-ready original capture wins in the successor branch.
                let _ = finished.send(());
                let proofs = publication.map_err(Error::Shared)?;
                let memory = first_receipt.await.map_err(|_| Error::RuntimeClosed)?;
                if let Some(memory) = memory {
                    let selected =
                        receipts::SelectedCaptures::new(original, &first.captures, proofs)?
                            .confirm(original, memory)?;
                    prefixes.remember(&selected);
                    progress.send_modify(|state| state.through = selected.selected_through());
                    drop(selected);
                    drop(first.captures);
                    Ok(None)
                } else {
                    // Return catalog ordering before the serial receipt-credit
                    // wait, which may need a standalone checkpoint to free it.
                    Ok(Some(Unconfirmed {
                        captures: first.captures,
                        proofs,
                    }))
                }
            }
            .await;
            recorded(result, &failure, lease)
        },
        async {
            let result = async {
                let next = if let Some(next) = carry.take().or_else(|| feed.try_recv()) {
                    Some(next)
                } else {
                    tokio::select! {
                        biased;
                        next = feed.recv() => next,
                        _ = first_finished => None,
                        () = lease.wait_fenced() => return Err(Error::Fenced),
                    }
                };
                let Some(next) = next else {
                    telemetry.node_publication_fallback(Fallback::NoSuccessor);
                    let _ = receipt.send(None);
                    return Ok(None);
                };
                let mut memory =
                    match reserve(original, authority.as_ref(), first_capture_count, &staged) {
                        Ok(memory) => memory,
                        Err(Error::Capacity("bundle pipeline headroom")) => {
                            telemetry.node_publication_fallback(Fallback::Headroom);
                            *carry = Some(next);
                            let _ = receipt.send(None);
                            return Ok(None);
                        }
                        Err(Error::Capacity(_)) => {
                            telemetry.node_publication_fallback(Fallback::Capacity);
                            *carry = Some(next);
                            let _ = receipt.send(None);
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    };
                // This is a nonblocking admission under ordering. It either
                // prepaid both receipt lifetimes or returns to the serial path.
                let first_memory = memory.reserved.split_retained(memory.first)?;
                let second_memory = memory.reserved.split_retained(memory.second)?;
                let _ = receipt.send(Some(first_memory));
                // The staged predecessor already carries its verified new
                // bases. A successor inherits them and may compose further
                // checkpoints in its available rows; neither callback gains
                // credit until that cohort's ordered authoritative selection.
                // No standalone checkpoint is dispatched under round ordering.
                let second = assemble(
                    authority,
                    feed,
                    carry,
                    checkpoints,
                    next,
                    None,
                    true,
                    lease,
                    telemetry,
                )
                .await?;
                let upload = async {
                    let next = telemetry
                        .publication_phase(Phase::Preparation, async {
                            let _scratch = scratch.lock().await;
                            round
                                .stage(
                                    Some(&staged),
                                    &second.captures,
                                    second.ready_checkpoints.values(),
                                    Some(preparation),
                                )
                                .await
                        })
                        .await?;
                    lease.check()?;
                    telemetry
                        .publication_phase(Phase::Upload, round.upload(next))
                        .await
                }
                .await;
                match recorded(upload, &failure, lease) {
                    Ok(uploaded) => Ok(Some(Successor {
                        cohort: second,
                        uploaded,
                        memory: second_memory,
                        _working: memory.reserved,
                    })),
                    Err(error) => {
                        let source = match error {
                            Error::Shared(source) => source,
                            other => Arc::new(other),
                        };
                        second.ready_checkpoints.complete(Err(Arc::clone(&source)));
                        Err(Error::Shared(source))
                    }
                }
            }
            .await;
            recorded(result, &failure, lease)
        }
    );
    // Both accepted uploads/metadata reads join even on failure or fencing.
    // Preserve the earliest source, not a secondary Fenced/channel error.
    if let Some(error) = failure.get() {
        if let Ok(Some(successor)) = successor {
            successor
                .cohort
                .ready_checkpoints
                .complete(Err(Arc::clone(error)));
        }
        return Err(Error::Shared(Arc::clone(error)));
    }
    let first_result = first_result?;
    let Some(successor) = successor? else {
        return Ok(first_result);
    };
    let prior = prefixes.for_captures(&successor.cohort.captures);
    let borrowed = prior
        .iter()
        .map(|selected| &selected.proof)
        .collect::<Vec<_>>();
    let result = telemetry
        .publication_phase(
            Phase::Selection,
            round.select(&successor.uploaded, &borrowed, lease),
        )
        .await
        .map_err(Arc::new);
    drop(borrowed);
    drop(prior);
    successor
        .cohort
        .ready_checkpoints
        .complete(result.as_ref().map(|_| ()).map_err(Arc::clone));
    let proofs = result.map_err(Error::Shared)?;
    let selected = receipts::SelectedCaptures::new(original, &successor.cohort.captures, proofs)?
        .confirm(original, successor.memory)?;
    prefixes.remember(&selected);
    progress.send_modify(|state| state.through = selected.selected_through());
    drop(selected);
    drop(successor.cohort.captures);
    drop(successor.uploaded);
    drop(staged);
    drop(round);
    drop(successor._working);
    Ok(None)
}
