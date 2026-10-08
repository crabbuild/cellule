//! Bundle selection consumed by the actor's existing serialized publisher.

use super::*;
use crate::publication::VerifiedBundleCapture;

// This is only a bounded opportunity for the already installed original feed.
// A stalled producer retains the ordinary root fallback, never a new ACK path.
const SELECTION_OPPORTUNITY: std::time::Duration = std::time::Duration::from_millis(100);

pub(super) async fn selected_prefix(
    publisher: &CellPublisher,
    durabilities: &[Option<PendingDurability>],
    commit_sequence: u64,
    position: cellule_ltx::Position,
) -> crate::Result<Option<Vec<VerifiedBundleCapture>>> {
    if durabilities.is_empty()
        || durabilities.iter().any(|pending| {
            pending
                .as_ref()
                .is_none_or(|pending| !pending.has_bundle_capture())
        })
    {
        return Ok(None);
    }
    let selected = async {
        let mut captures = Vec::with_capacity(durabilities.len());
        for pending in durabilities.iter().flatten() {
            let capture = pending
                .selected_capture()
                .await?
                .ok_or(Error::Control("bundle selection lost original capture"))?;
            captures.push(capture);
        }
        Ok::<_, Error>(captures)
    };
    let captures = match tokio::time::timeout(SELECTION_OPPORTUNITY, selected).await {
        Ok(captures) => captures?,
        Err(_) => return Ok(None),
    };
    let Some(newest) = captures.last() else {
        return Ok(None);
    };
    let proof = &newest.selected().proof;
    // A proof can cover later commands outside this task's retained cohort.
    // Keep the root fallback in that case; endpoint equality alone cannot
    // authorize pruning captures that this publisher does not own.
    if proof.commit_sequence() != commit_sequence || proof.position() != position {
        return Ok(None);
    }
    if publisher.control().value().bundle_binding != Some(proof.binding()) {
        return Err(Error::Fenced);
    }
    Ok(Some(captures))
}
