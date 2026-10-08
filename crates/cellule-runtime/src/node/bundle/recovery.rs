//! Failed-session inventory and exact selected-prefix reconstruction.
use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::node::FencedNodeSession;
use crate::node::directory::NodeRecord;

async fn record(layout: &cellule_ltx::CellStorageLayout, session: SessionId) -> Result<NodeRecord> {
    let (bytes, _) = layout
        .store()
        .get_with_etag_bounded(
            &layout.node_path(session.as_bytes()),
            crate::node::MAX_NODE_BYTES,
        )
        .await?;
    let record = NodeRecord::decode_canonical(&bytes)?;
    let actual_session = match &record {
        NodeRecord::Advertisement(node) => node.session,
        NodeRecord::Tombstone(node) => node.session,
    };
    if actual_session != session {
        return Err(Error::Fenced);
    }
    Ok(record)
}

/// Discovery remains advisory; only a fenced claim can attach recovered state.
/// Authenticate every shard without loading dense histories during discovery.
pub(crate) async fn inventory_for_owner(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
) -> Result<Vec<(crate::ApplicationId, Control)>> {
    let node = match record(layout, session).await {
        Ok(node) => node,
        // Scope-only catalog discovery also supports applications that do not
        // use a node directory. A bound Cell still fails the fenced lookup.
        Err(Error::Storage(cellule_store::StorageError::NotFound { .. })) => return Ok(Vec::new()),
        Err(source) => return Err(source),
    };
    let head = match node {
        NodeRecord::Advertisement(node) => node.bundle,
        NodeRecord::Tombstone(node) => node.bundle,
    };
    let Some(head) = head else {
        return Ok(Vec::new());
    };
    inventory_at(layout, session, head).await
}

async fn inventory_at(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<Vec<(crate::ApplicationId, Control)>> {
    Ok(index::binding_inventory(layout, session, head)
        .await?
        .into_iter()
        .filter(|binding| binding.phase != BindingPhase::Closed)
        .map(|binding| (binding.application, binding.control))
        .collect())
}

pub(crate) async fn fenced_inventory(
    layout: &cellule_ltx::CellStorageLayout,
    fenced: &FencedNodeSession,
) -> Result<Vec<(crate::ApplicationId, Control)>> {
    let Some(head) = fenced.bundle_head() else {
        return Ok(Vec::new());
    };
    let NodeRecord::Tombstone(current) = record(layout, fenced.session()).await? else {
        return Err(Error::Fenced);
    };
    if current.node != fenced.node()
        || current.claimant != Some(fenced.claimant())
        || current.claim_generation != fenced.claim_generation()
        || current.claim_expires_at_ms != Some(fenced.claim_expires_at_ms())
        || current.log.as_ref() != fenced.log()
        || current.bundle != Some(head)
    {
        return Err(Error::Fenced);
    }
    inventory_at(layout, fenced.session(), head).await
}

pub(crate) async fn selected_frames(
    layout: &cellule_ltx::CellStorageLayout,
    authority: &CellAuthority,
    control: &VersionedControl,
    fenced: &FencedNodeSession,
    limits: cellule_ltx::Limits,
) -> Result<Vec<cellule_ltx::VerifiedNodeFrame>> {
    let pin = control.value().bundle_binding.ok_or(Error::Fenced)?;
    let head = fenced.bundle_head().ok_or(Error::Fenced)?;
    if pin.session != fenced.session() || pin.epoch != head.epoch {
        return Err(Error::Fenced);
    }
    // Return original frames, including a materialized overlap, so the global
    // follower witness must agree byte-for-byte with selected native sequence.
    super::proof::load_coverage_at(layout, head, authority, control, limits)
        .await
        .map(|(_, frames)| frames)
}

/// A bound writer may retain a recovery overlay only after canonical fencing.
/// Keep the original pin until the complete suffix has a materialized root;
/// neither this attachment nor a manifest upload grants departure authority.
pub(crate) async fn ensure_attachment(
    layout: &cellule_ltx::CellStorageLayout,
    control: &Control,
    recovery: &crate::control::RecoveryOverlayRef,
) -> Result<()> {
    let pin = control.bundle_binding.ok_or(Error::Fenced)?;
    let NodeRecord::Tombstone(node) = record(layout, pin.session).await? else {
        return Err(Error::Fenced);
    };
    if node.claimant.is_none()
        || node.log.as_ref().is_none_or(|log| {
            log.phase() != crate::node::log_state::NodeLogPhase::Recovering
                || log.epoch() != pin.epoch
        })
        || recovery.leader_session != pin.session
        || recovery.log_epoch != pin.epoch
    {
        return Err(Error::Fenced);
    }
    let head = node.bundle.ok_or(Error::PendingPublication)?;
    let cells = [(*layout.application_id(), *control.cell.as_bytes())]
        .into_iter()
        .collect();
    let mut catalog = store::load_catalog_cells(layout, pin.session, head, &cells).await?;
    let binding = catalog.binding_mut(pin.digest)?;
    if !matches!(binding.phase, BindingPhase::Open | BindingPhase::Closing)
        || binding.control.incarnation != control.incarnation
        || binding.control.epoch != control.epoch
        || binding.control.code != control.code
        || binding.control.schema != control.schema
        || recovery.final_commit_sequence < binding.selected_commit
        || recovery.final_txid < binding.selected_position.txid
    {
        return Err(Error::PendingPublication);
    }
    Ok(())
}
