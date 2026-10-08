//! Failed-session inventory and exact selected-prefix reconstruction.
use super::*;
mod drain;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::node::FencedNodeSession;
use crate::node::directory::NodeRecord;
pub(crate) use drain::{BundleRecoveryDrain, logical_now};

pub(crate) type GenerationKey = ([u8; 16], [u8; 32], [u8; 16], u64);

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
#[derive(Default)]
pub(crate) struct OwnerBundleInventory {
    pub(crate) controls: Vec<(crate::ApplicationId, Control)>,
    pub(crate) closed: std::collections::BTreeSet<GenerationKey>,
}

pub(crate) async fn inventory_for_owner(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
) -> Result<OwnerBundleInventory> {
    let node = match record(layout, session).await {
        Ok(node) => node,
        // Scope-only catalog discovery also supports applications that do not
        // use a node directory. A bound Cell still fails the fenced lookup.
        Err(Error::Storage(cellule_store::StorageError::NotFound { .. })) => {
            return Ok(OwnerBundleInventory::default());
        }
        Err(source) => return Err(source),
    };
    let head = match node {
        NodeRecord::Advertisement(node) => node.bundle,
        NodeRecord::Tombstone(node) => node.bundle,
    };
    let Some(head) = head else {
        return Ok(OwnerBundleInventory::default());
    };
    inventory_at(layout, session, head).await
}

async fn inventory_at(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<OwnerBundleInventory> {
    let mut inventory = OwnerBundleInventory::default();
    for binding in index::binding_inventory(layout, session, head).await? {
        if binding.phase == BindingPhase::Closed {
            inventory.closed.insert((
                *binding.application.as_bytes(),
                *binding.control.cell.as_bytes(),
                *binding.control.incarnation.as_bytes(),
                binding.control.epoch,
            ));
        } else {
            inventory
                .controls
                .push((binding.application, binding.control));
        }
    }
    Ok(inventory)
}

async fn fenced_bindings(
    layout: &cellule_ltx::CellStorageLayout,
    fenced: &FencedNodeSession,
) -> Result<Vec<Binding>> {
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
    index::binding_inventory(layout, fenced.session(), head).await
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
    let (proof, frames) =
        super::proof::load_binding_at(layout, head, authority, control, limits).await?;
    let root = control.value().ltx_root().ok_or(Error::Fenced)?;
    match super::proof::checkpoint_prefix(&proof.binding, &frames, &root) {
        Ok(_) => {}
        Err(Error::PendingPublication)
            if root.commit_sequence > proof.commit_sequence()
                && root.position.txid > proof.position().txid => {}
        Err(source) => return Err(source),
    }
    // Recovery may resume between its root CAS and terminal catalog CAS. This
    // ahead root is only a verified base; the full follower witness still has
    // to agree and the complete terminal endpoint must match before closure.
    if proof.binding.control.ltx_root() != Some(root) {
        let mut current = proof.binding.clone();
        current.control = control.value().clone();
        super::proof::verify_base(layout, &current, limits).await?;
    }
    Ok(frames)
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
