//! Selected suffix verification and exact reconstruction overlays.
use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::node::NodeDirectory;
use crate::node::directory::NodeRecord;

impl NodeDirectory {
    /// Reopens one authority-pinned selected Cell suffix from canonical origin.
    /// This is a reconstruction capability, including after the original boot
    /// was fenced. It grants no writer lease, command ACK, or complete follower
    /// suffix: issued Fleet ranges above the selected head still require drain.
    pub async fn load_bundle_coverage(
        &self,
        authority: &CellAuthority,
        control: &VersionedControl,
        limits: cellule_ltx::Limits,
    ) -> Result<BundleCoverageProof> {
        let value = control.value();
        let pin = value.bundle_binding.ok_or(Error::PendingPublication)?;
        if authority.layout().node_path(pin.session.as_bytes())
            != self.layout.node_path(pin.session.as_bytes())
            || authority.layout().immutable_cache_identity()
                != self.layout.immutable_cache_identity()
        {
            return Err(Error::Fenced);
        }
        let (body, _) = self
            .layout
            .store()
            .get_with_etag_bounded(
                &self.layout.node_path(pin.session.as_bytes()),
                crate::node::MAX_NODE_BYTES,
            )
            .await?;
        let record = NodeRecord::decode_canonical(&body)?;
        let (session, head) = match record {
            NodeRecord::Advertisement(node) => (node.session, node.bundle),
            NodeRecord::Tombstone(node) => (node.session, node.bundle),
        };
        if session != pin.session {
            return Err(Error::Fenced);
        }
        let head = head.ok_or(Error::PendingPublication)?;
        let shards = [index::shard(
            authority.layout().application_id(),
            value.cell.as_bytes(),
        )]
        .into_iter()
        .collect();
        let mut catalog = store::load_catalog_shards(&self.layout, session, head, &shards).await?;
        let binding = catalog.binding_mut(pin.digest)?.clone();
        if binding.phase == BindingPhase::Provisional {
            return Err(Error::PendingPublication);
        }
        if head.epoch != pin.epoch
            || binding.control.bundle_binding != Some(pin)
            || binding.application.as_bytes() != authority.layout().application_id()
            || binding.control.cell != value.cell
            || binding.control.incarnation != value.incarnation
            || binding.control.epoch != value.epoch
            || binding.control.code != value.code
            || binding.control.schema != value.schema
        {
            return Err(Error::Fenced);
        }
        let frames = verify_binding(&self.layout, session, head.epoch, &binding, limits).await?;
        let current = value.ltx_root().ok_or(Error::Fenced)?;
        checkpoint_prefix(&binding, &frames, &current)?;
        Ok(BundleCoverageProof {
            pin,
            binding,
            head,
            session,
        })
    }
}

pub(super) async fn verify_binding(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    binding: &Binding,
    limits: cellule_ltx::Limits,
) -> Result<Vec<cellule_ltx::VerifiedNodeFrame>> {
    verify_base(layout, binding, limits).await?;
    let mut position = binding
        .control
        .ltx_root()
        .ok_or(Error::Node("bundle base absent"))?
        .position;
    let mut commit = binding
        .control
        .root
        .as_ref()
        .ok_or(Error::Node("bundle base absent"))?
        .commit_sequence;
    let mut frames = Vec::with_capacity(binding.locators.len());
    let mut sequence = 0;
    let mut first_commit = commit;
    for locator in &binding.locators {
        let object = locator
            .object
            .ok_or(Error::Node("bundle locator is unresolved"))?;
        let end = locator
            .offset
            .checked_add(locator.bytes)
            .ok_or(Error::Node("bundle locator overflow"))?;
        let bytes = layout
            .store()
            .range_get(
                &layout.node_coverage_bundle_path(session.as_bytes(), epoch, object.as_bytes()),
                locator.offset..end,
            )
            .await?;
        if bytes.len() as u64 != locator.bytes
            || *blake3::hash(&bytes).as_bytes() != *locator.frame_digest.as_bytes()
        {
            return Err(Error::Node("bundle frame digest differs"));
        }
        let frame = cellule_ltx::inspect_node_frame(bytes, limits)?;
        let scope = frame.scope();
        if scope.leader_session != *session.as_bytes()
            || scope.log_epoch != epoch
            || scope.application != *binding.application.as_bytes()
            || scope.cell != *binding.control.cell.as_bytes()
            || scope.incarnation != *binding.control.incarnation.as_bytes()
            || scope.cell_epoch != binding.control.epoch
            || scope.node_sequence <= sequence
            || (scope.commit_sequence == commit && frame.first_commit_sequence() != first_commit)
            || (scope.commit_sequence != commit
                && commit.checked_add(1) != Some(frame.first_commit_sequence()))
            || position.txid.checked_add(1) != Some(frame.segment().min_txid)
            || position.checksum != frame.segment().pre_checksum
        {
            return Err(Error::Node("bundle locator violates exact Cell range"));
        }
        first_commit = frame.first_commit_sequence();
        position = frame.segment().position();
        commit = scope.commit_sequence;
        sequence = scope.node_sequence;
        frames.push(frame);
    }
    if position != binding.selected_position
        || commit != binding.selected_commit
        || (!binding.locators.is_empty() && sequence != binding.selected_sequence)
    {
        return Err(Error::Node("bundle proof endpoint differs"));
    }
    Ok(frames)
}

pub(super) async fn verify_base(
    layout: &cellule_ltx::CellStorageLayout,
    binding: &Binding,
    limits: cellule_ltx::Limits,
) -> Result<()> {
    let base = binding
        .control
        .ltx_root()
        .ok_or(Error::Node("bundle base absent"))?;
    let replica = cellule_ltx::CellReplica::new(
        layout.for_application(*binding.application.as_bytes()),
        *binding.control.cell.as_bytes(),
        *binding.control.incarnation.as_bytes(),
        limits,
    )?;
    // Reconstructability requires origin dependencies, even if metadata was
    // authenticated earlier in this process. A cached root is not availability.
    replica
        .reachable_objects_bounded(&base, MAX_BASE_OBJECTS)
        .await?;
    Ok(())
}

impl BundleCoverageProof {
    /// Revalidates every bounded locator and builds an exact overlay for normal
    /// root preparation. This runs independently of the selecting bundle CAS.
    pub async fn recovery_overlay(
        &self,
        layout: &cellule_ltx::CellStorageLayout,
        limits: cellule_ltx::Limits,
    ) -> Result<cellule_ltx::RecoveryOverlay> {
        self.recovery_overlay_from(layout, limits, self.base()?)
            .await
    }

    pub(crate) async fn recovery_overlay_from(
        &self,
        layout: &cellule_ltx::CellStorageLayout,
        limits: cellule_ltx::Limits,
        base: cellule_ltx::RootRef,
    ) -> Result<cellule_ltx::RecoveryOverlay> {
        if layout.application_id() != self.binding.application.as_bytes() {
            return Err(Error::Fenced);
        }
        let frames =
            verify_binding(layout, self.session, self.head.epoch, &self.binding, limits).await?;
        let skip = checkpoint_prefix(&self.binding, &frames, &base)?;
        let entries = frames[skip..]
            .iter()
            .map(|frame| {
                cellule_ltx::bundle::BundleEntry::for_cell(
                    *self.binding.control.cell.as_bytes(),
                    *self.binding.control.incarnation.as_bytes(),
                    frame.segment().clone(),
                    frame.body().to_vec(),
                )
            })
            .collect();
        let bundle = cellule_ltx::bundle::Bundle::encode(entries, limits)?;
        Ok(cellule_ltx::RecoveryOverlay::new(
            base,
            bundle,
            self.position(),
            self.commit_sequence(),
        ))
    }
}

/// An exact complete capture boundary may checkpoint a prefix while later
/// captures continue selection. A row's final command number alone cannot
/// checkpoint inside a multi-frame physical capture.
pub(super) fn checkpoint_prefix(
    binding: &Binding,
    frames: &[cellule_ltx::VerifiedNodeFrame],
    root: &cellule_ltx::RootRef,
) -> Result<usize> {
    let base = binding
        .control
        .ltx_root()
        .ok_or(Error::Node("bundle base absent"))?;
    if root.cell != base.cell || root.incarnation != base.incarnation {
        return Err(Error::Fenced);
    }
    if root.commit_sequence == base.commit_sequence && root.position == base.position {
        return Ok(0);
    }
    for (index, frame) in frames.iter().enumerate() {
        if frame.scope().commit_sequence == root.commit_sequence
            && frame.segment().position() == root.position
        {
            if frames
                .get(index + 1)
                .is_some_and(|next| next.scope().commit_sequence == root.commit_sequence)
            {
                return Err(Error::Node("bundle checkpoint splits an assigned capture"));
            }
            return Ok(index + 1);
        }
    }
    Err(Error::PendingPublication)
}
