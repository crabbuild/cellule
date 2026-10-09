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
        load_coverage_at(&self.layout, head, authority, control, limits)
            .await
            .map(|(proof, _)| proof)
    }
}

pub(super) async fn load_coverage_at(
    layout: &cellule_ltx::CellStorageLayout,
    head: NodeBundleHead,
    authority: &CellAuthority,
    control: &VersionedControl,
    limits: cellule_ltx::Limits,
) -> Result<(BundleCoverageProof, Vec<cellule_ltx::VerifiedNodeFrame>)> {
    let (proof, frames) = load_binding_at(layout, head, authority, control, limits).await?;
    let current = control.value().ltx_root().ok_or(Error::Fenced)?;
    checkpoint_prefix(&proof.binding, &frames, &current)?;
    Ok((proof, frames))
}

pub(super) async fn load_binding_at(
    layout: &cellule_ltx::CellStorageLayout,
    head: NodeBundleHead,
    authority: &CellAuthority,
    control: &VersionedControl,
    limits: cellule_ltx::Limits,
) -> Result<(BundleCoverageProof, Vec<cellule_ltx::VerifiedNodeFrame>)> {
    let value = control.value();
    let pin = value.bundle_binding.ok_or(Error::PendingPublication)?;
    if authority.layout().node_path(pin.session.as_bytes())
        != layout.node_path(pin.session.as_bytes())
        || authority.layout().immutable_cache_identity() != layout.immutable_cache_identity()
    {
        return Err(Error::Fenced);
    }
    let cells = [(*authority.layout().application_id(), *value.cell.as_bytes())]
        .into_iter()
        .collect();
    let mut catalog = store::load_catalog_cells(layout, pin.session, head, &cells).await?;
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
    let frames = verify_binding(layout, pin.session, head.epoch, &binding, limits).await?;
    Ok((
        BundleCoverageProof {
            pin,
            binding,
            head,
            session: pin.session,
            live: None,
        },
        frames,
    ))
}

pub(super) async fn verify_binding(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    binding: &Binding,
    limits: cellule_ltx::Limits,
) -> Result<Vec<cellule_ltx::VerifiedNodeFrame>> {
    let mut frames = Vec::with_capacity(binding.locators.len());
    verify_binding_into(
        layout,
        session,
        epoch,
        binding,
        limits,
        None,
        Some(&mut frames),
    )
    .await?;
    Ok(frames)
}

async fn verify_binding_into(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    binding: &Binding,
    limits: cellule_ltx::Limits,
    origin: Option<&origin::OriginBundle>,
    mut frames: Option<&mut Vec<cellule_ltx::VerifiedNodeFrame>>,
) -> Result<()> {
    verify_base(layout, binding, limits).await?;
    let mut chain = BindingChain::new(binding)?;
    for locator in &binding.locators {
        let object = locator
            .object
            .ok_or(Error::Node("bundle locator is unresolved"))?;
        let end = locator
            .offset
            .checked_add(locator.bytes)
            .ok_or(Error::Node("bundle locator overflow"))?;
        let bytes =
            origin::read_range(layout, session, epoch, object, locator.offset..end, origin).await?;
        let frame = checked_frame(session, epoch, binding, locator, bytes, limits)?;
        chain.accept(FrameStep::from_frame(&frame))?;
        if let Some(frames) = &mut frames {
            frames.push(frame);
        }
    }
    chain.finish(binding)
}

/// Checked frame facts retained only within one verification operation. They
/// grant no proof or availability outside that operation and hold no body.
#[derive(Clone, Copy)]
pub(super) struct FrameStep {
    sequence: u64,
    first_commit: u64,
    commit: u64,
    min_txid: u64,
    pre_checksum: u64,
    position: cellule_ltx::Position,
}

impl FrameStep {
    pub(super) fn from_frame(frame: &cellule_ltx::VerifiedNodeFrame) -> Self {
        Self {
            sequence: frame.scope().node_sequence,
            first_commit: frame.first_commit_sequence(),
            commit: frame.scope().commit_sequence,
            min_txid: frame.segment().min_txid,
            pre_checksum: frame.segment().pre_checksum,
            position: frame.segment().position(),
        }
    }
}

pub(super) struct BindingChain {
    position: cellule_ltx::Position,
    commit: u64,
    sequence: u64,
    first_commit: u64,
}

impl BindingChain {
    pub(super) fn new(binding: &Binding) -> Result<Self> {
        let base = binding
            .control
            .ltx_root()
            .ok_or(Error::Node("bundle base absent"))?;
        Ok(Self {
            position: base.position,
            commit: base.commit_sequence,
            sequence: 0,
            first_commit: base.commit_sequence,
        })
    }

    pub(super) fn accept(&mut self, step: FrameStep) -> Result<()> {
        if step.sequence <= self.sequence
            || (step.commit == self.commit && step.first_commit != self.first_commit)
            || (step.commit != self.commit && self.commit.checked_add(1) != Some(step.first_commit))
            || self.position.txid.checked_add(1) != Some(step.min_txid)
            || self.position.checksum != step.pre_checksum
        {
            return Err(Error::Node("bundle locator violates exact Cell range"));
        }
        self.position = step.position;
        self.commit = step.commit;
        self.sequence = step.sequence;
        self.first_commit = step.first_commit;
        Ok(())
    }

    pub(super) fn finish(self, binding: &Binding) -> Result<()> {
        if self.position != binding.selected_position
            || self.commit != binding.selected_commit
            || (!binding.locators.is_empty() && self.sequence != binding.selected_sequence)
        {
            return Err(Error::Node("bundle proof endpoint differs"));
        }
        Ok(())
    }
}

pub(super) fn checked_frame(
    session: SessionId,
    epoch: u64,
    binding: &Binding,
    locator: &Locator,
    bytes: Bytes,
    limits: cellule_ltx::Limits,
) -> Result<cellule_ltx::VerifiedNodeFrame> {
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
    {
        return Err(Error::Node("bundle locator violates exact Cell range"));
    }
    Ok(frame)
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
