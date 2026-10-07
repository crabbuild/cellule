//! Materialized checkpoints and complete issued-range closure.
use super::*;
use crate::control::authority::CellAuthority;
use crate::node::log::CellIssuedRange;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};

impl NodeDirectory {
    /// Advances a Cell's authenticated reconstruction base only after its exact
    /// materialized root CAS at a complete capture boundary. Hot later ranges
    /// retain their locators while the covered prefix is released, without crediting
    /// an upload, approximate watermark, or root that ends inside a command group.
    pub async fn checkpoint_bundle_cell(
        &self,
        observed: &VersionedNodeAdvertisement,
        authority: &CellAuthority,
        pin: BundleBindingRef,
        limits: cellule_ltx::Limits,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let mut catalog = load_catalog(&self.layout, pin.session, head).await?;
        if pin.epoch != catalog.epoch {
            return Err(Error::Fenced);
        }
        let binding = catalog.binding_mut(pin.digest)?;
        if authority.layout().application_id() != binding.application.as_bytes()
            || authority.layout().node_path(pin.session.as_bytes())
                != self.layout.node_path(pin.session.as_bytes())
            || authority.layout().immutable_cache_identity()
                != self.layout.immutable_cache_identity()
        {
            return Err(Error::Fenced);
        }
        let current = authority
            .load(binding.control.cell)
            .await?
            .ok_or(Error::Fenced)?;
        let control = current.value();
        if control.bundle_binding != Some(pin)
            || control.epoch != binding.control.epoch
            || control.incarnation != binding.control.incarnation
            || control.code != binding.control.code
            || control.schema != binding.control.schema
        {
            return Err(Error::PendingPublication);
        }
        let root = control.ltx_root().ok_or(Error::PendingPublication)?;
        let frames = verify_binding(&self.layout, pin.session, head.epoch, binding, limits).await?;
        let prefix = checkpoint_prefix(binding, &frames, &root)?;
        if prefix == 0 && Some(root) == binding.control.ltx_root() {
            return if binding.locators.is_empty() {
                Ok(observed.clone())
            } else {
                Err(Error::PendingPublication)
            };
        }
        binding.control = control.clone();
        verify_base(&self.layout, binding, limits).await?;
        binding.locators.drain(..prefix);
        if binding.locators.is_empty() {
            binding.first_commit = binding.selected_commit;
        }
        let prepared = self.upload_catalog(Some(head), catalog, &[]).await?;
        self.select_catalog(observed, &prepared, now_ms).await
    }

    /// Freezes the original writer's complete issued endpoint from its native
    /// assignment barrier. A selected-only endpoint cannot close prior Fleet ACKs.
    pub async fn begin_bundle_close(
        &self,
        observed: &VersionedNodeAdvertisement,
        pin: BundleBindingRef,
        issued: CellIssuedRange,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let mut catalog = load_catalog(&self.layout, pin.session, head).await?;
        if pin.epoch != catalog.epoch {
            return Err(Error::Fenced);
        }
        let binding = catalog.binding_mut(pin.digest)?;
        let scope = issued.scope();
        if issued.leader_session() != pin.session
            || issued.log_epoch() != pin.epoch
            || scope.application != binding.application
            || scope.cell != binding.control.cell
            || scope.incarnation != binding.control.incarnation
            || scope.cell_epoch != binding.control.epoch
            || binding.phase == BindingPhase::Closed
            || binding.phase == BindingPhase::Provisional
        {
            return Err(Error::Fenced);
        }
        let terminal = (
            issued.last_node_sequence(),
            issued.commit_sequence(),
            issued.position(),
        );
        if binding.terminal.is_some_and(|before| before != terminal) {
            return Err(Error::Fenced);
        }
        binding.phase = BindingPhase::Closing;
        binding.terminal = Some(terminal);
        let prepared = self.upload_catalog(Some(head), catalog, &[]).await?;
        self.select_catalog(observed, &prepared, now_ms).await
    }

    /// Completes closure only at the exact frozen issued endpoint. Selection may
    /// continue for hot siblings and for previously assigned rows while Closing.
    pub async fn finish_bundle_close(
        &self,
        observed: &VersionedNodeAdvertisement,
        pin: BundleBindingRef,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let mut catalog = load_catalog(&self.layout, pin.session, head).await?;
        let binding = catalog.binding_mut(pin.digest)?;
        if pin.epoch != head.epoch {
            return Err(Error::Fenced);
        }
        if binding.phase == BindingPhase::Open
            || binding.terminal
                != Some((
                    binding.selected_sequence,
                    binding.selected_commit,
                    binding.selected_position,
                ))
        {
            return Err(Error::PendingPublication);
        }
        binding.phase = BindingPhase::Closed;
        let prepared = self.upload_catalog(Some(head), catalog, &[]).await?;
        self.select_catalog(observed, &prepared, now_ms).await
    }
}
