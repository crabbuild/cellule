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
        proof: &BundleCoverageProof,
        limits: cellule_ltx::Limits,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        self.checkpoint_bundle_cells(observed, &[(authority, proof)], limits, now_ms)
            .await
    }

    /// Checkpoints a bounded cohort of independently materialized roots with one
    /// index upload and one shared node CAS. Each root must match its opaque
    /// complete-capture proof; any missing/stale participant leaves the catalog
    /// unchanged. Callers retain their admissions until this operation joins.
    /// Fresh small-root verification overlaps at most eight operations within
    /// the original 4-MiB working allowance; larger graphs remain serial.
    pub async fn checkpoint_bundle_cells(
        &self,
        observed: &VersionedNodeAdvertisement,
        checkpoints: &[(&CellAuthority, &BundleCoverageProof)],
        limits: cellule_ltx::Limits,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        self.validate(&observed.advertisement, now_ms)?;
        if checkpoints.is_empty() || checkpoints.len() > MAX_FRAMES {
            return Err(Error::Capacity("bundle checkpoint count"));
        }
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let mut pins = std::collections::HashSet::new();
        let mut cells = std::collections::BTreeSet::new();
        for (_, proof) in checkpoints {
            if proof.pin.session != observed.advertisement.session
                || proof.pin.epoch != head.epoch
                || !pins.insert(proof.pin.digest)
            {
                return Err(Error::Fenced);
            }
            cells.insert((
                *proof.binding.application.as_bytes(),
                *proof.binding.control.cell.as_bytes(),
            ));
        }
        let mut catalog =
            store::load_catalog_cells(&self.layout, observed.advertisement.session, head, &cells)
                .await?;
        let mut changed = Vec::with_capacity(checkpoints.len());
        for (authority, proof) in checkpoints {
            let pin = proof.binding();
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
            let prefix = materialized_prefix(binding, proof, &root)?;
            if prefix == 0 && Some(root) == binding.control.ltx_root() {
                // Repeating an exact installed checkpoint is a no-op even if
                // newer captures remain selected beyond this materialized base.
                continue;
            }
            binding.control = control.clone();
            binding.locators.drain(..prefix);
            if binding.locators.is_empty() {
                binding.first_commit = binding.selected_commit;
            }
            changed.push(pin.digest);
        }
        if changed.is_empty() {
            return Ok(observed.clone());
        }
        {
            // These are only the original changed participants, never siblings.
            // Catalog edits remain private until every fresh root/dependency
            // joins the same admitted verifier used by shared selection.
            let bases = catalog
                .bindings
                .iter()
                .filter(|binding| {
                    binding
                        .control
                        .bundle_binding
                        .is_some_and(|pin| changed.contains(&pin.digest))
                })
                .collect::<Vec<_>>();
            if bases.len() != changed.len() {
                return Err(Error::Node("bundle checkpoint participant is absent"));
            }
            verification::verify_bases(&self.layout, &bases, limits).await?;
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
        let scope = issued.scope();
        let cells = [(*scope.application.as_bytes(), *scope.cell.as_bytes())]
            .into_iter()
            .collect();
        let mut catalog =
            store::load_catalog_cells(&self.layout, pin.session, head, &cells).await?;
        if pin.epoch != catalog.epoch {
            return Err(Error::Fenced);
        }
        let binding = catalog.binding_mut(pin.digest)?;
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
        issued: CellIssuedRange,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let scope = issued.scope();
        let cells = [(*scope.application.as_bytes(), *scope.cell.as_bytes())]
            .into_iter()
            .collect();
        let mut catalog =
            store::load_catalog_cells(&self.layout, pin.session, head, &cells).await?;
        let binding = catalog.binding_mut(pin.digest)?;
        if pin.epoch != head.epoch
            || issued.leader_session() != pin.session
            || issued.log_epoch() != pin.epoch
            || scope.application != binding.application
            || scope.cell != binding.control.cell
            || scope.incarnation != binding.control.incarnation
            || scope.cell_epoch != binding.control.epoch
            || binding.terminal
                != Some((
                    issued.last_node_sequence(),
                    issued.commit_sequence(),
                    issued.position(),
                ))
        {
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

/// The opaque proof authenticated a complete assigned capture endpoint before
/// selection. Matching its exact locator suffix against the fresh catalog
/// releases that prefix without re-reading data that materialization consumed.
/// A later checkpoint may have advanced the base; its retained locators must
/// still be the identical suffix of this proof. Root origin is verified by the
/// caller before the catalog CAS. An integer command watermark is insufficient.
fn materialized_prefix(
    binding: &Binding,
    proof: &BundleCoverageProof,
    root: &cellule_ltx::RootRef,
) -> Result<usize> {
    if binding.control.bundle_binding != Some(proof.pin)
        || binding.application != proof.binding.application
        || binding.control.cell != proof.binding.control.cell
        || binding.control.incarnation != proof.binding.control.incarnation
        || binding.control.epoch != proof.binding.control.epoch
        || binding.control.code != proof.binding.control.code
        || binding.control.schema != proof.binding.control.schema
        || root.cell != *binding.control.cell.as_bytes()
        || root.incarnation != *binding.control.incarnation.as_bytes()
    {
        return Err(Error::Fenced);
    }
    if root.commit_sequence != proof.commit_sequence() || root.position != proof.position() {
        return Err(Error::PendingPublication);
    }
    let base = binding
        .control
        .ltx_root()
        .ok_or(Error::PendingPublication)?;
    if base.commit_sequence == root.commit_sequence && base.position == root.position {
        return Ok(0);
    }
    if base.commit_sequence >= root.commit_sequence || base.position.txid >= root.position.txid {
        return Err(Error::PendingPublication);
    }
    let first = binding.locators.first().ok_or(Error::PendingPublication)?;
    let skip = proof
        .binding
        .locators
        .iter()
        .position(|locator| locator == first)
        .ok_or(Error::Node(
            "materialized proof does not continue the catalog base",
        ))?;
    let locators = &proof.binding.locators[skip..];
    if locators.is_empty() || binding.locators.get(..locators.len()) != Some(locators) {
        return Err(Error::Node(
            "materialized proof differs from selected catalog prefix",
        ));
    }
    Ok(locators.len())
}
