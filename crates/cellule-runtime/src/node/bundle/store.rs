//! Canonical immutable catalog I/O and departure guards.
use super::*;
use crate::node::directory::NodeRecord;
use crate::node::{NodeAdvertisement, NodeDirectory, VersionedNodeAdvertisement};

impl NodeDirectory {
    pub(super) async fn upload_catalog(
        &self,
        original: Option<NodeBundleHead>,
        catalog: Catalog,
        frames: &[cellule_ltx::VerifiedNodeFrame],
    ) -> Result<PreparedNodeBundle> {
        let staged = self.stage_catalog(original, catalog, frames, Vec::new())?;
        self.upload_node_bundle(staged).await
    }

    pub(super) fn stage_catalog(
        &self,
        original: Option<NodeBundleHead>,
        mut catalog: Catalog,
        frames: &[cellule_ltx::VerifiedNodeFrame],
        assignments: Vec<crate::node::log::AssignedCommitRange>,
    ) -> Result<std::sync::Arc<StagedNodeBundle>> {
        if self.layout.store().staging_write_prefix().is_some() {
            return Err(Error::Node(
                "bundle selection requires canonical origin storage",
            ));
        }
        catalog.predecessor = original.map(|head| head.digest);
        let (body, digest) = index::encode(&mut catalog, frames)?;
        let head = NodeBundleHead {
            epoch: catalog.epoch,
            digest,
            selected_through: catalog.selected_through,
        };
        Ok(std::sync::Arc::new(StagedNodeBundle {
            original,
            catalog,
            body,
            head,
            assignments,
        }))
    }

    /// Uploads already-encoded immutable bytes, without selecting authority or
    /// granting durability. Shared staged metadata remains usable by one exact
    /// successor while this PUT is pending. Accepted uploads must be joined by
    /// the caller; cancelling this future is not a publication or drain proof.
    pub async fn upload_node_bundle(
        &self,
        staged: std::sync::Arc<StagedNodeBundle>,
    ) -> Result<PreparedNodeBundle> {
        if self.layout.store().staging_write_prefix().is_some() {
            return Err(Error::Node(
                "bundle selection requires canonical origin storage",
            ));
        }
        self.layout
            .store()
            .put_exact(
                &self.layout.node_coverage_bundle_path(
                    staged.catalog.session.as_bytes(),
                    staged.catalog.epoch,
                    staged.head.digest.as_bytes(),
                ),
                staged.body.clone(),
            )
            .await?;
        Ok(PreparedNodeBundle(staged))
    }

    pub(super) async fn select_catalog(
        &self,
        observed: &VersionedNodeAdvertisement,
        prepared: &PreparedNodeBundle,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        self.select_catalog_with_coverage(observed, prepared, None, now_ms)
            .await
    }

    pub(super) async fn select_native_catalog(
        &self,
        observed: &VersionedNodeAdvertisement,
        prepared: &PreparedNodeBundle,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        self.select_catalog_with_coverage(
            observed,
            prepared,
            Some(prepared.head.selected_through),
            now_ms,
        )
        .await
    }

    async fn select_catalog_with_coverage(
        &self,
        observed: &VersionedNodeAdvertisement,
        prepared: &PreparedNodeBundle,
        native_through: Option<u64>,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        let mut base = observed.clone();
        if index::body_digest(&prepared.body)? != prepared.head.digest {
            return Err(Error::Node("bundle proposal digest differs"));
        }
        let mut last_conflict = None;
        for _ in 0..4 {
            self.validate_bundle_source(&base.advertisement, prepared, now_ms)?;
            if base.advertisement.bundle == Some(prepared.head) {
                validate_selected_coverage(&base.advertisement, native_through)?;
                return Ok(base);
            }
            if base.advertisement.bundle != prepared.original {
                return Err(Error::Fenced);
            }
            let mut next = base.advertisement.clone();
            next.bundle = Some(prepared.head);
            if let (Some(log), Some(through)) = (&next.log, native_through) {
                // Root materialization can cover later native ranges. Rebase
                // without regressing either that frontier or heartbeat state.
                next.log = Some(log.advance_tiered(next.node, through.max(log.tiered_through()))?);
            }
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(Error::Node("node generation overflow"))?;
            match self.update_advertisement(&base, next, now_ms).await {
                Ok(selected) => return Ok(selected),
                Err(source) => match self.load(prepared.catalog.session, now_ms).await {
                    Ok(Some(current)) if current.advertisement.bundle == Some(prepared.head) => {
                        self.validate_bundle_source(&current.advertisement, prepared, now_ms)?;
                        validate_selected_coverage(&current.advertisement, native_through)?;
                        return Ok(current);
                    }
                    Ok(Some(current))
                        if current.advertisement.bundle == prepared.original
                            && current.advertisement != base.advertisement =>
                    {
                        last_conflict = Some(source);
                        base = current;
                    }
                    _ => return Err(source),
                },
            }
        }
        Err(last_conflict.unwrap_or(Error::Node("bundle selection CAS contention")))
    }

    fn validate_bundle_source(
        &self,
        source: &NodeAdvertisement,
        prepared: &PreparedNodeBundle,
        now_ms: i64,
    ) -> Result<()> {
        self.validate(source, now_ms)?;
        if source.session != prepared.catalog.session
            || source.log.as_ref().is_some_and(|log| {
                log.epoch() != prepared.head.epoch
                    || log.phase() != crate::node::log_state::NodeLogPhase::Open
            })
        {
            return Err(Error::Fenced);
        }
        Ok(())
    }
}

fn validate_selected_coverage(source: &NodeAdvertisement, through: Option<u64>) -> Result<()> {
    if let (Some(log), Some(through)) = (&source.log, through)
        && log.tiered_through() < through
    {
        return Err(Error::Node(
            "selected bundle lacks canonical native coverage",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn load_catalog(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<Catalog> {
    index::load(layout, session, head, None).await
}

pub(super) async fn load_catalog_cells(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
    cells: &std::collections::BTreeSet<index::CellKey>,
) -> Result<Catalog> {
    index::load_cells(layout, session, head, cells, None, None).await
}

pub(super) async fn load_legacy_catalog(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: NodeBundleHead,
) -> Result<Catalog> {
    let (body, _) = layout
        .store()
        .get_with_etag_bounded(
            &layout.node_coverage_bundle_path(
                session.as_bytes(),
                head.epoch,
                head.digest.as_bytes(),
            ),
            MAX_BUNDLE_BYTES,
        )
        .await?;
    if *blake3::hash(&body).as_bytes() != *head.digest.as_bytes() {
        return Err(Error::Node("bundle catalog digest differs"));
    }
    let mut catalog = codec::decode(&body)?;
    if catalog.session != session
        || catalog.epoch != head.epoch
        || catalog.selected_through != head.selected_through
    {
        return Err(Error::Node("bundle catalog scope differs"));
    }
    for locator in catalog
        .bindings
        .iter_mut()
        .flat_map(|binding| &mut binding.locators)
    {
        if locator.object.is_none() {
            locator.object = Some(head.digest);
        }
    }
    Ok(catalog)
}
/// A Cell pin cannot exist outside the complete canonical inventory, including
/// when callers use the lower-level CellAuthority transition directly.
pub(crate) async fn ensure_enrollment(
    layout: &cellule_ltx::CellStorageLayout,
    control: &Control,
) -> Result<()> {
    let pin = control.bundle_binding.ok_or(Error::Fenced)?;
    let (body, _) = layout
        .store()
        .get_with_etag_bounded(
            &layout.node_path(pin.session.as_bytes()),
            crate::node::MAX_NODE_BYTES,
        )
        .await?;
    let NodeRecord::Advertisement(node) = NodeRecord::decode_canonical(&body)? else {
        return Err(Error::Fenced);
    };
    if node.session != pin.session
        || node.log.as_ref().is_some_and(|log| {
            log.epoch() != pin.epoch || log.phase() != crate::node::log_state::NodeLogPhase::Open
        })
    {
        return Err(Error::Fenced);
    }
    let head = node.bundle.ok_or(Error::PendingPublication)?;
    let cells = [(*layout.application_id(), *control.cell.as_bytes())]
        .into_iter()
        .collect();
    let mut catalog = load_catalog_cells(layout, pin.session, head, &cells).await?;
    let binding = catalog.binding_mut(pin.digest)?;
    if head.epoch != pin.epoch
        || binding.phase != BindingPhase::Provisional
        || binding.application.as_bytes() != layout.application_id()
        || binding.control.bundle_binding != Some(pin)
        || binding.control.cell != control.cell
        || binding.control.incarnation != control.incarnation
        || binding.control.epoch != control.epoch
        || binding.control.owner != control.owner
        || binding.control.root != control.root
        || binding.control.code != control.code
        || binding.control.schema != control.schema
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

/// Shared by every lower-level departure CAS. A caller cannot bypass closure
/// by using CellAuthority directly instead of the live actor's drain API.
pub(crate) async fn ensure_departure(
    layout: &cellule_ltx::CellStorageLayout,
    control: &Control,
) -> Result<()> {
    let Some(pin) = control.bundle_binding else {
        return Ok(());
    };
    let (body, _) = layout
        .store()
        .get_with_etag_bounded(
            &layout.node_path(pin.session.as_bytes()),
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
    let cells = [(*layout.application_id(), *control.cell.as_bytes())]
        .into_iter()
        .collect();
    let mut catalog = load_catalog_cells(layout, pin.session, head, &cells).await?;
    if pin.epoch != catalog.epoch {
        return Err(Error::Fenced);
    }
    let binding = catalog.binding_mut(pin.digest)?;
    let root = control.ltx_root().ok_or(Error::PendingPublication)?;
    if binding.phase != BindingPhase::Closed
        || !binding.locators.is_empty()
        || binding.application.as_bytes() != layout.application_id()
        || binding.control.cell != control.cell
        || binding.control.incarnation != control.incarnation
        || binding.control.epoch != control.epoch
        || root.commit_sequence != binding.selected_commit
        || root.position != binding.selected_position
        || binding.control.ltx_root() != Some(root)
    {
        return Err(Error::PendingPublication);
    }
    Ok(())
}

/// An immutable terminal catalog can outlive its boot record. Maintenance may
/// only treat that boot as drained after every binding checkpoint removed its
/// reconstruction dependencies under the exact materialized Cell root CAS.
pub(crate) async fn ensure_session_drained(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    head: Option<NodeBundleHead>,
) -> Result<()> {
    let Some(head) = head else {
        return Ok(());
    };
    index::ensure_drained(layout, session, head).await
}
