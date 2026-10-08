//! Original Cell pin enrollment in the complete node catalog.
use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::control::{ControlState, Transition};
use crate::identity::ApplicationId;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};

impl NodeDirectory {
    /// Creates an empty bundle lane before native issuance starts for this boot.
    /// The caller must hold startup admission closed until initialization and
    /// original Cell binding enrollment finish.
    pub async fn initialize_bundle_lane(
        &self,
        observed: &VersionedNodeAdvertisement,
        epoch: u64,
        now_ms: i64,
    ) -> Result<VersionedNodeAdvertisement> {
        if observed.advertisement().bundle_head().is_some()
            || epoch == 0
            || observed
                .advertisement()
                .log()
                .is_some_and(|log| log.active() || log.tiered_through() != 0)
        {
            return Err(Error::Node("bundle lane is already initialized or invalid"));
        }
        let catalog = Catalog {
            session: observed.advertisement().session(),
            epoch,
            predecessor: None,
            selected_through: 0,
            bindings: Vec::new(),
            index: None,
        };
        let prepared = self.upload_catalog(None, catalog, &[]).await?;
        self.select_catalog(observed, &prepared, now_ms).await
    }

    /// Reserves a provisional catalog entry before pinning the original Cell,
    /// then opens it only after the Cell CAS is confirmed. Cancellation or an
    /// ambiguous failure leaves a catalog obligation that blocks maintenance.
    pub async fn bind_bundle_cell(
        &self,
        observed: &VersionedNodeAdvertisement,
        authority: &CellAuthority,
        control: &VersionedControl,
        now_ms: i64,
    ) -> Result<(VersionedNodeAdvertisement, VersionedControl)> {
        self.validate(&observed.advertisement, now_ms)?;
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let value = control.value();
        let cells = [(*authority.layout().application_id(), *value.cell.as_bytes())]
            .into_iter()
            .collect();
        let mut catalog =
            store::load_catalog_cells(&self.layout, observed.advertisement.session, head, &cells)
                .await?;
        if value.state != ControlState::Serving
            || value.recovery.is_some()
            || value
                .owner
                .as_ref()
                .is_none_or(|owner| owner.session != catalog.session)
        {
            return Err(Error::Fenced);
        }
        if authority.layout().node_path(catalog.session.as_bytes())
            != self.layout.node_path(catalog.session.as_bytes())
            || authority.layout().immutable_cache_identity()
                != self.layout.immutable_cache_identity()
        {
            return Err(Error::Fenced);
        }
        let application = ApplicationId::from_bytes(*authority.layout().application_id());
        let existing = catalog.bindings.iter().find(|binding| {
            binding.application == application
                && binding.control.cell == value.cell
                && binding.phase != BindingPhase::Closed
        });
        if let Some(binding) = existing {
            if binding.control.incarnation != value.incarnation
                || binding.control.epoch != value.epoch
                || binding.control.owner != value.owner
                || binding.control.code != value.code
                || binding.control.schema != value.schema
                || binding.control.root != value.root
            {
                return Err(Error::Fenced);
            }
            if binding.phase == BindingPhase::Open {
                if binding.control.bundle_binding != value.bundle_binding {
                    return Err(Error::Fenced);
                }
                return Ok((observed.clone(), control.clone()));
            }
            if binding.phase != BindingPhase::Provisional {
                return Err(Error::Fenced);
            }
        }
        let pin = if let Some(binding) = existing {
            binding
                .control
                .bundle_binding
                .ok_or(Error::Node("provisional binding lacks pin"))?
        } else if value.bundle_binding.is_some() {
            // The provisional inventory CAS always precedes the original Cell
            // pin CAS. A pinned Cell absent from its shard is not enrollment.
            return Err(Error::Fenced);
        } else {
            let mut identity = value.encode()?;
            identity.extend_from_slice(b"cellule.bundle-binding.v1\0");
            identity.extend_from_slice(application.as_bytes());
            identity.extend_from_slice(&catalog.epoch.to_le_bytes());
            BundleBindingRef {
                session: catalog.session,
                epoch: catalog.epoch,
                digest: Digest::from_bytes(*blake3::hash(&identity).as_bytes()),
            }
        };
        if pin.session != catalog.session
            || pin.epoch != catalog.epoch
            || value.bundle_binding.is_some_and(|current| current != pin)
            || catalog.bindings.iter().any(|binding| {
                binding.phase == BindingPhase::Closed && binding.control.bundle_binding == Some(pin)
            })
        {
            return Err(Error::Fenced);
        }
        let mut next = value.clone();
        if next.bundle_binding.is_none() {
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or(Error::Control("revision overflow"))?;
            next.progress = next
                .progress
                .checked_add(1)
                .ok_or(Error::Control("progress overflow"))?;
            next.bundle_binding = Some(pin);
        }
        // Select the inventory obligation first. Otherwise cancellation after
        // Cell pinning could leave a pin absent from the complete node catalog.
        let reserved = if existing.is_none() {
            let base = next
                .ltx_root()
                .ok_or(Error::Node("bundle binding lacks published base"))?;
            catalog.bindings.push(Binding {
                application,
                first_commit: base.commit_sequence,
                control: next.clone(),
                phase: BindingPhase::Provisional,
                terminal: None,
                selected_sequence: 0,
                selected_commit: base.commit_sequence,
                selected_position: base.position,
                locators: Vec::new(),
            });
            catalog.bindings.sort_unstable_by_key(|binding| {
                binding
                    .control
                    .bundle_binding
                    .map(|pin| *pin.digest.as_bytes())
            });
            let prepared = self
                .upload_catalog(Some(head), catalog.clone(), &[])
                .await?;
            self.select_catalog(observed, &prepared, now_ms).await?
        } else {
            observed.clone()
        };
        let pinned = if value.bundle_binding.is_some() {
            control.clone()
        } else {
            match authority
                .transition(control, next.clone(), Transition::BindBundle)
                .await
            {
                Ok(pinned) => pinned,
                Err(source) => match authority.load(value.cell).await {
                    Ok(Some(current)) if current.value() == &next => current,
                    _ => return Err(source),
                },
            }
        };
        let binding = catalog.binding_mut(pin.digest)?;
        binding.control = pinned.value().clone();
        binding.phase = BindingPhase::Open;
        let prepared = self
            .upload_catalog(reserved.advertisement.bundle, catalog, &[])
            .await?;
        Ok((
            self.select_catalog(&reserved, &prepared, now_ms).await?,
            pinned,
        ))
    }
}
