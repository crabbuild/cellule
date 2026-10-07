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
        };
        let prepared = self.upload_catalog(None, catalog, &[]).await?;
        self.select_catalog(observed, &prepared, now_ms).await
    }

    /// Pins one original writer in Cell authority before enrolling it in the
    /// complete catalog. An ambiguous failure retains the pin and blocks departure.
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
        let mut catalog = load_catalog(&self.layout, observed.advertisement.session, head).await?;
        let value = control.value();
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
        let pinned = if let Some(pin) = value.bundle_binding {
            if pin.session != catalog.session || pin.epoch != catalog.epoch {
                return Err(Error::Fenced);
            }
            control.clone()
        } else {
            let mut identity = value.encode()?;
            identity.extend_from_slice(b"cellule.bundle-binding.v1\0");
            identity.extend_from_slice(application.as_bytes());
            identity.extend_from_slice(&catalog.epoch.to_le_bytes());
            let mut next = value.clone();
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or(Error::Control("revision overflow"))?;
            next.progress = next
                .progress
                .checked_add(1)
                .ok_or(Error::Control("progress overflow"))?;
            next.bundle_binding = Some(BundleBindingRef {
                session: catalog.session,
                epoch: catalog.epoch,
                digest: Digest::from_bytes(*blake3::hash(&identity).as_bytes()),
            });
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
        let pin = pinned
            .value()
            .bundle_binding
            .ok_or(Error::Node("Cell bundle pin missing"))?;
        if let Some(binding) = catalog
            .bindings
            .iter()
            .find(|binding| binding.control.bundle_binding == Some(pin))
        {
            if binding.phase != BindingPhase::Open || binding.control != *pinned.value() {
                return Err(Error::Fenced);
            }
            return Ok((observed.clone(), pinned));
        }
        // Enrolling two writer bindings for one Cell would allow a departed
        // epoch to rejoin under a new identity. Closed originals remain tombstones.
        if catalog.bindings.iter().any(|binding| {
            binding.application == application
                && binding.control.cell == value.cell
                && binding.phase != BindingPhase::Closed
        }) {
            return Err(Error::Fenced);
        }
        let base = pinned
            .value()
            .ltx_root()
            .ok_or(Error::Node("bundle binding lacks published base"))?;
        catalog.bindings.push(Binding {
            application,
            first_commit: base.commit_sequence,
            control: pinned.value().clone(),
            phase: BindingPhase::Open,
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
        let prepared = self.upload_catalog(Some(head), catalog, &[]).await?;
        Ok((
            self.select_catalog(observed, &prepared, now_ms).await?,
            pinned,
        ))
    }
}
