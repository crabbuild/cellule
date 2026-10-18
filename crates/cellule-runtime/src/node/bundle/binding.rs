//! Original Cell pin enrollment in the complete node catalog.
use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::control::{ControlState, Transition};
use crate::identity::ApplicationId;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};
use futures_util::StreamExt as _;

pub(crate) const MAX_BUNDLE_ENROLLMENTS: usize = 8;

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

    /// Reserves and pins one original Cell before opening SQL admission.
    pub async fn bind_bundle_cell(
        &self,
        observed: &VersionedNodeAdvertisement,
        authority: &CellAuthority,
        control: &VersionedControl,
        now_ms: i64,
    ) -> Result<(VersionedNodeAdvertisement, VersionedControl)> {
        let (node, mut pinned) = self
            .bind_bundle_cells(observed, &[(authority, control)], now_ms)
            .await?;
        Ok((
            node,
            pinned
                .pop()
                .ok_or(Error::Node("bundle enrollment lacks Cell"))?,
        ))
    }

    /// Enrolls at most eight Cells in one provisional/open catalog pair.
    ///
    /// The provisional inventory CAS precedes every original Cell pin. All pin
    /// attempts join before the Open catalog is selected. A failed or cancelled
    /// cohort leaves discoverable provisional obligations, never SQL admission.
    /// Results retain input order. Callers serialize the two catalog selections
    /// with publication and reload node/Cell authority after ambiguous failure.
    pub async fn bind_bundle_cells(
        &self,
        observed: &VersionedNodeAdvertisement,
        cells: &[(&CellAuthority, &VersionedControl)],
        now_ms: i64,
    ) -> Result<(VersionedNodeAdvertisement, Vec<VersionedControl>)> {
        self.validate(&observed.advertisement, now_ms)?;
        if cells.is_empty() || cells.len() > MAX_BUNDLE_ENROLLMENTS {
            return Err(Error::Node("invalid bundle enrollment cohort"));
        }
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        let keys = cells
            .iter()
            .map(|(authority, control)| {
                (
                    *authority.layout().application_id(),
                    *control.value().cell.as_bytes(),
                )
            })
            .collect::<std::collections::BTreeSet<_>>();
        if keys.len() != cells.len() {
            return Err(Error::Node("duplicate bundle enrollment Cell"));
        }
        let mut catalog =
            store::load_catalog_cells(&self.layout, observed.advertisement.session, head, &keys)
                .await?;
        let mut plans = Vec::with_capacity(cells.len());
        let mut reserve = false;
        let mut opening = false;
        for (authority, control) in cells {
            let (pin, next, new) = self.plan_binding(&catalog, authority, control)?;
            let exists = catalog.bindings.iter().any(|binding| {
                binding.control.bundle_binding == Some(pin) && binding.phase == BindingPhase::Open
            });
            opening |= !exists;
            if new {
                let base = next
                    .ltx_root()
                    .ok_or(Error::Node("bundle binding lacks published base"))?;
                catalog.bindings.push(Binding {
                    application: ApplicationId::from_bytes(*authority.layout().application_id()),
                    first_commit: base.commit_sequence,
                    control: next.clone(),
                    phase: BindingPhase::Provisional,
                    terminal: None,
                    selected_sequence: 0,
                    selected_commit: base.commit_sequence,
                    selected_position: base.position,
                    locators: Vec::new(),
                });
                reserve = true;
            }
            plans.push((pin, next));
        }
        if !opening {
            return Ok((
                observed.clone(),
                cells
                    .iter()
                    .map(|(_, control)| (*control).clone())
                    .collect(),
            ));
        }
        let (reserved, pin_concurrency) = if reserve {
            catalog.bindings.sort_unstable_by_key(|binding| {
                binding
                    .control
                    .bundle_binding
                    .map(|pin| *pin.digest.as_bytes())
            });
            let staged = self.stage_catalog(Some(head), catalog.clone(), &[], Vec::new())?;
            let concurrency = index::enrollment_pin_concurrency(&catalog, Some(&staged.body))?;
            if concurrency < cells.len() {
                return Err(Error::Capacity("bundle enrollment pin metadata"));
            }
            let prepared = self.upload_node_bundle(staged).await?;
            (
                self.select_catalog(observed, &prepared, now_ms).await?,
                concurrency,
            )
        } else {
            let concurrency = index::enrollment_pin_concurrency(&catalog, None)?;
            if concurrency < cells.len() {
                return Err(Error::Capacity("bundle enrollment pin metadata"));
            }
            (observed.clone(), concurrency)
        };
        // Join accepted sibling pins even if one fails. Their provisional
        // catalog entries remain the complete recovery/maintenance inventory.
        let pin_checks = cells
            .iter()
            .zip(&plans)
            .map(|((authority, control), (_, next))| async move {
                if control.value().bundle_binding.is_some() {
                    return Ok((*control).clone());
                }
                match authority
                    .transition(control, next.clone(), Transition::BindBundle)
                    .await
                {
                    Ok(pinned) => Ok(pinned),
                    Err(source) => match authority.load(control.value().cell).await {
                        Ok(Some(current)) if current.value() == next => Ok(current),
                        _ => Err(source),
                    },
                }
            })
            .collect::<Vec<_>>();
        let results = futures_util::stream::iter(pin_checks)
            .buffered(pin_concurrency)
            .collect::<Vec<_>>()
            .await;
        let pinned = results.into_iter().collect::<Result<Vec<_>>>()?;
        for ((pin, _), control) in plans.iter().zip(&pinned) {
            let binding = catalog.binding_mut(pin.digest)?;
            binding.control = control.value().clone();
            binding.phase = BindingPhase::Open;
        }
        let prepared = self
            .upload_catalog(reserved.advertisement.bundle, catalog, &[])
            .await?;
        Ok((
            self.select_catalog(&reserved, &prepared, now_ms).await?,
            pinned,
        ))
    }

    fn plan_binding(
        &self,
        catalog: &Catalog,
        authority: &CellAuthority,
        control: &VersionedControl,
    ) -> Result<(BundleBindingRef, Control, bool)> {
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
                return Ok((
                    binding.control.bundle_binding.ok_or(Error::Fenced)?,
                    value.clone(),
                    false,
                ));
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
        Ok((pin, next, existing.is_none()))
    }
}
