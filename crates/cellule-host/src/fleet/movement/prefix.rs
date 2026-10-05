//! Exact release or original sealed-suffix proof for fresh serving observations.
use super::*;

impl FleetActionExecutor {
    pub(super) async fn verify_serving_prefix(
        &self,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        required: ServingPrefix<'_>,
        root: cellule_runtime::ltx::RootRef,
    ) -> cellule_runtime::Result<()> {
        let spec = attempt.spec();
        match required {
            ServingPrefix::Released(required) => {
                self.runtime
                    .verify_root_prefix(
                        &inputs.catalog,
                        &inputs.authority,
                        inputs.replica.clone(),
                        required.to_ltx(spec.target.cell_id(), spec.incarnation),
                        root,
                        10_000,
                    )
                    .await?;
            }
            ServingPrefix::Recovered(recovery) => {
                // Charge canonical metadata and bounded manifest growth before
                // provider I/O; hold it through the suffix/origin verification.
                let _recovery_memory = self.runtime.try_reserve_node_bytes(8 << 20)?;
                let stores = if recovery.basis().control().recovery.is_some() {
                    Some(self.cells.recovery_inputs(spec).await.map_err(|source| {
                        Error::Facility {
                            name: "fleet-recovery-provider",
                            source,
                        }
                    })?)
                } else {
                    None
                };
                self.verify_materialized_prefix(
                    attempt,
                    inputs,
                    RecoveryPrefixInput {
                        original: recovery.basis().control(),
                        restored: recovery.restored(),
                        stores: stores.as_ref(),
                    },
                    root,
                )
                .await?;
            }
            ServingPrefix::ReceiverRecovered(recovery) => {
                let _recovery_memory = self.runtime.try_reserve_node_bytes(8 << 20)?;
                let basis = recovery.basis();
                let stores = if basis.control().recovery.is_some() {
                    Some(
                        self.cells
                            .receiver_recovery_inputs(basis.accepted(), basis.control())
                            .await
                            .map_err(|source| Error::Facility {
                                name: "fleet-receiver-recovery-provider",
                                source,
                            })?
                            .ok_or(Error::Peer("receiver recovery prerequisites unavailable"))?,
                    )
                } else {
                    None
                };
                self.verify_materialized_prefix(
                    attempt,
                    inputs,
                    RecoveryPrefixInput {
                        original: basis.control(),
                        restored: recovery.restored(),
                        stores: stores.as_ref(),
                    },
                    root,
                )
                .await?;
                let released = attempt.released().ok_or(Error::Fenced)?;
                self.runtime
                    .verify_root_prefix(
                        &inputs.catalog,
                        &inputs.authority,
                        inputs.replica.clone(),
                        released
                            .root
                            .to_ltx(spec.target.cell_id(), spec.incarnation),
                        root,
                        10_000,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn verify_materialized_prefix(
        &self,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        recovery: RecoveryPrefixInput<'_>,
        root: cellule_runtime::ltx::RootRef,
    ) -> cellule_runtime::Result<()> {
        let spec = attempt.spec();
        let RecoveryPrefixInput {
            original,
            restored,
            stores,
        } = recovery;
        let canonical = inputs
            .authority
            .acquisition_record(original.cell, original.incarnation, restored.epoch)
            .await?
            .ok_or(Error::AcquisitionHistoryIncomplete {
                cell: original.cell,
                incarnation: original.incarnation,
                epoch: restored.epoch,
            })?;
        // Journal shape is historical input, not proof of native materialization.
        if canonical.input() != original || canonical.materialized() != restored {
            return Err(Error::Control(
                "journal recovery differs from canonical acquisition",
            ));
        }
        if let Some(overlay) = &original.recovery {
            // An overlay may survive earlier interrupted claims. Its original
            // Cell epoch comes from the sealed manifest, never the latest claim.
            let stores = stores.ok_or(Error::Peer("recovered prefix lacks manifest store"))?;
            let inventory = stores
                .manifests
                .load_manifest(
                    overlay.leader_session,
                    overlay.log_epoch,
                    overlay.manifest_digest,
                )
                .await?;
            let mut rows = inventory.cells().iter().filter(|row| {
                row.application == spec.target.application()
                    && row.cell == original.cell
                    && row.incarnation == original.incarnation
                    && &row.recovery == overlay
            });
            let suffix = rows.next().ok_or(Error::Control(
                "recovered suffix is absent from its manifest",
            ))?;
            if rows.next().is_some() {
                return Err(Error::Control("recovered suffix manifest is ambiguous"));
            }
            let observed = inputs
                .authority
                .load(original.cell)
                .await?
                .ok_or(Error::Fenced)?;
            if observed.value().ltx_root() != Some(root) {
                return Err(Error::Fenced);
            }
            if observed.value().state == ControlState::Idle {
                self.runtime
                    .verify_recovered_idle_prefix(
                        &inputs.catalog,
                        &inputs.authority,
                        inputs.replica.clone(),
                        suffix,
                        &observed,
                        10_000,
                    )
                    .await?;
            } else {
                self.runtime
                    .verify_recovered_prefix(
                        &inputs.catalog,
                        &inputs.authority,
                        inputs.replica.clone(),
                        suffix,
                        root,
                        10_000,
                    )
                    .await?;
            }
        } else {
            self.runtime
                .verify_root_prefix(
                    &inputs.catalog,
                    &inputs.authority,
                    inputs.replica.clone(),
                    restored.ltx_root().ok_or(Error::Fenced)?,
                    root,
                    10_000,
                )
                .await?;
        }
        Ok(())
    }
}

struct RecoveryPrefixInput<'a> {
    original: &'a cellule_runtime::control::Control,
    restored: &'a cellule_runtime::control::Control,
    stores: Option<&'a crate::fleet::FleetRecoveryInputs>,
}
