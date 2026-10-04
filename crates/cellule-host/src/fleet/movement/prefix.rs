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
                // Charge canonical acquisition metadata, the 2-MiB manifest
                // envelope and decoded row/vector growth before provider I/O.
                // Hold the token through the complete suffix/origin proof.
                let _recovery_memory = self.runtime.try_reserve_node_bytes(8 << 20)?;
                let original = recovery.basis().control();
                let restored = recovery.restored();
                let canonical = inputs
                    .authority
                    .acquisition_record(original.cell, original.incarnation, restored.epoch)
                    .await?
                    .ok_or(Error::AcquisitionHistoryIncomplete {
                        cell: original.cell,
                        incarnation: original.incarnation,
                        epoch: restored.epoch,
                    })?;
                // Journal shape alone cannot prove native materialization. Bind
                // its entire original input/result to canonical acquisition.
                if canonical.input() != original || canonical.materialized() != restored {
                    return Err(Error::Control(
                        "journal recovery differs from canonical acquisition",
                    ));
                }
                if let Some(overlay) = &original.recovery {
                    // The overlay can survive interrupted earlier claims. Its
                    // original Cell epoch comes from the digest-verified sealed
                    // manifest, never from this later acquisition's epoch.
                    let stores = self.cells.recovery_inputs(spec).await.map_err(|source| {
                        Error::Facility {
                            name: "fleet-recovery-provider",
                            source,
                        }
                    })?;
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
            }
        }
        Ok(())
    }
}
