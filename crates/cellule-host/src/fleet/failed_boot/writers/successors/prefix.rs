use super::*;
use cellule_runtime::recovery::manifest::PinnedRecoveryCell;

pub(super) async fn verify(
    original: &OriginalWriterObservation,
    inputs: &FleetOriginalWriterSuccessorInputs,
    inventory: &FleetOriginalBootSuffixInventory,
    serving: CellServingObservation,
) -> Result<FleetOriginalWriterSuccessorProof> {
    let control = &original.control;
    let root = serving
        .position()
        .root
        .to_ltx(control.cell, control.incarnation);
    let runtime = inputs.host.runtime();
    let origin = runtime
        .verify_root_prefix(
            &inputs.catalog,
            &inputs.authority,
            inputs.replica.clone(),
            control.ltx_root().unwrap_or(root),
            root,
            10_000,
        )
        .await?;
    let mut required = Vec::<PinnedRecoveryCell>::new();
    if let Some(manifest) = inventory.manifest() {
        for row in manifest.cells() {
            if row.application == original.target.application()
                && row.cell == control.cell
                && row.incarnation == control.incarnation
                && row.cell_epoch == control.epoch
            {
                required.push(copy_suffix(row));
            }
        }
    }
    if let Some(overlay) = &control.recovery
        && !required.iter().any(|row| &row.recovery == overlay)
    {
        // Inherited overlays name an earlier boot/Cell epoch. Preserve that
        // exact digest-verified row; this original writer's epoch cannot replace it.
        let _memory = runtime.try_reserve_node_bytes(8 << 20)?;
        let manifest = inputs
            .manifests
            .load_manifest(
                overlay.leader_session,
                overlay.log_epoch,
                overlay.manifest_digest,
            )
            .await?;
        let mut rows = manifest.cells().iter().filter(|row| {
            row.application == original.target.application()
                && row.cell == control.cell
                && row.incarnation == control.incarnation
                && &row.recovery == overlay
        });
        let row = rows.next().ok_or(Error::Control(
            "original inherited suffix is absent from manifest",
        ))?;
        if rows.next().is_some() {
            return Err(Error::Control(
                "original inherited suffix manifest is ambiguous",
            ));
        }
        required.push(copy_suffix(row));
    }
    let mut suffixes = Vec::with_capacity(required.len());
    for row in &required {
        suffixes.push(
            runtime
                .verify_recovered_prefix(
                    &inputs.catalog,
                    &inputs.authority,
                    inputs.replica.clone(),
                    row,
                    root,
                    10_000,
                )
                .await?,
        );
    }
    Ok(FleetOriginalWriterSuccessorProof {
        original: original.clone(),
        node: inputs.node,
        serving,
        origin,
        suffixes,
    })
}

fn copy_suffix(row: &PinnedRecoveryCell) -> PinnedRecoveryCell {
    PinnedRecoveryCell {
        application: row.application,
        cell: row.cell,
        incarnation: row.incarnation,
        cell_epoch: row.cell_epoch,
        recovery: row.recovery.clone(),
    }
}
