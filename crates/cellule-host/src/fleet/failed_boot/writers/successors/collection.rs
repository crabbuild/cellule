use super::*;

impl FleetOriginalWriterSuccessorInventory {
    /// Collects every current native successor without starting relocation or
    /// recovery. One absolute deadline spans all original inputs, prefixes and
    /// global rechecks; missing/changed evidence returns no partial inventory.
    #[allow(clippy::too_many_arguments)]
    pub async fn collect(
        journal: &dyn FleetOriginalWriterJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        manifests: &RecoveryManifestStore,
        successors: &dyn FleetOriginalWriterSuccessors,
        request: &FleetFailedBootProcessRequest,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started = clock()?;
        let mut last = started;
        let mut now = || {
            let next = clock()?;
            interval(started, next)?;
            if next < last {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let original = FleetOriginalBootSuffixInventory::collect(
                journal, directory, processes, manifests, request, claimant, deadline, &mut now,
            )
            .await?;
            let roster = FleetRoster::collect(journal, original.snapshot(), deadline).await?;
            let mut proofs = Vec::new();
            let mut retained = Vec::new();
            for owner in original.writers().writers() {
                let inputs = successors
                    .successor(owner, original.snapshot())
                    .await
                    .map_err(adapter_error)?;
                validate_inputs(owner, &inputs)?;
                let serving = observe(owner, &inputs, &roster, directory, now()?).await?;
                let proof = prefix::verify(owner, &inputs, &original, serving).await?;
                // Keep this exact native host/backend mapping through the global
                // recheck. A provider refresh cannot silently substitute a node.
                retained.push(inputs);
                proofs.push(proof);
            }
            for (proof, inputs) in proofs.iter().zip(&retained) {
                let confirmed =
                    observe(proof.original(), inputs, &roster, directory, now()?).await?;
                if !confirmed.same_writer(proof.serving()) {
                    return Err(Error::Fenced);
                }
            }
            let confirmed = FleetOriginalBootSuffixInventory::collect(
                journal, directory, processes, manifests, request, claimant, deadline, &mut now,
            )
            .await?;
            if confirmed.snapshot() != original.snapshot()
                || confirmed.writers().record() != original.writers().record()
                || confirmed.recovered() != original.recovered()
                || confirmed.process() != original.process()
            {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            let finished = now()?;
            Ok(Self {
                original,
                proofs,
                started_at_ms: started,
                finished_at_ms: finished,
            })
        })
        .await
    }
}

fn validate_inputs(
    original: &OriginalWriterObservation,
    inputs: &FleetOriginalWriterSuccessorInputs,
) -> Result<()> {
    let entry = inputs.catalog.entry();
    if entry.cell() != original.target.cell_id()
        || entry.namespace() != original.target.namespace()
        || entry.partition() != original.target.partition()
        || inputs.replica.scope()
            != (
                *original.control.cell.as_bytes(),
                *original.control.incarnation.as_bytes(),
            )
        || original.control.cell != original.target.cell_id()
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

async fn observe(
    original: &OriginalWriterObservation,
    inputs: &FleetOriginalWriterSuccessorInputs,
    roster: &FleetRoster,
    directory: &NodeDirectory,
    now_ms: i64,
) -> Result<CellServingObservation> {
    crate::fleet::native_writer::observe(
        &original.target,
        original.control.incarnation,
        original.control.epoch,
        crate::fleet::native_writer::NativeWriter {
            node: inputs.node,
            host: &inputs.host,
            catalog: &inputs.catalog,
            authority: &inputs.authority,
        },
        roster,
        directory,
        now_ms,
    )
    .await
}
