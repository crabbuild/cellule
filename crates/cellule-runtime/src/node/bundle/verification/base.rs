//! Continuous admitted small-base verification; larger graphs stay serial.
use super::*;

pub(super) async fn verify(
    layout: &cellule_ltx::CellStorageLayout,
    bindings: &[Binding],
    limits: cellule_ltx::Limits,
) -> Result<()> {
    let mut serial = Vec::with_capacity(bindings.len());
    // Each slot owns the root read through complete dependency verification.
    // Refilling a completed slot avoids waiting for an unrelated slow root;
    // eight original 512-KiB charges still fit the released 4-MiB allowance.
    let mut reads = stream::iter(0..bindings.len())
        .map(|index| async move {
            let Some(plan) = proof::prepare_base_origin(layout, &bindings[index], limits).await?
            else {
                return Ok::<_, Error>(Some(index));
            };
            if plan.working_bytes() > MAX_BUNDLE_BYTES as usize / READ_CONCURRENCY {
                return Err(Error::Capacity("bundle base verification bytes"));
            }
            plan.verify().await?;
            Ok(None)
        })
        .buffer_unordered(READ_CONCURRENCY);
    while let Some(result) = reads.next().await {
        if let Some(index) = result? {
            serial.push(index);
        }
    }
    drop(reads);
    // All bounded operations have joined/dropped before a larger graph
    // takes the original serial working set. No partial plan grants CAS.
    for index in serial {
        proof::verify_base(layout, &bindings[index], limits).await?;
    }
    Ok(())
}
