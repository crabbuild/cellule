//! Admitted fresh small-base verification; larger graphs stay serial.
use super::*;

pub(super) async fn verify(
    layout: &cellule_ltx::CellStorageLayout,
    bindings: &[Binding],
    limits: cellule_ltx::Limits,
) -> Result<()> {
    for cohort in bindings.chunks(READ_CONCURRENCY) {
        let mut small = Vec::with_capacity(cohort.len());
        let mut serial = Vec::with_capacity(cohort.len());
        let mut plans = stream::iter(0..cohort.len())
            .map(|index| async move {
                Ok::<_, Error>((
                    index,
                    proof::prepare_base_origin(layout, &cohort[index], limits).await?,
                ))
            })
            .buffer_unordered(READ_CONCURRENCY);
        while let Some(plan) = plans.next().await {
            let (index, plan): (_, Option<cellule_ltx::RootOriginVerification>) = plan?;
            match plan {
                Some(plan) => small.push(plan),
                None => serial.push(index),
            }
        }
        drop(plans);
        let working = small.iter().try_fold(0_usize, |bytes, plan| {
            bytes
                .checked_add(plan.working_bytes())
                .ok_or(Error::Capacity("bundle base verification bytes"))
        })?;
        if working > MAX_BUNDLE_BYTES as usize {
            return Err(Error::Capacity("bundle base verification bytes"));
        }
        let mut reads = stream::iter(small)
            .map(|plan| async move { plan.verify().await.map(|_| ()) })
            .buffer_unordered(READ_CONCURRENCY);
        while let Some(result) = reads.next().await {
            result?;
        }
        drop(reads);
        // All bounded operations have joined/dropped before a larger graph
        // takes the original serial working set. No partial plan grants CAS.
        for index in serial {
            proof::verify_base(layout, &cohort[index], limits).await?;
        }
    }
    Ok(())
}
