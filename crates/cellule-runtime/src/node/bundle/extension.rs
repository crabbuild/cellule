//! Live induction over an exact immutable prefix; cold reads remain complete.
use super::*;

pub(super) fn verify(
    layout: &cellule_ltx::CellStorageLayout,
    prepared: &PreparedNodeBundle,
    lease: &crate::node::lease::NodeLeaseGuard,
    binding: &Binding,
    prefixes: &[&BundleCoverageProof],
    limits: cellule_ltx::Limits,
    origin: &origin::OriginBundle,
) -> Result<bool> {
    let Some(previous) = prefixes.iter().copied().find(|previous| {
        previous.session == prepared.catalog.session
            && previous.head.epoch == prepared.head.epoch
            && previous.binding.control == binding.control
            && previous.binding.application == binding.application
            && previous.binding.phase == binding.phase
            && previous.binding.terminal == binding.terminal
            && previous.binding.locators.len() < binding.locators.len()
            && binding.locators.starts_with(&previous.binding.locators)
            && prepared
                .original
                .is_some_and(|head| previous.head.selected_through <= head.selected_through)
            && previous
                .live
                .as_ref()
                .is_some_and(|live| live.matches_origin(layout, previous.session, lease, limits))
    }) else {
        return Ok(false);
    };
    let suffix = &binding.locators[previous.binding.locators.len()..];
    // A prefix capability authenticates only its original history. Missing
    // intermediate publications require the complete verifier, never inference
    // from a higher endpoint or an unrelated selected head.
    if suffix
        .iter()
        .any(|locator| locator.object != Some(prepared.head.digest))
    {
        return Ok(false);
    }
    let mut chain = proof::BindingChain::selected(&previous.binding);
    for locator in suffix {
        let end = locator
            .offset
            .checked_add(locator.bytes)
            .ok_or(Error::Node("bundle extent overflow"))?;
        let bytes = origin
            .range(
                prepared.catalog.session,
                prepared.head.epoch,
                prepared.head.digest,
                &(locator.offset..end),
            )?
            .ok_or(Error::Node("bundle extension lacks fresh origin"))?;
        let frame = proof::checked_frame(
            prepared.catalog.session,
            prepared.head.epoch,
            binding,
            locator,
            bytes,
            limits,
        )?;
        chain.accept(proof::FrameStep::from_frame(&frame))?;
    }
    chain.finish(binding)?;
    Ok(true)
}
