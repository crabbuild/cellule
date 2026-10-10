//! One bounded index maps new frame digests to their unique catalog locators.
use super::*;

pub(super) fn assign(
    catalog: &mut Catalog,
    frames: &[cellule_ltx::VerifiedNodeFrame],
    changed: &BTreeSet<u8>,
    offset: &mut u64,
) -> Result<Vec<Locator>> {
    if frames.len() > MAX_FRAMES {
        return Err(Error::Capacity("bundle frame count"));
    }
    if frames.is_empty() {
        return Ok(Vec::new());
    }
    // Four KiB of fixed stack planning replaces frame-by-frame full-catalog
    // scans. There is no additional retained allocation or admission budget.
    let mut ordered = [([0_u8; 32], 0_usize); MAX_FRAMES];
    for (index, frame) in frames.iter().enumerate() {
        ordered[index] = (frame.digest(), index);
    }
    let ordered = &mut ordered[..frames.len()];
    ordered.sort_unstable_by_key(|(digest, _)| *digest);
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::Node("bundle frame locator is not unique"));
    }
    let mut locations = [None; MAX_FRAMES];
    for (binding_index, binding) in catalog.bindings.iter().enumerate() {
        let mut shard_changed = None;
        for (locator_index, locator) in binding.locators.iter().enumerate() {
            if locator.object.is_some() {
                continue;
            }
            let Ok(index) =
                ordered.binary_search_by_key(locator.frame_digest.as_bytes(), |entry| entry.0)
            else {
                continue;
            };
            if !*shard_changed.get_or_insert_with(|| changed.contains(&binding_shard(binding))) {
                return Err(Error::Node("native frame belongs to an unchanged shard"));
            }
            let frame_index = ordered[index].1;
            if locations[frame_index]
                .replace((binding_index, locator_index))
                .is_some()
            {
                return Err(Error::Node("bundle frame locator is not unique"));
            }
        }
    }
    let mut native = Vec::with_capacity(frames.len());
    // Digest sorting is lookup only: persisted offsets follow the original
    // issued frame order, including multiple frames in one complete capture.
    for (index, frame) in frames.iter().enumerate() {
        let (binding, locator) =
            locations[index].ok_or(Error::Node("bundle frame locator is not unique"))?;
        let locator = catalog
            .bindings
            .get_mut(binding)
            .and_then(|binding| binding.locators.get_mut(locator))
            .ok_or(Error::Node("bundle frame locator is not unique"))?;
        locator.offset = *offset;
        locator.bytes = frame.encoded().len() as u64;
        native.push(locator.clone());
        *offset = offset
            .checked_add(locator.bytes)
            .ok_or(Error::Capacity("bundle bytes"))?;
    }
    Ok(native)
}
