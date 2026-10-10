// Reference CNB3 encoder from aaed329, retained only for byte compatibility.
// Keep its two-pass shard construction independent of the production plan.
use super::*;

pub(in crate::node::bundle) fn encode_original(
    catalog: &mut Catalog,
    frames: &[cellule_ltx::VerifiedNodeFrame],
) -> Result<(Bytes, Digest)> {
    catalog.validate()?;
    if frames.len() > MAX_FRAMES {
        return Err(Error::Capacity("bundle frame count"));
    }
    let groups = grouped(catalog);
    let mut shards = match &catalog.index {
        Some(index) => {
            if index.root.session != catalog.session || index.root.epoch != catalog.epoch {
                return Err(Error::Fenced);
            }
            if groups.keys().any(|id| !index.loaded.contains_key(id)) {
                return Err(Error::Node("bundle modifies an unloaded catalog shard"));
            }
            index.root.shards.clone()
        }
        None => vec![None; SHARDS],
    };
    let changed: BTreeSet<_> = match &catalog.index {
        Some(index) => index
            .loaded
            .iter()
            .filter_map(|(id, original)| {
                (groups.get(id).map(Vec::as_slice).unwrap_or(&[]) != original.as_slice())
                    .then_some(*id)
            })
            .collect(),
        None => groups.keys().copied().collect(),
    };
    let mut histories = BTreeMap::new();
    let mut new = BTreeSet::new();
    for binding in &catalog.bindings {
        if !changed.contains(&binding_shard(binding)) {
            continue;
        }
        let pin = history::pin(binding)?;
        if let Some(original) = catalog
            .index
            .as_ref()
            .and_then(|index| index.histories.get(&pin))
        {
            let same = match &original.loaded {
                Some(loaded) => loaded == &binding.locators,
                None => binding.locators.as_slice() == std::slice::from_ref(&original.extent),
            };
            if same {
                histories.insert(pin, original.clone());
                continue;
            }
            if original.loaded.is_none() {
                return Err(Error::Node("bundle modifies an unloaded Cell history"));
            }
        }
        if binding.locators.is_empty() {
            continue;
        }
        let bytes = history::encode(catalog.session, catalog.epoch, binding)?;
        let native_bytes = binding
            .locators
            .iter()
            .try_fold(0_u64, |total, locator| total.checked_add(locator.bytes))
            .filter(|bytes| *bytes <= MAX_SUFFIX_BYTES)
            .ok_or(Error::Capacity("bundle history native bytes"))?;
        histories.insert(
            pin,
            history::History {
                extent: Locator {
                    object: None,
                    offset: 0,
                    bytes: bytes.len() as u64,
                    frame_digest: Digest::from_bytes([0; 32]),
                },
                count: binding.locators.len(),
                native_bytes,
                loaded: Some(binding.locators.clone()),
            },
        );
        new.insert(pin);
    }
    let mut offset = HEADER_BYTES as u64;
    for id in &changed {
        let rows = groups.get(id).cloned().unwrap_or_default();
        if rows.is_empty() {
            shards[usize::from(*id)] = None;
            continue;
        }
        let bytes = encode_leaf_original(&leaf(catalog, rows), &histories)?.len() as u64;
        shards[usize::from(*id)] = Some(Shard {
            extent: Locator {
                object: None,
                offset,
                bytes,
                frame_digest: Digest::from_bytes([0; 32]),
            },
            bindings: groups[id].len(),
        });
        offset = offset
            .checked_add(bytes)
            .ok_or(Error::Capacity("bundle shard bytes"))?;
    }
    let native = native::assign(catalog, frames, &changed, &mut offset)?;
    let mut history_bodies = Vec::new();
    for pin in new {
        // Catalog validation established strict pin order before mutation;
        // assigning native extents changes no pin or control field.
        let binding = catalog
            .bindings
            .binary_search_by_key(&Some(pin), |binding| {
                binding
                    .control
                    .bundle_binding
                    .map(|pin| *pin.digest.as_bytes())
            })
            .ok()
            .and_then(|index| catalog.bindings.get(index))
            .ok_or(Error::Node("bundle history binding is absent"))?;
        let bytes = history::encode(catalog.session, catalog.epoch, binding)?;
        let reference = histories
            .get_mut(&pin)
            .ok_or(Error::Node("bundle history plan is absent"))?;
        if bytes.len() as u64 != reference.extent.bytes {
            return Err(Error::Node("bundle history size changed during encoding"));
        }
        reference.extent.offset = offset;
        reference.extent.frame_digest = Digest::from_bytes(*blake3::hash(&bytes).as_bytes());
        reference.loaded = Some(binding.locators.clone());
        offset = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::Capacity("bundle bytes"))?;
        history_bodies.push(bytes);
    }
    if offset > MAX_BUNDLE_BYTES {
        return Err(Error::Capacity("bundle bytes"));
    }
    // Detached leaf encoding replaces every nonempty locator array with its
    // authenticated history extent. Native offset updates cannot change those
    // compact rows, so retain the original groups instead of cloning them again.
    let mut bodies = Vec::new();
    for id in &changed {
        if let Some(reference) = &mut shards[usize::from(*id)] {
            let body = encode_leaf_original(&leaf(catalog, groups[id].clone()), &histories)?;
            if body.len() as u64 != reference.extent.bytes {
                return Err(Error::Node("bundle shard size changed during encoding"));
            }
            reference.extent.frame_digest = Digest::from_bytes(*blake3::hash(&body).as_bytes());
            bodies.push(body);
        }
    }
    let root = Root {
        detached: true,
        session: catalog.session,
        epoch: catalog.epoch,
        predecessor: catalog.predecessor,
        selected_through: catalog.selected_through,
        object_bytes: offset,
        shards,
        frames: native,
    };
    let header = codec::encode(&root)?;
    let digest = Digest::from_bytes(*blake3::hash(&header).as_bytes());
    let mut body = Vec::with_capacity(offset as usize);
    body.extend_from_slice(&header);
    for shard in bodies {
        body.extend_from_slice(&shard);
    }
    for frame in frames {
        body.extend_from_slice(frame.encoded());
    }
    for history in history_bodies {
        body.extend_from_slice(&history);
    }
    let body = Bytes::from(body);
    verify_local(&body, &root)?;
    Ok((body, digest))
}
use crate::codec::BoundedEncoder;

fn encode_leaf_original(
    catalog: &Catalog,
    histories: &BTreeMap<[u8; 32], history::History>,
) -> Result<Bytes> {
    let mut compact = catalog.clone();
    compact.index = None;
    for binding in &mut compact.bindings {
        if let Some(history) = histories.get(&history::pin(binding)?) {
            binding.locators = vec![history.extent.clone()];
        } else if !binding.locators.is_empty() {
            return Err(Error::Node("bundle leaf lacks detached history"));
        }
    }
    let inline = crate::node::bundle::codec::encode_leaf(&compact)?;
    let mut e = BoundedEncoder::new(crate::node::bundle::MAX_BUNDLE_BYTES as u32)?;
    e.write_bytes(b"CBL3")?;
    e.write_bytes(&inline)?;
    e.write_count(
        compact
            .bindings
            .iter()
            .filter(|binding| !binding.locators.is_empty())
            .count(),
    )?;
    for binding in &compact.bindings {
        if let Some(history) = histories.get(&history::pin(binding)?) {
            e.write_bytes(&history::pin(binding)?)?;
            e.write_count(history.count)?;
            e.write_u64(history.native_bytes)?;
        }
    }
    Ok(Bytes::from(e.finish()))
}
