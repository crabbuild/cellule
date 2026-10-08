//! One immutable upload holds changed shards, native frames and small histories.
use super::*;

pub(super) fn encode(
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
        let bytes = history::encode_leaf(&leaf(catalog, rows), &histories)?.len() as u64;
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
    let mut native = Vec::with_capacity(frames.len());
    for frame in frames {
        let digest = Digest::from_bytes(frame.digest());
        let mut matches = 0;
        for binding in &mut catalog.bindings {
            let id = binding_shard(binding);
            for locator in &mut binding.locators {
                if locator.object.is_none() && locator.frame_digest == digest {
                    if !changed.contains(&id) {
                        return Err(Error::Node("native frame belongs to an unchanged shard"));
                    }
                    locator.offset = offset;
                    locator.bytes = frame.encoded().len() as u64;
                    matches += 1;
                }
            }
        }
        if matches != 1 {
            return Err(Error::Node("bundle frame locator is not unique"));
        }
        native.push(Locator {
            object: None,
            offset,
            bytes: frame.encoded().len() as u64,
            frame_digest: digest,
        });
        offset = offset
            .checked_add(frame.encoded().len() as u64)
            .ok_or(Error::Capacity("bundle bytes"))?;
    }
    let mut history_bodies = Vec::new();
    for pin in new {
        let binding = catalog
            .bindings
            .iter()
            .find(|binding| history::pin(binding).ok() == Some(pin))
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
    let groups = grouped(catalog);
    let mut bodies = Vec::new();
    for id in &changed {
        if let Some(reference) = &mut shards[usize::from(*id)] {
            let body = history::encode_leaf(&leaf(catalog, groups[id].clone()), &histories)?;
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

fn verify_local(body: &Bytes, expected: &Root) -> Result<()> {
    let root = codec::decode(&body[..HEADER_BYTES])?;
    if &root != expected || body.len() as u64 != root.object_bytes {
        return Err(Error::Node("bundle index self-verification differs"));
    }
    let mut offset = HEADER_BYTES as u64;
    let mut local_histories = BTreeMap::new();
    for (id, shard) in root.shards.iter().enumerate() {
        let Some(shard) = shard else {
            continue;
        };
        if shard.extent.object.is_some() {
            continue;
        }
        if shard.extent.offset != offset {
            return Err(Error::Node("bundle local shard extents are not canonical"));
        }
        let (leaf, histories) =
            decode_leaf_with_histories(extent_bytes(body, &shard.extent)?, shard, id as u8, &root)?;
        for binding in leaf.bindings {
            let pin = history::pin(&binding)?;
            if let Some(history) = histories
                .get(&pin)
                .filter(|history| history.extent.object.is_none())
                && local_histories
                    .insert(pin, (binding, history.clone()))
                    .is_some()
            {
                return Err(Error::Node("bundle local history is not unique"));
            }
        }
        offset += shard.extent.bytes;
    }
    for frame in &root.frames {
        if frame.offset != offset {
            return Err(Error::Node("bundle local native extents differ"));
        }
        extent_bytes(body, frame)?;
        offset += frame.bytes;
    }
    let mut native_locators = Vec::new();
    for (binding, history) in local_histories.values() {
        if history.extent.offset != offset {
            return Err(Error::Node("bundle local histories are not canonical"));
        }
        let locators = history::decode(
            &extent_bytes(body, &history.extent)?,
            root.session,
            root.epoch,
            binding,
            history,
        )?;
        native_locators.extend(
            locators
                .into_iter()
                .filter(|locator| locator.object.is_none()),
        );
        offset += history.extent.bytes;
    }
    if offset != root.object_bytes
        || native_locators.len() != root.frames.len()
        || root.frames.iter().any(|frame| {
            native_locators
                .iter()
                .filter(|locator| *locator == frame)
                .count()
                != 1
        })
    {
        return Err(Error::Node(
            "bundle local history/native extent coverage differs",
        ));
    }
    Ok(())
}
