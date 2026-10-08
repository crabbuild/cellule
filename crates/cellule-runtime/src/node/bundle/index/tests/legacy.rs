use super::*;

pub(in crate::node::bundle) fn encode_inline(
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
            // A partial catalog may modify only a shard it authenticated first.
            if groups.keys().any(|id| !index.loaded.contains_key(id)) {
                return Err(Error::Node("bundle modifies an unloaded catalog shard"));
            }
            index.root.shards.clone()
        }
        None => vec![None; SHARDS],
    };
    let changed: BTreeSet<u8> = match &catalog.index {
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
    // Native extents follow the changed shards; changing numeric offsets never
    // changes the leaf encoding length. Compute sizes before filling offsets.
    let mut offset = HEADER_BYTES as u64;
    for id in &changed {
        let rows = groups.get(id).cloned().unwrap_or_default();
        if rows.is_empty() {
            shards[usize::from(*id)] = None;
        } else {
            let bytes = crate::node::bundle::codec::encode_leaf(&leaf(catalog, rows))?.len() as u64;
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
    if offset > MAX_BUNDLE_BYTES {
        return Err(Error::Capacity("bundle bytes"));
    }
    let groups = grouped(catalog);
    let mut bodies = Vec::new();
    for id in &changed {
        if let Some(reference) = &mut shards[usize::from(*id)] {
            let body = crate::node::bundle::codec::encode_leaf(&leaf(catalog, groups[id].clone()))?;
            if body.len() as u64 != reference.extent.bytes {
                return Err(Error::Node("bundle shard size changed during encoding"));
            }
            reference.extent.frame_digest = Digest::from_bytes(*blake3::hash(&body).as_bytes());
            bodies.push(body);
        }
    }
    let root = Root {
        detached: false,
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
    for leaf in bodies {
        body.extend_from_slice(&leaf);
    }
    for frame in frames {
        body.extend_from_slice(frame.encoded());
    }
    let body = Bytes::from(body);
    verify_local(&body, &root)?;
    Ok((body, digest))
}

/// Self-verification covers all bytes of a newly uploaded object. References
/// to old immutable shards remain authenticated by the predecessor selection.
fn verify_local(body: &Bytes, expected: &Root) -> Result<()> {
    let root = codec::decode(&body[..HEADER_BYTES])?;
    if &root != expected || body.len() as u64 != root.object_bytes {
        return Err(Error::Node("bundle index self-verification differs"));
    }
    let mut offset = HEADER_BYTES as u64;
    let mut locators = Vec::new();
    for (id, shard) in root.shards.iter().enumerate() {
        let Some(shard) = shard else { continue };
        if shard.extent.object.is_some() {
            continue;
        }
        if shard.extent.offset != offset {
            return Err(Error::Node("bundle local shard extents are not canonical"));
        }
        let bytes = extent_bytes(body, &shard.extent)?;
        let leaf = decode_leaf(bytes, shard, id as u8, &root)?;
        locators.extend(
            leaf.bindings
                .into_iter()
                .flat_map(|binding| binding.locators)
                .filter(|locator| locator.object.is_none()),
        );
        offset += shard.extent.bytes;
    }
    if locators.len() != root.frames.len() {
        return Err(Error::Node("bundle local native extent count differs"));
    }
    for frame in &root.frames {
        if frame.offset != offset
            || locators.iter().filter(|locator| *locator == frame).count() != 1
        {
            return Err(Error::Node("bundle local native extents differ"));
        }
        extent_bytes(body, frame)?;
        offset += frame.bytes;
    }
    if offset != root.object_bytes {
        return Err(Error::Node("bundle has unauthenticated trailing bytes"));
    }
    Ok(())
}
