//! Canonical fixed-size authenticated root header; leaf codecs remain CNB1.
use super::*;
use crate::codec::{BoundedDecoder, BoundedEncoder, read_fixed};

fn write_extent(e: &mut BoundedEncoder, extent: &Locator) -> Result<()> {
    e.write_bool(extent.object.is_some())?;
    if let Some(object) = extent.object {
        e.write_bytes(object.as_bytes())?;
    }
    e.write_u64(extent.offset)?;
    e.write_u64(extent.bytes)?;
    e.write_bytes(extent.frame_digest.as_bytes())?;
    Ok(())
}
fn read_extent(d: &mut BoundedDecoder<'_>) -> Result<Locator> {
    let object = if d.read_bool()? {
        Some(Digest::from_bytes(read_fixed(
            d,
            "bundle index object length",
        )?))
    } else {
        None
    };
    Ok(Locator {
        object,
        offset: d.read_u64()?,
        bytes: d.read_u64()?,
        frame_digest: Digest::from_bytes(read_fixed(d, "bundle index digest length")?),
    })
}

pub(super) fn encode(root: &Root) -> Result<Bytes> {
    validate(root)?;
    let mut e = BoundedEncoder::new((HEADER_BYTES - 12) as u32)?;
    e.write_bytes(root.session.as_bytes())?;
    e.write_u64(root.epoch)?;
    e.write_bool(root.predecessor.is_some())?;
    if let Some(previous) = root.predecessor {
        e.write_bytes(previous.as_bytes())?;
    }
    e.write_u64(root.selected_through)?;
    e.write_u64(root.object_bytes)?;
    for shard in &root.shards {
        e.write_bool(shard.is_some())?;
        if let Some(shard) = shard {
            write_extent(&mut e, &shard.extent)?;
            e.write_count(shard.bindings)?;
        }
    }
    e.write_count(root.frames.len())?;
    for frame in &root.frames {
        write_extent(&mut e, frame)?;
    }
    let payload = e.finish();
    let mut header = Vec::with_capacity(HEADER_BYTES);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    header.extend_from_slice(&payload);
    header.resize(HEADER_BYTES, 0);
    Ok(Bytes::from(header))
}

pub(super) fn decode(header: &[u8]) -> Result<Root> {
    if header.len() != HEADER_BYTES || header.get(..8) != Some(MAGIC.as_slice()) {
        return Err(Error::Node("unsupported bundle index header"));
    }
    let length = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| Error::Node("truncated bundle index header"))?,
    ) as usize;
    let end = 12_usize
        .checked_add(length)
        .filter(|end| *end <= HEADER_BYTES)
        .ok_or(Error::Node("bundle index header length"))?;
    if header[end..].iter().any(|byte| *byte != 0) {
        return Err(Error::Node("noncanonical bundle index padding"));
    }
    let mut d = BoundedDecoder::new(&header[12..end], (HEADER_BYTES - 12) as u32)?;
    let session = SessionId::from_bytes(read_fixed(&mut d, "bundle index session length")?);
    let epoch = d.read_u64()?;
    let predecessor = if d.read_bool()? {
        Some(Digest::from_bytes(read_fixed(
            &mut d,
            "bundle index predecessor length",
        )?))
    } else {
        None
    };
    let selected_through = d.read_u64()?;
    let object_bytes = d.read_u64()?;
    let mut shards = Vec::with_capacity(SHARDS);
    for _ in 0..SHARDS {
        shards.push(if d.read_bool()? {
            Some(Shard {
                extent: read_extent(&mut d)?,
                bindings: d.read_count()?,
            })
        } else {
            None
        });
    }
    let count = d.read_count()?;
    if count > MAX_FRAMES {
        return Err(Error::Capacity("bundle index native frame count"));
    }
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        frames.push(read_extent(&mut d)?);
    }
    d.finish()?;
    let root = Root {
        session,
        epoch,
        predecessor,
        selected_through,
        object_bytes,
        shards,
        frames,
    };
    validate(&root)?;
    Ok(root)
}

fn validate(root: &Root) -> Result<()> {
    if root.epoch == 0
        || root.session.as_bytes().iter().all(|byte| *byte == 0)
        || root.shards.len() != SHARDS
        || root.frames.len() > MAX_FRAMES
        || root.object_bytes < HEADER_BYTES as u64
        || root.object_bytes > MAX_BUNDLE_BYTES
    {
        return Err(Error::Node("invalid bundle index bounds"));
    }
    let mut bindings = 0_usize;
    let mut local_end = HEADER_BYTES as u64;
    for shard in root.shards.iter().flatten() {
        bindings = bindings
            .checked_add(shard.bindings)
            .ok_or(Error::Capacity("bundle binding count"))?;
        if shard.bindings == 0 || bindings > MAX_BINDINGS {
            return Err(Error::Capacity("bundle binding count"));
        }
        validate_extent(&shard.extent)?;
        if shard.extent.object.is_none() {
            if shard.extent.offset != local_end {
                return Err(Error::Node("noncanonical bundle index local shards"));
            }
            local_end += shard.extent.bytes;
        }
    }
    for frame in &root.frames {
        validate_extent(frame)?;
        if frame.object.is_some() || frame.offset != local_end {
            return Err(Error::Node("noncanonical bundle index native extents"));
        }
        local_end += frame.bytes;
    }
    if local_end != root.object_bytes {
        return Err(Error::Node("bundle index object length differs"));
    }
    Ok(())
}
fn validate_extent(extent: &Locator) -> Result<()> {
    if extent.offset < HEADER_BYTES as u64
        || extent.bytes == 0
        || extent
            .offset
            .checked_add(extent.bytes)
            .is_none_or(|end| end > MAX_BUNDLE_BYTES)
        || extent.frame_digest.as_bytes().iter().all(|byte| *byte == 0)
        || extent
            .object
            .is_some_and(|digest| digest.as_bytes().iter().all(|byte| *byte == 0))
    {
        return Err(Error::Node("invalid bundle index extent"));
    }
    Ok(())
}
