//! Detached authenticated histories: sibling rows retain references, not arrays.
use super::*;
use crate::codec::{BoundedDecoder, BoundedEncoder, read_fixed};

pub(super) const MAX_HISTORY_BYTES: u64 = 32 << 10;
const LEAF_MAGIC: &[u8] = b"CBL3";
const HISTORY_MAGIC: &[u8] = b"CLH3";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct History {
    pub(super) extent: Locator,
    pub(super) count: usize,
    pub(super) native_bytes: u64,
    // None means the binding carries exactly the authenticated extent above.
    // Such a row cannot be verified as native frames or modified as a suffix.
    pub(super) loaded: Option<Vec<Locator>>,
}

pub(super) fn pin(binding: &Binding) -> Result<[u8; 32]> {
    binding
        .control
        .bundle_binding
        .map(|pin| *pin.digest.as_bytes())
        .ok_or(Error::Node("bundle history lacks Cell pin"))
}

pub(super) fn encode(session: SessionId, epoch: u64, binding: &Binding) -> Result<Bytes> {
    if binding.locators.is_empty() || binding.locators.len() > MAX_LOCATORS {
        return Err(Error::Capacity("bundle history count"));
    }
    let mut e = BoundedEncoder::new(MAX_HISTORY_BYTES as u32)?;
    e.write_bytes(HISTORY_MAGIC)?;
    e.write_bytes(session.as_bytes())?;
    e.write_u64(epoch)?;
    e.write_bytes(&pin(binding)?)?;
    e.write_count(binding.locators.len())?;
    for locator in &binding.locators {
        super::codec::write_extent(&mut e, locator)?;
    }
    Ok(Bytes::from(e.finish()))
}

pub(super) fn decode(
    body: &Bytes,
    session: SessionId,
    epoch: u64,
    binding: &Binding,
    history: &History,
) -> Result<Vec<Locator>> {
    if body.len() as u64 != history.extent.bytes
        || *blake3::hash(body).as_bytes() != *history.extent.frame_digest.as_bytes()
    {
        return Err(Error::Node("bundle history digest differs"));
    }
    let mut d = BoundedDecoder::new(body, MAX_HISTORY_BYTES as u32)?;
    if d.read_bytes()? != HISTORY_MAGIC
        || read_fixed::<16>(&mut d, "bundle history session length")? != *session.as_bytes()
        || d.read_u64()? != epoch
        || read_fixed::<32>(&mut d, "bundle history pin length")? != pin(binding)?
    {
        return Err(Error::Fenced);
    }
    let count = d.read_count()?;
    if count == 0 || count > MAX_LOCATORS || count != history.count {
        return Err(Error::Capacity("bundle history count"));
    }
    let mut locators = Vec::with_capacity(count);
    let mut native_bytes = 0_u64;
    for _ in 0..count {
        let locator = super::codec::read_extent(&mut d)?;
        // Native locators from the complete legacy format may precede the
        // fixed index header, so the history preserves that older byte offset.
        if locator.bytes == 0
            || locator
                .offset
                .checked_add(locator.bytes)
                .is_none_or(|end| end > MAX_BUNDLE_BYTES)
            || locator
                .frame_digest
                .as_bytes()
                .iter()
                .all(|byte| *byte == 0)
            || locator
                .object
                .is_some_and(|object| object.as_bytes().iter().all(|byte| *byte == 0))
        {
            return Err(Error::Node("invalid bundle history native extent"));
        }
        native_bytes = native_bytes
            .checked_add(locator.bytes)
            .filter(|bytes| *bytes <= MAX_SUFFIX_BYTES)
            .ok_or(Error::Capacity("bundle history native bytes"))?;
        locators.push(locator);
    }
    d.finish()?;
    if native_bytes != history.native_bytes {
        return Err(Error::Node("bundle history native byte count differs"));
    }
    Ok(locators)
}

pub(super) fn encode_leaf(
    catalog: &Catalog,
    histories: &BTreeMap<[u8; 32], History>,
) -> Result<Bytes> {
    let mut compact = catalog.clone();
    compact.index = None;
    for binding in &mut compact.bindings {
        if let Some(history) = histories.get(&pin(binding)?) {
            binding.locators = vec![history.extent.clone()];
        } else if !binding.locators.is_empty() {
            return Err(Error::Node("bundle leaf lacks detached history"));
        }
    }
    let inline = super::super::codec::encode_leaf(&compact)?;
    let mut e = BoundedEncoder::new(MAX_BUNDLE_BYTES as u32)?;
    e.write_bytes(LEAF_MAGIC)?;
    e.write_bytes(&inline)?;
    e.write_count(
        compact
            .bindings
            .iter()
            .filter(|binding| !binding.locators.is_empty())
            .count(),
    )?;
    for binding in &compact.bindings {
        if let Some(history) = histories.get(&pin(binding)?) {
            e.write_bytes(&pin(binding)?)?;
            e.write_count(history.count)?;
            e.write_u64(history.native_bytes)?;
        }
    }
    Ok(Bytes::from(e.finish()))
}

pub(super) fn decode_leaf(body: &Bytes) -> Result<(Catalog, BTreeMap<[u8; 32], History>)> {
    if body.get(..8) != Some(b"\0\0\0\x04CBL3".as_slice()) {
        return Ok((super::super::codec::decode_leaf(body)?, BTreeMap::new()));
    }
    let mut d = BoundedDecoder::new(body, MAX_BUNDLE_BYTES as u32)?;
    if d.read_bytes()? != LEAF_MAGIC {
        return Err(Error::Node("unsupported detached bundle leaf"));
    }
    let catalog = super::super::codec::decode_leaf(&Bytes::copy_from_slice(d.read_bytes()?))?;
    let count = d.read_count()?;
    if count
        != catalog
            .bindings
            .iter()
            .filter(|binding| !binding.locators.is_empty())
            .count()
    {
        return Err(Error::Node("bundle history descriptor count differs"));
    }
    let mut histories = BTreeMap::new();
    for binding in &catalog.bindings {
        if binding.locators.is_empty() {
            continue;
        }
        if binding.locators.len() != 1
            || read_fixed::<32>(&mut d, "bundle history pin length")? != pin(binding)?
        {
            return Err(Error::Node("bundle history descriptor scope differs"));
        }
        let history = History {
            extent: binding.locators[0].clone(),
            count: d.read_count()?,
            native_bytes: d.read_u64()?,
            loaded: None,
        };
        super::codec::validate_extent(&history.extent)?;
        if history.extent.bytes > MAX_HISTORY_BYTES
            || history.count == 0
            || history.count > MAX_LOCATORS
            || history.native_bytes == 0
            || history.native_bytes > MAX_SUFFIX_BYTES
            || histories.insert(pin(binding)?, history).is_some()
        {
            return Err(Error::Capacity("bundle history descriptor bounds"));
        }
    }
    d.finish()?;
    Ok((catalog, histories))
}

pub(in crate::node::bundle) fn validate_deferred(
    catalog: &Catalog,
    binding: &Binding,
) -> Result<()> {
    let pin = pin(binding)?;
    if let Some(history) = catalog
        .index
        .as_ref()
        .and_then(|index| index.histories.get(&pin))
        && history.loaded.is_none()
        && binding.locators.as_slice() != std::slice::from_ref(&history.extent)
    {
        return Err(Error::Node("bundle modifies an unloaded Cell history"));
    }
    Ok(())
}
