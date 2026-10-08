//! Bounded binary catalog and verbatim native frame extents.
use super::*;
use crate::codec::{BoundedDecoder, BoundedEncoder, read_fixed};

const MAGIC: &[u8] = b"CNB1";

fn position_write(e: &mut BoundedEncoder, p: cellule_ltx::Position) -> Result<()> {
    e.write_u64(p.txid)?;
    e.write_u64(p.checksum)?;
    Ok(())
}
fn position_read(d: &mut BoundedDecoder<'_>) -> Result<cellule_ltx::Position> {
    Ok(cellule_ltx::Position {
        txid: d.read_u64()?,
        checksum: d.read_u64()?,
    })
}

fn metadata(catalog: &Catalog, frames: usize) -> Result<BoundedEncoder> {
    catalog.validate()?;
    let mut e = BoundedEncoder::new(MAX_BUNDLE_BYTES as u32)?;
    e.write_bytes(MAGIC)?;
    e.write_bytes(catalog.session.as_bytes())?;
    e.write_u64(catalog.epoch)?;
    e.write_bool(catalog.predecessor.is_some())?;
    if let Some(digest) = catalog.predecessor {
        e.write_bytes(digest.as_bytes())?;
    }
    e.write_u64(catalog.selected_through)?;
    e.write_count(catalog.bindings.len())?;
    for binding in &catalog.bindings {
        if binding.locators.len() > MAX_INLINE_LOCATORS {
            return Err(Error::Capacity("inline bundle locator count"));
        }
        e.write_bytes(binding.application.as_bytes())?;
        e.write_u64(binding.first_commit)?;
        e.write_bytes(&binding.control.encode()?)?;
        e.write_u8(match binding.phase {
            BindingPhase::Provisional => 3,
            BindingPhase::Open => 0,
            BindingPhase::Closing => 1,
            BindingPhase::Closed => 2,
        })?;
        e.write_bool(binding.terminal.is_some())?;
        if let Some((sequence, commit, position)) = binding.terminal {
            e.write_u64(sequence)?;
            e.write_u64(commit)?;
            position_write(&mut e, position)?;
        }
        e.write_u64(binding.selected_sequence)?;
        e.write_u64(binding.selected_commit)?;
        position_write(&mut e, binding.selected_position)?;
        e.write_count(binding.locators.len())?;
        for locator in &binding.locators {
            e.write_bool(locator.object.is_some())?;
            if let Some(digest) = locator.object {
                e.write_bytes(digest.as_bytes())?;
            }
            e.write_u64(locator.offset)?;
            e.write_u64(locator.bytes)?;
            e.write_bytes(locator.frame_digest.as_bytes())?;
        }
    }
    e.write_count(frames)?;
    Ok(e)
}

#[cfg(test)]
pub(super) fn encode(
    catalog: &mut Catalog,
    frames: &[cellule_ltx::VerifiedNodeFrame],
) -> Result<Bytes> {
    if frames.len() > MAX_FRAMES {
        return Err(Error::Capacity("bundle frame count"));
    }
    let mut offset = metadata(catalog, frames.len())?.finish().len() as u64;
    for frame in frames {
        offset = offset
            .checked_add(4)
            .ok_or(Error::Capacity("bundle extent"))?;
        let digest = Digest::from_bytes(frame.digest());
        let matches = catalog
            .bindings
            .iter_mut()
            .flat_map(|binding| &mut binding.locators)
            .filter(|locator| locator.object.is_none() && locator.frame_digest == digest)
            .map(|locator| {
                locator.offset = offset;
                locator.bytes = frame.encoded().len() as u64;
            })
            .count();
        if matches != 1 {
            return Err(Error::Node("bundle frame locator is not unique"));
        }
        offset = offset
            .checked_add(frame.encoded().len() as u64)
            .ok_or(Error::Capacity("bundle bytes"))?;
    }
    let mut e = metadata(catalog, frames.len())?;
    for frame in frames {
        e.write_bytes(frame.encoded())?;
    }
    Ok(Bytes::from(e.finish()))
}

pub(super) fn encode_leaf(catalog: &Catalog) -> Result<Bytes> {
    Ok(Bytes::from(metadata(catalog, 0)?.finish()))
}

pub(super) fn decode(body: &Bytes) -> Result<Catalog> {
    decode_inner(body, true)
}

pub(super) fn decode_leaf(body: &Bytes) -> Result<Catalog> {
    decode_inner(body, false)
}

fn decode_inner(body: &Bytes, complete_object: bool) -> Result<Catalog> {
    let mut d = BoundedDecoder::new(body, MAX_BUNDLE_BYTES as u32)?;
    if d.read_bytes()? != MAGIC {
        return Err(Error::Node("unsupported node bundle format"));
    }
    let session = SessionId::from_bytes(read_fixed(&mut d, "node bundle fixed field length")?);
    let epoch = d.read_u64()?;
    let predecessor = if d.read_bool()? {
        Some(Digest::from_bytes(read_fixed(
            &mut d,
            "node bundle fixed field length",
        )?))
    } else {
        None
    };
    let selected_through = d.read_u64()?;
    let count = d.read_count()?;
    if count > MAX_BINDINGS {
        return Err(Error::Capacity("bundle binding count"));
    }
    let mut bindings = Vec::with_capacity(count);
    for _ in 0..count {
        let application = crate::identity::ApplicationId::from_bytes(read_fixed(
            &mut d,
            "bundle application length",
        )?);
        let first_commit = d.read_u64()?;
        let control = Control::decode(d.read_bytes()?)?;
        let phase = match d.read_u8()? {
            3 => BindingPhase::Provisional,
            0 => BindingPhase::Open,
            1 => BindingPhase::Closing,
            2 => BindingPhase::Closed,
            _ => return Err(Error::Node("invalid bundle binding phase")),
        };
        let terminal = if d.read_bool()? {
            Some((d.read_u64()?, d.read_u64()?, position_read(&mut d)?))
        } else {
            None
        };
        let selected_sequence = d.read_u64()?;
        let selected_commit = d.read_u64()?;
        let selected_position = position_read(&mut d)?;
        let count = d.read_count()?;
        if count > MAX_INLINE_LOCATORS {
            return Err(Error::Capacity("bundle locator count"));
        }
        let mut locators = Vec::with_capacity(count);
        for _ in 0..count {
            locators.push(Locator {
                object: if d.read_bool()? {
                    Some(Digest::from_bytes(read_fixed(
                        &mut d,
                        "node bundle fixed field length",
                    )?))
                } else {
                    None
                },
                offset: d.read_u64()?,
                bytes: d.read_u64()?,
                frame_digest: Digest::from_bytes(read_fixed(
                    &mut d,
                    "node bundle fixed field length",
                )?),
            });
        }
        bindings.push(Binding {
            application,
            first_commit,
            control,
            phase,
            terminal,
            selected_sequence,
            selected_commit,
            selected_position,
            locators,
        });
    }
    let count = d.read_count()?;
    if count > MAX_FRAMES || (!complete_object && count != 0) {
        return Err(Error::Capacity("bundle frame count"));
    }
    let mut local = Vec::with_capacity(count);
    for _ in 0..count {
        let frame = d.read_bytes()?;
        // This offset comes from the borrowed input, rather than trusting an
        // encoded locator or scanning other node objects for a matching row.
        let offset = frame.as_ptr() as usize - body.as_ptr() as usize;
        local.push((
            offset as u64,
            frame.len() as u64,
            Digest::from_bytes(*blake3::hash(frame).as_bytes()),
        ));
    }
    d.finish()?;
    let catalog = Catalog {
        session,
        epoch,
        predecessor,
        selected_through,
        bindings,
        index: None,
    };
    catalog.validate()?;
    let locators: Vec<_> = catalog
        .bindings
        .iter()
        .flat_map(|binding| &binding.locators)
        .filter(|locator| locator.object.is_none())
        .map(|locator| (locator.offset, locator.bytes, locator.frame_digest))
        .collect();
    if complete_object
        && (locators.len() != local.len()
            || local
                .iter()
                .any(|extent| locators.iter().filter(|locator| *locator == extent).count() != 1))
    {
        return Err(Error::Node(
            "bundle local extents differ from complete manifest",
        ));
    }
    Ok(catalog)
}
