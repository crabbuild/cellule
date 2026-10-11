//! Bounded Node Bundle v1 catalog rows; native frames live in indexed extents.
use super::*;
use crate::codec::{BoundedDecoder, BoundedEncoder, read_fixed};

const MAGIC: &[u8] = b"cellule.catalog-rows.v1";

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

pub(super) struct LeafEncoding {
    pub(super) body: Vec<u8>,
    pub(super) extents: Vec<([u8; 32], std::ops::Range<usize>)>,
}

struct Metadata {
    encoder: BoundedEncoder,
    extents: Vec<([u8; 32], std::ops::Range<usize>)>,
}

fn metadata(catalog: &Catalog) -> Result<Metadata> {
    catalog.validate()?;
    let mut e = BoundedEncoder::new(MAX_BUNDLE_BYTES as u32)?;
    let mut extents = Vec::new();
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
        if binding.locators.len() > 1 {
            return Err(Error::Capacity("bundle row history descriptor count"));
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
        let pin = binding
            .control
            .bundle_binding
            .ok_or(Error::Node("bundle catalog lacks Cell pin"))?
            .digest;
        for locator in &binding.locators {
            let start = e.encoded_len();
            index::write_extent(&mut e, locator)?;
            extents.push((*pin.as_bytes(), start..e.encoded_len()));
        }
    }
    Ok(Metadata {
        encoder: e,
        extents,
    })
}

#[cfg(test)]
pub(super) fn encode_leaf(catalog: &Catalog) -> Result<Bytes> {
    Ok(Bytes::from(encode_leaf_plan(catalog)?.body))
}

pub(super) fn encode_leaf_plan(catalog: &Catalog) -> Result<LeafEncoding> {
    let metadata = metadata(catalog)?;
    Ok(LeafEncoding {
        body: metadata.encoder.finish(),
        extents: metadata.extents,
    })
}

pub(super) fn decode_leaf(body: &Bytes) -> Result<Catalog> {
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
        if count > 1 {
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
    Ok(catalog)
}
