use crate::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
fn fixed<const N: usize>(d: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    d.read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("export identity length"))
}
impl WireValue for Row {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.id.encode(e)?;
        self.label.encode(e)?;
        self.units.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: u32::decode(d)?,
            label: String::decode(d)?,
            units: u64::decode(d)?,
        })
    }
}
impl WireValue for Version {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self(fixed(d)?))
    }
}
impl WireValue for Snapshot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.version.encode(e)?;
        self.revision.encode(e)?;
        self.rows.encode(e)?;
        e.write_bytes(&self.digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            version: Version::decode(d)?,
            revision: u64::decode(d)?,
            rows: u32::decode(d)?,
            digest: fixed(d)?,
        })
    }
}
impl WireValue for Change {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Put {
                expected_revision,
                row,
            } => {
                e.write_u8(0)?;
                expected_revision.encode(e)?;
                row.encode(e)
            }
            Self::Delete {
                expected_revision,
                id,
            } => {
                e.write_u8(1)?;
                expected_revision.encode(e)?;
                id.encode(e)
            }
            Self::Replace {
                expected_revision,
                rows,
            } => {
                e.write_u8(3)?;
                expected_revision.encode(e)?;
                e.write_count(rows.len())?;
                for row in rows {
                    row.encode(e)?;
                }
                Ok(())
            }
            Self::Seal {
                expected_revision,
                version,
            } => {
                e.write_u8(2)?;
                expected_revision.encode(e)?;
                version.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Put {
                expected_revision: u64::decode(d)?,
                row: Row::decode(d)?,
            }),
            1 => Ok(Self::Delete {
                expected_revision: u64::decode(d)?,
                id: u32::decode(d)?,
            }),
            2 => Ok(Self::Seal {
                expected_revision: u64::decode(d)?,
                version: Version::decode(d)?,
            }),
            3 => {
                let expected_revision = u64::decode(d)?;
                let count = d.read_count()?;
                if count > MAX_ROWS as usize {
                    return Err(CodecError::Limit);
                }
                let rows = (0..count)
                    .map(|_| Row::decode(d))
                    .collect::<Result<_, _>>()?;
                Ok(Self::Replace {
                    expected_revision,
                    rows,
                })
            }
            _ => Err(CodecError::Invalid("unknown export draft change")),
        }
    }
}
impl WireValue for DataOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied(revision) => {
                e.write_u8(0)?;
                revision.encode(e)
            }
            Self::Sealed(snapshot) => {
                e.write_u8(1)?;
                snapshot.encode(e)
            }
            Self::Conflict => e.write_u8(2),
            Self::Missing => e.write_u8(3),
            Self::Capacity => e.write_u8(4),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Applied(u64::decode(d)?)),
            1 => Ok(Self::Sealed(Snapshot::decode(d)?)),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::Missing),
            4 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("unknown export draft outcome")),
        }
    }
}
impl WireValue for DatasetInfo {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.revision.encode(e)?;
        self.rows.encode(e)?;
        self.versions.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            revision: u64::decode(d)?,
            rows: u32::decode(d)?,
            versions: u32::decode(d)?,
        })
    }
}
impl WireValue for PageRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.snapshot.encode(e)?;
        self.after.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            snapshot: Snapshot::decode(d)?,
            after: u32::decode(d)?,
        })
    }
}
impl WireValue for Page {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.snapshot.encode(e)?;
        self.after.encode(e)?;
        e.write_count(self.rows.len())?;
        for row in &self.rows {
            row.encode(e)?;
        }
        self.more.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let snapshot = Snapshot::decode(d)?;
        let after = u32::decode(d)?;
        let count = d.read_count()?;
        if count > PAGE_ROWS as usize {
            return Err(CodecError::Limit);
        }
        let rows = (0..count)
            .map(|_| Row::decode(d))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            snapshot,
            after,
            rows,
            more: bool::decode(d)?,
        })
    }
}
impl WireValue for Request {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.id)?;
        e.write_bytes(&self.source_cell)?;
        self.snapshot.encode(e)?;
        self.deadline_ms.encode(e)?;
        self.endpoint.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: fixed(d)?,
            source_cell: fixed(d)?,
            snapshot: Snapshot::decode(d)?,
            deadline_ms: i64::decode(d)?,
            endpoint: String::decode(d)?,
        })
    }
}
impl WireValue for StartOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Started(id) => {
                e.write_u8(0)?;
                e.write_bytes(id)
            }
            Self::AlreadyBound => e.write_u8(1),
            Self::Conflict => e.write_u8(2),
            Self::Capacity => e.write_u8(3),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Started(fixed(d)?)),
            1 => Ok(Self::AlreadyBound),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("unknown export run outcome")),
        }
    }
}
