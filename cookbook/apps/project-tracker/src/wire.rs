use crate::model::*;
use cellule_runtime::{
    Result,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
};

pub(crate) fn encode<T: WireValue>(value: &T, limit: u32) -> Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
pub(crate) fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
fn version(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<(), CodecError> {
    if decoder.read_u8()? != 1 {
        return Err(CodecError::Invalid("unsupported tracker value version"));
    }
    Ok(())
}
fn fixed(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<[u8; 32], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("tracker digest length differs"))
}
fn values<T: WireValue>(
    encoder: &mut BoundedEncoder,
    values: &[T],
    maximum: usize,
) -> std::result::Result<(), CodecError> {
    if values.len() > maximum {
        return Err(CodecError::Limit);
    }
    encoder.write_count(values.len())?;
    for value in values {
        value.encode(encoder)?;
    }
    Ok(())
}
fn list<T: WireValue>(
    decoder: &mut BoundedDecoder<'_>,
    maximum: usize,
) -> std::result::Result<Vec<T>, CodecError> {
    let count = decoder.read_count()?;
    if count > maximum {
        return Err(CodecError::Limit);
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(T::decode(decoder)?);
    }
    Ok(values)
}
macro_rules! key {
    ($($name:ident),+ $(,)?) => {$ (
        impl WireValue for $name {
            fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
                encoder.write_u8(1)?;
                encoder.write_text(self.as_str())
            }
            fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
                version(decoder)?;
                Self::new(decoder.read_text()?)
            }
        }
    )+};
}
key!(ProjectKey, IssueId, AttachmentId, Assignee);
// Every nested domain value has an explicit version and stable field order.
// Add a codec version when these persisted producers and consumers evolve.
macro_rules! structure {
    ($name:ident {$($field:ident),+ $(,)?}) => {
        impl WireValue for $name {
            fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
                encoder.write_u8(1)?;
                $(self.$field.encode(encoder)?;)+
                Ok(())
            }
            fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
                version(decoder)?;
                Ok(Self {$($field: WireValue::decode(decoder)?),+})
            }
        }
    };
}
macro_rules! enumeration {
    ($name:ident {$($tag:literal => $variant:ident),+ $(,)?}) => {
        impl WireValue for $name {
            fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
                encoder.write_u8(1)?;
                encoder.write_u8(match self {$(Self::$variant => $tag),+})
            }
            fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
                version(decoder)?;
                match decoder.read_u8()? { $($tag => Ok(Self::$variant)),+, _ => Err(CodecError::Invalid("unknown tracker enum tag")) }
            }
        }
    };
}
enumeration!(IssueStatus {1 => Open, 2 => Closed});
enumeration!(Decision {1 => Applied, 2 => Unchanged, 3 => NotFound, 4 => Conflict, 5 => Exists, 6 => Invalid, 7 => Capacity, 8 => BindingMismatch});
enumeration!(ProjectionOutcome {1 => Applied, 2 => Unchanged, 3 => Stale, 4 => Conflict, 5 => Capacity});
structure!(IssueFields {
    title,
    description,
    assignee,
    status
});
structure!(ProjectChange { project, mutation });
structure!(AttachmentLink {
    expected_revision,
    publication
});
structure!(ChangeOutcome { decision, version });
structure!(DashboardPageRequest { after, limit });
impl WireValue for ProjectMutation {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        match self {
            Self::Create { name } => {
                encoder.write_u8(1)?;
                name.encode(encoder)
            }
            Self::Rename {
                expected_revision,
                name,
            } => {
                encoder.write_u8(2)?;
                expected_revision.encode(encoder)?;
                name.encode(encoder)
            }
            Self::CreateIssue { id, fields } => {
                encoder.write_u8(3)?;
                id.encode(encoder)?;
                fields.encode(encoder)
            }
            Self::EditIssue {
                id,
                expected_revision,
                fields,
            } => {
                encoder.write_u8(4)?;
                id.encode(encoder)?;
                expected_revision.encode(encoder)?;
                fields.encode(encoder)
            }
        }
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(match decoder.read_u8()? {
            1 => Self::Create {
                name: String::decode(decoder)?,
            },
            2 => Self::Rename {
                expected_revision: i64::decode(decoder)?,
                name: String::decode(decoder)?,
            },
            3 => Self::CreateIssue {
                id: IssueId::decode(decoder)?,
                fields: IssueFields::decode(decoder)?,
            },
            4 => Self::EditIssue {
                id: IssueId::decode(decoder)?,
                expected_revision: i64::decode(decoder)?,
                fields: IssueFields::decode(decoder)?,
            },
            _ => return Err(CodecError::Invalid("unknown tracker mutation tag")),
        })
    }
}
impl WireValue for AttachmentDescriptor {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.project.encode(encoder)?;
        self.issue.encode(encoder)?;
        self.id.encode(encoder)?;
        self.name.encode(encoder)?;
        self.bytes.encode(encoder)?;
        encoder.write_bytes(&self.digest)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            project: ProjectKey::decode(decoder)?,
            issue: IssueId::decode(decoder)?,
            id: AttachmentId::decode(decoder)?,
            name: String::decode(decoder)?,
            bytes: u32::decode(decoder)?,
            digest: fixed(decoder)?,
        })
    }
}
impl WireValue for AttachmentPublication {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.descriptor.encode(encoder)?;
        encoder.write_bytes(&self.etag)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            descriptor: AttachmentDescriptor::decode(decoder)?,
            etag: fixed(decoder)?,
        })
    }
}
impl WireValue for Issue {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.id.encode(encoder)?;
        self.revision.encode(encoder)?;
        self.fields.encode(encoder)?;
        values(encoder, &self.attachments, MAX_ISSUE_ATTACHMENTS)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            id: IssueId::decode(decoder)?,
            revision: i64::decode(decoder)?,
            fields: IssueFields::decode(decoder)?,
            attachments: list(decoder, MAX_ISSUE_ATTACHMENTS)?,
        })
    }
}
impl WireValue for ProjectState {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.project.encode(encoder)?;
        self.name.encode(encoder)?;
        self.revision.encode(encoder)?;
        values(encoder, &self.issues, MAX_ISSUES)?;
        encoder.write_bytes(&self.effect_id)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            project: ProjectKey::decode(decoder)?,
            name: String::decode(decoder)?,
            revision: i64::decode(decoder)?,
            issues: list(decoder, MAX_ISSUES)?,
            effect_id: fixed(decoder)?,
        })
    }
}
impl WireValue for ProjectSummary {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.project.encode(encoder)?;
        self.name.encode(encoder)?;
        self.revision.encode(encoder)?;
        self.issues.encode(encoder)?;
        self.open.encode(encoder)?;
        self.attachments.encode(encoder)?;
        encoder.write_bytes(&self.digest)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            project: ProjectKey::decode(decoder)?,
            name: String::decode(decoder)?,
            revision: i64::decode(decoder)?,
            issues: u32::decode(decoder)?,
            open: u32::decode(decoder)?,
            attachments: u32::decode(decoder)?,
            digest: fixed(decoder)?,
        })
    }
}
impl WireValue for ProjectVersion {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        self.summary.encode(encoder)?;
        encoder.write_bytes(&self.effect_id)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            summary: ProjectSummary::decode(decoder)?,
            effect_id: fixed(decoder)?,
        })
    }
}
impl WireValue for DashboardPage {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u8(1)?;
        values(encoder, &self.projects, 16)?;
        self.next.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            projects: list(decoder, 16)?,
            next: Option::<ProjectKey>::decode(decoder)?,
        })
    }
}
