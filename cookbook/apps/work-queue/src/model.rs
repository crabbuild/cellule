use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};

/// Version-one application message, deduplicated by a permanent UUID business key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// Canonical lowercase hyphenated nonzero UUID.
    pub id: String,
    /// Trimmed, nonempty text of at most 512 bytes without control characters.
    pub body: String,
}
impl Job {
    /// Validates the producer's business identity and bounds before admission.
    pub fn validate(&self) -> Result<[u8; 16], CodecError> {
        let id = job_identity(&self.id)?;
        if self.body.is_empty()
            || self.body.len() > 512
            || self.body.trim() != self.body
            || self.body.chars().any(char::is_control)
        {
            return Err(CodecError::Invalid(
                "job requires a canonical UUID and bounded text",
            ));
        }
        Ok(id)
    }
}
pub(crate) fn job_identity(value: &str) -> Result<[u8; 16], CodecError> {
    let id = uuid::Uuid::parse_str(value).map_err(|_| CodecError::Invalid("invalid job UUID"))?;
    if id.is_nil() || id.to_string() != value {
        return Err(CodecError::Invalid(
            "job UUID must be nonzero and canonical",
        ));
    }
    Ok(*id.as_bytes())
}

/// Canonical composite continuation after one inspected job and delivery kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionCursor {
    /// Canonical nonzero job UUID.
    pub job_id: String,
    /// False for a delivery row, true for its dead-letter row.
    pub dead: bool,
}
impl WireValue for InspectionCursor {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        job_identity(&self.job_id)?;
        self.job_id.encode(e)?;
        self.dead.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let cursor = Self {
            job_id: String::decode(d)?,
            dead: bool::decode(d)?,
        };
        job_identity(&cursor.job_id)?;
        Ok(cursor)
    }
}
/// Codec-version-two inspection request; explicit bounded keyset pagination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectionPageRequest {
    /// Last observed composite key, or null to begin.
    pub after: Option<InspectionCursor>,
    /// Maximum rows, 1 through 100.
    pub limit: u32,
}
impl Default for InspectionPageRequest {
    fn default() -> Self {
        Self {
            after: None,
            limit: 100,
        }
    }
}
impl WireValue for InspectionPageRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.after.encode(e)?;
        self.limit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            after: Option::<InspectionCursor>::decode(d)?,
            limit: u32::decode(d)?,
        })
    }
}
impl WireValue for Job {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.id.encode(encoder)?;
        self.body.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            id: String::decode(decoder)?,
            body: String::decode(decoder)?,
        };
        value.validate()?;
        Ok(value)
    }
}
/// One permanently recorded external action or dead-letter inspection row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Delivery {
    /// Stable job identity.
    pub job: Job,
    /// Whether this row records dead-letter inspection instead of delivery.
    pub dead: bool,
}
impl WireValue for Delivery {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.job.encode(encoder)?;
        self.dead.encode(encoder)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            job: Job::decode(decoder)?,
            dead: bool::decode(decoder)?,
        })
    }
}
/// Bounded current-read inspection page with lifetime totals and a continuation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Inspection {
    /// Whether the simulated external receiver accepts new delivery work.
    pub enabled: bool,
    /// Lifetime count of unique actions.
    pub delivered: i64,
    /// Lifetime count of unique dead-letter inspection records.
    pub dead: i64,
    /// Up to 100 rows in business-key order; totals can exceed this list.
    pub rows: Vec<Delivery>,
    /// Continue after this composite key; null ends the observed traversal.
    pub next: Option<InspectionCursor>,
}
impl WireValue for Inspection {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.enabled.encode(e)?;
        self.delivered.encode(e)?;
        self.dead.encode(e)?;
        if self.rows.len() > 100 {
            return Err(CodecError::Invalid("too many inspection rows"));
        }
        e.write_count(self.rows.len())?;
        for row in &self.rows {
            row.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            enabled: bool::decode(d)?,
            delivered: i64::decode(d)?,
            dead: i64::decode(d)?,
            rows: {
                let count = u32::decode(d)?;
                if count > 100 {
                    return Err(CodecError::Invalid("too many inspection rows"));
                }
                (0..count)
                    .map(|_| Delivery::decode(d))
                    .collect::<Result<_, _>>()?
            },
            next: Option::<InspectionCursor>::decode(d)?,
        })
    }
}
/// Published business outcome of an idempotent receiver action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    /// New action durably recorded.
    Recorded,
    /// Identical business key and body already recorded.
    Duplicate,
    /// The receiver is deliberately unavailable; retry the queue lease.
    Unavailable,
    /// A reused business identity supplied different bytes.
    Conflict,
}
impl WireValue for RecordOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        let tag: u8 = match self {
            Self::Recorded => 0,
            Self::Duplicate => 1,
            Self::Unavailable => 2,
            Self::Conflict => 3,
        };
        tag.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            0 => Ok(Self::Recorded),
            1 => Ok(Self::Duplicate),
            2 => Ok(Self::Unavailable),
            3 => Ok(Self::Conflict),
            _ => Err(CodecError::Invalid("invalid receiver outcome")),
        }
    }
}
