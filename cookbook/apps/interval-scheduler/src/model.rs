use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Nonzero canonical UUID used as a persistent schedule or definition identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduleId([u8; 16]);
impl ScheduleId {
    /// Accepts lowercase, hyphenated UUID text; rejects alternate spellings and zero.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        let id = uuid::Uuid::parse_str(value)
            .map_err(|_| CodecError::Invalid("invalid reminder UUID"))?;
        if id.is_nil() || id.to_string() != value {
            return Err(CodecError::Invalid(
                "reminder UUID must be nonzero and canonical",
            ));
        }
        Ok(Self(*id.as_bytes()))
    }
    /// Validates a binary identity from a typed framework message.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, CodecError> {
        if bytes == [0; 16] {
            return Err(CodecError::Invalid("reminder UUID cannot be zero"));
        }
        Ok(Self(bytes))
    }
    /// Returns the version-one routing and wire bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}
impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}
impl Serialize for ScheduleId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for ScheduleId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}
impl WireValue for ScheduleId {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::from_bytes(
            d.read_bytes()?
                .try_into()
                .map_err(|_| CodecError::Invalid("reminder UUID length"))?,
        )
    }
}
/// Bounded domain payload delivered with each reminder occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reminder {
    /// Trimmed nonempty title of at most 80 UTF-8 bytes.
    pub title: String,
    /// Trimmed nonempty message of at most 512 UTF-8 bytes.
    pub message: String,
}
impl Reminder {
    /// Checks ingress limits without accessing storage.
    pub fn validate(&self) -> Result<(), CodecError> {
        let valid = |text: &str, limit: usize| {
            !text.is_empty()
                && text.len() <= limit
                && text.trim() == text
                && !text.chars().any(char::is_control)
        };
        if !valid(&self.title, 80) || !valid(&self.message, 512) {
            return Err(CodecError::Invalid(
                "reminder requires bounded trimmed title and message",
            ));
        }
        Ok(())
    }
}
impl WireValue for Reminder {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.title.encode(e)?;
        self.message.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let reminder = Self {
            title: String::decode(d)?,
            message: String::decode(d)?,
        };
        reminder.validate()?;
        Ok(reminder)
    }
}
// A definition identity is derived from the retained upsert request. Native
// Cron generations restart after delete/recreate; this identity prevents a new
// lifetime from colliding with previously delivered occurrence keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Definition {
    pub id: ScheduleId,
    pub reminder: Reminder,
}
impl WireValue for Definition {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.id.encode(e)?;
        self.reminder.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            id: ScheduleId::decode(d)?,
            reminder: Reminder::decode(d)?,
        })
    }
}
/// Application-owned schedule operations; no arbitrary Cron target is exposed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    /// Creates or replaces the definition and starts at an absolute timestamp.
    Upsert {
        /// Stable schedule identity.
        id: ScheduleId,
        /// Domain reminder content.
        reminder: Reminder,
        /// Fixed interval in milliseconds, 1000 through one 365-day year.
        interval_ms: u64,
        /// First due time, at or after request issuance and at most five years ahead.
        next_due_ms: i64,
    },
    /// Stops future ticks; already-published delivery intents remain eligible.
    Pause {
        /// Schedule identity.
        id: ScheduleId,
    },
    /// Re-enables at an explicit due time; preserves the native occurrence counter.
    Resume {
        /// Schedule identity.
        id: ScheduleId,
        /// Absolute due time at or after request issuance, within five years.
        next_due_ms: i64,
    },
    /// Deletes the schedule; previously-published occurrences can still arrive.
    Delete {
        /// Schedule identity.
        id: ScheduleId,
    },
}
impl Change {
    /// Returns the schedule whose fixed shard owns the operation.
    pub fn id(&self) -> ScheduleId {
        match self {
            Self::Upsert { id, .. }
            | Self::Pause { id }
            | Self::Resume { id, .. }
            | Self::Delete { id } => *id,
        }
    }
    /// Checks domain and native interval/time bounds before retaining or dispatching.
    pub fn validate(&self, issued_at_ms: i64) -> Result<(), CodecError> {
        if issued_at_ms < 0 {
            return Err(CodecError::Invalid("negative issuance timestamp"));
        }
        let due = match self {
            Self::Upsert {
                reminder,
                interval_ms,
                next_due_ms,
                ..
            } => {
                reminder.validate()?;
                if !(1000..=365 * 24 * 60 * 60 * 1000).contains(interval_ms) {
                    return Err(CodecError::Invalid(
                        "reminder interval must be one second through one year",
                    ));
                }
                Some(*next_due_ms)
            }
            Self::Resume { next_due_ms, .. } => Some(*next_due_ms),
            _ => None,
        };
        if due.is_some_and(|due| {
            due < issued_at_ms || due > issued_at_ms.saturating_add(5 * 365 * 24 * 60 * 60 * 1000)
        }) {
            return Err(CodecError::Invalid(
                "first due time is outside the request's five-year window",
            ));
        }
        Ok(())
    }
}
/// One materialized schedule, mapped from its receipt-bound native state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Schedule {
    /// Stable schedule key.
    pub id: ScheduleId,
    /// Upsert request identity distinguishing definition lifetimes.
    pub definition: ScheduleId,
    /// Domain content.
    pub reminder: Reminder,
    /// Fixed interval in milliseconds.
    pub interval_ms: u64,
    /// Next due occurrence's absolute Unix timestamp.
    pub next_due_ms: i64,
    /// Number of published occurrences since the last upsert; pause/resume preserves it.
    pub occurrence: u64,
    /// Whether future ticks may fire.
    pub enabled: bool,
    /// Native generation, advanced by upsert and material control changes.
    pub generation: u64,
}
/// One bounded native schedule page; query each shard explicitly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SchedulePage {
    /// Schedules in canonical binary identity order.
    pub schedules: Vec<Schedule>,
    /// Resume after this identity; null marks the last page.
    pub next: Option<ScheduleId>,
}
/// SQL inbox row for one durably delivered occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Delivery {
    /// Positive monotonic receiver-local row ID used for keyset pagination.
    pub row: i64,
    /// Stable schedule identity.
    pub schedule: ScheduleId,
    /// Upsert request identity separating deletion/recreation lifetimes.
    pub definition: ScheduleId,
    /// Native generation at the source tick.
    pub generation: u64,
    /// Positive occurrence counter from the source.
    pub occurrence: u64,
    /// Original scheduled timestamp; delivery can be late.
    pub scheduled_at_ms: i64,
    /// Reminder content pinned by that source definition.
    pub reminder: Reminder,
}
impl WireValue for Delivery {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.row.encode(e)?;
        self.schedule.encode(e)?;
        self.definition.encode(e)?;
        self.generation.encode(e)?;
        self.occurrence.encode(e)?;
        self.scheduled_at_ms.encode(e)?;
        self.reminder.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            row: i64::decode(d)?,
            schedule: ScheduleId::decode(d)?,
            definition: ScheduleId::decode(d)?,
            generation: u64::decode(d)?,
            occurrence: u64::decode(d)?,
            scheduled_at_ms: i64::decode(d)?,
            reminder: Reminder::decode(d)?,
        })
    }
}
/// Bounded keyset request for one schedule's deliveries across all lifetimes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryPageRequest {
    /// Exact schedule to inspect.
    pub schedule: ScheduleId,
    /// Last observed positive row ID, or null to start.
    pub after: Option<i64>,
    /// Maximum items, 1 through 100.
    pub limit: u32,
}
impl WireValue for DeliveryPageRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.schedule.encode(e)?;
        self.after.encode(e)?;
        self.limit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            schedule: ScheduleId::decode(d)?,
            after: Option::<i64>::decode(d)?,
            limit: u32::decode(d)?,
        })
    }
}
/// One bounded receiver page, ordered by durable receiver row ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DeliveryPage {
    /// Delivered occurrence records.
    pub deliveries: Vec<Delivery>,
    /// Resume after this row; null means the observed traversal ended.
    pub next: Option<i64>,
}
impl WireValue for DeliveryPage {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.deliveries.len() > 100 {
            return Err(CodecError::Invalid("delivery page exceeds 100"));
        }
        e.write_count(self.deliveries.len())?;
        for item in &self.deliveries {
            item.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = d.read_count()?;
        if count > 100 {
            return Err(CodecError::Invalid("delivery page exceeds 100"));
        }
        Ok(Self {
            deliveries: (0..count)
                .map(|_| Delivery::decode(d))
                .collect::<Result<_, _>>()?,
            next: Option::<i64>::decode(d)?,
        })
    }
}
/// Durable receiver result; identical retries retain their original row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    /// The invocation was inserted or already matches this stored row.
    Recorded {
        /// Positive receiver row ID.
        row: i64,
    },
    /// An identical occurrence key already carries different time or content.
    Conflict,
}
impl WireValue for RecordOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Recorded { row } => {
                0_u8.encode(e)?;
                row.encode(e)
            }
            Self::Conflict => 1_u8.encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            0 => Ok(Self::Recorded {
                row: i64::decode(d)?,
            }),
            1 => Ok(Self::Conflict),
            _ => Err(CodecError::Invalid("unknown reminder outcome")),
        }
    }
}
