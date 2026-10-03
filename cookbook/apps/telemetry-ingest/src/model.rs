use cellule_app::CellKey;
use cellule_runtime::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
/// Permanent event capacity per device; no sequence IDs are evicted or reused.
pub const MAX_EVENTS: usize = 128;
/// Maximum events in one independently processed ingress envelope.
pub const MAX_BATCH: usize = 8;
/// Maximum event-time minute buckets in one immutable device window.
pub const MAX_MINUTES: usize = 16;
/// Maximum projected devices per declared summary shard.
pub const MAX_DEVICES: usize = 16;
/// Maximum permanently completed physical Queue messages in the local audit profile.
pub const MAX_AUDITS: usize = 128;
/// Synthetic signed integer values are bounded to one million milliunits.
pub const MAX_VALUE: i64 = 1_000_000;
/// A canonical entity key, bound to one device SQL Cell.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeviceKey(String);
impl DeviceKey {
    /// Constructs a 1..48-byte lowercase ASCII slug starting with a letter.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 48
            || !value.as_bytes()[0].is_ascii_lowercase()
            || !value
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'-')
            || value.ends_with('-')
            || value.contains("--")
        {
            return Err(Error::Identity("invalid canonical telemetry device key"));
        }
        Ok(Self(value))
    }
    /// Canonical display identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Exact canonical entity-routing bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
impl CellKey for DeviceKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
impl TryFrom<String> for DeviceKey {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}
impl From<DeviceKey> for String {
    fn from(value: DeviceKey) -> Self {
        value.0
    }
}
impl fmt::Display for DeviceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
macro_rules! uuid_key {
    ($name:ident, $doc:literal) => {
        #[doc=$doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name([u8; 16]);
        impl $name {
            /// Reads an exact nonzero canonical lowercase hyphenated UUID.
            pub fn parse(value: &str) -> Result<Self> {
                let id = uuid::Uuid::parse_str(value)
                    .map_err(|_| Error::Identity("invalid telemetry UUID"))?;
                if id.is_nil() || id.to_string() != value {
                    return Err(Error::Identity(
                        "telemetry UUID must be canonical and nonzero",
                    ));
                }
                Ok(Self(*id.as_bytes()))
            }
            /// Exact native identity bytes.
            pub fn bytes(&self) -> [u8; 16] {
                self.0
            }
            pub(crate) fn from_bytes(value: [u8; 16]) -> Result<Self> {
                Self::parse(&uuid::Uuid::from_bytes(value).to_string())
            }
        }
        impl TryFrom<String> for $name {
            type Error = Error;
            fn try_from(value: String) -> Result<Self> {
                Self::parse(&value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                uuid::Uuid::from_bytes(value.0).to_string()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                uuid::Uuid::from_bytes(self.0).fmt(f)
            }
        }
    };
}
uuid_key!(
    BatchId,
    "Caller-selected native Queue producer-dedup identity; Queue retention is bounded."
);
uuid_key!(
    MessageId,
    "Physical native Queue message identity, permanently bound in the audit Cell."
);
/// Immutable event-time window; all unique in-window events count, including lower sequences.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// Inclusive positive minute-aligned start, in simulated Unix milliseconds.
    pub start_ms: i64,
    /// Number of minute buckets, 1..16; the end is exclusive.
    pub minutes: u32,
}
impl Window {
    /// Verifies alignment, bounded width, and checked signed time arithmetic.
    pub fn validate(&self) -> Result<()> {
        if self.start_ms < 60_000
            || self.start_ms % 60_000 != 0
            || !(1..=MAX_MINUTES as u32).contains(&self.minutes)
            || self
                .start_ms
                .checked_add(i64::from(self.minutes) * 60_000)
                .is_none_or(|v| v == i64::MAX)
        {
            return Err(Error::Command("invalid telemetry event-time window"));
        }
        Ok(())
    }
    /// Returns the bounded bucket offset when an event belongs to this window.
    pub fn bucket(&self, at_ms: i64) -> Option<usize> {
        let delta = at_ms.checked_sub(self.start_ms)?;
        if delta < 0 || delta >= i64::from(self.minutes) * 60_000 {
            None
        } else {
            usize::try_from(delta / 60_000).ok()
        }
    }
}
/// One permanently bound source sequence and exact synthetic reading.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Canonical device entity identity.
    pub device: DeviceKey,
    /// Positive source sequence below the signed SQLite maximum; gaps are permitted.
    pub sequence: i64,
    /// Positive simulated event time, distinct from Queue admission time.
    pub at_ms: i64,
    /// Bounded signed integer synthetic measurement; no floating-point aggregation.
    pub value_milli: i64,
}
impl Event {
    /// Validates syntax and arithmetic bounds; the device command enforces its window.
    pub fn validate(&self) -> Result<()> {
        if !(1..i64::MAX).contains(&self.sequence)
            || !(1..i64::MAX).contains(&self.at_ms)
            || !(-MAX_VALUE..=MAX_VALUE).contains(&self.value_milli)
        {
            return Err(Error::Command("invalid bounded telemetry event"));
        }
        Ok(())
    }
}
/// Bounded immutable ingress envelope; entries commit independently in their device Cells.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    /// Queue producer identity; permanent deduplication lives at each device sequence.
    pub id: BatchId,
    /// One to eight exact events, with no duplicate device/sequence pairs in the envelope.
    pub events: Vec<Event>,
}
impl Batch {
    /// Verifies payload bounds before native Queue preparation.
    pub fn validate(&self) -> Result<()> {
        if self.events.is_empty() || self.events.len() > MAX_BATCH {
            return Err(Error::Command("telemetry batch requires 1..8 events"));
        }
        for (index, event) in self.events.iter().enumerate() {
            event.validate()?;
            if self.events[..index]
                .iter()
                .any(|v| v.device == event.device && v.sequence == event.sequence)
            {
                return Err(Error::Command("duplicate telemetry sequence in one batch"));
            }
        }
        Ok(())
    }
}
/// Register one canonical device and its immutable event-time window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    /// Canonical entity key.
    pub device: DeviceKey,
    /// Exact window; identical registrations are harmless and changed windows conflict.
    pub window: Window,
}
/// One retained event and its arrival-order classification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedEvent {
    /// Exact permanent event bytes.
    pub event: Event,
    /// Its sequence was below the observed maximum when first accepted.
    pub reordered: bool,
}
/// Complete bounded source history and latest native summary intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceState {
    /// Canonical source key.
    pub device: DeviceKey,
    /// Immutable event-time admission window.
    pub window: Window,
    /// Positive domain revision; maintenance and repeats do not advance it.
    pub revision: i64,
    /// Complete permanent history, sorted by sequence and bounded to 128 rows.
    pub events: Vec<RecordedEvent>,
    /// Latest transactional summary intent; receipt remains source-local.
    pub effect_id: [u8; 32],
}
/// One event-time minute's cumulative device contribution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    /// Absolute minute-aligned simulated time.
    pub start_ms: i64,
    /// Unique accepted event count.
    pub count: u32,
    /// Exact integer sum, including unique reordered events.
    pub sum_milli: i64,
}
/// Full monotonic device contribution, replacing its earlier summary atomically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSnapshot {
    /// Canonical source key, also selecting its fixed summary shard.
    pub device: DeviceKey,
    /// Immutable admission window.
    pub window: Window,
    /// Full source revision.
    pub revision: i64,
    /// Unique accepted events; excludes duplicate attempts and business rejections.
    pub accepted: u32,
    /// Arrival-dependent count of unique events accepted below the observed sequence maximum.
    pub reordered: u32,
    /// Highest observed sequence, which does not prove contiguous coverage.
    pub max_sequence: i64,
    /// Complete observed prefix beginning at sequence one; zero denotes a gap at one.
    pub contiguous_sequence: i64,
    /// Highest-sequence reading; lower sequences never overwrite it.
    pub latest: Option<Event>,
    /// Complete bounded window contributions, including empty minutes.
    pub buckets: Vec<Bucket>,
    /// Digest of the entire authoritative source state, excluding the transport intent ID.
    pub digest: [u8; 32],
}
impl DeviceState {
    /// Checks complete bounded history, canonical order, and source/window binding.
    pub fn validate(&self) -> Result<()> {
        self.window.validate()?;
        if !(1..i64::MAX).contains(&self.revision)
            || self.events.len() > MAX_EVENTS
            || self.revision != self.events.len() as i64 + 1
        {
            return Err(Error::Command("invalid telemetry source bounds"));
        }
        for (index, row) in self.events.iter().enumerate() {
            row.event.validate()?;
            if row.event.device != self.device
                || self.window.bucket(row.event.at_ms).is_none()
                || index > 0 && self.events[index - 1].event.sequence >= row.event.sequence
            {
                return Err(Error::Command(
                    "telemetry source history binding or ordering differs",
                ));
            }
        }
        Ok(())
    }
    /// Computes a complete deterministic contribution from the actual retained events.
    pub fn snapshot(&self) -> Result<DeviceSnapshot> {
        self.validate()?;
        let mut buckets: Vec<_> = (0..self.window.minutes)
            .map(|index| Bucket {
                start_ms: self.window.start_ms + i64::from(index) * 60_000,
                count: 0,
                sum_milli: 0,
            })
            .collect();
        let mut contiguous = 0;
        for row in &self.events {
            let index = self
                .window
                .bucket(row.event.at_ms)
                .ok_or(Error::Command("stored event outside window"))?;
            buckets[index].count += 1;
            buckets[index].sum_milli += row.event.value_milli;
            if row.event.sequence == contiguous + 1 {
                contiguous = row.event.sequence;
            }
        }
        let mut state = self.clone();
        state.effect_id = [0; 32];
        let digest = *blake3::hash(&crate::wire::encode(&state, 64 << 10)?).as_bytes();
        let latest = self.events.last().map(|v| v.event.clone());
        let value = DeviceSnapshot {
            device: self.device.clone(),
            window: self.window,
            revision: self.revision,
            accepted: self.events.len() as u32,
            reordered: self.events.iter().filter(|v| v.reordered).count() as u32,
            max_sequence: latest.as_ref().map_or(0, |v| v.sequence),
            contiguous_sequence: contiguous,
            latest,
            buckets,
            digest,
        };
        value.validate()?;
        Ok(value)
    }
}
impl DeviceSnapshot {
    /// Verifies a complete bounded contribution before a receiver transaction.
    pub fn validate(&self) -> Result<()> {
        self.window.validate()?;
        if !(1..i64::MAX).contains(&self.revision)
            || self.accepted > MAX_EVENTS as u32
            || self.revision != i64::from(self.accepted) + 1
            || self.reordered > self.accepted
            || self.buckets.len() != self.window.minutes as usize
            || self.digest == [0; 32]
            || self.contiguous_sequence < 0
            || self.contiguous_sequence > i64::from(self.accepted)
            || self.max_sequence < i64::from(self.accepted)
            || self.max_sequence < self.contiguous_sequence
            || self.max_sequence == i64::MAX
        {
            return Err(Error::Command("invalid telemetry contribution"));
        }
        let mut count = 0u32;
        for (index, bucket) in self.buckets.iter().enumerate() {
            if bucket.start_ms != self.window.start_ms + index as i64 * 60_000
                || bucket.count > MAX_EVENTS as u32
                || !(-i64::from(bucket.count) * MAX_VALUE..=i64::from(bucket.count) * MAX_VALUE)
                    .contains(&bucket.sum_milli)
            {
                return Err(Error::Command("invalid telemetry bucket contribution"));
            }
            count = count
                .checked_add(bucket.count)
                .ok_or(Error::Command("telemetry bucket count overflow"))?;
        }
        if count != self.accepted {
            return Err(Error::Command("telemetry contribution count differs"));
        }
        match &self.latest {
            None if self.accepted == 0
                && self.max_sequence == 0
                && self.contiguous_sequence == 0 => {}
            Some(event)
                if self.accepted > 0
                    && event.device == self.device
                    && event.sequence == self.max_sequence
                    && self.window.bucket(event.at_ms).is_some() =>
            {
                event.validate()?;
            }
            _ => return Err(Error::Command("telemetry latest reading differs")),
        }
        Ok(())
    }
}
/// Source state and the native outbox identity emitted in the same transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    /// Full bounded contribution and source revision.
    pub snapshot: DeviceSnapshot,
    /// Stable native intent identity.
    pub effect_id: [u8; 32],
}
/// Explicit durable source business decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// New registration or event committed with its native summary intent.
    Applied,
    /// Identical registration or event already exists; no domain change or new intent.
    Duplicate,
    /// Device has not been registered.
    NotRegistered,
    /// Existing sequence or window is bound to different bytes.
    Conflict,
    /// Event lies outside the immutable registered window.
    OutsideWindow,
    /// Permanent source event or revision capacity reached.
    Capacity,
    /// Input or entity target is invalid.
    Invalid,
    /// Consumer roster did not authorize this source key; no source dispatch occurred.
    NotInRoster,
}
/// Published source answer; rejected decisions have no version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceOutcome {
    /// Source business decision.
    pub decision: Decision,
    /// Present for applied or identical existing state.
    pub version: Option<Version>,
}
/// Durable receiver answers; an older contribution cannot subtract newer state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOutcome {
    /// Newer complete contribution installed.
    Applied,
    /// Identical revision and contribution already present.
    Duplicate,
    /// Older revision accepted harmlessly without regression.
    Stale,
    /// Same revision has different content or an immutable window changed.
    Conflict,
    /// Fixed summary shard device capacity reached.
    Capacity,
}
/// Persisted diagnostic source receipt; audit validation pins its exact device Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceReceipt {
    /// Full Cell identity bytes.
    pub cell: [u8; 32],
    /// Original incarnation bytes.
    pub incarnation: [u8; 16],
    /// Exact original published source position.
    pub commit_sequence: u64,
}
impl From<cellule_runtime::Receipt> for SourceReceipt {
    fn from(value: cellule_runtime::Receipt) -> Self {
        Self {
            cell: *value.cell.as_bytes(),
            incarnation: *value.incarnation.as_bytes(),
            commit_sequence: value.commit_sequence,
        }
    }
}
/// One independently committed entry result observed while completing a batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryResult {
    /// Exact event, preserving its sequence and bytes.
    pub event: Event,
    /// Source answer, or explicit roster exclusion.
    pub outcome: DeviceOutcome,
    /// Original source receipt, absent only for roster exclusion.
    pub source: Option<SourceReceipt>,
}
/// Complete permanent processing audit, published before native Queue acknowledgment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    /// Physical native Queue message identity; independent from its bounded producer-dedup key.
    pub message: MessageId,
    /// Complete original payload.
    pub batch: Batch,
    /// One independently observed outcome for every original entry, in payload order.
    pub results: Vec<EntryResult>,
}
impl Completion {
    /// Verifies complete payload coverage and explicit source outcome/evidence classification.
    pub fn validate(&self) -> Result<()> {
        self.batch.validate()?;
        if self.results.len() != self.batch.events.len() {
            return Err(Error::Command("incomplete telemetry batch audit"));
        }
        for (event, result) in self.batch.events.iter().zip(&self.results) {
            if result.event != *event {
                return Err(Error::Command("telemetry audit event differs"));
            }
            match result.outcome.decision {
                Decision::Applied | Decision::Duplicate => {
                    let version = result
                        .outcome
                        .version
                        .as_ref()
                        .ok_or(Error::Command("successful event has no source version"))?;
                    version.snapshot.validate()?;
                    if version.snapshot.device != event.device
                        || version.effect_id == [0; 32]
                        || result.source.is_none()
                    {
                        return Err(Error::Command("invalid source audit binding"));
                    }
                }
                Decision::NotInRoster
                    if result.outcome.version.is_none() && result.source.is_none() => {}
                Decision::NotRegistered
                | Decision::Conflict
                | Decision::OutsideWindow
                | Decision::Capacity
                | Decision::Invalid
                    if result.outcome.version.is_none() && result.source.is_some() => {}
                _ => {
                    return Err(Error::Command(
                        "telemetry audit outcome classification differs",
                    ));
                }
            }
            if let Some(source) = &result.source
                && (source.cell == [0; 32]
                    || source.incarnation == [0; 16]
                    || source.commit_sequence == 0)
            {
                return Err(Error::Command("invalid retained telemetry source receipt"));
            }
        }
        Ok(())
    }
}
/// Permanent physical-message audit result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditOutcome {
    /// Complete bounded result, identical across Queue redelivery.
    Complete(Completion),
    /// Physical message identity was already bound to a different payload.
    Conflict,
    /// Permanent local audit profile is full; retain the Queue message unacknowledged.
    Capacity,
}
/// Bounded event-time keyset request for one independently committed summary shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPageRequest {
    /// Last absolute minute start from this shard, or null for the first page.
    pub after_ms: Option<i64>,
    /// Requested rows, 1..16.
    pub limit: u32,
}
/// One summary shard's current totals and a bounded bucket page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPage {
    /// Total projected device count in this shard.
    pub devices: u32,
    /// Total unique projected events, including buckets outside this page.
    pub accepted: u32,
    /// Up to 16 current grouped minute contributions.
    pub buckets: Vec<Bucket>,
    /// Last returned minute when more rows exist; each page has its own receipt.
    pub next_ms: Option<i64>,
}
