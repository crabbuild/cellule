use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};

/// Version-one canonical ASCII device slug; renaming attributes never changes it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeviceKey(String);
impl DeviceKey {
    /// Accepts 1–64 lowercase ASCII letters, digits, and internal hyphens.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(CodecError::Invalid(
                "device key must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }
    /// Stable bytes used by the framework's entity partition derivation.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    /// Canonical display and directory key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for DeviceKey {
    type Error = CodecError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<DeviceKey> for String {
    fn from(value: DeviceKey) -> Self {
        value.0
    }
}
impl cellule_app::CellKey for DeviceKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl WireValue for DeviceKey {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}
/// Bounded descriptive attributes. Key identity is deliberately separate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attributes {
    /// Human-readable name, 1–120 UTF-8 bytes, without surrounding whitespace or controls.
    pub name: String,
    /// Location, with the same bounds as name.
    pub location: String,
    /// Whether the device is enabled.
    pub enabled: bool,
}
impl Attributes {
    /// Validates bounded application data before preparing a command.
    pub fn validate(&self) -> Result<(), CodecError> {
        if [&self.name, &self.location].iter().any(|v| {
            v.is_empty() || v.len() > 120 || v.trim() != *v || v.chars().any(char::is_control)
        }) {
            return Err(CodecError::Invalid(
                "device attributes require 1..120 unpadded bytes without controls",
            ));
        }
        Ok(())
    }
}
impl WireValue for Attributes {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.name.encode(e)?;
        self.location.encode(e)?;
        self.enabled.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            name: String::decode(d)?,
            location: String::decode(d)?,
            enabled: bool::decode(d)?,
        })
    }
}
/// Full state projected from a device's independent transaction domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Device {
    /// Stable canonical identity.
    pub key: DeviceKey,
    /// Current bounded attributes.
    pub attributes: Attributes,
    /// Positive monotonic revision; updates never reuse a previous revision.
    pub revision: i64,
}
impl Device {
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        self.attributes.validate()?;
        if self.revision <= 0 {
            return Err(CodecError::Invalid("device revision must be positive"));
        }
        Ok(())
    }
}
impl WireValue for Device {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.key.encode(e)?;
        self.attributes.encode(e)?;
        self.revision.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            key: DeviceKey::decode(d)?,
            attributes: Attributes::decode(d)?,
            revision: i64::decode(d)?,
        })
    }
}
/// Immutable compare-and-set command. Zero registers; positive revisions update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    /// Explicit key, checked against the selected source Cell by the handler.
    pub key: DeviceKey,
    /// Zero requires absence; otherwise the exact positive revision must match.
    pub expected_revision: i64,
    /// Complete replacement of descriptive attributes.
    pub attributes: Attributes,
}
impl Change {
    /// Validates application bounds; the server repeats validation transactionally.
    pub fn validate(&self) -> Result<(), CodecError> {
        self.attributes.validate()?;
        if self.expected_revision < 0 || self.expected_revision == i64::MAX {
            return Err(CodecError::Invalid(
                "expected revision must be 0..i64::MAX-1",
            ));
        }
        Ok(())
    }
}
impl WireValue for Change {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.key.encode(e)?;
        self.expected_revision.encode(e)?;
        self.attributes.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            key: DeviceKey::decode(d)?,
            expected_revision: i64::decode(d)?,
            attributes: Attributes::decode(d)?,
        })
    }
}
/// Device state and its most recent transactional directory intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedDevice {
    /// Authoritative entity state.
    pub device: Device,
    /// Native Effect identity emitted in the same commit.
    pub effect_id: [u8; 32],
}
impl WireValue for PublishedDevice {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.device.encode(e)?;
        e.write_bytes(&self.effect_id)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let device = Device::decode(d)?;
        let effect_id = d
            .read_bytes()?
            .try_into()
            .map_err(|_| CodecError::Invalid("effect identity must be 32 bytes"))?;
        Ok(Self { device, effect_id })
    }
}
/// Durable business decisions; infrastructure failures keep their native evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "published", rename_all = "snake_case")]
pub enum ChangeOutcome {
    /// State and intent committed atomically; directory delivery is still asynchronous.
    Applied(PublishedDevice),
    /// Registration found an existing device, or an edit's revision did not match.
    Conflict,
    /// A positive revision edit targeted an absent device.
    NotFound,
    /// Invalid attributes, revision, or key/Cell binding.
    Invalid,
}
impl WireValue for ChangeOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied(v) => {
                1_u8.encode(e)?;
                v.encode(e)
            }
            Self::Conflict => 2_u8.encode(e),
            Self::NotFound => 3_u8.encode(e),
            Self::Invalid => 4_u8.encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Applied(PublishedDevice::decode(d)?)),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::NotFound),
            4 => Ok(Self::Invalid),
            _ => Err(CodecError::Invalid("unknown device outcome")),
        }
    }
}
/// Receiver decisions prevent delayed or duplicated delivery from reverting state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOutcome {
    /// A previously absent or newer revision was installed.
    Applied,
    /// The exact state was already present.
    Unchanged,
    /// A newer revision is already present; this old intent was accepted harmlessly.
    Stale,
    /// The same revision claimed different attributes: terminal invariant violation.
    Conflict,
    /// New registration exceeds the receiver's bounded capacity.
    Capacity,
}
impl WireValue for ProjectionOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Applied => 1_u8,
            Self::Unchanged => 2,
            Self::Stale => 3,
            Self::Conflict => 4,
            Self::Capacity => 5,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Applied),
            2 => Ok(Self::Unchanged),
            3 => Ok(Self::Stale),
            4 => Ok(Self::Conflict),
            5 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("unknown projection outcome")),
        }
    }
}
/// Bounded current directory pagination, ordered by canonical key.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Exclusive cursor; None starts from the first key.
    pub after: Option<DeviceKey>,
    /// 1..100 devices per read.
    pub limit: u32,
}
impl WireValue for PageRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.after.encode(e)?;
        self.limit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            after: Option::<DeviceKey>::decode(d)?,
            limit: u32::decode(d)?,
        })
    }
}
/// Bounded page; each read observes a directory commit, not a multi-page snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    /// Devices in ascending key order.
    pub devices: Vec<Device>,
    /// Exclusive next cursor, present only when more rows exist.
    pub next: Option<DeviceKey>,
}
impl WireValue for Page {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.devices.len() > 100 {
            return Err(CodecError::Limit);
        }
        e.write_count(self.devices.len())?;
        for v in &self.devices {
            v.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = d.read_count()?;
        if count > 100 {
            return Err(CodecError::Limit);
        }
        let mut devices = Vec::with_capacity(count);
        for _ in 0..count {
            devices.push(Device::decode(d)?);
        }
        Ok(Self {
            devices,
            next: Option::<DeviceKey>::decode(d)?,
        })
    }
}
/// Source-ledger progress for the device's latest intent; no directory receipt is implied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    /// Source state exists; directory delivery has not been acknowledged.
    Pending,
    /// Native source acknowledgement confirms the receiver accepted this intent.
    Delivered,
    /// Terminal rejection or expiration; inspect the native status and update to repair.
    Failed,
    /// Ledger evidence is no longer available; this does not prove absence at the receiver.
    Unavailable,
}
/// Progress read from the source only, separate from eventually consistent lookup.
#[derive(Clone, Debug, Serialize)]
pub struct Progress {
    /// State whose latest intent was inspected.
    pub published: PublishedDevice,
    /// Delivery state at the observed source commit.
    pub state: ProjectionState,
    /// Attempts retained by the native ledger, if available.
    pub attempts: Option<u32>,
}
