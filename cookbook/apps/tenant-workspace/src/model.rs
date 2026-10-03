use crate::Error;
use cellule_runtime::{
    MutationIdentity, Receipt,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
    identity::{CellId, IncarnationId, RequestId},
};
use serde::{Deserialize, Serialize};

pub(crate) fn canonical_slug(value: &str, max: usize) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > max
        || value.starts_with('-')
        || value.ends_with('-')
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(Error::Invalid("noncanonical slug"));
    }
    Ok(())
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn unhex<const N: usize>(text: &str) -> Result<[u8; N], Error> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("invalid canonical hexadecimal value"));
    }
    let digit = |b: u8| {
        if b.is_ascii_digit() {
            b - b'0'
        } else {
            b - b'a' + 10
        }
    };
    let mut bytes = [0; N];
    for (i, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        bytes[i] = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Ok(bytes)
}
/// Canonical configured project key. This bounded installation admits launch and support.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectKey(String);
impl ProjectKey {
    /// Validates the slug and applies the application's two-project admission policy.
    pub fn parse(value: &str) -> Result<Self, Error> {
        canonical_slug(value, 32)?;
        if !matches!(value, "launch" | "support") {
            return Err(Error::NotFound);
        }
        Ok(Self(value.into()))
    }
    /// Returns the canonical logical resource name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
/// One project document in one tenant's entity Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    /// Trimmed title of 1–128 bytes.
    pub title: String,
    /// Description of at most 512 bytes without control characters.
    pub description: String,
    /// Positive monotonically increasing revision.
    pub revision: i64,
    /// Verified subject responsible for the last change.
    pub updated_by: String,
}
impl WireValue for Project {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.title.encode(e)?;
        self.description.encode(e)?;
        self.revision.encode(e)?;
        self.updated_by.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            title: String::decode(d)?,
            description: String::decode(d)?,
            revision: i64::decode(d)?,
            updated_by: String::decode(d)?,
        })
    }
}
/// Frozen conditional project edit; null revision creates only an absent document.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectChange {
    /// Exact prior revision, or null for creation.
    pub expected_revision: Option<i64>,
    /// New title.
    pub title: String,
    /// New description.
    pub description: String,
}
impl ProjectChange {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.title.is_empty()
            || self.title.len() > 128
            || self.title.trim() != self.title
            || self.title.chars().any(char::is_control)
            || self.description.len() > 512
            || self.description.trim() != self.description
            || self.description.chars().any(char::is_control)
            || self
                .expected_revision
                .is_some_and(|v| v <= 0 || v == i64::MAX)
        {
            return Err(Error::Invalid("invalid project document or revision"));
        }
        Ok(())
    }
}
/// Native command input stamps the authenticated subject into the request bytes.
#[derive(Clone, Debug)]
pub struct ProjectInput {
    pub(crate) actor: String,
    pub(crate) change: ProjectChange,
}
impl WireValue for ProjectInput {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.actor.encode(e)?;
        self.change.expected_revision.encode(e)?;
        self.change.title.encode(e)?;
        self.change.description.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            actor: String::decode(d)?,
            change: ProjectChange {
                expected_revision: Option::<i64>::decode(d)?,
                title: String::decode(d)?,
                description: String::decode(d)?,
            },
        })
    }
}
/// Durable project success or conditional business rejection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", content = "project", rename_all = "snake_case")]
pub enum ProjectOutcome {
    /// The returned document follows durable publication.
    Applied(Project),
    /// An observed revision or absence condition did not match.
    Conflict,
    /// An update selected an absent project document.
    NotFound,
    /// Invalid native domain input was durably rejected.
    Invalid,
}
impl WireValue for ProjectOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied(p) => {
                1u8.encode(e)?;
                p.encode(e)
            }
            Self::Conflict => 2u8.encode(e),
            Self::NotFound => 3u8.encode(e),
            Self::Invalid => 4u8.encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Applied(Project::decode(d)?)),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::NotFound),
            4 => Ok(Self::Invalid),
            _ => Err(CodecError::Invalid("invalid project outcome tag")),
        }
    }
}
/// Retained five-minute request identity, generated before HTTP dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Canonical nonzero UUID.
    pub request_id: String,
    /// Original issue timestamp, in Unix milliseconds.
    pub issued_at_ms: i64,
    /// Original expiry timestamp, within five minutes of issuance.
    pub expires_at_ms: i64,
}
impl Identity {
    /// Generates an identity for a new logical operation. Keep it unchanged on retry.
    pub fn new() -> Result<Self, Error> {
        let native = cellule_cookbook_support::new_identity()?;
        Ok(Self {
            request_id: uuid::Uuid::from_bytes(*native.request_id.as_bytes()).to_string(),
            issued_at_ms: native.issued_at_ms,
            expires_at_ms: native.expires_at_ms,
        })
    }
    pub(crate) fn native(&self) -> Result<MutationIdentity, Error> {
        let id = uuid::Uuid::parse_str(&self.request_id)
            .map_err(|_| Error::Invalid("invalid request UUID"))?;
        if id.is_nil()
            || id.to_string() != self.request_id
            || self.issued_at_ms < 0
            || self.issued_at_ms > cellule_cookbook_support::now_ms()?.saturating_add(300000)
            || self.expires_at_ms <= self.issued_at_ms
            || self.expires_at_ms.saturating_sub(self.issued_at_ms) > 300000
        {
            return Err(Error::Invalid("invalid retained request identity"));
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(*id.as_bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
/// Original logical resource retained alongside the mutation; it never selects a raw target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Resource {
    /// One configured project; its key must match the authenticated route.
    Project {
        /// Canonical project key.
        key: String,
    },
    /// The authenticated tenant's workspace preferences.
    Preferences,
}
/// Version-one frozen mutation. Principal and operation are retained before sending.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation<T> {
    /// Original tenant claim, checked against the verified principal.
    pub tenant: crate::Tenant,
    /// Original resource claim, checked against the authorized route.
    pub resource: Resource,
    /// Current format version, exactly one.
    pub version: u32,
    /// Subject that must match the authenticated bearer membership.
    pub subject: String,
    /// Original command identity.
    pub identity: Identity,
    /// Frozen domain change.
    pub change: T,
}
impl<T> Mutation<T> {
    pub(crate) fn authorize(
        &self,
        principal: &crate::Principal,
    ) -> Result<MutationIdentity, Error> {
        if self.subject != principal.subject() || self.tenant != principal.tenant() {
            return Err(Error::Forbidden);
        }
        if self.version != 1 {
            return Err(Error::Invalid("unsupported mutation version"));
        }
        self.identity.native()
    }
}
/// Explicit receipt fields are checked against the authorized target before a read.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptData {
    /// Canonical Cell ID; cannot select or provision a target.
    pub cell: String,
    /// Exact incarnation that produced the observation.
    pub incarnation: String,
    /// Observed committed sequence.
    pub commit_sequence: u64,
}
impl From<Receipt> for ReceiptData {
    fn from(r: Receipt) -> Self {
        Self {
            cell: hex(r.cell.as_bytes()),
            incarnation: hex(r.incarnation.as_bytes()),
            commit_sequence: r.commit_sequence,
        }
    }
}
impl ReceiptData {
    /// Parses an observation receipt; clients still check its exact target.
    pub fn native(&self) -> Result<Receipt, Error> {
        Ok(Receipt {
            cell: CellId::from_bytes(unhex(&self.cell)?),
            incarnation: IncarnationId::from_bytes(unhex(&self.incarnation)?),
            commit_sequence: self.commit_sequence,
        })
    }
}
/// Opaque native KV version serialized as 56 canonical hexadecimal characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version(pub(crate) [u8; 28]);
impl Serialize for Version {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex(&self.0))
    }
}
impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self(
            unhex(&String::deserialize(d)?).map_err(serde::de::Error::custom)?,
        ))
    }
}
/// The two application preference names; arbitrary KV keys are not public.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreferenceKey {
    /// Theme, light or dark.
    Theme,
    /// Locale, en or fr.
    Locale,
}
impl PreferenceKey {
    pub(crate) fn text(self) -> &'static str {
        match self {
            Self::Theme => "theme",
            Self::Locale => "locale",
        }
    }
}
/// Frozen conditional preference edit, available only to tenant admins.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreferenceChange {
    /// Application preference name.
    pub key: PreferenceKey,
    /// Exact observed version; null requires absence.
    pub expected: Option<Version>,
    /// New supported value.
    pub value: String,
}
impl PreferenceChange {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !match self.key {
            PreferenceKey::Theme => matches!(self.value.as_str(), "light" | "dark"),
            PreferenceKey::Locale => matches!(self.value.as_str(), "en" | "fr"),
        } {
            return Err(Error::Invalid("unsupported preference value"));
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreferenceValue {
    pub value: String,
    pub updated_by: String,
}
/// One live tenant preference and its native version.
#[derive(Debug, Serialize)]
pub struct Preference {
    /// Application key.
    pub key: PreferenceKey,
    /// Supported value.
    pub value: String,
    /// Verified subject that last edited it.
    pub updated_by: String,
    /// Version for the next conditional edit.
    pub version: Version,
}
