use serde::{Deserialize, Serialize};

use cellule_runtime::Error;

/// Canonical organization identity, stable across processes and storage restores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Organization(String);

impl Organization {
    /// Accepts lowercase ASCII slugs of one through 64 bytes.
    pub fn new(value: impl Into<String>) -> cellule_runtime::Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(Error::Identity(
                "organization must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the canonical routing bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Opaque 28-byte KV version, serialized as exactly 56 lowercase hex characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version(pub(crate) [u8; 28]);

impl Serialize for Version {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value: String = self.0.iter().map(|byte| format!("{byte:02x}")).collect();
        serializer.serialize_str(&value)
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() != 56
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom(
                "version must be 56 lowercase hexadecimal characters",
            ));
        }
        let mut bytes = [0; 28];
        for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let digit = |b: u8| {
                if b.is_ascii_digit() {
                    b - b'0'
                } else {
                    b - b'a' + 10
                }
            };
            bytes[index] = digit(pair[0]) * 16 + digit(pair[1]);
        }
        Ok(Self(bytes))
    }
}

/// Allowed preference values; objects and arbitrary binary content are excluded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Preference {
    /// A feature flag or notification toggle.
    Boolean(bool),
    /// A bounded label, locale, or theme name.
    Text(String),
    /// An integer application preference.
    Integer(i64),
}

impl Preference {
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        if let Self::Text(value) = self
            && (value.is_empty()
                || value.len() > 256
                || value.trim() != value
                || value.chars().any(char::is_control))
        {
            return Err(Error::Command(
                "preference text must be trimmed, nonempty, and at most 256 bytes",
            ));
        }
        Ok(())
    }
}

/// Every edit checks the logical live value before any member of the bundle writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "version",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Expected {
    /// Create only while no live setting exists.
    Absent,
    /// Edit or delete only the exact version previously observed.
    Version(Version),
}

/// One conditional preference update; null value deletes the setting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edit {
    /// Canonical dotted preference name, at most 64 ASCII bytes.
    pub key: String,
    /// Required live-version precondition.
    pub expected: Expected,
    /// New scalar value, or null for deletion.
    pub value: Option<Preference>,
    /// Absolute Unix milliseconds of logical expiry; null is permanent.
    pub expires_at_ms: Option<i64>,
}

/// A preference returned with its opaque version and absolute expiry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Setting {
    /// Canonical preference name.
    pub key: String,
    /// Domain value.
    pub value: Preference,
    /// Required token for future conditional writes.
    pub version: Version,
    /// Logical expiry in Unix milliseconds.
    pub expires_at_ms: Option<i64>,
}

/// One bounded current-read page; concurrent edits can change subsequent pages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Page {
    /// Live settings in bytewise name order.
    pub settings: Vec<Setting>,
    /// Resume after this key; null ends the traversal.
    pub next: Option<String>,
}

pub(crate) fn validate_key(key: &str) -> cellule_runtime::Result<()> {
    if key.is_empty()
        || key.len() > 64
        || !key.as_bytes()[0].is_ascii_lowercase()
        || key.ends_with('.')
        || key.contains("..")
        || !key.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')
        })
    {
        return Err(Error::Command(
            "setting key must be a canonical dotted lowercase name",
        ));
    }
    Ok(())
}
