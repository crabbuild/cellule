use serde::{Deserialize, Serialize};

/// Canonical relative file name, used unchanged for stable Blob sharding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileKey(String);
impl FileKey {
    /// Accepts 1–256 lowercase ASCII path bytes; rejects empty, dot, and parent segments.
    pub fn new(value: impl Into<String>) -> cellule_runtime::Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || !value.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'/' | b'-' | b'_' | b'.')
            })
            || value
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(cellule_runtime::Error::Identity(
                "file key must be a canonical relative lowercase path",
            ));
        }
        Ok(Self(value))
    }
    /// Returns canonical object-routing bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Published content token, serialized as 64 lowercase hexadecimal characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Etag(pub(crate) [u8; 32]);
impl From<[u8; 32]> for Etag {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}
impl Serialize for Etag {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(
            &self
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )
    }
}
impl<'de> Deserialize<'de> for Etag {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom(
                "ETag must be 64 lowercase hexadecimal characters",
            ));
        }
        let mut bytes = [0; 32];
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

/// Create-only or conditional replacement policy, checked at completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "etag",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Publication {
    /// Complete only while the file is absent.
    Missing,
    /// Complete only while the previous published ETag still matches.
    Match(Etag),
}

/// One explicit phase of a file upload or deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Write {
    /// Open an upload whose lifetime is checked by the primitive.
    Begin {
        /// Stable client-chosen upload identity.
        upload: [u8; 16],
        /// Publication precondition evaluated again at completion.
        publication: Publication,
        /// Absolute Unix milliseconds after which staging is abandoned.
        expires_at_ms: i64,
    },
    /// Stage one nonempty part, at most 256 KiB.
    Part {
        /// Upload receiving this part.
        upload: [u8; 16],
        /// One-based part number, at most 32.
        number: u32,
        /// Immutable source bytes retained by the caller for resumption.
        bytes: Vec<u8>,
    },
    /// Atomically publish 1–32 contiguous parts.
    Complete {
        /// Upload to publish.
        upload: [u8; 16],
        /// Total contiguous part count.
        parts: u32,
    },
    /// Abandon an upload; staged artifact bytes are not collected here.
    Abort {
        /// Upload to abandon.
        upload: [u8; 16],
    },
    /// Remove only the exact published content token observed by the caller.
    Delete {
        /// Required publication token.
        etag: Etag,
    },
}

/// Published file information, independent of local working files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Metadata {
    /// Canonical file name.
    pub key: String,
    /// Required token for replacement, deletion, and stable downloads.
    pub etag: Etag,
    /// Content size in bytes.
    pub size: u64,
    /// Number of immutable parts.
    pub parts: u32,
}

/// Verified bounded byte range and the publication it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range {
    /// Exact publication metadata.
    pub metadata: Metadata,
    /// Byte offset.
    pub offset: u64,
    /// Verified bytes, bounded by the application's 256 KiB read limit.
    pub bytes: Vec<u8>,
}
