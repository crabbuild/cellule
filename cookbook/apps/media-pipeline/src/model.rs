use cellule_runtime::{Error, MutationIdentity, identity::RequestId};
use serde::{Deserialize, Serialize};
/// Maximum encoded source or result size, in one native Blob part.
pub const MAX_BYTES: usize = 256 << 10;
/// Maximum decoded source width and height.
pub const MAX_DIMENSION: u32 = 1024;
/// Maximum thumbnail bounding box. RGBA output remains below the encoded byte bound.
pub const MAX_SIDE: u32 = 128;
/// Immutable manifest link. Digests cover the full verified bytes; ETags identify manifests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Canonical content or transformation key in its declared namespace.
    pub key: [u8; 32],
    /// BLAKE3 of the encoded PNG.
    pub digest: [u8; 32],
    /// Native manifest ETag.
    pub etag: [u8; 32],
    /// Encoded byte count.
    pub bytes: u64,
    /// Decoded PNG width.
    pub width: u32,
    /// Decoded PNG height.
    pub height: u32,
}
impl Artifact {
    /// Validates the application's resource bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.bytes == 0
            || self.bytes > MAX_BYTES as u64
            || self.width == 0
            || self.height == 0
            || self.width > MAX_DIMENSION
            || self.height > MAX_DIMENSION
        {
            return Err(Error::Command("artifact exceeds PNG bounds"));
        }
        Ok(())
    }
}
/// Frozen run input. Credentials remain in the embedding's environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Nonzero caller-selected workflow identity.
    pub id: [u8; 16],
    /// Source manifest in the immutable source namespace.
    pub source: Artifact,
    /// Thumbnail bounding box, preserving aspect ratio.
    pub side: u32,
    /// Absolute Activity deadline.
    pub deadline_ms: i64,
    /// Authenticated numeric loopback adapter URL, frozen for this run.
    pub endpoint: String,
}
impl Request {
    /// Validates identity, source, operation, deadline, and local transport scope.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.source.validate()?;
        if self.id == [0; 16]
            || self.source.key != self.source.digest
            || !(1..=MAX_SIDE).contains(&self.side)
            || self.deadline_ms <= 0
        {
            return Err(Error::Command("invalid thumbnail request"));
        }
        let url = url::Url::parse(&self.endpoint).map_err(|source| Error::PeerTransport {
            context: "parse media adapter URL",
            source: Box::new(source),
        })?;
        if url.scheme() != "http"
            || url.host_str() != Some("127.0.0.1")
            || url.port().is_none()
            || url.path() != "/"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != self.endpoint
        {
            return Err(Error::Command(
                "media adapter must use canonical numeric loopback URL",
            ));
        }
        Ok(())
    }
    /// Stable business output identity independent of Activity lease and Workflow attempt.
    pub fn output_key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.media.thumbnail.png.image-0.25.10.v1\0");
        hash.update(&self.source.digest);
        hash.update(&self.side.to_be_bytes());
        *hash.finalize().as_bytes()
    }
    /// Checks a result before linking it into Workflow state.
    pub fn verify_result(&self, result: &Artifact) -> cellule_runtime::Result<()> {
        result.validate()?;
        let (width, height) = crate::processing::thumbnail_dimensions(
            self.source.width,
            self.source.height,
            self.side,
        )?;
        if result.key != self.output_key() || result.width != width || result.height != height {
            return Err(Error::Command(
                "thumbnail completion differs from frozen transformation",
            ));
        }
        Ok(())
    }
}
/// Durable Workflow state, including any uncertainty after native failure or expiry.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    /// Frozen source and operation.
    pub request: Request,
    /// Native action ID expected by this transition.
    pub action: Option<[u8; 16]>,
    /// Verified published result, absent until completion is recorded.
    pub result: Option<Artifact>,
    /// Bounded failure detail; absence of a result does not prove absence of external publication.
    pub failure: Option<String>,
}
/// Retained command identity for serialized caller evidence.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Original request bytes.
    pub request: [u8; 16],
    /// Original logical issue time.
    pub issued_at_ms: i64,
    /// Original validity boundary.
    pub expires_at_ms: i64,
}
impl From<MutationIdentity> for Identity {
    fn from(value: MutationIdentity) -> Self {
        Self {
            request: *value.request_id.as_bytes(),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}
impl Identity {
    /// Reconstructs the exact identity; expiry never creates new absence evidence.
    pub fn native(self) -> MutationIdentity {
        MutationIdentity {
            request_id: RequestId::from_bytes(self.request),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|source| Error::PeerTransport {
        context: "encode media state",
        source: Box::new(source),
    })?;
    if bytes.len() > 8192 {
        return Err(Error::Command("media state exceeds 8 KiB"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 8192 {
        return Err(Error::Command("media state exceeds 8 KiB"));
    }
    serde_json::from_slice(bytes).map_err(|source| Error::PeerTransport {
        context: "decode media state",
        source: Box::new(source),
    })
}
impl cellule_runtime::codec::WireValue for Artifact {
    fn encode(
        &self,
        e: &mut cellule_runtime::codec::BoundedEncoder,
    ) -> Result<(), cellule_runtime::codec::CodecError> {
        self.key.to_vec().encode(e)?;
        self.digest.to_vec().encode(e)?;
        self.etag.to_vec().encode(e)?;
        self.bytes.encode(e)?;
        self.width.encode(e)?;
        self.height.encode(e)
    }
    fn decode(
        d: &mut cellule_runtime::codec::BoundedDecoder<'_>,
    ) -> Result<Self, cellule_runtime::codec::CodecError> {
        use cellule_runtime::codec::CodecError;
        Ok(Self {
            key: Vec::<u8>::decode(d)?
                .try_into()
                .map_err(|_| CodecError::Invalid("media key length"))?,
            digest: Vec::<u8>::decode(d)?
                .try_into()
                .map_err(|_| CodecError::Invalid("media digest length"))?,
            etag: Vec::<u8>::decode(d)?
                .try_into()
                .map_err(|_| CodecError::Invalid("media ETag length"))?,
            bytes: u64::decode(d)?,
            width: u32::decode(d)?,
            height: u32::decode(d)?,
        })
    }
}
impl cellule_runtime::codec::WireValue for Request {
    fn encode(
        &self,
        e: &mut cellule_runtime::codec::BoundedEncoder,
    ) -> Result<(), cellule_runtime::codec::CodecError> {
        self.id.to_vec().encode(e)?;
        self.source.encode(e)?;
        self.side.encode(e)?;
        self.deadline_ms.encode(e)?;
        self.endpoint.encode(e)
    }
    fn decode(
        d: &mut cellule_runtime::codec::BoundedDecoder<'_>,
    ) -> Result<Self, cellule_runtime::codec::CodecError> {
        use cellule_runtime::codec::CodecError;
        Ok(Self {
            id: Vec::<u8>::decode(d)?
                .try_into()
                .map_err(|_| CodecError::Invalid("media workflow ID length"))?,
            source: Artifact::decode(d)?,
            side: u32::decode(d)?,
            deadline_ms: i64::decode(d)?,
            endpoint: String::decode(d)?,
        })
    }
}
/// Stable business result for run admission, distinct from native execution status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    /// A newly bound native run was created.
    Started([u8; 16]),
    /// The ID already names this exact frozen request.
    AlreadyBound,
    /// The ID permanently names different request bytes.
    Conflict,
    /// The application's 1024 permanent bindings are full.
    Capacity,
}
impl cellule_runtime::codec::WireValue for StartOutcome {
    fn encode(
        &self,
        e: &mut cellule_runtime::codec::BoundedEncoder,
    ) -> Result<(), cellule_runtime::codec::CodecError> {
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
    fn decode(
        d: &mut cellule_runtime::codec::BoundedDecoder<'_>,
    ) -> Result<Self, cellule_runtime::codec::CodecError> {
        use cellule_runtime::codec::CodecError;
        match d.read_u8()? {
            0 => Ok(Self::Started(
                d.read_bytes()?
                    .try_into()
                    .map_err(|_| CodecError::Invalid("media run ID length"))?,
            )),
            1 => Ok(Self::AlreadyBound),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("unknown media start outcome")),
        }
    }
}
