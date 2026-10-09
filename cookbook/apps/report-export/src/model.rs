use cellule_runtime::{Error, MutationIdentity, identity::RequestId};
use serde::{Deserialize, Serialize};
/// Maximum rows in one draft or sealed dataset.
pub const MAX_ROWS: u32 = 512;
/// Fixed page size; pagination and Workflow bounds share this contract.
pub const PAGE_ROWS: u32 = 32;
/// Maximum retained immutable dataset versions.
pub const MAX_VERSIONS: u32 = 16;
/// Maximum encoded CSV page, without its header.
pub const MAX_CHUNK: usize = 16 << 10;
/// Maximum complete CSV file, in one native Blob part.
pub const MAX_BYTES: usize = 256 << 10;
/// Canonical synthetic regional count row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    /// Stable application row ID; gaps are allowed, duplicate IDs are not.
    pub id: u32,
    /// Region label, at most 128 UTF-8 bytes, beginning with an alphabetic character.
    pub label: String,
    /// Synthetic unit count, at most one billion.
    pub units: u64,
}
impl Row {
    /// Rejects invalid data before command dispatch or CSV acceptance.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if !(1..=MAX_ROWS).contains(&self.id)
            || self.label.len() > 128
            || !self.label.chars().next().is_some_and(char::is_alphabetic)
            || self
                .label
                .chars()
                .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r'))
            || self.units > 1_000_000_000
        {
            return Err(Error::Command("invalid regional count row"));
        }
        Ok(())
    }
}
/// Caller-selected nonzero immutable version identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version(pub [u8; 16]);
impl Version {
    /// Validates a reusable version identity.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.0 == [0; 16] {
            return Err(Error::Identity("zero dataset version"));
        }
        Ok(())
    }
}
/// Immutable sealed dataset description, captured inside one SQL command transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Permanent named version.
    pub version: Version,
    /// Draft revision captured by sealing.
    pub revision: u64,
    /// Exact number of sorted rows in this version.
    pub rows: u32,
    /// Canonical digest of all ordered row IDs, labels, and units.
    pub digest: [u8; 32],
}
impl Snapshot {
    /// Checks sealed metadata limits before binding it into an export.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.version.validate()?;
        if self.revision > i64::MAX as u64 || self.rows > MAX_ROWS {
            return Err(Error::Command("invalid sealed dataset bounds"));
        }
        Ok(())
    }
}
/// Conditional draft changes and atomic immutable sealing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "operation", rename_all = "snake_case")]
pub enum Change {
    /// Insert or replace one draft row when its global revision matches.
    Put {
        /// Exact draft revision observed by the caller.
        expected_revision: u64,
        /// Bounded domain row.
        row: Row,
    },
    /// Remove one draft row when its global revision matches.
    Delete {
        /// Exact draft revision observed by the caller.
        expected_revision: u64,
        /// Domain row to remove.
        id: u32,
    },
    /// Replace the complete bounded draft in one conditional transaction.
    Replace {
        /// Exact global draft revision.
        expected_revision: u64,
        /// Canonically ordered unique domain rows, at most 512.
        rows: Vec<Row>,
    },
    /// Atomically freeze all current rows into an immutable named version.
    Seal {
        /// Exact revision to freeze.
        expected_revision: u64,
        /// Permanent version identity.
        version: Version,
    },
}
impl Change {
    /// Checks stable identity and payload limits without contacting storage.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        let expected = match self {
            Self::Put {
                expected_revision,
                row,
            } => {
                row.validate()?;
                *expected_revision
            }
            Self::Delete {
                expected_revision,
                id,
            } => {
                if !(1..=MAX_ROWS).contains(id) {
                    return Err(Error::Command("invalid row ID"));
                }
                *expected_revision
            }
            Self::Replace {
                expected_revision,
                rows,
            } => {
                crate::dataset_digest(rows)?;
                *expected_revision
            }
            Self::Seal {
                expected_revision,
                version,
            } => {
                version.validate()?;
                *expected_revision
            }
        };
        if expected >= i64::MAX as u64 {
            return Err(Error::Command("draft revision exhausted or invalid"));
        }
        Ok(())
    }
}
/// Durable domain outcomes, including rejections.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOutcome {
    /// A draft mutation advanced the revision once.
    Applied(u64),
    /// A version is sealed, including replay under a fresh command identity.
    Sealed(Snapshot),
    /// The expected draft revision or permanent version binding differs.
    Conflict,
    /// The requested draft row is absent.
    Missing,
    /// Permanent version capacity is full.
    Capacity,
}
/// Coherent bounded draft and version counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetInfo {
    /// Current global draft revision.
    pub revision: u64,
    /// Current draft row count.
    pub rows: u32,
    /// Permanent sealed version count.
    pub versions: u32,
}
/// Native paginated query input pinned to a full sealed description.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageRequest {
    /// Full expected immutable version.
    pub snapshot: Snapshot,
    /// Exclusive previous row ID; zero starts the traversal.
    pub after: u32,
}
/// One coherent ordered page of at most 32 sealed rows.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Page {
    /// Verified version description.
    pub snapshot: Snapshot,
    /// Exclusive input cursor.
    pub after: u32,
    /// Canonically ordered rows.
    pub rows: Vec<Row>,
    /// Whether another page exists.
    pub more: bool,
}
/// Native verified immutable Blob manifest link.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Deterministic page or report key.
    pub key: [u8; 32],
    /// Digest of the complete encoded CSV bytes.
    pub digest: [u8; 32],
    /// Native manifest ETag.
    pub etag: [u8; 32],
    /// Encoded byte count.
    pub bytes: u32,
}
impl Artifact {
    /// Checks the common one-part Blob bound.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.bytes == 0 || self.bytes as usize > MAX_BYTES {
            return Err(Error::Command("invalid CSV artifact size"));
        }
        Ok(())
    }
}
/// Frozen export request. Credentials stay in the embedding's environment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Nonzero permanent export run identity.
    pub id: [u8; 16],
    /// Exact SQL dataset Cell identity in the authorized application and tenant.
    pub source_cell: [u8; 32],
    /// Immutable sealed version to export.
    pub snapshot: Snapshot,
    /// Absolute one-hour Activity deadline.
    pub deadline_ms: i64,
    /// Frozen canonical numeric loopback adapter URL.
    pub endpoint: String,
}
impl Request {
    /// Validates scope, bounds, and the fixed local adapter protocol.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.snapshot.validate()?;
        if self.id == [0; 16] || self.source_cell == [0; 32] || self.deadline_ms <= 0 {
            return Err(Error::Identity(
                "invalid export request identity or deadline",
            ));
        }
        let url = url::Url::parse(&self.endpoint).map_err(|source| Error::PeerTransport {
            context: "parse export adapter URL",
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
                "export adapter must use canonical numeric loopback URL",
            ));
        }
        Ok(())
    }
    /// Stable report identity independent of native run, lease, and retry attempt.
    pub fn output_key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.report.csv-1.4.0.v1\0");
        hash.update(&self.source_cell);
        hash.update(&self.snapshot.version.0);
        hash.update(&self.snapshot.digest);
        *hash.finalize().as_bytes()
    }
    /// Stable page identity, keyed by the exclusive sealed row cursor.
    pub fn page_key(&self, after: u32) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.report.page.v1\0");
        hash.update(&self.output_key());
        hash.update(&after.to_be_bytes());
        *hash.finalize().as_bytes()
    }
}
/// One verified page publication and its ordered progress boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    /// Exact immutable CSV page manifest.
    pub artifact: Artifact,
    /// Exclusive input cursor.
    pub after: u32,
    /// Final row ID in this page.
    pub last: u32,
    /// Number of complete CSV records.
    pub rows: u32,
    /// Whether the sealed version has another page.
    pub more: bool,
}
impl Chunk {
    /// Checks page key, forward progress, and resource limits.
    pub fn validate(&self, request: &Request) -> cellule_runtime::Result<()> {
        self.artifact.validate()?;
        if self.artifact.key != request.page_key(self.after)
            || self.artifact.bytes as usize > MAX_CHUNK
            || self.after >= self.last
            || self.last > MAX_ROWS
            || self.rows == 0
            || self.rows > PAGE_ROWS
            || self.last - self.after < self.rows
            || self.more && self.rows != PAGE_ROWS
        {
            return Err(Error::Command("invalid export page progress"));
        }
        Ok(())
    }
}
/// Final verified downloadable report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    /// Durable full CSV manifest.
    pub artifact: Artifact,
    /// Exact reconstructed row count.
    pub rows: u32,
    /// Digest reconstructed from canonical parsed rows.
    pub dataset_digest: [u8; 32],
}
impl Report {
    /// Verifies the completion against the frozen sealed dataset.
    pub fn validate(&self, request: &Request) -> cellule_runtime::Result<()> {
        self.artifact.validate()?;
        if self.artifact.key != request.output_key()
            || self.rows != request.snapshot.rows
            || self.dataset_digest != request.snapshot.digest
        {
            return Err(Error::Command(
                "report completion differs from sealed input",
            ));
        }
        Ok(())
    }
}
/// The next external Activity's frozen operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "operation", rename_all = "snake_case")]
pub enum Work {
    /// Export one sealed page and publish its immutable Blob.
    Page {
        /// Full pinned export request.
        request: Request,
        /// Exclusive SQL keyset cursor.
        after: u32,
    },
    /// Reconstruct and verify all page records, then publish the complete CSV.
    Finalize {
        /// Full pinned export request.
        request: Request,
        /// Ordered verified page manifests recorded by the Workflow.
        chunks: Vec<Chunk>,
    },
}
impl Work {
    /// Returns the operation's fixed request.
    pub fn request(&self) -> &Request {
        match self {
            Self::Page { request, .. } | Self::Finalize { request, .. } => request,
        }
    }
}
/// Typed native Activity completion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Completion {
    /// A page has been durably published and verified.
    Page(Chunk),
    /// The complete CSV has passed row and digest verification.
    Report(Report),
}
/// Native Workflow progress over a sealed dataset.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    /// Permanent source and encoding binding.
    pub request: Request,
    /// Ordered immutable page links, at most 16.
    pub chunks: Vec<Chunk>,
    /// Number of page rows already recorded by durable transitions.
    pub rows: u32,
    /// Exclusive cursor of the final recorded page.
    pub cursor: u32,
    /// Whether the expected Activity is finalization.
    pub finalizing: bool,
    /// Exact native action expected by the next transition.
    pub action: Option<[u8; 16]>,
    /// Complete verified output, if native completion has been recorded.
    pub report: Option<Report>,
    /// Bounded error or uncertainty after failure or deadline expiry.
    pub failure: Option<String>,
}
impl State {
    /// Verifies ordered progress and terminal invariants after recovery.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.request.validate()?;
        crate::encoding::validate_chunks(&self.request, &self.chunks, false)?;
        let count = self.chunks.iter().map(|chunk| chunk.rows).sum::<u32>();
        if count != self.rows
            || self.chunks.last().map_or(0, |chunk| chunk.last) != self.cursor
            || self.rows > self.request.snapshot.rows
            || self.finalizing && self.rows != self.request.snapshot.rows
            || self.report.is_some() && self.failure.is_some()
            || self.action.is_some() && (self.report.is_some() || self.failure.is_some())
            || self.failure.as_ref().is_some_and(|text| text.len() > 512)
        {
            return Err(Error::Command("invalid stored export progress"));
        }
        if let Some(report) = &self.report {
            report.validate(&self.request)?;
            if !self.finalizing {
                return Err(Error::Command("report recorded before finalization"));
            }
        }
        Ok(())
    }
}
/// Retained original native mutation identity.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Stable request ID.
    pub request: [u8; 16],
    /// Original issue time.
    pub issued_at_ms: i64,
    /// Original five-minute validity boundary.
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
    /// Validates evidence without interpreting expiry as an absent outcome.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.request == [0; 16]
            || self
                .expires_at_ms
                .checked_sub(self.issued_at_ms)
                .is_none_or(|window| !(1..=300_000).contains(&window))
        {
            return Err(Error::Identity("invalid retained export mutation identity"));
        }
        Ok(())
    }
    /// Reconstructs the original identity without generating a new request.
    pub fn native(self) -> MutationIdentity {
        MutationIdentity {
            request_id: RequestId::from_bytes(self.request),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }
}
/// Durable export run admission outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartOutcome {
    /// A new native run was created.
    Started([u8; 16]),
    /// The permanent ID already binds this exact request.
    AlreadyBound,
    /// Permanent input identity conflict.
    Conflict,
    /// Permanent run-binding capacity is full.
    Capacity,
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|source| Error::PeerTransport {
        context: "encode export state",
        source: Box::new(source),
    })?;
    if bytes.len() > 32768 {
        return Err(Error::Command("export state exceeds 32 KiB"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 32768 {
        return Err(Error::Command("export state exceeds 32 KiB"));
    }
    serde_json::from_slice(bytes).map_err(|source| Error::PeerTransport {
        context: "decode export state",
        source: Box::new(source),
    })
}
