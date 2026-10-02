//! Immutable file parts with resumable staging and conditional manifest publication.
//! Domain libraries own keys and limits; applications own artifact scope and lifecycle.

mod application;
mod model;
mod plan;

pub use application::{FileVault, Files, compile};
pub use model::{Etag, FileKey, Metadata, Publication, Range, Write};
pub use plan::{PlanError, RetainedUpload};

use cellule_app::ApplicationHandle;
use cellule_runtime::{
    BlobNamespace, CellTarget, Committed, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::blob::{
        BlobCommand, BlobCondition, BlobMetadata, BlobMutation, BlobMutationOutcome, BlobQuery,
        BlobQueryResult,
    },
};

/// Application bound on one immutable file part or range.
pub const PART_BYTES: usize = 256 << 10;
/// Application bound on the number of parts per file (8 MiB total).
pub const MAX_PARTS: u32 = 32;

/// Read failures retain framework, provider, integrity, or decoding causes.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Framework invocation failure.
    #[error(transparent)]
    Invocation(#[from] InvocationError<BlobQueryResult>),
    /// Invalid application arguments or stored metadata.
    #[error(transparent)]
    Domain(#[from] cellule_runtime::Error),
}

/// One authorized file capability and its stable shard target.
#[derive(Clone)]
pub struct FileClient {
    handle: ApplicationHandle<FileVault>,
    blobs: BlobNamespace<Files>,
    target: CellTarget,
    key: FileKey,
}
impl FileClient {
    /// Binds a file after the embedding application installs its private artifact store.
    pub fn new(
        handle: ApplicationHandle<FileVault>,
        key: FileKey,
    ) -> cellule_runtime::Result<Self> {
        Ok(Self {
            target: handle.target_for_scope(application::NAMESPACE, key.as_bytes())?,
            blobs: handle.blob::<Files>()?,
            handle,
            key,
        })
    }
    /// Returns the resolved file shard for application-owned provisioning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Stages bounded part bytes and retains exact evidence before Cell dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        write: Write,
    ) -> Result<
        cellule_runtime::PreparedCommand<BlobCommand<Files>>,
        InvocationError<BlobMutationOutcome>,
    > {
        let mutation = self.mutation(write).map_err(InvocationError::NotStarted)?;
        self.blobs.prepare_mutation(identity, mutation).await
    }
    /// Applies a phase through the same staging and durable publication path.
    pub async fn write(
        &self,
        identity: MutationIdentity,
        write: Write,
    ) -> Result<Committed<BlobMutationOutcome>, InvocationError<BlobMutationOutcome>> {
        self.prepare(identity, write).await?.execute().await
    }
    /// Resolves the exact prepared phase after cancellation or an uncertain reply.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<cellule_runtime::Resolution, InvocationError<Vec<u8>>> {
        if evidence.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign file shard evidence"),
            ));
        }
        self.handle.resolve(evidence).await
    }
    /// Returns only published metadata; staged uploads remain invisible.
    pub async fn head(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Metadata>>, ReadError> {
        let observed = self
            .blobs
            .query(
                BlobQuery::Head {
                    key: self.key.as_bytes().to_vec(),
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Head(metadata) = observed.output else {
            return Err(cellule_runtime::Error::Command("unexpected file metadata result").into());
        };
        Ok(Observed {
            receipt: observed.receipt,
            output: metadata.map(decode).transpose()?,
        })
    }
    /// Reads at most 256 KiB with framework integrity verification on every required part.
    pub async fn read(
        &self,
        offset: u64,
        limit: u32,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Range>>, ReadError> {
        if limit == 0 || limit as usize > PART_BYTES {
            return Err(cellule_runtime::Error::Command(
                "file range limit must be 1 through 256 KiB",
            )
            .into());
        }
        let observed = self
            .blobs
            .query(
                BlobQuery::Read {
                    key: self.key.as_bytes().to_vec(),
                    offset,
                    limit,
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Read(range) = observed.output else {
            return Err(cellule_runtime::Error::Command("unexpected file range result").into());
        };
        Ok(Observed {
            receipt: observed.receipt,
            output: range
                .map(|range| {
                    Ok::<_, cellule_runtime::Error>(Range {
                        metadata: decode(range.metadata)?,
                        offset: range.offset,
                        bytes: range.bytes,
                    })
                })
                .transpose()?,
        })
    }
    fn mutation(&self, write: Write) -> cellule_runtime::Result<BlobMutation> {
        let key = self.key.as_bytes().to_vec();
        let mutation = match write {
            Write::Begin {
                upload,
                publication,
                expires_at_ms,
            } => BlobMutation::Begin {
                key,
                upload_id: upload,
                condition: match publication {
                    Publication::Missing => BlobCondition::Missing,
                    Publication::Match(etag) => BlobCondition::Etag(etag.0),
                },
                content_type: Some("application/octet-stream".into()),
                metadata: Vec::new(),
                expires_at_ms,
            },
            Write::Part {
                upload,
                number,
                bytes,
            } => {
                if number == 0 || number > MAX_PARTS || bytes.is_empty() || bytes.len() > PART_BYTES
                {
                    return Err(cellule_runtime::Error::Command(
                        "file part must be nonempty, at most 256 KiB, and numbered 1 through 32",
                    ));
                }
                BlobMutation::PutPart {
                    key,
                    upload_id: upload,
                    part_number: number,
                    payload: bytes,
                }
            }
            Write::Complete { upload, parts } => {
                if parts == 0 || parts > MAX_PARTS {
                    return Err(cellule_runtime::Error::Command(
                        "file requires 1 through 32 parts",
                    ));
                }
                BlobMutation::Complete {
                    key,
                    upload_id: upload,
                    part_count: parts,
                }
            }
            Write::Abort { upload } => BlobMutation::Abort {
                key,
                upload_id: upload,
            },
            Write::Delete { etag } => BlobMutation::Delete {
                key,
                condition: BlobCondition::Etag(etag.0),
            },
        };
        Ok(mutation)
    }
}
fn decode(metadata: BlobMetadata) -> cellule_runtime::Result<Metadata> {
    let key = std::str::from_utf8(&metadata.key)?.to_owned();
    FileKey::new(key.clone())?;
    if metadata.size == 0
        || metadata.part_count == 0
        || metadata.size > PART_BYTES as u64 * u64::from(MAX_PARTS)
        || metadata.part_count > MAX_PARTS
    {
        return Err(cellule_runtime::Error::Command(
            "stored file exceeds the vault application limits",
        ));
    }
    Ok(Metadata {
        key,
        etag: Etag(metadata.etag),
        size: metadata.size,
        parts: metadata.part_count,
    })
}
