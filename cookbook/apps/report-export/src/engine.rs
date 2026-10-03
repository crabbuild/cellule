use crate::{Artifacts, Chunk, Completion, DataClient, PageRequest, Report, Request, Upload, Work};
/// Export errors preserve native provider/codec failures and explicit version mismatch.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// The named sealed version or authorized source Cell differs from the frozen input.
    #[error("sealed dataset identity, version, or content differs from frozen export")]
    VersionMismatch,
    /// Malformed progress or CSV evidence.
    #[error("invalid export work: {0}")]
    Invalid(&'static str),
    /// Native invocation, provider, decoding, or publication source.
    #[error(transparent)]
    Source(#[from] crate::BoxError),
}
/// Application-owned external export adapter, operating entirely through typed capabilities.
#[derive(Clone)]
pub struct ExportEngine {
    data: DataClient,
    files: Artifacts,
}
impl ExportEngine {
    /// Binds authorized dataset and immutable CSV capabilities; no writer takeover is performed.
    pub fn new(data: DataClient, files: Artifacts) -> Self {
        Self { data, files }
    }
    async fn pin(&self, request: &Request) -> Result<(), ExportError> {
        request
            .validate()
            .map_err(|source| ExportError::Source(source.into()))?;
        if request.source_cell != *self.data.target().cell_id().as_bytes()
            || self
                .data
                .snapshot(request.snapshot.version, None)
                .await?
                .output
                .as_ref()
                != Some(&request.snapshot)
        {
            return Err(ExportError::VersionMismatch);
        }
        Ok(())
    }
    /// Produces one bounded immutable page or a fully verified final CSV.
    /// All computation occurs after SQL queries return, outside a SQLite command transaction.
    pub async fn execute(&self, work: Work) -> Result<Completion, ExportError> {
        self.pin(work.request()).await?;
        match work {
            Work::Page { request, after } => {
                if after > crate::MAX_ROWS {
                    return Err(ExportError::Invalid("page cursor exceeds row ID bound"));
                }
                let page = self
                    .data
                    .page(
                        PageRequest {
                            snapshot: request.snapshot.clone(),
                            after,
                        },
                        None,
                    )
                    .await?
                    .output;
                if page.snapshot != request.snapshot || page.after != after || page.rows.is_empty()
                {
                    return Err(ExportError::Invalid(
                        "page does not advance within sealed version",
                    ));
                }
                let last = page
                    .rows
                    .last()
                    .ok_or(ExportError::Invalid("empty page"))?
                    .id;
                if page.rows.first().is_none_or(|row| row.id <= after) {
                    return Err(ExportError::Invalid("page row cursor regresses"));
                }
                let rows = page.rows.len() as u32;
                let bytes = crate::encode_page(&page.rows)?;
                let artifact = self
                    .files
                    .publish(&Upload::new(request.page_key(after), bytes)?)
                    .await?;
                let chunk = Chunk {
                    artifact,
                    after,
                    last,
                    rows,
                    more: page.more,
                };
                chunk
                    .validate(&request)
                    .map_err(|source| ExportError::Source(source.into()))?;
                Ok(Completion::Page(chunk))
            }
            Work::Finalize { request, chunks } => {
                crate::encoding::validate_chunks(&request, &chunks, true)
                    .map_err(|source| ExportError::Source(source.into()))?;
                let mut verified = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    let (artifact, bytes) = self
                        .files
                        .read(chunk.artifact.key, None)
                        .await?
                        .output
                        .ok_or(ExportError::Invalid("required CSV page missing"))?;
                    if artifact != chunk.artifact {
                        return Err(ExportError::Invalid("CSV page manifest changed"));
                    }
                    verified.push((chunk, bytes));
                }
                let rows = crate::reconstruct(&request, &verified)?;
                let bytes = crate::encode_report(&rows)?;
                let artifact = self
                    .files
                    .publish(&Upload::new(request.output_key(), bytes)?)
                    .await?;
                let report = Report {
                    artifact,
                    rows: rows.len() as u32,
                    dataset_digest: crate::dataset_digest(&rows)
                        .map_err(|source| ExportError::Source(source.into()))?,
                };
                report
                    .validate(&request)
                    .map_err(|source| ExportError::Source(source.into()))?;
                Ok(Completion::Report(report))
            }
        }
    }
}
