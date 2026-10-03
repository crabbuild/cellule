//! Sealed SQL versions, bounded CSV page Activities, and verified immutable report publication.
mod activity;
mod application;
mod artifacts;
mod commands;
mod dataset;
mod definition;
mod encoding;
mod engine;
mod model;
mod service;
mod sql;
mod wire;
pub use activity::adapter_token;
pub use application::{DATA, Dataset, ExportApplication, FILES, Files, RUNS, Runs, compile};
pub use artifacts::{Artifacts, Upload};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, InvocationError, MutationIdentity, Observed, PreparedCommand, Receipt, Resolution,
    primitives::workflow::WorkflowStatus,
};
pub use commands::Start;
pub use dataset::ChangeDataset;
pub use encoding::{dataset_digest, decode_page, encode_page, encode_report, reconstruct};
pub use engine::{ExportEngine, ExportError};
pub use model::{
    Artifact, Change, Chunk, Completion, DataOutcome, DatasetInfo, Identity, MAX_BYTES, MAX_CHUNK,
    MAX_ROWS, MAX_VERSIONS, PAGE_ROWS, Page, PageRequest, Report, Request, Row, Snapshot,
    StartOutcome, State, Version, Work,
};
pub use service::{open, spawn_exports};
/// Retains native invocation, provider, CSV decoding, and task error sources.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Authorized relational dataset capability, with conditional writes and immutable reads.
#[derive(Clone)]
pub struct DataClient {
    handle: ApplicationHandle<ExportApplication>,
    target: CellTarget,
}
impl DataClient {
    /// Binds the embedding-selected tenant and application to the one declared dataset Cell.
    pub fn new(handle: ApplicationHandle<ExportApplication>) -> cellule_runtime::Result<Self> {
        Ok(Self {
            target: handle.target_for_scope(DATA, b"fixed-export-dataset")?,
            handle,
        })
    }
    /// Stable source target for application-owned opening and request pinning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Prepares an exact conditional draft mutation or version-sealing request.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<PreparedCommand<ChangeDataset>, InvocationError<DataOutcome>> {
        change.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ChangeDataset>(&self.target, identity, change)
            .await
    }
    /// Reads coherent live draft counters.
    pub async fn info(&self, minimum: Option<Receipt>) -> Result<Observed<DatasetInfo>, BoxError> {
        Ok(self
            .handle
            .query::<dataset::ReadDataset>(&self.target, minimum, ())
            .await?)
    }
    /// Reads one immutable sealed description.
    pub async fn snapshot(
        &self,
        version: Version,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Snapshot>>, BoxError> {
        version.validate()?;
        Ok(self
            .handle
            .query::<dataset::ReadSnapshot>(&self.target, minimum, version)
            .await?)
    }
    /// Reads one ordered bounded page pinned to exact version metadata.
    pub async fn page(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Page>, BoxError> {
        page.snapshot.validate()?;
        if page.after > MAX_ROWS {
            return Err("invalid export cursor".into());
        }
        Ok(self
            .handle
            .query::<dataset::ReadPage>(&self.target, minimum, page)
            .await?)
    }
    /// Resolves only original dataset mutation evidence.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if evidence.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign export dataset evidence"),
            ));
        }
        self.handle.resolve(evidence).await
    }
}
/// Native run identity and durable export progress.
#[derive(Clone, Debug, serde::Serialize)]
pub struct View {
    /// Exact native run identity.
    pub run_id: [u8; 16],
    /// Native execution status.
    pub status: String,
    /// Pinned definition identity.
    pub definition: String,
    /// Verified bounded export progress and manifest links.
    pub state: State,
}
/// Typed export request, inspection, and retained outcome resolution.
#[derive(Clone)]
pub struct ExportClient {
    handle: ApplicationHandle<ExportApplication>,
}
impl ExportClient {
    /// Binds the embedding-authorized application and tenant.
    pub fn new(handle: ApplicationHandle<ExportApplication>) -> Self {
        Self { handle }
    }
    /// Prepares a permanently bound export run against a sealed version.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        request: Request,
    ) -> Result<PreparedCommand<Start>, InvocationError<StartOutcome>> {
        request.validate().map_err(InvocationError::NotStarted)?;
        let target = self
            .handle
            .target_for_scope(RUNS, &request.id)
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<Start>(&target, identity, request)
            .await
    }
    /// Reads current native run and verifies all recorded ordered page boundaries.
    pub async fn get(
        &self,
        id: [u8; 16],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<View>>, BoxError> {
        let observed = self
            .handle
            .workflow::<Runs>()?
            .state(id.to_vec(), minimum)
            .await?;
        let output = observed
            .output
            .map(|run| {
                let state: State = model::decode(&run.state)?;
                state.validate()?;
                if state.request.id != id {
                    return Err(cellule_runtime::Error::Identity("export run ID differs"));
                }
                let status = match run.status {
                    WorkflowStatus::Running => "running",
                    WorkflowStatus::Completed => "completed",
                    WorkflowStatus::Failed => "failed",
                    WorkflowStatus::Cancelled => "cancelled",
                    WorkflowStatus::Paused => "paused",
                };
                Ok(View {
                    run_id: run.run_id,
                    status: status.into(),
                    definition: format!("{:?}", run.definition_digest),
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output,
        })
    }
    /// Resolves only original Workflow request evidence, never a Blob or dataset mutation.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        let target = self
            .handle
            .target_for_scope(RUNS, b"fixed-export-run")
            .map_err(InvocationError::NotStarted)?;
        if evidence.target() != &target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign export Workflow evidence"),
            ));
        }
        self.handle.resolve(evidence).await
    }
}
