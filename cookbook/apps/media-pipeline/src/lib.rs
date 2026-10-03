//! Immutable image artifacts and recoverable local thumbnail processing.
mod activity;
mod application;
mod artifacts;
mod commands;
mod definition;
mod model;
mod processing;
mod service;
pub use activity::{Submission, adapter_token};
pub use application::{MediaApplication, RESULTS, RUNS, Results, Runs, SOURCES, Sources, compile};
pub use artifacts::{Artifacts, Kind, Upload};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    InvocationError, MutationIdentity, Observed, PreparedCommand, Receipt,
    primitives::workflow::WorkflowStatus,
};
pub use commands::Start;
pub use model::{
    Artifact, Identity, MAX_BYTES, MAX_DIMENSION, MAX_SIDE, Request, StartOutcome, State,
};
pub use processing::{ImageError, dimensions, sample_png, thumbnail};
pub use service::{open, spawn_processing};
/// Preserves concrete framework, provider, decoding, and task error sources across adapters.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Current run and business state; a missing link does not prove absence of an external artifact.
#[derive(Clone, Debug, serde::Serialize)]
pub struct View {
    /// Exact current native run.
    pub run_id: [u8; 16],
    /// Native run status.
    pub status: String,
    /// Pinned definition identity.
    pub definition: String,
    /// Bounded business state.
    pub state: State,
}
/// Typed thumbnail commands and reads after the embedding authenticates its caller.
#[derive(Clone)]
pub struct MediaClient {
    handle: ApplicationHandle<MediaApplication>,
}
impl MediaClient {
    /// Binds the embedding-selected application and tenant.
    pub fn new(handle: ApplicationHandle<MediaApplication>) -> Self {
        Self { handle }
    }
    /// Retains exact mutation evidence before starting a permanently bound run.
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
    /// Reads current Workflow state at an optional same-Cell receipt.
    pub async fn get(
        &self,
        id: [u8; 16],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<View>>, BoxError> {
        let result = self
            .handle
            .workflow::<Runs>()?
            .state(id.to_vec(), minimum)
            .await?;
        let output = result
            .output
            .map(|run| {
                let state: State = model::decode(&run.state)?;
                state.request.validate()?;
                if state.request.id != id {
                    return Err(cellule_runtime::Error::Command(
                        "media run identity differs",
                    ));
                }
                if let Some(artifact) = &state.result {
                    state.request.verify_result(artifact)?;
                }
                if state.action.is_some() && (state.result.is_some() || state.failure.is_some())
                    || state.result.is_some() && state.failure.is_some()
                {
                    return Err(cellule_runtime::Error::Command(
                        "invalid media Workflow state",
                    ));
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
            receipt: result.receipt,
            output,
        })
    }
    /// Resolves original prepared command evidence after a lost reply.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<cellule_runtime::Resolution, InvocationError<Vec<u8>>> {
        let expected = self
            .handle
            .target_for_scope(RUNS, b"fixed-media-shard")
            .map_err(InvocationError::NotStarted)?;
        if evidence.target() != &expected {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign media Workflow evidence"),
            ));
        }
        self.handle.resolve(evidence).await
    }
}
