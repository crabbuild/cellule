//! Pinned release artifacts and independently durable conditional deployment compensation.
mod activity;
mod application;
mod artifacts;
mod client;
mod definition;
mod flow;
mod model;
mod pipeline;
mod records;
mod release_application;
mod service;
mod sql;
mod target;
mod wire;

pub use activity::{ArtifactObject, observe_pipeline, validate_token};
pub use application::{TARGETS, TargetApplication, Targets, compile_target};
pub use artifacts::{ArtifactPhase, ArtifactUpload, ReleaseArtifacts, UploadIdentity};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
};
pub use client::{ReleaseClient, ReleaseView};
pub use flow::{ApproveRelease, ReconcileRelease, ReplyRelease, RollbackRelease, StartRelease};
pub use model::{
    Artifact, Deployment, MAX_ARTIFACT_BYTES, MAX_DEPLOYMENTS, MAX_SOURCE_BYTES, MAX_TARGETS,
    ReleaseId, Selection, TargetAction, TargetName, TargetOutcome, TargetRecord, TargetState,
    TargetWork,
};
pub use pipeline::{
    Acknowledgment, ActivityReport, Approval, ArtifactPublication, ControlOutcome, PipelinePhase,
    PipelineStage, PipelineState, PipelineWork, Projection, Reconcile, ReleaseRecord, ReleaseSpec,
    ReleaseStatus, validate_origin,
};
pub use records::{GetReleaseRecord, ProjectRelease};
pub use release_application::{
    ARTIFACTS, Artifacts, FLOWS, Flows, RECORDS, Records, ReleaseApplication, compile_release,
};
pub use service::{
    ServiceError, open_release, open_release_after_rollout, spawn_record_delivery,
    spawn_release_workers,
};
pub use target::{ApplyTarget, GetTargetArtifact, GetTargetRecord, GetTargetState};

/// Concrete invocation, provider, and query errors preserved across application adapters.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Typed local target operations after the embedding has authorized the caller.
#[derive(Clone)]
pub struct TargetClient {
    handle: ApplicationHandle<TargetApplication>,
}
impl TargetClient {
    /// Binds an embedding-selected tenant and simulator application.
    pub fn new(handle: ApplicationHandle<TargetApplication>) -> Self {
        Self { handle }
    }
    /// Exact declared target transaction domain, independent of the named slot.
    pub fn target(&self) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(TARGETS, b"fixed-release-target-shard")
    }
    /// Freezes native request evidence before an idempotent deployment or compensation.
    /// A committed Conflict is a stored generation refusal; inspect the domain outcome.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        input: TargetWork,
    ) -> Result<PreparedCommand<ApplyTarget>, InvocationError<TargetOutcome>> {
        input.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ApplyTarget>(
                &self.target().map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Reads permanent external operation evidence at an optional same-Cell receipt.
    pub async fn record(
        &self,
        release: ReleaseId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<TargetRecord>>, BoxError> {
        release.validate()?;
        Ok(self
            .handle
            .query::<GetTargetRecord>(&self.target()?, minimum, release)
            .await?)
    }
    /// Reads and verifies exact installed bytes; rolled-back artifacts remain inspectable.
    pub async fn artifact(
        &self,
        release: ReleaseId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Vec<u8>>>, BoxError> {
        release.validate()?;
        Ok(self
            .handle
            .query::<GetTargetArtifact>(&self.target()?, minimum, release)
            .await?)
    }
    /// Reads current target selection and generation; this never publishes an advancement command.
    pub async fn state(
        &self,
        name: TargetName,
        minimum: Option<Receipt>,
    ) -> Result<Observed<TargetState>, BoxError> {
        name.validate()?;
        Ok(self
            .handle
            .query::<GetTargetState>(&self.target()?, minimum, name)
            .await?)
    }
    /// Resolves original publication evidence without replaying external target work.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if evidence.target() != &self.target().map_err(InvocationError::NotStarted)? {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign deployment target evidence",
            )));
        }
        self.handle.resolve(evidence).await
    }
}
