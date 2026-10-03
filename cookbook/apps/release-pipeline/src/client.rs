use crate::{
    Acknowledgment, Approval, ApproveRelease, ControlOutcome, FLOWS, Flows, PipelineState, RECORDS,
    Reconcile, ReconcileRelease, ReleaseApplication, ReleaseId, ReleaseRecord, ReleaseSpec,
    ReplyRelease, RollbackRelease, StartRelease, model::decode,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution, primitives::workflow::WorkflowStatus,
};
/// Exact native lifetime and validated immutable domain state, separate from receiver progress.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseView {
    /// Native status; verified active deployments deliberately remain running.
    pub status: String,
    /// Exact pinned definition identity.
    pub definition: String,
    /// Current pure coordination state.
    pub state: PipelineState,
}
/// Typed domain capabilities after application authentication and role policy.
#[derive(Clone)]
pub struct ReleaseClient<const V: u8> {
    handle: ApplicationHandle<ReleaseApplication<V>>,
}
impl<const V: u8> ReleaseClient<V> {
    /// Binds the authenticated tenant and versioned embedding application.
    pub fn new(handle: ApplicationHandle<ReleaseApplication<V>>) -> Self {
        Self { handle }
    }
    /// Selects a declared fixed-shard namespace; receipts are local to its exact Cell.
    pub fn target(
        &self,
        namespace: cellule_runtime::NamespaceId,
    ) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(namespace, b"fixed-release-shard")
    }
    /// Freezes immutable submission evidence; retain before dispatch or asynchronous work.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        spec: ReleaseSpec,
    ) -> Result<PreparedCommand<StartRelease<V>>, InvocationError<ControlOutcome>> {
        spec.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<StartRelease<V>>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                spec,
            )
            .await
    }
    /// Freezes one authorized human decision on the exact original input.
    pub async fn prepare_approval(
        &self,
        identity: MutationIdentity,
        vote: Approval,
    ) -> Result<PreparedCommand<ApproveRelease<V>>, InvocationError<ControlOutcome>> {
        self.handle
            .prepare_command::<ApproveRelease<V>>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                vote,
            )
            .await
    }
    /// Records cancellation or compensation intent; it never directly cancels the native run.
    pub async fn prepare_rollback(
        &self,
        identity: MutationIdentity,
        spec: ReleaseSpec,
    ) -> Result<PreparedCommand<RollbackRelease<V>>, InvocationError<ControlOutcome>> {
        spec.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<RollbackRelease<V>>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                spec,
            )
            .await
    }
    /// Freezes one bounded operator retry without changing target preconditions.
    pub async fn prepare_reconcile(
        &self,
        identity: MutationIdentity,
        input: Reconcile,
    ) -> Result<PreparedCommand<ReconcileRelease<V>>, InvocationError<ControlOutcome>> {
        self.handle
            .prepare_command::<ReconcileRelease<V>>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Exact signed-domain callback capability; network ingress must authorize its causal source.
    pub async fn prepare_reply(
        &self,
        identity: MutationIdentity,
        reply: Acknowledgment,
    ) -> Result<PreparedCommand<ReplyRelease<V>>, InvocationError<ControlOutcome>> {
        reply
            .projection
            .validate()
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ReplyRelease<V>>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                reply,
            )
            .await
    }
    /// Reads current coordination at an optional Workflow-local receipt.
    pub async fn workflow(
        &self,
        release: ReleaseId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ReleaseView>>, crate::BoxError> {
        let value = self
            .handle
            .workflow::<Flows<V>>()?
            .state(release.bytes().to_vec(), minimum)
            .await?;
        let output = value
            .output
            .map(|run| -> Result<ReleaseView, crate::BoxError> {
                let state: PipelineState = decode(&run.state)?;
                state.validate()?;
                if state.spec.release != release
                    || state.run_id != run.run_id
                    || crate::definition::digest(state.version) != run.definition_digest
                {
                    return Err(
                        Error::Identity("native release definition or identity differs").into(),
                    );
                }
                let status = match run.status {
                    WorkflowStatus::Running => "running",
                    WorkflowStatus::Paused => "paused",
                    WorkflowStatus::Completed => "completed",
                    WorkflowStatus::Cancelled => "cancelled",
                    WorkflowStatus::Failed => "failed",
                };
                Ok(ReleaseView {
                    status: status.into(),
                    definition: format!("{:?}", run.definition_digest),
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: value.receipt,
            output,
        })
    }
    /// Reads receiver-local progress; this does not infer delivery from a source receipt.
    pub async fn record(
        &self,
        release: ReleaseId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ReleaseRecord>>, crate::BoxError> {
        Ok(self
            .handle
            .query::<crate::GetReleaseRecord>(&self.target(RECORDS)?, minimum, release)
            .await?)
    }
    /// Resolves frozen Workflow mutation evidence without repeating external work.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if evidence.target() != &self.target(FLOWS).map_err(InvocationError::NotStarted)? {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign release Workflow mutation evidence",
            )));
        }
        self.handle.resolve(evidence).await
    }
}
