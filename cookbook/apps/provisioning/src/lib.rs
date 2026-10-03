//! Durable simulated resource provisioning, polling, cancellation, and retryable cleanup.
mod activity;
mod application;
mod definition;
mod directory;
mod flows;
mod model;
mod provider;
mod service;
mod sql;
mod wire;
pub use activity::{observe_provider, validate_provider_token};
pub use application::{
    DIRECTORY, Directory, FLOWS, Flows, PROVIDER, Provider, ProvisioningApplication, compile,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution, primitives::workflow::WorkflowStatus,
};
pub use directory::{ChangeResource, ProjectResource};
pub use flows::{DeleteResource, ReconcileResource, ReplyResource, StartResource};
pub use model::{
    Advance, Advanced, Call, Change, DeliveryOutcome, Id, MAX_ACTIVITIES, MAX_MESSAGES,
    MAX_RECONCILIATIONS, MAX_RESOURCES, MAX_STAGE_ATTEMPTS, Observation, Outcome, Page, Phase,
    Projection, ProviderAction, ProviderDue, ProviderPhase, ProviderResource, ProviderWork,
    Reconcile, Reply, ReplyValue, Resource, ResourcePage, ResourceStatus, Spec, Stage, State, Work,
    target, validate_endpoint,
};
pub use provider::{AdvanceProvider, ApplyProvider};
pub use service::{ServiceError, open, spawn_provider_lifecycle, spawn_workers};
/// Retained native invocation, provider, codec, HTTP, and task source errors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Typed authenticated application capability for the three declared domains.
#[derive(Clone)]
pub struct ProvisioningClient {
    handle: ApplicationHandle<ProvisioningApplication>,
}
impl ProvisioningClient {
    /// Binds the embedding's authenticated tenant and application handle.
    pub fn new(handle: ApplicationHandle<ProvisioningApplication>) -> Self {
        Self { handle }
    }
    /// Resolves declared fixed-shard scope; receipts remain local to this exact Cell.
    pub fn target(
        &self,
        namespace: cellule_runtime::NamespaceId,
    ) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(namespace, b"fixed-provisioning-shard")
    }
    /// Prepares resource request or cleanup intent; retain identity before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<PreparedCommand<ChangeResource>, InvocationError<Outcome>> {
        if let Change::Request(spec) = &change {
            spec.validate().map_err(InvocationError::NotStarted)?;
        }
        self.handle
            .prepare_command::<ChangeResource>(
                &self
                    .target(DIRECTORY)
                    .map_err(InvocationError::NotStarted)?,
                identity,
                change,
            )
            .await
    }
    /// Prepares an explicit operator retry of the same permanent provider operations.
    pub async fn prepare_reconcile(
        &self,
        identity: MutationIdentity,
        input: Reconcile,
    ) -> Result<PreparedCommand<ReconcileResource>, InvocationError<DeliveryOutcome>> {
        self.handle
            .prepare_command::<ReconcileResource>(
                &self.target(FLOWS).map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Prepares one idempotent create/delete inside the independently owned provider.
    pub async fn prepare_provider(
        &self,
        identity: MutationIdentity,
        input: ProviderWork,
    ) -> Result<PreparedCommand<ApplyProvider>, InvocationError<DeliveryOutcome>> {
        input.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ApplyProvider>(
                &self.target(PROVIDER).map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Prepares a provider-owned bounded advancement; no timestamp is accepted from callers.
    pub async fn prepare_advance(
        &self,
        identity: MutationIdentity,
        input: Advance,
    ) -> Result<PreparedCommand<AdvanceProvider>, InvocationError<Advanced>> {
        if !(1..=16).contains(&input.limit) {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid provider advancement bound",
            )));
        }
        self.handle
            .prepare_command::<AdvanceProvider>(
                &self.target(PROVIDER).map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Resolves original native outcome evidence without dispatch.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        let targets = [DIRECTORY, FLOWS, PROVIDER]
            .into_iter()
            .map(|ns| self.target(ns))
            .collect::<cellule_runtime::Result<Vec<_>>>()
            .map_err(InvocationError::NotStarted)?;
        if !targets.contains(evidence.target()) {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign provisioning mutation evidence",
            )));
        }
        self.handle.resolve(evidence).await
    }
    /// Reads one directory projection at an optional directory receipt.
    pub async fn resource(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Resource>>, BoxError> {
        Ok(self
            .handle
            .query::<directory::GetResource>(&self.target(DIRECTORY)?, minimum, id)
            .await?)
    }
    /// Lists bounded current projections; pages do not establish a cross-command snapshot.
    pub async fn resources(
        &self,
        page: Page,
        minimum: Option<Receipt>,
    ) -> Result<Observed<ResourcePage>, BoxError> {
        page.validate()?;
        Ok(self
            .handle
            .query::<directory::ListResources>(&self.target(DIRECTORY)?, minimum, page)
            .await?)
    }
    /// Reads the independent provider's current durable virtual volume.
    pub async fn provider(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProviderResource>>, BoxError> {
        Ok(self
            .handle
            .query::<provider::GetProviderResource>(&self.target(PROVIDER)?, minimum, id)
            .await?)
    }
    /// Reads the earliest provider-local deadline; only advancement can change its lifecycle.
    pub async fn next_provider_due(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<ProviderDue>, BoxError> {
        Ok(self
            .handle
            .query::<provider::NextProviderDue>(&self.target(PROVIDER)?, minimum, ())
            .await?)
    }
    /// Reads native lifecycle progress separately from directory and provider visibility.
    pub async fn workflow(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<FlowView>>, BoxError> {
        let observed = self
            .handle
            .workflow::<Flows>()?
            .state(id.bytes().to_vec(), minimum)
            .await?;
        let output = observed
            .output
            .map(|run| -> Result<FlowView, BoxError> {
                let state: State = model::decode(&run.state)?;
                state.validate()?;
                if state.spec.id != id || state.run_id != run.run_id {
                    return Err("native resource lifetime binding differs".into());
                }
                Ok(FlowView {
                    run_id: run.run_id,
                    status: match run.status {
                        WorkflowStatus::Running => "running",
                        WorkflowStatus::Paused => "paused",
                        WorkflowStatus::Completed => "completed",
                        WorkflowStatus::Failed => "failed",
                        WorkflowStatus::Cancelled => "cancelled",
                    }
                    .into(),
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output,
        })
    }
}
/// Native resource lifetime, retained while active and completed only after cleanup proof.
#[derive(Clone, Debug, serde::Serialize)]
pub struct FlowView {
    /// Exact native Workflow lifetime.
    pub run_id: [u8; 16],
    /// Native status; an active resource deliberately has a running Workflow.
    pub status: String,
    /// Causally validated application state.
    pub state: State,
}
