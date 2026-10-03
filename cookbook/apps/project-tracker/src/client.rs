use crate::{
    AttachmentLink, ChangeOutcome, ChangeProject, DASHBOARD, DashboardPage, DashboardPageRequest,
    GetProject, Issue, IssueId, LinkAttachment, ListDashboard, LookupProject, PROJECTS,
    ProjectChange, ProjectKey, ProjectState, ProjectSummary, ProjectTracker, ProjectVersion,
    ProjectionOutcome, Projects, wire,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution, primitives::effects::EffectState,
};
use serde::Serialize;

/// Native source-ledger status; missing or failed evidence is never treated as dashboard completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    /// Latest transactional intent is ready or leased.
    Pending,
    /// The receiver accepted the summary, including harmless stale-state delivery.
    Delivered,
    /// Receiver rejection, exhausted attempts, or expiry requires investigation.
    Failed,
    /// Source ledger evidence is unavailable; this proves no receiver absence.
    Unavailable,
}
/// Coherent source state and its latest asynchronous projection evidence.
#[derive(Clone, Debug, Serialize)]
pub struct ProjectionProgress {
    /// Latest source revision and transactional intent.
    pub version: ProjectVersion,
    /// Native ledger classification.
    pub state: ProjectionState,
    /// Retained native attempts, when evidence exists.
    pub attempts: Option<u32>,
}
/// Progress errors retain their query or ledger source.
#[derive(Debug, thiserror::Error)]
pub enum ProgressError {
    /// Source query failure.
    #[error(transparent)]
    Source(#[from] InvocationError<Option<ProjectState>>),
    /// Native ledger query failure.
    #[error(transparent)]
    Ledger(#[from] InvocationError<Option<cellule_runtime::primitives::effects::EffectStatus>>),
    /// Registry, codec, or receiver evidence contract failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Three observations raced with edits; retry this read rather than infer completion.
    #[error("project changed during bounded projection observation")]
    Changed,
}
/// Tenant/application/project-scoped capability. The embedding authorizes before constructing it.
#[derive(Clone)]
pub struct ProjectClient {
    handle: ApplicationHandle<ProjectTracker>,
    key: ProjectKey,
    target: CellTarget,
}
impl ProjectClient {
    /// Binds an authorized handle to one canonical aggregate identity.
    pub fn new(
        handle: ApplicationHandle<ProjectTracker>,
        key: ProjectKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(PROJECTS, key.as_bytes())?;
        Ok(Self {
            handle,
            key,
            target,
        })
    }
    /// Stable source target for explicit application enrollment and ownership.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Freezes exact command evidence before native dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        input: ProjectChange,
    ) -> Result<PreparedCommand<ChangeProject>, InvocationError<ChangeOutcome>> {
        input.validate().map_err(InvocationError::NotStarted)?;
        if input.project != self.key {
            return Err(InvocationError::NotStarted(Error::Identity(
                "tracker mutation targets another aggregate",
            )));
        }
        self.handle
            .prepare_command::<ChangeProject>(&self.target, identity, input)
            .await
    }
    /// Publishes a domain change and its summary intent together; the receipt is source-local.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        input: ProjectChange,
    ) -> Result<Committed<ChangeOutcome>, InvocationError<ChangeOutcome>> {
        self.prepare(identity, input).await?.execute().await
    }
    pub(crate) async fn prepare_verified_link(
        &self,
        identity: MutationIdentity,
        input: AttachmentLink,
    ) -> Result<PreparedCommand<LinkAttachment>, InvocationError<ChangeOutcome>> {
        input.validate().map_err(InvocationError::NotStarted)?;
        if input.publication.descriptor.project != self.key {
            return Err(InvocationError::NotStarted(Error::Identity(
                "tracker attachment targets another aggregate",
            )));
        }
        self.handle
            .prepare_command::<LinkAttachment>(&self.target, identity, input)
            .await
    }
    /// Reads a complete bounded aggregate at or beyond a receipt from this exact source Cell.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProjectState>>, InvocationError<Option<ProjectState>>> {
        self.handle
            .query::<GetProject>(&self.target, minimum, self.key.clone())
            .await
    }
    /// Reads one issue using the same complete authoritative source transaction.
    pub async fn issue(
        &self,
        id: &IssueId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Issue>>, InvocationError<Option<ProjectState>>> {
        let value = self.get(minimum).await?;
        Ok(Observed {
            receipt: value.receipt,
            output: value
                .output
                .and_then(|v| v.issues.into_iter().find(|v| &v.id == id)),
        })
    }
    /// Resolves only retained evidence from this exact aggregate, without refreshing its identity.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign tracker source evidence",
            )));
        }
        self.handle.resolve(pending).await
    }
    /// Reads one historical native intent in this project Cell without exposing its lease token.
    /// Missing evidence proves no dashboard absence; a source receipt remains source-local.
    pub async fn effect_status(
        &self,
        effect_id: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<
        Observed<Option<cellule_runtime::primitives::effects::EffectStatus>>,
        InvocationError<Option<cellule_runtime::primitives::effects::EffectStatus>>,
    > {
        if effect_id == [0; 32] {
            return Err(InvocationError::NotStarted(Error::Identity(
                "zero tracker effect identity",
            )));
        }
        self.handle
            .effects::<Projects>(self.target.clone())
            .map_err(InvocationError::NotStarted)?
            .status(effect_id, minimum)
            .await
    }
    /// Observes state and its native ledger coherently, retrying at most three concurrent edits.
    pub async fn progress(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProjectionProgress>>, ProgressError> {
        for _ in 0..3 {
            let read = self.get(minimum).await?;
            let Some(state) = read.output else {
                return Ok(Observed {
                    output: None,
                    receipt: read.receipt,
                });
            };
            let version = ProjectVersion {
                summary: state.summary()?,
                effect_id: state.effect_id,
            };
            let ledger = self
                .handle
                .effects::<Projects>(self.target.clone())?
                .status(state.effect_id, Some(read.receipt))
                .await?;
            let verified = self.get(Some(ledger.receipt)).await?;
            if verified.output.as_ref() != Some(&state) {
                continue;
            }
            let (state, attempts) = match ledger.output {
                None => (ProjectionState::Unavailable, None),
                Some(status) => {
                    let state = match status.state {
                        EffectState::Ready | EffectState::Leased => ProjectionState::Pending,
                        EffectState::Failed => ProjectionState::Failed,
                        EffectState::Delivered => match wire::decode::<ProjectionOutcome>(
                            status.result.as_deref().ok_or(Error::Command(
                                "tracker settled projection has no receiver result",
                            ))?,
                            16,
                        )? {
                            ProjectionOutcome::Applied
                            | ProjectionOutcome::Unchanged
                            | ProjectionOutcome::Stale => ProjectionState::Delivered,
                            ProjectionOutcome::Conflict | ProjectionOutcome::Capacity => {
                                ProjectionState::Failed
                            }
                        },
                    };
                    (state, Some(status.attempt))
                }
            };
            return Ok(Observed {
                output: Some(ProjectionProgress {
                    version,
                    state,
                    attempts,
                }),
                receipt: ledger.receipt,
            });
        }
        Err(ProgressError::Changed)
    }
}
/// Authorized tenant's independently committed read model.
#[derive(Clone)]
pub struct DashboardClient {
    handle: ApplicationHandle<ProjectTracker>,
    target: CellTarget,
}
impl DashboardClient {
    /// Selects the one dashboard within an authenticated application/tenant scope.
    pub fn new(handle: ApplicationHandle<ProjectTracker>) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(DASHBOARD, b"tenant-dashboard")?;
        Ok(Self { handle, target })
    }
    /// Stable dashboard target for explicit enrollment.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Looks up a projected revision, requiring only a dashboard-local receipt if supplied.
    pub async fn lookup(
        &self,
        key: ProjectKey,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProjectSummary>>, InvocationError<Option<ProjectSummary>>> {
        self.handle
            .query::<LookupProject>(&self.target, minimum, key)
            .await
    }
    /// Reads 1..16 current summaries; pages are independent transactions.
    pub async fn list(
        &self,
        page: DashboardPageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<DashboardPage>, InvocationError<DashboardPage>> {
        self.handle
            .query::<ListDashboard>(&self.target, minimum, page)
            .await
    }
}
