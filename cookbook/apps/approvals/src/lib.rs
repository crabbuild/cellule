//! Human purchase decisions, durable timers, and idempotent external reminders.
mod application;
mod definition;
mod mailbox;
mod model;
mod service;
pub use application::{APPROVALS, ApprovalApplication, Approvals, compile};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    InvocationError, MutationIdentity, Observed, PreparedCommand, Receipt, Resolution,
    primitives::workflow::{
        WorkflowCancelCommand, WorkflowControl, WorkflowControlAction, WorkflowControlCommand,
        WorkflowGetQuery, WorkflowGetRequest, WorkflowOutcome, WorkflowSignal,
        WorkflowSignalCommand, WorkflowStart, WorkflowStartCommand, WorkflowStatus,
    },
    registry::{Query, QueryContext},
};
pub use mailbox::{MailRecord, read_mail};
pub use model::{
    ApprovalState, AuditEntry, Choice, Employee, HistoryPage, HistoryRequest, MailReceipt, Phase,
    Purchase, PurchaseId, Reminder,
};
pub use service::{ServiceError, open, spawn_reminders};
/// Prepared start retains the exact frozen event and request identity.
pub type PreparedStart = PreparedCommand<WorkflowStartCommand<Approvals>>;
/// Prepared vote retains native signal identity, exact run, actor, and choice.
pub type PreparedVote = PreparedCommand<WorkflowSignalCommand<Approvals>>;
/// Requester-only controls; restarting replaces the native current-run history.
#[derive(Clone, Debug)]
pub enum Control {
    /// Pause timers and new Activity claims once no Activity is leased.
    Pause,
    /// Resume a paused run; the absolute deadline does not move.
    Resume,
    /// Replace a terminal run with a newly frozen submission of the same purchase.
    Restart {
        /// Frozen purchase for the next run.
        purchase: Purchase,
    },
}
/// Current exact run and its bounded business state.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ApprovalView {
    /// Exact native run identity required by signals and controls.
    pub run_id: [u8; 16],
    /// Pinned executable definition digest.
    pub definition: String,
    /// Native serving status, including pause and cancellation.
    pub status: String,
    /// Native event sequence, scoped to this run.
    pub event_sequence: u64,
    /// Definition-owned state and business-transition history.
    pub state: ApprovalState,
}
/// Read failures retain the original runtime or codec source.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Native query preparation, dispatch, or receipt failure.
    #[error("approval query failed: {0}")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Invalid definition-owned bytes.
    #[error(transparent)]
    Codec(#[from] cellule_runtime::codec::CodecError),
    /// Definition encoding failure with its original JSON source.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
}
/// Typed domain client bound to the embedding's authenticated principal.
/// The embedding creates the tenant handle and verifies the principal before constructing it.
#[derive(Clone)]
pub struct ApprovalClient {
    handle: ApplicationHandle<ApprovalApplication>,
    principal: Employee,
}
impl ApprovalClient {
    /// Binds a verified employee to an application-selected tenant.
    pub fn new(handle: ApplicationHandle<ApprovalApplication>, principal: Employee) -> Self {
        Self { handle, principal }
    }
    /// Freezes the submitted event. The caller must stamp and retain its original logical times.
    pub async fn prepare_submit(
        &self,
        identity: MutationIdentity,
        purchase: Purchase,
    ) -> Result<PreparedStart, InvocationError<WorkflowOutcome>> {
        purchase
            .validate()
            .map_err(|error| InvocationError::NotStarted(error.into()))?;
        if purchase.requester != self.principal {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::PeerAuthorization("submission principal differs"),
            ));
        }
        let mut event = definition::SUBMIT.to_vec();
        event.extend(model::encode(&purchase).map_err(InvocationError::NotStarted)?);
        let target = self
            .handle
            .target_for_scope(APPROVALS, &purchase.id.bytes())
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<WorkflowStartCommand<Approvals>>(
                &target,
                identity,
                WorkflowStart {
                    workflow_id: purchase.id.bytes().to_vec(),
                    request_id: identity.request_id,
                    event,
                },
            )
            .await
    }
    /// Prepares a signal after authorization. An existing vote cannot be overwritten.
    pub async fn prepare_vote(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
        choice: Choice,
    ) -> Result<PreparedVote, InvocationError<WorkflowOutcome>> {
        let view = self.get(id, None).await.map_err(|source| {
            InvocationError::NotStarted(cellule_runtime::Error::PeerTransport {
                context: "authorize approval decision",
                source: Box::new(source),
            })
        })?;
        if !view
            .output
            .is_some_and(|view| view.state.purchase.approvers.contains(&self.principal))
        {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::PeerAuthorization("actor is not an assigned approver"),
            ));
        }
        self.frozen_vote(identity, id, run_id, signal_id, choice)
            .await
    }
    async fn frozen_vote(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
        choice: Choice,
    ) -> Result<PreparedVote, InvocationError<WorkflowOutcome>> {
        if signal_id == [0; 16] {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Command("signal identity must be nonzero"),
            ));
        }
        let mut event = definition::VOTE.to_vec();
        event.extend(
            model::encode(&model::Vote {
                actor: self.principal.clone(),
                choice,
            })
            .map_err(InvocationError::NotStarted)?,
        );
        let target = self
            .handle
            .target_for_scope(APPROVALS, &id.bytes())
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<WorkflowSignalCommand<Approvals>>(
                &target,
                identity,
                WorkflowSignal {
                    workflow_id: id.bytes().to_vec(),
                    run_id,
                    signal_id,
                    event,
                },
            )
            .await
    }
    /// Reads the current run at an optional source receipt. Team members may inspect requests.
    pub async fn get(
        &self,
        id: PurchaseId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ApprovalView>>, ReadError> {
        let result = self
            .handle
            .workflow::<Approvals>()
            .map_err(|source| ReadError::Query(Box::new(source)))?
            .state(id.bytes().to_vec(), minimum)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        let output = result
            .output
            .map(|run| {
                let state: ApprovalState = model::decode(&run.state)?;
                state.validate()?;
                Ok::<_, cellule_runtime::Error>(ApprovalView {
                    run_id: run.run_id,
                    definition: format!("{:?}", run.definition_digest),
                    status: match run.status {
                        WorkflowStatus::Running => "running",
                        WorkflowStatus::Completed => "completed",
                        WorkflowStatus::Failed => "failed",
                        WorkflowStatus::Cancelled => "cancelled",
                        WorkflowStatus::Paused => "paused",
                    }
                    .into(),
                    event_sequence: run.event_sequence,
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
    /// Reads bounded business transitions for the current run, scoped to the same receipt domain.
    pub async fn history(
        &self,
        page: HistoryRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<HistoryPage>, ReadError> {
        let target = self
            .handle
            .target_for_scope(APPROVALS, &page.id.bytes())
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        self.handle
            .query::<History>(&target, minimum, page)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Resolves exact retained command evidence without dispatching again.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(evidence).await
    }
    async fn authorize_control(
        &self,
        id: PurchaseId,
    ) -> Result<(), InvocationError<WorkflowOutcome>> {
        let view = self.get(id, None).await.map_err(|source| {
            InvocationError::NotStarted(cellule_runtime::Error::PeerTransport {
                context: "authorize approval control",
                source: Box::new(source),
            })
        })?;
        if !view
            .output
            .is_some_and(|view| view.state.purchase.requester == self.principal)
        {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::PeerAuthorization(
                    "only the requester controls this workflow",
                ),
            ));
        }
        Ok(())
    }
    /// Prepares pause or resume under requester authorization. Restarting replaces current-run history.
    pub async fn prepare_control(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        action: Control,
    ) -> Result<PreparedCommand<WorkflowControlCommand<Approvals>>, InvocationError<WorkflowOutcome>>
    {
        self.authorize_control(id).await?;
        self.frozen_control(identity, id, run_id, action).await
    }
    async fn frozen_control(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        action: Control,
    ) -> Result<PreparedCommand<WorkflowControlCommand<Approvals>>, InvocationError<WorkflowOutcome>>
    {
        let action = match action {
            Control::Pause => WorkflowControlAction::Pause,
            Control::Resume => WorkflowControlAction::Resume,
            Control::Restart { purchase } => {
                purchase
                    .validate()
                    .map_err(|error| InvocationError::NotStarted(error.into()))?;
                if purchase.id != id || purchase.requester != self.principal {
                    return Err(InvocationError::NotStarted(
                        cellule_runtime::Error::PeerAuthorization("restart changes purchase scope"),
                    ));
                }
                let mut event = definition::SUBMIT.to_vec();
                event.extend(model::encode(&purchase).map_err(InvocationError::NotStarted)?);
                WorkflowControlAction::Restart {
                    request_id: identity.request_id,
                    event,
                }
            }
        };
        let target = self
            .handle
            .target_for_scope(APPROVALS, &id.bytes())
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<WorkflowControlCommand<Approvals>>(
                &target,
                identity,
                WorkflowControl {
                    workflow_id: id.bytes().to_vec(),
                    run_id,
                    action,
                },
            )
            .await
    }
    /// Prepares requester cancellation. Already-published mailbox records remain external facts.
    pub async fn prepare_cancel(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
    ) -> Result<PreparedCommand<WorkflowCancelCommand<Approvals>>, InvocationError<WorkflowOutcome>>
    {
        self.authorize_control(id).await?;
        self.frozen_cancel(identity, id, run_id, signal_id).await
    }
    async fn frozen_cancel(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
    ) -> Result<PreparedCommand<WorkflowCancelCommand<Approvals>>, InvocationError<WorkflowOutcome>>
    {
        let target = self
            .handle
            .target_for_scope(APPROVALS, &id.bytes())
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<WorkflowCancelCommand<Approvals>>(
                &target,
                identity,
                WorkflowSignal {
                    workflow_id: id.bytes().to_vec(),
                    run_id,
                    signal_id,
                    event: b"requester-cancel.v1".to_vec(),
                },
            )
            .await
    }
    /// Resolves an original vote using only its frozen bytes and authenticated actor.
    /// A later restart may change assignment; resolution never authorizes a new dispatch.
    pub async fn resolve_vote(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
        choice: Choice,
    ) -> Result<Resolution, ReadError> {
        let prepared = self
            .frozen_vote(identity, id, run_id, signal_id, choice)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        self.resolve(prepared.evidence())
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Resolves an original requester control without consulting mutable current-run policy.
    pub async fn resolve_control(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        action: Control,
    ) -> Result<Resolution, ReadError> {
        let prepared = self
            .frozen_control(identity, id, run_id, action)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        self.resolve(prepared.evidence())
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Resolves a frozen requester cancellation without dispatching it again.
    pub async fn resolve_cancel(
        &self,
        identity: MutationIdentity,
        id: PurchaseId,
        run_id: [u8; 16],
        signal_id: [u8; 16],
    ) -> Result<Resolution, ReadError> {
        let prepared = self
            .frozen_cancel(identity, id, run_id, signal_id)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        self.resolve(prepared.evidence())
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
}
/// Bounded business history; no caller-selected SQL or arbitrary event payloads are exposed.
pub struct History;
impl Query for History {
    const MODULE: &'static str = "approvals.requests";
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = HistoryRequest;
    type Output = HistoryPage;
    fn execute(
        context: &mut QueryContext<'_>,
        input: HistoryRequest,
    ) -> cellule_runtime::Result<HistoryPage> {
        if !(1..=10).contains(&input.limit) || input.after == Some(0) {
            return Err(cellule_runtime::Error::Command(
                "invalid business history page",
            ));
        }
        let run = WorkflowGetQuery::<Approvals>::execute(
            context,
            WorkflowGetRequest {
                workflow_id: input.id.bytes().to_vec(),
            },
        )?
        .ok_or(cellule_runtime::Error::Command("purchase is missing"))?;
        if run.run_id != input.run_id {
            return Err(cellule_runtime::Error::Command(
                "history run changed; begin a new traversal",
            ));
        }
        let state: ApprovalState = model::decode(&run.state)?;
        state.validate()?;
        let mut entries: Vec<_> = state
            .audit
            .into_iter()
            .filter(|entry| input.after.is_none_or(|after| entry.sequence > after))
            .take(input.limit as usize + 1)
            .collect();
        let more = entries.len() > input.limit as usize;
        entries.truncate(input.limit as usize);
        let next = if more {
            entries.last().map(|entry| entry.sequence)
        } else {
            None
        };
        Ok(HistoryPage {
            run_id: run.run_id,
            entries,
            next,
        })
    }
}
