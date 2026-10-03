use crate::*;
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
    primitives::workflow::{WorkflowGetQuery, WorkflowGetRequest},
};

/// Authorized ticket capability. The embedding checks participants and agent membership before dispatch.
#[derive(Clone)]
pub struct TicketClient {
    pub(crate) handle: ApplicationHandle<SupportDesk>,
    target: CellTarget,
    key: TicketKey,
}
impl TicketClient {
    /// Binds a stable ticket entity within the handle's authorized tenant.
    pub fn new(
        handle: ApplicationHandle<SupportDesk>,
        key: TicketKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(TICKETS, key.as_bytes())?;
        Ok(Self {
            handle,
            target,
            key,
        })
    }
    /// Source Cell for provisioning, ownership, receipts, and exact pending evidence.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Permanent ticket key.
    pub fn key(&self) -> &TicketKey {
        &self.key
    }
    /// Freezes typed exact input and identity before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<PreparedCommand<ChangeTicket>, InvocationError<Outcome>> {
        action.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ChangeTicket>(
                &self.target,
                identity,
                Change {
                    ticket: self.key.clone(),
                    action,
                },
            )
            .await
    }
    /// Commits local state and any required native intents in one durable publication.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<Committed<Outcome>, InvocationError<Outcome>> {
        self.prepare(identity, action).await?.execute().await
    }
    /// Observes source metadata; a source receipt proves no independent Workflow or Blob progress.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Ticket>>, InvocationError<Option<Ticket>>> {
        self.handle
            .query::<GetTicket>(&self.target, minimum, ())
            .await
    }
    /// Returns a bounded coherent source conversation page.
    pub async fn messages(
        &self,
        request: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<MessagePage>, InvocationError<MessagePage>> {
        self.handle
            .query::<ListMessages>(&self.target, minimum, request)
            .await
    }
    /// Resolves the exact original request without issuing a replacement identity.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign support ticket request"),
            ));
        }
        self.handle.resolve(pending).await
    }
    pub(crate) async fn prepare_verified_link(
        &self,
        identity: MutationIdentity,
        link: AttachmentLink,
    ) -> Result<PreparedCommand<commands::LinkAttachment>, InvocationError<Outcome>> {
        if link.publication.descriptor.ticket != self.key {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign support attachment link"),
            ));
        }
        self.handle
            .prepare_command::<commands::LinkAttachment>(&self.target, identity, link)
            .await
    }
}

/// Authorized observation capability for the independent deadline and notification host.
#[derive(Clone)]
pub struct CoordinationClient {
    handle: ApplicationHandle<SupportDesk>,
    key: TicketKey,
    target: CellTarget,
}
impl CoordinationClient {
    /// Uses the coordinator's application handle; its host owns the Workflow Cells.
    pub fn new(
        handle: ApplicationHandle<SupportDesk>,
        key: TicketKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(TICKETS, key.as_bytes())?;
        Ok(Self {
            handle,
            key,
            target,
        })
    }
    /// Reads independent native deadline progress and verifies the frozen generation.
    pub async fn deadline(&self, deadline: &Deadline) -> Result<Option<DeadlineView>, BoxError> {
        deadline.validate()?;
        if deadline.ticket != self.key {
            return Err(cellule_runtime::Error::Identity("foreign support deadline").into());
        }
        let target = self.handle.target_for_scope(DEADLINES, &deadline.key())?;
        self.handle
            .query::<WorkflowGetQuery<Deadlines>>(
                &target,
                None,
                WorkflowGetRequest {
                    workflow_id: deadline.key().to_vec(),
                },
            )
            .await?
            .output
            .map(|run| {
                let state: DeadlineState = wire::from_json(&run.state, 4096)?;
                if state.ticket != *deadline {
                    return Err(
                        cellule_runtime::Error::Command("observed deadline input differs").into(),
                    );
                }
                Ok(DeadlineView {
                    status: format!("{:?}", run.status).to_lowercase(),
                    definition: format!("{:?}", run.definition_digest),
                    state,
                })
            })
            .transpose()
    }
    /// Reads independent notification state; an absent run does not invalidate the source intent.
    pub async fn notification(
        &self,
        notification: &Notification,
    ) -> Result<Option<NotificationView>, BoxError> {
        notification.validate()?;
        if notification.deadline.ticket != self.key
            || notification.source_cell != *self.target.cell_id().as_bytes()
        {
            return Err(cellule_runtime::Error::Identity("foreign support notification").into());
        }
        let target = self
            .handle
            .target_for_scope(NOTIFICATIONS, &notification.key())?;
        self.handle
            .query::<WorkflowGetQuery<Notifications>>(
                &target,
                None,
                WorkflowGetRequest {
                    workflow_id: notification.key().to_vec(),
                },
            )
            .await?
            .output
            .map(|run| {
                let state: NotificationState = wire::from_json(&run.state, 32768)?;
                state.validate()?;
                if state.ticket != *notification {
                    return Err(cellule_runtime::Error::Command(
                        "observed notification input differs",
                    )
                    .into());
                }
                Ok(NotificationView {
                    status: format!("{:?}", run.status).to_lowercase(),
                    definition: format!("{:?}", run.definition_digest),
                    state,
                })
            })
            .transpose()
    }
}
