//! Event-local scarce inventory and generation-checked asynchronous hold deadlines.
mod application;
mod commands;
mod definition;
mod model;
mod queries;
mod service;
mod sql;
pub use application::{DEADLINES, Deadlines, INVENTORY, InventoryCells, Reservations, compile};
use cellule_app::{ApplicationHandle, CellType};
use cellule_runtime::{
    CatalogRole, CellModule, CellTarget, Committed, InvocationError, MutationIdentity, Observed,
    PendingMutation, PreparedCommand, Receipt, Resolution,
    primitives::workflow::{WorkflowGetQuery, WorkflowGetRequest},
};
pub use commands::{ChangeReservation, ExpireHold};
pub use definition::ScheduleDeadline;
pub use model::{
    Action, BuyerKey, Change, DeadlineState, DeadlineTicket, Decision, EventKey, Expiration,
    ExpirationOutcome, Hold, HoldId, HoldState, Inventory, MAX_HOLD_MS, MAX_HOLDS, MAX_SEATS,
    Outcome, Page, PageRequest,
};
pub use queries::{ListHolds, ReadHold, ReadInventory};
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, spawn_deadlines};
pub(crate) fn domain_target(
    scope: &CellTarget,
    event: &EventKey,
) -> cellule_runtime::Result<CellTarget> {
    let partition = CellType::new(
        InventoryCells::NAME,
        "events",
        INVENTORY,
        CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()?
    .entity_partition(event.as_bytes())?;
    CellTarget::new(scope.tenant(), scope.application(), INVENTORY, &partition)
}
/// Native Workflow observations retain their source query and decode failures.
#[derive(Debug, thiserror::Error)]
pub enum DeadlineReadError {
    /// Original typed query failure.
    #[error("deadline query failed: {0}")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Definition state or runtime contract failure.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
}
/// Current independent Workflow observation. Confirmed/expired seat state remains authoritative.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DeadlineView {
    /// Native running/completed/cancelled/failed status.
    pub status: String,
    /// Executable definition pinned by the current run.
    pub definition: String,
    /// Current definition-owned timer state.
    pub state: DeadlineState,
}
/// Event capability. The embedding authorizes event administration and buyer identities before use.
#[derive(Clone)]
pub struct ReservationClient {
    handle: ApplicationHandle<Reservations>,
    event: EventKey,
    target: CellTarget,
}
impl ReservationClient {
    /// Binds an already-authorized event to its stable entity Cell.
    pub fn new(
        handle: ApplicationHandle<Reservations>,
        event: EventKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(INVENTORY, event.as_bytes())?;
        Ok(Self {
            handle,
            event,
            target,
        })
    }
    /// Stable source target for explicit ownership and provisioning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Validates and freezes evidence before dispatch; retain identity and action unchanged.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<PreparedCommand<ChangeReservation>, InvocationError<Outcome>> {
        action
            .validate()
            .map_err(|e| InvocationError::NotStarted(e.into()))?;
        self.handle
            .prepare_command::<ChangeReservation>(
                &self.target,
                identity,
                Change {
                    event: self.event.clone(),
                    action,
                },
            )
            .await
    }
    /// Publishes one local decision; hold success durably includes its deadline-start intent.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<Committed<Outcome>, InvocationError<Outcome>> {
        self.prepare(identity, action).await?.execute().await
    }
    /// Observes current source counters, optionally gated by a same-event receipt.
    pub async fn inventory(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Inventory>>, InvocationError<Option<Inventory>>> {
        self.handle
            .query::<ReadInventory>(&self.target, minimum, ())
            .await
    }
    /// Reads one permanent hold, with explicit source receipt scope.
    pub async fn hold(
        &self,
        id: HoldId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Hold>>, InvocationError<Option<Hold>>> {
        self.handle
            .query::<ReadHold>(&self.target, minimum, id)
            .await
    }
    /// Reads a coherent inventory and 1..100 permanent hold records per page.
    pub async fn list(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Page>, InvocationError<Page>> {
        self.handle
            .query::<ListHolds>(&self.target, minimum, page)
            .await
    }
    pub(crate) async fn active(&self) -> Result<Observed<Page>, InvocationError<Page>> {
        self.handle
            .query::<queries::ActiveHolds>(&self.target, None, ())
            .await
    }
    /// Resolves exact same-event pending evidence; expiry is never absence proof.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign event mutation evidence"),
            ));
        }
        self.handle.resolve(pending).await
    }
    /// Independent timer observation; None means the deadline start is still pending or absent.
    pub async fn deadline(
        &self,
        ticket: &DeadlineTicket,
    ) -> Result<Option<DeadlineView>, DeadlineReadError> {
        if ticket.event != self.event {
            return Err(cellule_runtime::Error::Identity("foreign event deadline ticket").into());
        }
        let key = ticket.workflow_id();
        let target = self.handle.target_for_scope(DEADLINES, &key)?;
        let read = self
            .handle
            .query::<WorkflowGetQuery<Deadlines>>(
                &target,
                None,
                WorkflowGetRequest {
                    workflow_id: key.to_vec(),
                },
            )
            .await
            .map_err(|e| DeadlineReadError::Query(Box::new(e)))?;
        read.output
            .map(|run| {
                let state: DeadlineState = definition::decode(&run.state)?;
                if state.ticket != *ticket {
                    return Err(cellule_runtime::Error::Command(
                        "observed deadline ticket differs",
                    )
                    .into());
                }
                Ok(DeadlineView {
                    status: format!("{:?}", run.status).to_lowercase(),
                    definition: format!("{:?}", run.definition_digest),
                    state,
                })
            })
            .transpose()
    }
}
