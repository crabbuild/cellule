//! Customer-local credit invariants and permanent idempotent business settlement.
//! The embedding application owns authorization, external work, and allowance policy.
mod application;
mod commands;
mod model;
mod queries;
mod sql;
pub use application::{Credits, NAMESPACE, Quotas, compile};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
};
pub use commands::ChangeQuota;
pub use model::{
    Account, Action, Change, CustomerKey, Decision, MAX_CREDITS, MAX_RESERVATIONS, Outcome, Page,
    PageRequest, Reservation, ReservationId, ReservationState,
};
pub use queries::{ListReservations, ReadAccount};
/// Typed capability bound to one authorized customer and its stable entity Cell.
#[derive(Clone)]
pub struct QuotaClient {
    handle: ApplicationHandle<Quotas>,
    customer: CustomerKey,
    target: CellTarget,
}
impl QuotaClient {
    /// Derives the customer Cell. Authenticate and authorize before constructing this capability.
    pub fn new(
        handle: ApplicationHandle<Quotas>,
        customer: CustomerKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(NAMESPACE, customer.as_bytes())?;
        Ok(Self {
            handle,
            customer,
            target,
        })
    }
    /// Stable target for explicit application provisioning and ownership.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Validates and prepares immutable native evidence before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<PreparedCommand<ChangeQuota>, InvocationError<Outcome>> {
        action
            .validate()
            .map_err(|e| InvocationError::NotStarted(cellule_runtime::Error::from(e)))?;
        self.handle
            .prepare_command::<ChangeQuota>(
                &self.target,
                identity,
                Change {
                    customer: self.customer.clone(),
                    action,
                },
            )
            .await
    }
    /// Publishes one durable decision. Retain the same identity and action on native retry.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<Committed<Outcome>, InvocationError<Outcome>> {
        self.prepare(identity, action).await?.execute().await
    }
    /// Reads account facts at or beyond a receipt from this customer.
    pub async fn account(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Account>>, InvocationError<Option<Account>>> {
        self.handle
            .query::<ReadAccount>(&self.target, minimum, ())
            .await
    }
    /// Reads 1..100 records and coherent account facts. Each page observes its own commit.
    pub async fn list(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Page>, InvocationError<Page>> {
        self.handle
            .query::<ListReservations>(&self.target, minimum, page)
            .await
    }
    /// Observes original evidence, restricted to this exact customer; never creates a new request.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign customer quota evidence"),
            ));
        }
        self.handle.resolve(pending).await
    }
}
