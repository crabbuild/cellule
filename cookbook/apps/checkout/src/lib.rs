//! Durable checkout with stock reservations, permanent payment identities, and explicit compensation.
mod activity;
mod application;
mod definition;
mod inventory;
mod model;
mod orders;
mod payments;
mod sagas;
mod service;
mod sql;
mod wire;
pub use activity::{observe_payment, validate_payment_token};
pub use application::{
    CheckoutApplication, INVENTORY, Inventory, ORDERS, Orders, PAYMENTS, Payments, SAGAS, Sagas,
    compile,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution, primitives::workflow::WorkflowStatus,
};
pub use inventory::{SeedStock, StockStep};
pub use model::{
    Call, DeliveryOutcome, Id, MAX_MESSAGES, MAX_ORDERS, MAX_PRODUCTS, MAX_RECONCILIATIONS,
    Operation, Order, OrderChange, OrderOutcome, OrderResult, OrderSpec, OrderStatus, OrdersPage,
    Page, Payment, PaymentAction, PaymentObservation, PaymentPolicy, PaymentStatus, PaymentWork,
    Phase, Reconcile, Reply, ReplyValue, Reservation, ReservationStatus, SagaState, Seed, Stock,
    StockQuery, target, validate_endpoint, validate_sku,
};
pub use orders::{ChangeOrder, OrderStep};
pub use payments::ApplyPayment;
pub use sagas::{ReconcileSaga, ReplySaga, StartSaga};
pub use service::{ServiceError, open, spawn_workers};
/// Retained provider, invocation, task, HTTP, and codec source errors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Authenticated embedding capability for the declared fixed checkout domains.
#[derive(Clone)]
pub struct CheckoutClient {
    handle: ApplicationHandle<CheckoutApplication>,
}
impl CheckoutClient {
    /// Binds the embedding's authenticated tenant/application handle.
    pub fn new(handle: ApplicationHandle<CheckoutApplication>) -> Self {
        Self { handle }
    }
    /// Resolves a declared namespace; receipts remain scoped to this exact Cell.
    pub fn target(
        &self,
        namespace: cellule_runtime::NamespaceId,
    ) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(namespace, b"fixed-checkout-shard")
    }
    /// Prepares placement or cancellation; retain identity before dispatch.
    pub async fn prepare_order(
        &self,
        identity: MutationIdentity,
        change: OrderChange,
    ) -> Result<PreparedCommand<ChangeOrder>, InvocationError<OrderOutcome>> {
        if let OrderChange::Place(spec) = &change {
            spec.validate().map_err(InvocationError::NotStarted)?;
        }
        self.handle
            .prepare_command::<ChangeOrder>(
                &self.target(ORDERS).map_err(InvocationError::NotStarted)?,
                identity,
                change,
            )
            .await
    }
    /// Prepares a permanent seed; identical replay never replenishes inventory.
    pub async fn prepare_seed(
        &self,
        identity: MutationIdentity,
        seed: Seed,
    ) -> Result<PreparedCommand<SeedStock>, InvocationError<DeliveryOutcome>> {
        seed.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<SeedStock>(
                &self
                    .target(INVENTORY)
                    .map_err(InvocationError::NotStarted)?,
                identity,
                seed,
            )
            .await
    }
    /// Prepares the same external business operation inside the independent simulator.
    pub async fn prepare_payment(
        &self,
        identity: MutationIdentity,
        work: PaymentWork,
    ) -> Result<PreparedCommand<ApplyPayment>, InvocationError<DeliveryOutcome>> {
        work.spec.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ApplyPayment>(
                &self.target(PAYMENTS).map_err(InvocationError::NotStarted)?,
                identity,
                work,
            )
            .await
    }
    /// Prepares one caller-retained reconciliation token; availability is checked atomically.
    pub async fn prepare_reconcile(
        &self,
        identity: MutationIdentity,
        input: Reconcile,
    ) -> Result<PreparedCommand<ReconcileSaga>, InvocationError<DeliveryOutcome>> {
        self.handle
            .prepare_command::<ReconcileSaga>(
                &self.target(SAGAS).map_err(InvocationError::NotStarted)?,
                identity,
                input,
            )
            .await
    }
    /// Resolves original native evidence without creating a new business action.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if ![ORDERS, INVENTORY, SAGAS, PAYMENTS]
            .into_iter()
            .map(|ns| self.target(ns))
            .collect::<cellule_runtime::Result<Vec<_>>>()
            .map_err(InvocationError::NotStarted)?
            .contains(evidence.target())
        {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign checkout mutation evidence",
            )));
        }
        self.handle.resolve(evidence).await
    }
    /// Reads the order's current SQL projection at an optional order receipt.
    pub async fn order(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Order>>, BoxError> {
        Ok(self
            .handle
            .query::<orders::GetOrder>(&self.target(ORDERS)?, minimum, id)
            .await?)
    }
    /// Lists bounded current orders with a receiver-local keyset cursor.
    pub async fn orders(
        &self,
        page: Page,
        minimum: Option<Receipt>,
    ) -> Result<Observed<OrdersPage>, BoxError> {
        page.validate()?;
        Ok(self
            .handle
            .query::<orders::ListOrders>(&self.target(ORDERS)?, minimum, page)
            .await?)
    }
    /// Reads coherent inventory counters at an inventory receipt.
    pub async fn stock(
        &self,
        sku: String,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Stock>>, BoxError> {
        validate_sku(&sku)?;
        Ok(self
            .handle
            .query::<inventory::GetStock>(&self.target(INVENTORY)?, minimum, StockQuery { sku })
            .await?)
    }
    /// Reads the permanent reservation, including compensation tombstones.
    pub async fn reservation(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Reservation>>, BoxError> {
        Ok(self
            .handle
            .query::<inventory::GetReservation>(&self.target(INVENTORY)?, minimum, id)
            .await?)
    }
    /// Reads an independently durable simulator record at a payment receipt.
    pub async fn payment(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Payment>>, BoxError> {
        Ok(self
            .handle
            .query::<payments::GetPayment>(&self.target(PAYMENTS)?, minimum, id)
            .await?)
    }
    /// Reads native saga state; order and stock visibility require their own reads.
    pub async fn saga(
        &self,
        id: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<SagaView>>, BoxError> {
        let observed = self
            .handle
            .workflow::<Sagas>()?
            .state(id.bytes().to_vec(), minimum)
            .await?;
        let output = observed
            .output
            .map(|run| -> Result<SagaView, BoxError> {
                let state: SagaState = model::decode(&run.state)?;
                state.validate()?;
                if state.spec.id != id || state.run_id != run.run_id {
                    return Err("native checkout business binding differs".into());
                }
                Ok(SagaView {
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
/// Native saga execution and its causal acknowledgments.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SagaView {
    /// Native lifetime; no fresh run is created for operator reconciliation.
    pub run_id: [u8; 16],
    /// Native execution status; business settlement is in the state/result.
    pub status: String,
    /// Validated domain phase and pending work.
    pub state: SagaState,
}
