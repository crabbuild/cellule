//! Durable source subscriber fan-out, bounded Workflow retries, and idempotent HTTP delivery.
mod application;
mod commands;
mod definition;
mod http_activity;
mod model;
mod queries;
mod service;
mod sql;
pub use application::{
    DELIVERIES, Deliveries, FEED, Feed, RECEIVER, Receiver, WebhookApplication, compile,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, InvocationError, MutationIdentity, Observed, PreparedCommand, Receipt, Resolution,
    primitives::workflow::WorkflowOutcome,
};
pub use commands::{ChangeFeed, ReceiveDelivery, SetReceiverPolicy};
pub use definition::StartDelivery;
pub use http_activity::receiver_token;
pub use model::{
    Acknowledgement, Action, Classification, DELIVERY_MS, Decision, DeliveryState, DeliveryTicket,
    Endpoint, EventId, HttpAttempt, Key, MAX_EVENTS, MAX_PAYLOAD, MAX_ROUNDS, MAX_SUBSCRIBERS,
    Outcome, Phase, PublishedDelivery, PublishedEvent, ReceiverMode, ReceiverOutcome,
    ReceiverPolicy, ReceiverRecord, Subscription, SubscriptionList,
};
pub use service::{ServiceError, open, spawn_fanout, spawn_http_activities};
/// Read failures retain the native operation or original state decoding error.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Native query failure, including its original source chain.
    #[error("webhook query failed: {0}")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Definition codec or invariant failure.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
}
/// Reusable typed client; embedding must authorize every source and receiver capability.
#[derive(Clone)]
pub struct WebhookClient {
    handle: ApplicationHandle<WebhookApplication>,
    feed: CellTarget,
    receiver: CellTarget,
}
impl WebhookClient {
    /// Binds the embedding-selected tenant/application to the fixed publisher and receiver Cells.
    pub fn new(handle: ApplicationHandle<WebhookApplication>) -> cellule_runtime::Result<Self> {
        let feed = handle.target_for_scope(FEED, b"publisher")?;
        let receiver = handle.target_for_scope(RECEIVER, b"receiver")?;
        Ok(Self {
            handle,
            feed,
            receiver,
        })
    }
    /// Publisher receipt domain.
    pub fn feed_target(&self) -> &CellTarget {
        &self.feed
    }
    /// Receiver receipt domain; source receipts must never be used here.
    pub fn receiver_target(&self) -> &CellTarget {
        &self.receiver
    }
    /// Freezes exact source bytes and identity before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<PreparedCommand<ChangeFeed>, InvocationError<Outcome>> {
        self.handle
            .prepare_command::<ChangeFeed>(&self.feed, identity, action)
            .await
    }
    /// Publishes a retained source mutation; uncertain callers keep the prepared evidence.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        action: Action,
    ) -> Result<cellule_runtime::Committed<Outcome>, InvocationError<Outcome>> {
        self.prepare(identity, action).await?.execute().await
    }
    /// Resolves exact source command evidence without dispatching it again.
    pub async fn resolve(
        &self,
        evidence: &cellule_runtime::PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(evidence).await
    }
    /// Reads an accepted event and its original subscriber snapshot.
    pub async fn event(
        &self,
        id: EventId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<PublishedEvent>>, ReadError> {
        self.handle
            .query::<queries::ReadEvent>(&self.feed, minimum, id)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Reads the complete bounded subscription set, at most sixteen permanent keys.
    pub async fn subscriptions(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<SubscriptionList>, ReadError> {
        self.handle
            .query::<queries::ListSubscriptions>(&self.feed, minimum, ())
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Configures a synthetic receiver fault after embedding authorization.
    pub async fn policy(
        &self,
        identity: MutationIdentity,
        policy: ReceiverPolicy,
    ) -> Result<cellule_runtime::Committed<()>, InvocationError<()>> {
        self.handle
            .prepare_command::<SetReceiverPolicy>(&self.receiver, identity, policy)
            .await?
            .execute()
            .await
    }
    /// Freezes a receiver request. HTTP admission must verify bearer, header key, and ticket scope.
    pub async fn prepare_receive(
        &self,
        identity: MutationIdentity,
        ticket: DeliveryTicket,
    ) -> Result<PreparedCommand<ReceiveDelivery>, InvocationError<ReceiverOutcome>> {
        self.handle
            .prepare_command::<ReceiveDelivery>(&self.receiver, identity, ticket)
            .await
    }
    /// Reads one permanent receiver record in its own receipt domain.
    pub async fn received(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ReceiverRecord>>, ReadError> {
        self.handle
            .query::<queries::ReadReceiver>(&self.receiver, minimum, key.to_vec())
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))
    }
    /// Prepares the internal delivery receiver. Production peer admission accepts signed source Effects.
    pub async fn prepare_delivery(
        &self,
        identity: MutationIdentity,
        ticket: DeliveryTicket,
    ) -> Result<PreparedCommand<StartDelivery>, InvocationError<WorkflowOutcome>> {
        let target = self
            .handle
            .target_for_scope(DELIVERIES, &ticket.key())
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<StartDelivery>(&target, identity, ticket)
            .await
    }
    /// Reads exact native run and validated bounded business state.
    pub async fn delivery(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<DeliveryView>>, ReadError> {
        let observed = self
            .handle
            .workflow::<Deliveries>()
            .map_err(|source| ReadError::Query(Box::new(source)))?
            .state(key.to_vec(), minimum)
            .await
            .map_err(|source| ReadError::Query(Box::new(source)))?;
        let output = observed
            .output
            .map(|run| {
                let state: DeliveryState = model::decode_json(&run.state, 32768)?;
                state.validate()?;
                if state.ticket.key() != key {
                    return Err(cellule_runtime::Error::Command(
                        "stored delivery key differs",
                    ));
                }
                Ok(DeliveryView {
                    run_id: run.run_id,
                    native_status: format!("{:?}", run.status),
                    event_sequence: run.event_sequence,
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }
}
/// Exact native run and independently observable delivery business phase.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DeliveryView {
    /// Exact native run identifier.
    pub run_id: [u8; 16],
    /// Native serving status, separate from delivered/failed business phase.
    pub native_status: String,
    /// Monotonic event sequence within this run.
    pub event_sequence: u64,
    /// Original ticket, attempt history, and conservative external uncertainty.
    pub state: DeliveryState,
}
