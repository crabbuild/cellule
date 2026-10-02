use super::{Result, client, http, print, receipt};
use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_cookbook_webhook_delivery::{
    Action, Classification, Decision, DeliveryView, Endpoint, EventId, Key, Phase, ReceiverMode,
    ReceiverPolicy, WebhookApplication, WebhookClient, spawn_fanout, spawn_http_activities,
};
use std::time::Duration;
async fn terminal(node: &LocalNode, client: &WebhookClient, key: [u8; 32]) -> Result<DeliveryView> {
    let until = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if !node.is_ready() {
            return Err("webhook worker lost readiness".into());
        }
        if let Some(view) = client.delivery(key, None).await?.output
            && !matches!(view.state.phase, Phase::InFlight | Phase::Backoff)
        {
            return Ok(view);
        }
        if tokio::time::Instant::now() >= until {
            return Err("delivery did not reach a terminal phase".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
pub(super) async fn run(node: &LocalNode) -> Result<()> {
    let client = client(node).await?;
    let address = http::install(node, client.clone(), 0, http::Options::default()).await?;
    let endpoint = Endpoint::new(format!("http://{address}/deliver"))?;
    let topic = Key::new(format!("demo-{}", uuid::Uuid::now_v7()))?;
    let modes = [
        ("demo-good", ReceiverMode::Good),
        ("demo-drop", ReceiverMode::DropOnce),
        ("demo-transient", ReceiverMode::TransientOnce),
        ("demo-terminal", ReceiverMode::Terminal),
    ];
    let previous = client.subscriptions(None).await?.output.subscriptions;
    for (name, mode) in modes {
        let subscription = Key::new(name)?;
        let revision = previous
            .iter()
            .find(|value| value.id == subscription)
            .map_or(0, |value| value.revision);
        client
            .change(
                new_identity()?,
                Action::Subscribe {
                    id: subscription.clone(),
                    topic: topic.clone(),
                    endpoint: endpoint.clone(),
                    enabled: true,
                    expected_revision: revision,
                },
            )
            .await?;
        client
            .policy(new_identity()?, ReceiverPolicy { subscription, mode })
            .await?;
    }
    let event_id = EventId::from_bytes(*uuid::Uuid::now_v7().as_bytes())?;
    let action = Action::Publish {
        id: event_id,
        topic,
        payload: "synthetic order created".into(),
    };
    let identity = new_identity()?;
    let published = client.change(identity, action.clone()).await?;
    if client.change(identity, action.clone()).await? != published {
        return Err("retained source replay changed".into());
    }
    let repeated = client.change(new_identity()?, action).await?;
    if repeated.output.decision != Decision::ExistingEvent
        || repeated.output.event != published.output.event
    {
        return Err("fresh event replay changed fan-out snapshot".into());
    }
    let event = published
        .output
        .event
        .ok_or("publication omitted event facts")?;
    if event.deliveries.len() != 4 {
        return Err("demo did not snapshot four subscribers".into());
    }
    let handle = node.application_handle::<WebhookApplication>(super::TENANT)?;
    spawn_fanout(node, handle.clone()).await?;
    spawn_http_activities(node, handle)?;
    let mut deliveries = Vec::with_capacity(4);
    for item in &event.deliveries {
        let view = terminal(node, &client, item.ticket.key()).await?;
        let observed = client.received(item.ticket.key(), None).await?;
        let record = observed.output.ok_or("receiver record missing")?;
        if record.ticket != item.ticket {
            return Err("receiver snapshot changed".into());
        }
        match item.ticket.subscription.as_str() {
            "demo-terminal"
                if view.state.phase == Phase::Failed
                    && !record.applied
                    && view.state.attempts.len() == 1
                    && view.state.attempts[0].status == Some(422) => {}
            "demo-good" if view.state.phase == Phase::Delivered && record.applied => {}
            "demo-drop"
                if view.state.phase == Phase::Delivered
                    && record.applied
                    && record.requests >= 2
                    && view.state.attempts.first().is_some_and(|value| {
                        value.status.is_none()
                            && value.classification == Classification::Retryable
                            && value.may_have_applied
                    }) => {}
            "demo-transient"
                if view.state.phase == Phase::Delivered
                    && record.applied
                    && record.requests >= 2
                    && view.state.attempts.first().is_some_and(|value| {
                        value.status == Some(503)
                            && value.classification == Classification::Retryable
                    }) => {}
            _ => {
                return Err(
                    "subscriber journey did not satisfy its receiver fault contract".into(),
                );
            }
        }
        deliveries.push(serde_json::json!({"key":item.ticket.key_hex(),"delivery":view,"received":record,"receiver_receipt":receipt(observed.receipt)}));
    }
    print(
        serde_json::json!({"scenario":"passed","event_uuid":uuid::Uuid::from_bytes(event_id.bytes()).to_string(),"event":event,"source_receipt":receipt(published.receipt),"deliveries":deliveries,"checks":["atomic-subscriber-fanout","retained-source-replay","fresh-event-idempotency","actual-socket-reply-drop","stable-external-delivery-key","transient-http-retry","terminal-http-rejection","one-receiver-action","owned-worker-drain"]}),
    )
}
