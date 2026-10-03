use crate::{
    Action, DELIVERIES, DELIVERY_MS, Decision, DeliveryTicket, FEED, Feed, MAX_EVENTS, MAX_PAYLOAD,
    MAX_SUBSCRIBERS, Outcome, PublishedDelivery, PublishedEvent, Receiver, ReceiverMode,
    ReceiverOutcome, ReceiverPolicy, ReceiverRecord, StartDelivery, Subscription,
    model::{decode_wire, encode_wire},
    sql,
};
use cellule_runtime::{
    CellModule, CellTarget, Error, partition_for_shard,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult},
    shard_for_scope,
};
fn result(
    decision: Decision,
    subscription: Option<Subscription>,
    event: Option<PublishedEvent>,
) -> CommandResult<Outcome> {
    let value = Outcome {
        decision,
        subscription,
        event,
    };
    if decision.success() {
        CommandResult::Success(value)
    } else {
        CommandResult::Rejected(value)
    }
}
/// Publishes source state and every subscriber intent in one SQL transaction.
pub struct ChangeFeed;
impl Command for ChangeFeed {
    const MODULE: &'static str = Feed::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Action;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        action: Action,
    ) -> cellule_runtime::Result<CommandResult<Outcome>> {
        match action {
            Action::Subscribe {
                id,
                topic,
                endpoint,
                enabled,
                expected_revision,
            } => {
                if expected_revision < 0 {
                    return Ok(result(Decision::Invalid, None, None));
                }
                let selected = context.sql(&sql::batch(
                    &format!(
                        "SELECT {} FROM subscriptions WHERE subscription_key=?1",
                        sql::SUB_FIELDS
                    ),
                    vec![SqlValue::Text(id.as_str().into())],
                ))?;
                let previous = match sql::rows(&selected)? {
                    [] => None,
                    [row] => Some(sql::subscription(row)?),
                    _ => return Err(Error::Command("subscription uniqueness violated")),
                };
                let revision = if let Some(previous) = previous {
                    if previous.revision != expected_revision {
                        return Ok(result(Decision::Conflict, Some(previous), None));
                    }
                    if previous.topic == topic
                        && previous.endpoint == endpoint
                        && previous.enabled == enabled
                    {
                        return Ok(result(Decision::Unchanged, Some(previous), None));
                    }
                    previous
                        .revision
                        .checked_add(1)
                        .ok_or(Error::Command("subscription revision overflow"))?
                } else {
                    if expected_revision != 0 {
                        return Ok(result(Decision::NotFound, None, None));
                    }
                    if sql::count(
                        &context.sql(&sql::batch("SELECT count(*) FROM subscriptions", vec![]))?,
                    )? >= MAX_SUBSCRIBERS as i64
                    {
                        return Ok(result(Decision::Capacity, None, None));
                    }
                    1
                };
                let value = Subscription {
                    id,
                    topic,
                    endpoint,
                    enabled,
                    revision,
                };
                sql::changed(&context.sql(&sql::batch("INSERT INTO subscriptions(subscription_key,topic,endpoint,enabled,revision) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(subscription_key) DO UPDATE SET topic=excluded.topic,endpoint=excluded.endpoint,enabled=excluded.enabled,revision=excluded.revision",vec![SqlValue::Text(value.id.as_str().into()),SqlValue::Text(value.topic.as_str().into()),SqlValue::Text(value.endpoint.as_str().into()),SqlValue::Integer(i64::from(value.enabled)),SqlValue::Integer(revision)]))?)?;
                Ok(result(Decision::Subscribed, Some(value), None))
            }
            Action::Publish { id, topic, payload } => {
                if payload.is_empty() || payload.len() > MAX_PAYLOAD {
                    return Ok(result(Decision::Invalid, None, None));
                }
                let selected = context.sql(&sql::batch(
                    "SELECT record FROM published_events WHERE event_id=?1",
                    vec![SqlValue::Blob(id.bytes().to_vec())],
                ))?;
                match sql::rows(&selected)? {
                    [] => {}
                    [row] => {
                        let [SqlValue::Blob(bytes)] = row.as_slice() else {
                            return Err(Error::Command("invalid stored published event"));
                        };
                        let value: PublishedEvent = decode_wire(bytes, 32768)?;
                        if value.id != id {
                            return Err(Error::Command("stored event identity differs"));
                        }
                        let decision = if value.topic == topic && value.payload == payload {
                            Decision::ExistingEvent
                        } else {
                            Decision::Conflict
                        };
                        return Ok(result(decision, None, Some(value)));
                    }
                    _ => return Err(Error::Command("published event uniqueness violated")),
                }
                if sql::count(
                    &context.sql(&sql::batch("SELECT count(*) FROM published_events", vec![]))?,
                )? >= MAX_EVENTS
                {
                    return Ok(result(Decision::Capacity, None, None));
                }
                let selected=context.sql(&sql::batch(&format!("SELECT {} FROM subscriptions WHERE topic=?1 AND enabled=1 ORDER BY subscription_key LIMIT 17",sql::SUB_FIELDS),vec![SqlValue::Text(topic.as_str().into())]))?;
                let subscriptions = sql::rows(&selected)?;
                if subscriptions.len() > MAX_SUBSCRIBERS {
                    return Err(Error::Command("subscription bound violated"));
                }
                let mut deliveries = Vec::with_capacity(subscriptions.len());
                for row in subscriptions {
                    let subscription = sql::subscription(row)?;
                    let ticket = DeliveryTicket {
                        source_cell: context.target().cell_id().as_bytes().to_vec(),
                        event_id: id,
                        subscription: subscription.id,
                        subscription_revision: subscription.revision,
                        topic: topic.clone(),
                        payload: payload.clone(),
                        endpoint: subscription.endpoint,
                        created_at_ms: context.now_ms(),
                        deadline_ms: context
                            .now_ms()
                            .checked_add(DELIVERY_MS)
                            .ok_or(Error::Command("delivery deadline overflow"))?,
                    };
                    let effect = context.emit_effect(&EffectCommandIntent {
                        target: CellTarget::new(
                            context.target().tenant(),
                            context.target().application(),
                            DELIVERIES,
                            &partition_for_shard(shard_for_scope(DELIVERIES, &ticket.key(), 2)?),
                        )?,
                        command_id: StartDelivery::ID,
                        codec_version: 1,
                        input: encode_wire(&ticket, 4096)?,
                        expires_at_ms: context
                            .now_ms()
                            .checked_add(7 * 24 * 60 * 60 * 1000)
                            .ok_or(Error::Command("delivery intent retention overflow"))?,
                    })?;
                    deliveries.push(PublishedDelivery {
                        ticket,
                        effect_id: effect.to_vec(),
                    });
                }
                let value = PublishedEvent {
                    id,
                    topic,
                    payload,
                    deliveries,
                };
                sql::changed(&context.sql(&sql::batch(
                    "INSERT INTO published_events(event_id,record) VALUES(?1,?2)",
                    vec![
                        SqlValue::Blob(id.bytes().to_vec()),
                        SqlValue::Blob(encode_wire(&value, 32768)?),
                    ],
                ))?)?;
                Ok(result(Decision::Published, None, Some(value)))
            }
        }
    }
}
/// Administrative receiver fault policy; embedding must authorize before dispatch.
pub struct SetReceiverPolicy;
impl Command for SetReceiverPolicy {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ReceiverPolicy;
    type Output = ();
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: ReceiverPolicy,
    ) -> cellule_runtime::Result<CommandResult<()>> {
        let existing = context.sql(&sql::batch(
            "SELECT mode FROM receiver_policies WHERE subscription_key=?1",
            vec![SqlValue::Text(input.subscription.as_str().into())],
        ))?;
        if sql::rows(&existing)?.is_empty()
            && sql::count(&context.sql(&sql::batch(
                "SELECT count(*) FROM receiver_policies",
                vec![],
            ))?)?
                >= MAX_SUBSCRIBERS as i64
        {
            return Err(Error::Command("receiver policy capacity exhausted"));
        }
        let mode = match input.mode {
            ReceiverMode::Good => 0,
            ReceiverMode::DropOnce => 1,
            ReceiverMode::TransientOnce => 2,
            ReceiverMode::Terminal => 3,
        };
        sql::changed(&context.sql(&sql::batch("INSERT INTO receiver_policies(subscription_key,mode) VALUES(?1,?2) ON CONFLICT(subscription_key) DO UPDATE SET mode=excluded.mode",vec![SqlValue::Text(input.subscription.as_str().into()),SqlValue::Integer(mode)]))?)?;
        Ok(CommandResult::Success(()))
    }
}
/// Idempotent receiver action. HTTP retry identity is independent of native request identity.
pub struct ReceiveDelivery;
impl Command for ReceiveDelivery {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = DeliveryTicket;
    type Output = ReceiverOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        ticket: DeliveryTicket,
    ) -> cellule_runtime::Result<CommandResult<ReceiverOutcome>> {
        ticket.validate()?;
        let source = CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            FEED,
            &partition_for_shard(0),
        )?;
        if ticket.source_cell != source.cell_id().as_bytes() {
            return Ok(CommandResult::Rejected(ReceiverOutcome {
                status: 403,
                drop_reply: false,
                record: None,
            }));
        }
        let key = ticket.key();
        let encoded = encode_wire(&ticket, 4096)?;
        let selected = context.sql(&sql::batch(
            "SELECT ticket,requests,applied FROM received_deliveries WHERE delivery_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?;
        let previous = match sql::rows(&selected)? {
            [] => None,
            [row] => Some(sql::receiver_record(row)?),
            _ => return Err(Error::Command("receiver uniqueness violated")),
        };
        if previous.as_ref().is_some_and(|v| v.ticket != ticket) {
            return Ok(CommandResult::Rejected(ReceiverOutcome {
                status: 409,
                drop_reply: false,
                record: None,
            }));
        }
        if previous.is_none()
            && sql::count(&context.sql(&sql::batch(
                "SELECT count(*) FROM received_deliveries",
                vec![],
            ))?)?
                >= 4096
        {
            return Ok(CommandResult::Rejected(ReceiverOutcome {
                status: 503,
                drop_reply: false,
                record: None,
            }));
        }
        let requests = previous.as_ref().map_or(1, |v| (v.requests + 1).min(20));
        let mode = context.sql(&sql::batch(
            "SELECT mode FROM receiver_policies WHERE subscription_key=?1",
            vec![SqlValue::Text(ticket.subscription.as_str().into())],
        ))?;
        let mode = match sql::rows(&mode)? {
            [] => 0,
            [row] => match row.as_slice() {
                [SqlValue::Integer(mode)] => *mode,
                _ => return Err(Error::Command("invalid stored receiver policy")),
            },
            _ => return Err(Error::Command("receiver policy uniqueness violated")),
        };
        if !(0..=3).contains(&mode) {
            return Err(Error::Command("invalid stored receiver mode"));
        }
        let was_applied = previous.as_ref().is_some_and(|v| v.applied);
        let first = previous.is_none();
        let status = if was_applied {
            200
        } else if mode == 3 {
            422
        } else if mode == 2 && first {
            503
        } else {
            200
        };
        let applied = was_applied || status == 200;
        sql::changed(&context.sql(&sql::batch("INSERT INTO received_deliveries(delivery_key,ticket,requests,applied) VALUES(?1,?2,?3,?4) ON CONFLICT(delivery_key) DO UPDATE SET requests=excluded.requests,applied=excluded.applied",vec![SqlValue::Blob(key.to_vec()),SqlValue::Blob(encoded),SqlValue::Integer(i64::from(requests)),SqlValue::Integer(i64::from(applied))]))?)?;
        // The actual HTTP server closes only after this command's durable reply.
        Ok(CommandResult::Success(ReceiverOutcome {
            status,
            drop_reply: mode == 1 && first && applied,
            record: Some(ReceiverRecord {
                ticket,
                requests,
                applied,
            }),
        }))
    }
}
