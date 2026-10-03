use crate::{sql, wire, *};
use cellule_runtime::{
    CellModule, CellTarget, Error, Result, partition_for_shard,
    primitives::{
        effects::EffectCommandIntent,
        sql::{SqlBatch, SqlResultSet, SqlValue},
    },
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

pub(crate) fn load(
    mut sql: impl FnMut(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    target: Option<&CellTarget>,
) -> Result<Option<Ticket>> {
    let rows = sql::one(sql(&sql::batch(
        "SELECT ticket_key,state FROM ticket WHERE singleton=1 LIMIT 2",
        vec![],
    ))?)?
    .rows;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Text(key), SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid ticket SQL row"));
            };
            let value: Ticket = wire::from_json(bytes, 131072)?;
            value.validate()?;
            if value.key.as_str() != key {
                return Err(Error::Identity("stored support ticket key differs"));
            }
            if let Some(target) = target
                && (target != &ticket_target(target, &value.key)?
                    || value
                        .notifications
                        .iter()
                        .any(|r| r.notification.source_cell != *target.cell_id().as_bytes()))
            {
                return Err(Error::Identity(
                    "stored ticket or escalation source differs from target",
                ));
            }
            Ok(Some(value))
        }
        _ => Err(Error::Command("ticket singleton violated")),
    }
}
fn save(context: &CommandContext<'_, '_>, ticket: &Ticket) -> Result<()> {
    ticket.validate()?;
    sql::changed(context.sql(&sql::batch(
        "UPDATE ticket SET state=?1 WHERE singleton=1 AND ticket_key=?2",
        vec![
            SqlValue::Blob(wire::json(ticket, 131072)?),
            SqlValue::Text(ticket.key.as_str().into()),
        ],
    ))?)
}
fn outcome(decision: Decision, ticket: Option<Ticket>) -> CommandResult<Outcome> {
    let value = Outcome { decision, ticket };
    if matches!(decision, Decision::Applied | Decision::Duplicate) {
        CommandResult::Success(value)
    } else {
        CommandResult::Rejected(value)
    }
}
fn due(now: i64, due_at_ms: i64) -> bool {
    due_at_ms > now
        && now
            .checked_add(MAX_DEADLINE_MS)
            .is_some_and(|max| due_at_ms <= max)
}
fn intent<T: cellule_runtime::codec::WireValue>(
    context: &mut CommandContext<'_, '_>,
    namespace: cellule_runtime::NamespaceId,
    command_id: u32,
    input: &T,
) -> Result<[u8; 32]> {
    context.emit_effect(&EffectCommandIntent {
        target: CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            namespace,
            &partition_for_shard(0),
        )?,
        command_id,
        codec_version: 1,
        input: wire::encode(input, 8192)?,
        expires_at_ms: context
            .now_ms()
            .checked_add(MAX_DEADLINE_MS)
            .ok_or(Error::Command("support intent expiry overflow"))?,
    })
}
fn install_deadline(
    context: &mut CommandContext<'_, '_>,
    ticket: &mut Ticket,
    due_at_ms: i64,
) -> Result<()> {
    ticket.deadline = Deadline {
        ticket: ticket.key.clone(),
        generation: ticket.generation,
        due_at_ms,
    };
    ticket.deadline_effect = intent(context, DEADLINES, ScheduleDeadline::ID, &ticket.deadline)?;
    ticket.escalated = false;
    Ok(())
}
fn bump(ticket: &mut Ticket) -> Result<()> {
    ticket.revision = ticket
        .revision
        .checked_add(1)
        .ok_or(Error::Command("ticket revision overflow"))?;
    revision(ticket.revision)
}
fn message(
    mut sql: impl FnMut(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    id: &MessageId,
) -> Result<Option<Message>> {
    let rows = sql::one(sql(&sql::batch(
        "SELECT body FROM messages WHERE message_id=?1 LIMIT 2",
        vec![SqlValue::Text(id.as_str().into())],
    ))?)?
    .rows;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [SqlValue::Blob(bytes)] => {
                let value: Message = wire::from_json(bytes, 16384)?;
                value.validate()?;
                if value.id != *id {
                    return Err(Error::Command("message permanent identity differs"));
                }
                Ok(Some(value))
            }
            _ => Err(Error::Command("invalid message row")),
        },
        _ => Err(Error::Command("message uniqueness violated")),
    }
}
/// Atomic local conversation, assignment, resolution, and deadline intent.
pub struct ChangeTicket;
impl Command for ChangeTicket {
    const MODULE: &'static str = Tickets::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Change,
    ) -> Result<CommandResult<Outcome>> {
        if input.action.validate().is_err()
            || context.target() != &ticket_target(context.target(), &input.ticket)?
        {
            return Ok(outcome(Decision::Invalid, None));
        }
        let existing = load(|batch| context.sql(batch), Some(context.target()))?;
        if let Action::Open {
            subject,
            requester,
            due_at_ms,
            endpoint,
        } = input.action
        {
            if let Some(ticket) = existing {
                let original = sql::one(context.sql(&sql::batch(
                    "SELECT ticket FROM opening WHERE singleton=1",
                    vec![],
                ))?)?
                .rows;
                let [row] = original.as_slice() else {
                    return Err(Error::Command("missing opening binding"));
                };
                let [SqlValue::Blob(bytes)] = row.as_slice() else {
                    return Err(Error::Command("invalid opening binding"));
                };
                let action = Action::Open {
                    subject,
                    requester,
                    due_at_ms,
                    endpoint,
                };
                return Ok(outcome(
                    if wire::json(&action, 4096)? == *bytes {
                        Decision::Duplicate
                    } else {
                        Decision::Conflict
                    },
                    Some(ticket),
                ));
            }
            if !due(context.now_ms(), due_at_ms) {
                return Ok(outcome(Decision::Invalid, None));
            }
            let opening = Action::Open {
                subject: subject.clone(),
                requester: requester.clone(),
                due_at_ms,
                endpoint: endpoint.clone(),
            };
            let mut ticket = Ticket {
                key: input.ticket.clone(),
                subject,
                requester,
                status: Status::Open,
                agent: None,
                revision: 1,
                generation: 1,
                deadline: Deadline {
                    ticket: input.ticket,
                    generation: 1,
                    due_at_ms,
                },
                escalated: false,
                endpoint,
                deadline_effect: [0; 32],
                message_count: 0,
                attachments: vec![],
                notifications: vec![],
            };
            install_deadline(context, &mut ticket, due_at_ms)?;
            ticket.validate()?;
            sql::changed(context.sql(&sql::batch(
                "INSERT INTO ticket(singleton,ticket_key,state) VALUES(1,?1,?2)",
                vec![
                    SqlValue::Text(ticket.key.as_str().into()),
                    SqlValue::Blob(wire::json(&ticket, 131072)?),
                ],
            ))?)?;
            sql::changed(context.sql(&sql::batch(
                "INSERT INTO opening(singleton,ticket) VALUES(1,?1)",
                vec![SqlValue::Blob(wire::json(&opening, 4096)?)],
            ))?)?;
            return Ok(outcome(Decision::Applied, Some(ticket)));
        }
        let Some(mut ticket) = existing else {
            return Ok(outcome(Decision::NotFound, None));
        };
        // Permanent identity lookup precedes revision and state checks, preserving
        // exact retries even after resolution or when permanent history is full.
        if let Action::Message { message: value, .. } = &input.action
            && let Some(original) = message(|batch| context.sql(batch), &value.id)?
        {
            return Ok(outcome(
                if original == *value {
                    Decision::Duplicate
                } else {
                    Decision::Conflict
                },
                Some(ticket),
            ));
        }
        let expected = match &input.action {
            Action::Message {
                expected_revision, ..
            }
            | Action::Assign {
                expected_revision, ..
            }
            | Action::Resolve { expected_revision }
            | Action::Reopen {
                expected_revision, ..
            } => *expected_revision,
            Action::Open { .. } => return Err(Error::Command("invalid ticket dispatch")),
        };
        if ticket.revision != expected {
            return Ok(outcome(Decision::Conflict, Some(ticket)));
        }
        if matches!(input.action, Action::Reopen { .. }) != (ticket.status == Status::Resolved) {
            return Ok(outcome(Decision::WrongState, Some(ticket)));
        }
        match input.action {
            Action::Message { message, .. } => {
                if ticket.message_count as usize >= MAX_MESSAGES {
                    return Ok(outcome(Decision::Capacity, Some(ticket)));
                }
                ticket.message_count += 1;
                sql::changed(context.sql(&sql::batch(
                    "INSERT INTO messages(sequence,message_id,body) VALUES(?1,?2,?3)",
                    vec![
                        SqlValue::Integer(i64::from(ticket.message_count)),
                        SqlValue::Text(message.id.as_str().into()),
                        SqlValue::Blob(wire::json(&message, 16384)?),
                    ],
                ))?)?;
            }
            Action::Assign {
                agent, due_at_ms, ..
            } => {
                if ticket.generation >= MAX_GENERATIONS {
                    return Ok(outcome(Decision::Capacity, Some(ticket)));
                }
                if !due(context.now_ms(), due_at_ms) {
                    return Ok(outcome(Decision::Invalid, Some(ticket)));
                }
                ticket.generation += 1;
                ticket.agent = Some(agent);
                install_deadline(context, &mut ticket, due_at_ms)?;
            }
            Action::Resolve { .. } => {
                ticket.generation += 1;
                ticket.status = Status::Resolved;
                ticket.escalated = false;
                ticket.deadline_effect = [0; 32];
            }
            Action::Reopen { due_at_ms, .. } => {
                if ticket.generation >= MAX_GENERATIONS {
                    return Ok(outcome(Decision::Capacity, Some(ticket)));
                }
                if !due(context.now_ms(), due_at_ms) {
                    return Ok(outcome(Decision::Invalid, Some(ticket)));
                }
                ticket.generation += 1;
                ticket.status = Status::Open;
                install_deadline(context, &mut ticket, due_at_ms)?;
            }
            Action::Open { .. } => return Err(Error::Command("invalid ticket dispatch")),
        }
        bump(&mut ticket)?;
        save(context, &ticket)?;
        Ok(outcome(Decision::Applied, Some(ticket)))
    }
}
/// Internal signed expiration receiver; resolution and generation checks are atomic here.
pub struct EscalateTicket;
impl Command for EscalateTicket {
    const MODULE: &'static str = Tickets::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Escalation;
    type Output = EscalationOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Escalation,
    ) -> Result<CommandResult<EscalationOutcome>> {
        if input.deadline.validate().is_err()
            || input.fired_at_ms < input.deadline.due_at_ms
            || input.fired_at_ms > context.now_ms()
            || context.target() != &ticket_target(context.target(), &input.deadline.ticket)?
        {
            return Ok(CommandResult::Rejected(EscalationOutcome::Invalid));
        }
        let Some(mut ticket) = load(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Success(EscalationOutcome::Unchanged));
        };
        if ticket.status != Status::Open
            || ticket.generation != input.deadline.generation
            || ticket.escalated
        {
            return Ok(CommandResult::Success(EscalationOutcome::Unchanged));
        }
        if ticket.deadline != input.deadline {
            return Ok(CommandResult::Rejected(EscalationOutcome::Invalid));
        }
        let notification = Notification {
            deadline: ticket.deadline.clone(),
            source_cell: *context.target().cell_id().as_bytes(),
            agent: ticket.agent.clone(),
            endpoint: ticket.endpoint.clone(),
            escalated_at_ms: context.now_ms(),
        };
        notification.validate()?;
        let effect_id = intent(
            context,
            NOTIFICATIONS,
            ScheduleNotification::ID,
            &notification,
        )?;
        ticket.notifications.push(EscalationRecord {
            notification,
            effect_id,
        });
        ticket.escalated = true;
        bump(&mut ticket)?;
        save(context, &ticket)?;
        Ok(CommandResult::Success(EscalationOutcome::Escalated))
    }
}
/// Verified immutable-reference receiver; no public raw-link API bypasses Blob verification.
pub struct LinkAttachment;
impl Command for LinkAttachment {
    const MODULE: &'static str = Tickets::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = AttachmentLink;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: AttachmentLink,
    ) -> Result<CommandResult<Outcome>> {
        input.publication.descriptor.validate()?;
        revision(input.expected_revision)?;
        if input.publication.etag == [0; 32]
            || context.target()
                != &ticket_target(context.target(), &input.publication.descriptor.ticket)?
        {
            return Ok(outcome(Decision::Invalid, None));
        }
        let Some(mut ticket) = load(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(outcome(Decision::NotFound, None));
        };
        if let Some(original) = ticket
            .attachments
            .iter()
            .find(|v| v.descriptor.id == input.publication.descriptor.id)
        {
            return Ok(outcome(
                if *original == input.publication {
                    Decision::Duplicate
                } else {
                    Decision::Conflict
                },
                Some(ticket),
            ));
        }
        let rejection = if ticket.revision != input.expected_revision {
            Some(Decision::Conflict)
        } else if ticket.status != Status::Open {
            Some(Decision::WrongState)
        } else if ticket.attachments.len() >= MAX_ATTACHMENTS {
            Some(Decision::Capacity)
        } else {
            None
        };
        if let Some(rejection) = rejection {
            return Ok(outcome(rejection, Some(ticket)));
        }
        ticket.attachments.push(input.publication);
        bump(&mut ticket)?;
        save(context, &ticket)?;
        Ok(outcome(Decision::Applied, Some(ticket)))
    }
}
/// Coherent metadata query in the ticket receipt domain.
pub struct GetTicket;
impl Query for GetTicket {
    const MODULE: &'static str = Tickets::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<Ticket>;
    fn execute(context: &mut QueryContext<'_>, _: ()) -> Result<Option<Ticket>> {
        load(|batch| context.sql(batch), None)
    }
}
/// Bounded keyset conversation query with coherent ticket metadata.
pub struct ListMessages;
impl Query for ListMessages {
    const MODULE: &'static str = Tickets::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = MessagePage;
    fn execute(context: &mut QueryContext<'_>, page: PageRequest) -> Result<MessagePage> {
        if !(1..=16).contains(&page.limit) || page.after > MAX_MESSAGES as u32 {
            return Err(Error::Command("invalid support conversation page"));
        }
        let ticket = load(|batch| context.sql(batch), None)?;
        let selected = sql::one(context.sql(&sql::batch(
            "SELECT sequence,body FROM messages WHERE sequence>?1 ORDER BY sequence LIMIT ?2",
            vec![
                SqlValue::Integer(i64::from(page.after)),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?)?;
        let mut messages = Vec::with_capacity(selected.rows.len());
        for row in selected.rows {
            let [SqlValue::Integer(sequence), SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid conversation row"));
            };
            let sequence = u32::try_from(*sequence)
                .map_err(|_| Error::Command("invalid conversation sequence"))?;
            let message: Message = wire::from_json(bytes, 16384)?;
            message.validate()?;
            if sequence <= page.after || sequence > ticket.as_ref().map_or(0, |t| t.message_count) {
                return Err(Error::Command("conversation metadata differs"));
            }
            messages.push(MessageEntry { sequence, message });
        }
        let more = messages.len() > page.limit as usize;
        messages.truncate(page.limit as usize);
        let next = if more {
            messages.last().map(|v| v.sequence)
        } else {
            None
        };
        Ok(MessagePage {
            ticket,
            messages,
            next,
        })
    }
}
