use crate::{
    Action, Change, DEADLINES, DeadlineTicket, Decision, Expiration, ExpirationOutcome, Hold,
    HoldState, InventoryCells, MAX_HOLD_MS, MAX_HOLDS, Outcome, ScheduleDeadline, domain_target,
    sql,
};
use cellule_runtime::{
    CellModule, CellTarget, Error,
    codec::{BoundedEncoder, WireValue},
    identity::RequestId,
    partition_for_shard,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult},
    shard_for_scope,
};
fn decision(value: Decision, hold: Option<Hold>) -> CommandResult<Outcome> {
    let output = Outcome {
        decision: value,
        hold,
    };
    if value.success() {
        CommandResult::Success(output)
    } else {
        CommandResult::Rejected(output)
    }
}
/// Atomic seat allocation, generation changes, buyer settlement, and deadline intent.
pub struct ChangeReservation;
impl Command for ChangeReservation {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Change,
    ) -> cellule_runtime::Result<CommandResult<Outcome>> {
        if input.action.validate().is_err()
            || context.target() != &domain_target(context.target(), &input.event)?
        {
            return Ok(decision(Decision::Invalid, None));
        }
        let existing = sql::event(&context.sql(&sql::event_query())?)?;
        if existing.as_ref().is_some_and(|v| v.0 != input.event) {
            return Err(Error::Command("stored event differs from entity target"));
        }
        if let Action::Initialize { seats } = input.action {
            if existing.is_some() {
                return Ok(decision(Decision::AlreadyInitialized, None));
            }
            sql::changed(&context.sql(&sql::batch(
                "INSERT INTO event(singleton,event_key,seat_count,revision) VALUES(1,?1,?2,1)",
                vec![
                    SqlValue::Text(input.event.as_str().into()),
                    SqlValue::Integer(i64::from(seats)),
                ],
            ))?)?;
            let result=context.sql(&sql::batch("WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<?1) INSERT INTO seats(number,generation) SELECT n,0 FROM numbers",vec![SqlValue::Integer(i64::from(seats))]))?;
            if !matches!(result.as_slice(),[r] if r.rows_affected==u64::from(seats)) {
                return Err(Error::Command("seat initialization count violated"));
            }
            return Ok(decision(Decision::Initialized, None));
        }
        let Some((_, seats)) = existing else {
            return Ok(decision(Decision::NotFound, None));
        };
        match input.action {
            Action::Hold {
                id,
                seat,
                buyer,
                deadline_ms,
            } => {
                if let Some(hold) = sql::hold(&context.sql(&sql::hold_query(id))?, &input.event)? {
                    if hold.buyer != buyer {
                        return Ok(decision(Decision::Forbidden, None));
                    }
                    let matching =
                        hold.ticket.seat == seat && hold.ticket.deadline_ms == deadline_ms;
                    return Ok(decision(
                        if matching {
                            Decision::ExistingHold
                        } else {
                            Decision::Conflict
                        },
                        Some(hold),
                    ));
                }
                if seat > seats
                    || deadline_ms <= context.now_ms()
                    || deadline_ms
                        > context
                            .now_ms()
                            .checked_add(MAX_HOLD_MS)
                            .ok_or(Error::Command("deadline bound overflow"))?
                {
                    return Ok(decision(Decision::Invalid, None));
                }
                let count = context.sql(&sql::batch("SELECT count(*) FROM holds", vec![]))?;
                let [row] = sql::rows(&count)? else {
                    return Err(Error::Command("missing hold count"));
                };
                let [SqlValue::Integer(count)] = row.as_slice() else {
                    return Err(Error::Command("invalid hold count"));
                };
                if *count >= MAX_HOLDS {
                    return Ok(decision(Decision::Capacity, None));
                }
                let occupied = sql::hold(
                    &context.sql(&sql::batch(
                        &format!(
                            "SELECT {} FROM holds WHERE seat=?1 AND state IN(0,1)",
                            sql::HOLD_FIELDS
                        ),
                        vec![SqlValue::Integer(i64::from(seat))],
                    ))?,
                    &input.event,
                )?;
                if let Some(hold) = occupied {
                    if hold.state == HoldState::Confirmed
                        || hold.ticket.deadline_ms > context.now_ms()
                    {
                        return Ok(decision(Decision::Occupied, None));
                    }
                    // Lazy expiration and replacement are atomic. The old Workflow may
                    // still fire later; its permanent ID and generation cannot match this hold.
                    sql::transition(context, hold.ticket.id, HoldState::Expired)?;
                }
                let selected = context.sql(&sql::batch(
                    "SELECT generation FROM seats WHERE number=?1",
                    vec![SqlValue::Integer(i64::from(seat))],
                ))?;
                let [row] = sql::rows(&selected)? else {
                    return Err(Error::Command("missing seat generation"));
                };
                let [SqlValue::Integer(generation)] = row.as_slice() else {
                    return Err(Error::Command("invalid seat generation"));
                };
                let generation = generation
                    .checked_add(1)
                    .ok_or(Error::Command("seat generation overflow"))?;
                let ticket = DeadlineTicket {
                    event: input.event,
                    id,
                    seat,
                    generation,
                    deadline_ms,
                };
                let mut encoder = BoundedEncoder::new(1024)?;
                ticket.encode(&mut encoder)?;
                let effect = context.emit_effect(&EffectCommandIntent {
                    target: CellTarget::new(
                        context.target().tenant(),
                        context.target().application(),
                        DEADLINES,
                        &partition_for_shard(shard_for_scope(DEADLINES, &ticket.workflow_id(), 2)?),
                    )?,
                    command_id: ScheduleDeadline::ID,
                    codec_version: 1,
                    input: encoder.finish(),
                    expires_at_ms: context
                        .now_ms()
                        .checked_add(7 * 24 * 60 * 60 * 1000)
                        .ok_or(Error::Command("deadline intent retention overflow"))?,
                })?;
                sql::changed(&context.sql(&sql::batch(
                    "UPDATE seats SET generation=?1 WHERE number=?2 AND generation=?3",
                    vec![
                        SqlValue::Integer(generation),
                        SqlValue::Integer(i64::from(seat)),
                        SqlValue::Integer(generation - 1),
                    ],
                ))?)?;
                sql::changed(&context.sql(&sql::batch("INSERT INTO holds(id,seat,generation,buyer,deadline_ms,state,start_effect) VALUES(?1,?2,?3,?4,?5,0,?6)",vec![SqlValue::Blob(id.as_bytes().to_vec()),SqlValue::Integer(i64::from(seat)),SqlValue::Integer(generation),SqlValue::Text(buyer.as_str().into()),SqlValue::Integer(deadline_ms),SqlValue::Blob(effect.to_vec())]))?)?;
                sql::bump_revision(context)?;
                Ok(decision(
                    Decision::Held,
                    Some(Hold {
                        ticket,
                        buyer,
                        state: HoldState::Held,
                        start_effect: effect.to_vec(),
                    }),
                ))
            }
            action @ (Action::Confirm { .. } | Action::Cancel { .. }) => {
                let confirming = matches!(action, Action::Confirm { .. });
                let (id, generation, buyer) = match action {
                    Action::Confirm {
                        id,
                        generation,
                        buyer,
                    }
                    | Action::Cancel {
                        id,
                        generation,
                        buyer,
                    } => (id, generation, buyer),
                    _ => return Err(Error::Command("invalid settlement dispatch")),
                };
                let Some(mut hold) = sql::hold(&context.sql(&sql::hold_query(id))?, &input.event)?
                else {
                    return Ok(decision(Decision::NotFound, None));
                };
                if buyer != hold.buyer {
                    return Ok(decision(Decision::Forbidden, None));
                }
                if generation != hold.ticket.generation {
                    return Ok(decision(Decision::Conflict, Some(hold)));
                }
                let terminal = if confirming {
                    HoldState::Confirmed
                } else {
                    HoldState::Cancelled
                };
                if hold.state == terminal {
                    return Ok(decision(
                        if confirming {
                            Decision::AlreadyConfirmed
                        } else {
                            Decision::AlreadyCancelled
                        },
                        Some(hold),
                    ));
                }
                if hold.state == HoldState::Expired {
                    return Ok(decision(Decision::Expired, Some(hold)));
                }
                if hold.state != HoldState::Held {
                    return Ok(decision(Decision::Closed, Some(hold)));
                }
                // Deadline precedence uses recorded command time, even if asynchronous
                // timer delivery is delayed. Persist this expiry alongside the rejection.
                if hold.ticket.deadline_ms <= context.now_ms() {
                    sql::transition(context, id, HoldState::Expired)?;
                    hold.state = HoldState::Expired;
                    return Ok(decision(Decision::Expired, Some(hold)));
                }
                sql::transition(context, id, terminal)?;
                hold.state = terminal;
                Ok(decision(
                    if confirming {
                        Decision::Confirmed
                    } else {
                        Decision::Cancelled
                    },
                    Some(hold),
                ))
            }
            _ => Err(Error::Command("invalid inventory dispatch")),
        }
    }
}
/// Internal deadline receiver. The embedding permits only signed deadline delivery here.
pub struct ExpireHold;
impl Command for ExpireHold {
    const MODULE: &'static str = InventoryCells::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Expiration;
    type Output = ExpirationOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Expiration,
    ) -> cellule_runtime::Result<CommandResult<ExpirationOutcome>> {
        if input.ticket.validate().is_err()
            || input.fired_at_ms < input.ticket.deadline_ms
            || context.target() != &domain_target(context.target(), &input.ticket.event)?
        {
            return Ok(CommandResult::Rejected(ExpirationOutcome::Invalid));
        }
        let Some(hold) = sql::hold(
            &context.sql(&sql::hold_query(input.ticket.id))?,
            &input.ticket.event,
        )?
        else {
            return Ok(CommandResult::Success(ExpirationOutcome::Unchanged));
        };
        if hold.ticket != input.ticket {
            return Ok(CommandResult::Rejected(ExpirationOutcome::Invalid));
        }
        if hold.state != HoldState::Held {
            return Ok(CommandResult::Success(ExpirationOutcome::Unchanged));
        }
        sql::transition(context, input.ticket.id, HoldState::Expired)?;
        Ok(CommandResult::Success(ExpirationOutcome::Expired))
    }
}
// The native Workflow start request identity is derived from event + hold, not
// reused from the original user command. Separate event-local hold IDs cannot collide.
pub(crate) fn deadline_request_id(ticket: &DeadlineTicket) -> RequestId {
    let key = ticket.workflow_id();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&key[..16]);
    RequestId::from_bytes(bytes)
}
