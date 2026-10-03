use crate::{
    Classification, DELIVERIES, Deliveries, DeliveryState, DeliveryTicket, HttpAttempt, MAX_ROUNDS,
    Phase,
    model::{HttpInput, decode_json, decode_wire, encode_json, encode_wire},
    sql,
};
use cellule_runtime::{
    CellModule, Digest, Error,
    identity::RequestId,
    partition_for_shard,
    primitives::{
        sql::SqlValue,
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowOutcome,
            WorkflowStart, WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
    shard_for_scope,
};
pub(crate) struct DeliveryDefinition;
pub(crate) static DEFINITION: DeliveryDefinition = DeliveryDefinition;
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.webhook.workflow.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
fn decision(
    state: DeliveryState,
    actions: Vec<WorkflowAction>,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.validate()?;
    let terminal = !matches!(state.phase, Phase::InFlight | Phase::Backoff);
    let encoded = encode_json(&state, 32768)?;
    Ok(WorkflowDecision {
        status: if terminal {
            WorkflowStatus::Completed
        } else {
            WorkflowStatus::Running
        },
        state: encoded.clone(),
        result: terminal.then_some(encoded),
        actions,
    })
}
fn dispatch(
    mut state: DeliveryState,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    state.round = state
        .round
        .checked_add(1)
        .ok_or(Error::Command("delivery round overflow"))?;
    if state.round > MAX_ROUNDS {
        return Err(Error::Command("delivery retry bound violated"));
    }
    state.phase = Phase::InFlight;
    state.action_id = Some(context.action_id(0).to_vec());
    let action = WorkflowAction::Activity {
        activity_type: crate::http_activity::HTTP_TYPE.into(),
        input: encode_json(
            &HttpInput {
                ticket: state.ticket.clone(),
                round: state.round,
            },
            8192,
        )?,
        due_at_ms: context.now_ms(),
        expires_at_ms: state.ticket.deadline_ms,
    };
    decision(state, vec![action])
}
impl WorkflowDefinition for DeliveryDefinition {
    fn digest(&self) -> Digest {
        digest()
    }
    fn transition(
        &self,
        state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if state.is_empty() {
            let ticket: DeliveryTicket = decode_wire(event, 4096)?;
            ticket.validate()?;
            let state = DeliveryState {
                ticket,
                phase: Phase::Expired,
                round: 0,
                action_id: None,
                attempts: vec![],
                failure: None,
            };
            if context.now_ms() >= state.ticket.deadline_ms {
                return decision(state, vec![]);
            }
            return dispatch(state, &context);
        }
        let mut state: DeliveryState = decode_json(state, 32768)?;
        state.validate()?;
        if let Some(id) = event.strip_prefix(b"timer\0") {
            if state.phase != Phase::Backoff
                || state.action_id.as_deref() != Some(id)
                || id.len() != 16
            {
                return Err(Error::Command("unexpected webhook retry timer"));
            }
            state.action_id = None;
            if context.now_ms() >= state.ticket.deadline_ms {
                state.phase = Phase::Expired;
                return decision(state, vec![]);
            }
            return dispatch(state, &context);
        }
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported webhook Workflow event"))?;
        if payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.phase != Phase::InFlight
            || state.action_id.as_deref() != Some(&payload[1..17])
        {
            return Err(Error::Command("unexpected webhook Activity completion"));
        }
        let length = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid webhook completion length"))?,
        ) as usize;
        if length != payload.len() - 21 {
            return Err(Error::Command("webhook completion length differs"));
        }
        state.action_id = None;
        if payload[0] == 1 {
            // Native expiry or handler failure can follow an externally applied action.
            // Preserve that uncertainty instead of asserting the receiver did no work.
            state.phase = Phase::Failed;
            state.failure = Some(crate::http_activity::bounded_details(
                &String::from_utf8_lossy(&payload[21..]),
            ));
            return decision(state, vec![]);
        }
        let attempt: HttpAttempt = decode_json(&payload[21..], 4096)?;
        attempt.validate(&state.ticket)?;
        if attempt.round != state.round {
            return Err(Error::Command("HTTP completion round differs"));
        }
        let classification = attempt.classification;
        state.attempts.push(attempt);
        match classification {
            Classification::Delivered => state.phase = Phase::Delivered,
            Classification::Permanent => state.phase = Phase::Failed,
            Classification::Retryable => {
                if state.round == MAX_ROUNDS {
                    state.phase = Phase::Exhausted;
                } else if context.now_ms() >= state.ticket.deadline_ms {
                    state.phase = Phase::Expired;
                } else {
                    state.phase = Phase::Backoff;
                    state.action_id = Some(context.action_id(0).to_vec());
                    let delay = 100_i64
                        .checked_mul(i64::from(state.round))
                        .ok_or(Error::Command("retry delay overflow"))?;
                    let due_at_ms = context
                        .now_ms()
                        .checked_add(delay)
                        .ok_or(Error::Command("retry time overflow"))?
                        .min(state.ticket.deadline_ms);
                    return decision(state, vec![WorkflowAction::Timer { due_at_ms }]);
                }
            }
        }
        decision(state, vec![])
    }
}
/// Authenticated SQL Effect receiver that permanently binds a delivery to its frozen ticket.
pub struct StartDelivery;
impl Command for StartDelivery {
    const MODULE: &'static str = Deliveries::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = DeliveryTicket;
    type Output = WorkflowOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        ticket: DeliveryTicket,
    ) -> cellule_runtime::Result<CommandResult<WorkflowOutcome>> {
        ticket.validate()?;
        let key = ticket.key();
        let source = cellule_runtime::CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            crate::FEED,
            &partition_for_shard(0),
        )?;
        if ticket.source_cell != source.cell_id().as_bytes()
            || context.target().partition()
                != partition_for_shard(shard_for_scope(DELIVERIES, &key, 2)?)
        {
            return Err(Error::Identity("delivery target or source scope differs"));
        }
        let encoded = encode_wire(&ticket, 4096)?;
        let selected = context.sql(&sql::batch(
            "SELECT ticket FROM delivery_bindings WHERE delivery_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => {}
            [row] => {
                let [SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("invalid stored delivery binding"));
                };
                if original != &encoded {
                    return Err(Error::Command(
                        "delivery key is bound to different ticket bytes",
                    ));
                }
                return Ok(CommandResult::Success(WorkflowOutcome::AlreadyExists));
            }
            _ => return Err(Error::Command("delivery binding uniqueness violated")),
        }
        // Each of the 1024 source events fans out to at most 16 permanent bindings.
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM delivery_bindings",
            vec![],
        ))?)?
            >= crate::MAX_EVENTS * crate::MAX_SUBSCRIBERS as i64
        {
            return Err(Error::Command("delivery binding capacity exhausted"));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO delivery_bindings(delivery_key,ticket) VALUES(?1,?2)",
            vec![
                SqlValue::Blob(key.to_vec()),
                SqlValue::Blob(encoded.clone()),
            ],
        ))?)?;
        let mut request = [0; 16];
        request.copy_from_slice(&key[..16]);
        match WorkflowStartCommand::<Deliveries>::execute(
            context,
            WorkflowStart {
                workflow_id: key.to_vec(),
                request_id: RequestId::from_bytes(request),
                event: encoded,
            },
        )? {
            CommandResult::Success(outcome @ WorkflowOutcome::Applied { .. }) => {
                Ok(CommandResult::Success(outcome))
            }
            _ => Err(Error::Command(
                "unbound native delivery run; installation rolled back",
            )),
        }
    }
}
