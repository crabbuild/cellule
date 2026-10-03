use crate::{
    DeadlineState, DeadlineTicket, Deadlines, Expiration, ExpireHold, INVENTORY, domain_target,
};
use cellule_runtime::{
    CellModule, Digest, Error,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    partition_for_shard,
    primitives::{
        effects::EffectCommandIntent,
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowStart,
            WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
    shard_for_scope,
};
pub(crate) struct DeadlineDefinition;
pub(crate) static DEFINITION: DeadlineDefinition = DeadlineDefinition;
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.reservation.workflow.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
pub(crate) fn encode<T: WireValue>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(2048)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
pub(crate) fn decode<T: WireValue>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, 2048)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
impl WorkflowDefinition for DeadlineDefinition {
    fn digest(&self) -> Digest {
        digest()
    }
    fn effect_targets(&self) -> &'static [cellule_runtime::NamespaceId] {
        &[INVENTORY]
    }
    fn transition(
        &self,
        state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        let mut state = if state.is_empty() {
            let ticket: DeadlineTicket = decode(event)?;
            ticket.validate()?;
            DeadlineState {
                ticket,
                timer_id: None,
                fired_at_ms: None,
            }
        } else {
            let value: DeadlineState = decode(state)?;
            value.ticket.validate()?;
            let timer = event
                .strip_prefix(b"timer\0")
                .ok_or(Error::Command("deadline accepts only its native timer"))?;
            if value.timer_id.as_deref() != Some(timer)
                || timer.len() != 16
                || value.fired_at_ms.is_some()
            {
                return Err(Error::Command("unexpected deadline timer"));
            }
            value
        };
        if context.now_ms() < state.ticket.deadline_ms {
            if state.timer_id.is_some() {
                return Err(Error::Command(
                    "deadline timer fired before recorded due time",
                ));
            }
            state.timer_id = Some(context.action_id(0).to_vec());
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: encode(&state)?,
                result: None,
                actions: vec![WorkflowAction::Timer {
                    due_at_ms: state.ticket.deadline_ms,
                }],
            });
        }
        state.fired_at_ms = Some(context.now_ms());
        let expiration = Expiration {
            ticket: state.ticket.clone(),
            fired_at_ms: context.now_ms(),
        };
        let intent = EffectCommandIntent {
            target: domain_target(context.source(), &state.ticket.event)?,
            command_id: ExpireHold::ID,
            codec_version: 1,
            input: encode(&expiration)?,
            expires_at_ms: context
                .now_ms()
                .checked_add(7 * 24 * 60 * 60 * 1000)
                .ok_or(Error::Command("expiration retention overflow"))?,
        };
        Ok(WorkflowDecision {
            status: WorkflowStatus::Completed,
            state: encode(&state)?,
            result: Some(encode(&expiration)?),
            actions: vec![WorkflowAction::Effect { intent }],
        })
    }
}
/// Internal deadline installation receiver; peer admission permits only committed source intents.
pub struct ScheduleDeadline;
impl Command for ScheduleDeadline {
    const MODULE: &'static str = Deadlines::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = DeadlineTicket;
    type Output = cellule_runtime::primitives::workflow::WorkflowOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        ticket: DeadlineTicket,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        ticket.validate()?;
        let key = ticket.workflow_id();
        if context.target().partition()
            != partition_for_shard(shard_for_scope(crate::DEADLINES, &key, 2)?)
        {
            return Err(Error::Identity(
                "deadline target differs from event and hold routing",
            ));
        }
        // Application-owned binding preserves the immutable ticket even after
        // native terminal history ages out. It publishes in the same transaction
        // as the first native start; no protected primitive tables are accessed.
        let encoded = encode(&ticket)?;
        let selected = context.sql(&crate::sql::batch(
            "SELECT ticket FROM deadline_bindings WHERE workflow_key=?1",
            vec![cellule_runtime::primitives::sql::SqlValue::Blob(
                key.to_vec(),
            )],
        ))?;
        match crate::sql::rows(&selected)? {
            [] => {}
            [row] => {
                let [cellule_runtime::primitives::sql::SqlValue::Blob(original)] = row.as_slice()
                else {
                    return Err(Error::Command("invalid deadline binding"));
                };
                if original != &encoded {
                    return Err(Error::Command("existing deadline ticket differs"));
                }
                return Ok(CommandResult::Success(
                    cellule_runtime::primitives::workflow::WorkflowOutcome::AlreadyExists,
                ));
            }
            _ => return Err(Error::Command("deadline binding uniqueness violated")),
        }
        crate::sql::changed(&context.sql(&crate::sql::batch(
            "INSERT INTO deadline_bindings(workflow_key,ticket) VALUES(?1,?2)",
            vec![
                cellule_runtime::primitives::sql::SqlValue::Blob(key.to_vec()),
                cellule_runtime::primitives::sql::SqlValue::Blob(encoded.clone()),
            ],
        ))?)?;
        let input = WorkflowStart {
            workflow_id: key.to_vec(),
            request_id: crate::commands::deadline_request_id(&ticket),
            event: encoded,
        };
        match WorkflowStartCommand::<Deadlines>::execute(context, input)? {
            CommandResult::Success(
                outcome @ cellule_runtime::primitives::workflow::WorkflowOutcome::Applied { .. },
            ) => Ok(CommandResult::Success(outcome)),
            _ => Err(Error::Command(
                "deadline has an unbound native run; installation rolled back",
            )),
        }
    }
}
