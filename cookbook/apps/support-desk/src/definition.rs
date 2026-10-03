use crate::{sql, wire, *};
use cellule_runtime::{
    CellModule, Digest, Error, partition_for_shard,
    primitives::{
        effects::EffectCommandIntent,
        sql::SqlValue,
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowOutcome,
            WorkflowStart, WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
};

pub(crate) struct DeadlineDefinition;
pub(crate) static DEFINITION: DeadlineDefinition = DeadlineDefinition;
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.support-desk.deadline-definition.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
impl WorkflowDefinition for DeadlineDefinition {
    fn digest(&self) -> Digest {
        digest()
    }
    fn effect_targets(&self) -> &'static [cellule_runtime::NamespaceId] {
        &[TICKETS]
    }
    fn transition(
        &self,
        bytes: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        let mut state = if bytes.is_empty() {
            let ticket: Deadline = wire::decode(event, 2048)?;
            ticket.validate()?;
            DeadlineState {
                ticket,
                timer_id: None,
                fired_at_ms: None,
            }
        } else {
            let state: DeadlineState = wire::from_json(bytes, 4096)?;
            state.ticket.validate()?;
            let timer = event
                .strip_prefix(b"timer\0")
                .ok_or(Error::Command("support deadline accepts only its timer"))?;
            if timer.len() != 16
                || state.timer_id.as_deref() != Some(timer)
                || state.fired_at_ms.is_some()
            {
                return Err(Error::Command("unexpected support deadline timer"));
            }
            state
        };
        if context.now_ms() < state.ticket.due_at_ms {
            if state.timer_id.is_some() {
                return Err(Error::Command("support timer fired early"));
            }
            state.timer_id = Some(context.action_id(0).to_vec());
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: wire::json(&state, 4096)?,
                result: None,
                actions: vec![WorkflowAction::Timer {
                    due_at_ms: state.ticket.due_at_ms,
                }],
            });
        }
        state.fired_at_ms = Some(context.now_ms());
        let callback = Escalation {
            deadline: state.ticket.clone(),
            fired_at_ms: context.now_ms(),
        };
        let intent = EffectCommandIntent {
            target: ticket_target(context.source(), &state.ticket.ticket)?,
            command_id: EscalateTicket::ID,
            codec_version: 1,
            input: wire::encode(&callback, 4096)?,
            expires_at_ms: context
                .now_ms()
                .checked_add(MAX_DEADLINE_MS)
                .ok_or(Error::Command("support callback expiry overflow"))?,
        };
        Ok(WorkflowDecision {
            status: WorkflowStatus::Completed,
            state: wire::json(&state, 4096)?,
            result: Some(wire::encode(&callback, 4096)?),
            actions: vec![WorkflowAction::Effect { intent }],
        })
    }
}
/// Signed start receiver permanently binding one ticket generation to its exact deadline.
pub struct ScheduleDeadline;
impl Command for ScheduleDeadline {
    const MODULE: &'static str = Deadlines::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = Deadline;
    type Output = WorkflowOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        ticket: Deadline,
    ) -> cellule_runtime::Result<CommandResult<WorkflowOutcome>> {
        ticket.validate()?;
        if context.target().partition() != partition_for_shard(0) {
            return Err(Error::Identity("foreign support deadline shard"));
        }
        let key = ticket.key();
        let encoded = wire::encode(&ticket, 2048)?;
        let rows = sql::one(context.sql(&sql::batch(
            "SELECT ticket FROM support_bindings WHERE workflow_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)?
        .rows;
        match rows.as_slice() {
            [] => {}
            [row] => {
                let [SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("invalid support deadline binding"));
                };
                if *original != encoded {
                    return Err(Error::Command(
                        "deadline generation already binds different bytes",
                    ));
                }
                return Ok(CommandResult::Success(WorkflowOutcome::AlreadyExists));
            }
            _ => {
                return Err(Error::Command(
                    "support deadline binding uniqueness violated",
                ));
            }
        }
        if sql::count(context.sql(&sql::batch("SELECT count(*) FROM support_bindings", vec![]))?)?
            >= 4096
        {
            return Err(Error::Command(
                "support deadline binding capacity exhausted",
            ));
        }
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO support_bindings(workflow_key,ticket) VALUES(?1,?2)",
            vec![
                SqlValue::Blob(key.to_vec()),
                SqlValue::Blob(encoded.clone()),
            ],
        ))?)?;
        let mut id = [0; 16];
        id.copy_from_slice(&key[..16]);
        match WorkflowStartCommand::<Deadlines>::execute(
            context,
            WorkflowStart {
                workflow_id: key.to_vec(),
                request_id: cellule_runtime::identity::RequestId::from_bytes(id),
                event: encoded,
            },
        )? {
            CommandResult::Success(outcome @ WorkflowOutcome::Applied { .. }) => {
                Ok(CommandResult::Success(outcome))
            }
            _ => Err(Error::Command(
                "support deadline has an unbound native run; start rolled back",
            )),
        }
    }
}
