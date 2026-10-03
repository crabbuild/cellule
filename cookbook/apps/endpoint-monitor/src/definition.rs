use crate::{
    Check, Health, MAX_CHECKS, Probe, ProbeState, Probes, RecordCheck, StartOutcome, Ticket,
    model::{decode, encode},
    sql,
    wire::{decode_wire, encode_wire},
};
use cellule_runtime::{
    CellModule, CellTarget, Digest, Error,
    identity::RequestId,
    partition_for_shard,
    primitives::{
        cron::CronInvocation,
        effects::EffectCommandIntent,
        sql::SqlValue,
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowOutcome,
            WorkflowStart, WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
};
pub(crate) struct Definition;
pub(crate) static DEFINITION: Definition = Definition;
pub(crate) fn digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.monitor.workflow.v1\0");
    for source in [
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
fn completed(
    mut state: ProbeState,
    probe: Probe,
    context: &WorkflowContext,
) -> cellule_runtime::Result<WorkflowDecision> {
    probe.validate()?;
    let check = Check {
        ticket: state.ticket.clone(),
        probe,
    };
    check.validate()?;
    state.action = None;
    state.check = Some(check.clone());
    let bytes = encode(&state)?;
    // Completion and result intent publish in the Workflow transaction. SQL visibility
    // follows its separate signed delivery and cannot be inferred from this receipt.
    Ok(WorkflowDecision {
        status: WorkflowStatus::Completed,
        state: bytes.clone(),
        result: Some(bytes),
        actions: vec![WorkflowAction::Effect {
            intent: EffectCommandIntent {
                target: CellTarget::new(
                    context.source().tenant(),
                    context.source().application(),
                    crate::CHECKS,
                    &partition_for_shard(0),
                )?,
                command_id: RecordCheck::ID,
                codec_version: 1,
                input: encode_wire(&check, 4096)?,
                expires_at_ms: context
                    .now_ms()
                    .checked_add(7 * 24 * 60 * 60 * 1000)
                    .ok_or(Error::Command("check delivery expiry overflow"))?,
            },
        }],
    })
}
impl WorkflowDefinition for Definition {
    fn effect_targets(&self) -> &'static [cellule_runtime::NamespaceId] {
        &[crate::CHECKS]
    }

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
            let ticket: Ticket = decode_wire(event, 2048)?;
            ticket.validate()?;
            let deadline = ticket
                .scheduled_at_ms
                .checked_add(crate::PROBE_LIFETIME_MS)
                .ok_or(Error::Command("probe deadline overflow"))?;
            let state = ProbeState {
                ticket,
                action: Some(context.action_id(0)),
                check: None,
            };
            if context.now_ms() >= deadline {
                return completed(
                    state,
                    Probe {
                        health: Health::Unknown,
                        status: None,
                        observed_at_ms: context.now_ms(),
                        reason: "deadline".into(),
                    },
                    &context,
                );
            }
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: encode(&state)?,
                result: None,
                actions: vec![WorkflowAction::Activity {
                    activity_type: crate::activity::TYPE.into(),
                    input: encode(&state.ticket)?,
                    due_at_ms: context.now_ms(),
                    expires_at_ms: deadline,
                }],
            });
        }
        let state: ProbeState = decode(state)?;
        state.ticket.validate()?;
        let payload = event
            .strip_prefix(b"activity\0")
            .ok_or(Error::Command("unsupported monitor Workflow event"))?;
        if state.check.is_some()
            || payload.len() < 21
            || !matches!(payload[0], 0 | 1)
            || state.action.as_ref().map(|id| id.as_slice()) != Some(&payload[1..17])
        {
            return Err(Error::Command("unexpected monitor Activity completion"));
        }
        let length = u32::from_be_bytes(
            payload[17..21]
                .try_into()
                .map_err(|_| Error::Command("invalid completion length"))?,
        ) as usize;
        if length != payload.len() - 21 {
            return Err(Error::Command("monitor completion length differs"));
        }
        let probe = if payload[0] == 0 {
            decode(&payload[21..])?
        } else {
            Probe {
                health: Health::Unknown,
                status: None,
                observed_at_ms: context.now_ms(),
                reason: "execution_unknown".into(),
            }
        };
        completed(state, probe, &context)
    }
}
/// Authenticated Cron receiver permanently binding each scheduled occurrence to one native run.
pub struct StartProbe;
impl Command for StartProbe {
    const MODULE: &'static str = Probes::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = CronInvocation;
    type Output = StartOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: CronInvocation,
    ) -> cellule_runtime::Result<CommandResult<StartOutcome>> {
        let source = CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            crate::SCHEDULES,
            &partition_for_shard(0),
        )?;
        let ticket = Ticket {
            source_cell: *source.cell_id().as_bytes(),
            monitor: crate::Id::from_bytes(input.schedule_id)?,
            definition: crate::schedules::definition(&input.payload)?,
            generation: input.generation,
            occurrence: input.occurrence,
            scheduled_at_ms: input.scheduled_at_ms,
        };
        ticket.validate()?;
        let key = ticket.key();
        let bytes = encode_wire(&ticket, 2048)?;
        let selected = context.sql(&sql::batch(
            "SELECT ticket FROM probe_bindings WHERE check_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => {}
            [row] => {
                let [SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("invalid probe binding"));
                };
                return Ok(if original == &bytes {
                    CommandResult::Success(StartOutcome::AlreadyBound)
                } else {
                    CommandResult::Rejected(StartOutcome::Conflict)
                });
            }
            _ => return Err(Error::Command("probe binding uniqueness violated")),
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM probe_bindings", vec![]))?)?
            >= MAX_CHECKS
        {
            return Ok(CommandResult::Rejected(StartOutcome::Capacity));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO probe_bindings(check_key,ticket) VALUES(?1,?2)",
            vec![SqlValue::Blob(key.to_vec()), SqlValue::Blob(bytes.clone())],
        ))?)?;
        let mut request = [0; 16];
        request.copy_from_slice(&key[..16]);
        match WorkflowStartCommand::<Probes>::execute(
            context,
            WorkflowStart {
                workflow_id: key.to_vec(),
                request_id: RequestId::from_bytes(request),
                event: bytes,
            },
        )? {
            CommandResult::Success(WorkflowOutcome::Applied { run_id, .. }) => {
                Ok(CommandResult::Success(StartOutcome::Started(run_id)))
            }
            _ => Err(Error::Command(
                "unbound native probe run; binding rolled back",
            )),
        }
    }
}
