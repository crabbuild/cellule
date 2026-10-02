use crate::{
    DeliveryOutcome, Flows, MAX_MESSAGES, MAX_RECONCILIATIONS, MAX_RESOURCES, Reconcile, Reply,
    ReplyValue, Spec,
    model::{Start, encode},
    sql,
    wire::encode_wire,
};
use cellule_runtime::{
    CellModule, Error,
    identity::RequestId,
    primitives::{
        sql::SqlValue,
        workflow::{
            WorkflowOutcome, WorkflowSignal, WorkflowSignalCommand, WorkflowStart,
            WorkflowStartCommand, WorkflowStatus,
        },
    },
    registry::{Command, CommandContext, CommandResult},
};
fn binding(
    context: &CommandContext<'_, '_>,
    id: crate::Id,
) -> cellule_runtime::Result<Option<([u8; 16], bool)>> {
    let results = context.sql(&sql::batch(
        "SELECT run_id,terminal FROM flow_bindings WHERE resource_id=?1",
        vec![sql::id(id)],
    ))?;
    match sql::rows(&results)? {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Blob(bytes), SqlValue::Integer(terminal)] = row.as_slice() else {
                return Err(Error::Command("invalid resource Flow binding"));
            };
            if !matches!(terminal, 0 | 1) {
                return Err(Error::Command("invalid resource Flow terminal flag"));
            }
            Ok(Some((
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Identity("invalid Flow run identity"))?,
                *terminal == 1,
            )))
        }
        _ => Err(Error::Command("resource Flow binding uniqueness violated")),
    }
}
fn signal(
    context: &mut CommandContext<'_, '_>,
    id: crate::Id,
    run_id: [u8; 16],
    signal_id: [u8; 16],
    event: Vec<u8>,
) -> cellule_runtime::Result<WorkflowOutcome> {
    let result = WorkflowSignalCommand::<Flows>::execute(
        context,
        WorkflowSignal {
            workflow_id: id.bytes().to_vec(),
            run_id,
            signal_id,
            event,
        },
    )?;
    let CommandResult::Success(outcome @ WorkflowOutcome::Applied { .. }) = result else {
        return Err(Error::Command(
            "resource Flow signal not applied; binding rolled back",
        ));
    };
    Ok(outcome)
}
/// Permanent immutable start binding; early deletion intent is read atomically.
pub struct StartResource;
impl Command for StartResource {
    const MODULE: &'static str = Flows::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = Spec;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: Spec,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        spec.validate()?;
        let bytes = encode(&spec)?;
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM flow_bindings WHERE resource_id=?1",
            vec![sql::id(spec.id)],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM flow_bindings", vec![]))?)?
            >= MAX_RESOURCES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let deletion = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM deletion_bindings WHERE resource_id=?1",
            vec![sql::id(spec.id)],
        ))?)?;
        if deletion.as_ref().is_some_and(|original| original != &bytes) {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        let result = WorkflowStartCommand::<Flows>::execute(
            context,
            WorkflowStart {
                workflow_id: spec.id.bytes().to_vec(),
                request_id: RequestId::from_bytes(spec.id.bytes()),
                event: encode(&Start {
                    spec: spec.clone(),
                    delete_requested: deletion.is_some(),
                })?,
            },
        )?;
        let CommandResult::Success(WorkflowOutcome::Applied { run_id, .. }) = result else {
            return Err(Error::Command(
                "unbound native resource Flow; publication rolled back",
            ));
        };
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO flow_bindings(resource_id,spec,run_id,terminal) VALUES(?1,?2,?3,0)",
            vec![
                sql::id(spec.id),
                SqlValue::Blob(bytes),
                SqlValue::Blob(run_id.to_vec()),
            ],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Signed cancellation/deprovisioning receiver; controls may arrive before start.
pub struct DeleteResource;
impl Command for DeleteResource {
    const MODULE: &'static str = Flows::NAME;
    const ID: u32 = 12;
    const CODEC_VERSION: u32 = 1;
    type Input = Spec;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: Spec,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        spec.validate()?;
        let bytes = encode(&spec)?;
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM deletion_bindings WHERE resource_id=?1",
            vec![sql::id(spec.id)],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM flow_bindings WHERE resource_id=?1",
            vec![sql::id(spec.id)],
        ))?)?
            && original != bytes
        {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM deletion_bindings",
            vec![],
        ))?)?
            >= MAX_RESOURCES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        if let Some((run_id, false)) = binding(context, spec.id)? {
            let mut hash = blake3::Hasher::new();
            hash.update(b"cookbook.provisioning.delete-signal.v1\0");
            hash.update(&spec.id.bytes());
            let mut id = [0; 16];
            id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
            let mut event = crate::definition::DELETE_PREFIX.to_vec();
            event.extend(encode_wire(&spec, 2048)?);
            signal(context, spec.id, run_id, id, event)?;
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO deletion_bindings(resource_id,spec) VALUES(?1,?2)",
            vec![sql::id(spec.id), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Permanent callback receiver; native completion and terminal binding publish together.
pub struct ReplyResource;
impl Command for ReplyResource {
    const MODULE: &'static str = Flows::NAME;
    const ID: u32 = 13;
    const CODEC_VERSION: u32 = 1;
    type Input = Reply;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        reply: Reply,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        reply.validate()?;
        let key = reply.call.key();
        let bytes = encode(&reply)?;
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT reply FROM flow_replies WHERE message_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        let Some((run_id, _)) = binding(context, reply.call.spec.id)? else {
            return Ok(sql::classify(DeliveryOutcome::NotFound));
        };
        if run_id != reply.call.run_id {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM flow_replies", vec![]))?)?
            >= MAX_MESSAGES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let mut id = [0; 16];
        id.copy_from_slice(&key[..16]);
        let mut event = crate::definition::REPLY_PREFIX.to_vec();
        event.extend(encode_wire(&reply, 4096)?);
        let outcome = signal(context, reply.call.spec.id, run_id, id, event)?;
        if reply.value == ReplyValue::Deleted {
            if !matches!(
                outcome,
                WorkflowOutcome::Applied {
                    status: WorkflowStatus::Completed,
                    ..
                }
            ) {
                return Err(Error::Command(
                    "resource deletion did not complete native lifetime",
                ));
            }
            sql::changed(&context.sql(&sql::batch("UPDATE flow_bindings SET terminal=1 WHERE resource_id=?1 AND run_id=?2 AND terminal=0",vec![sql::id(reply.call.spec.id),SqlValue::Blob(run_id.to_vec())]))?)?;
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO flow_replies(message_key,reply) VALUES(?1,?2)",
            vec![SqlValue::Blob(key.to_vec()), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Bounded operator reconciliation preserving the same provider business operations.
pub struct ReconcileResource;
impl Command for ReconcileResource {
    const MODULE: &'static str = Flows::NAME;
    const ID: u32 = 18;
    const CODEC_VERSION: u32 = 1;
    type Input = Reconcile;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Reconcile,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM reconciliations WHERE resource_id=?1 AND token=?2",
            vec![sql::id(input.resource), sql::id(input.token)],
        ))?)?
            == 1
        {
            return Ok(CommandResult::Success(DeliveryOutcome::Applied));
        }
        let Some((run_id, false)) = binding(context, input.resource)? else {
            return Ok(sql::classify(DeliveryOutcome::InvalidState));
        };
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM reconciliations WHERE resource_id=?1",
            vec![sql::id(input.resource)],
        ))?)?
            >= i64::from(MAX_RECONCILIATIONS)
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.provisioning.reconcile-signal.v1\0");
        hash.update(&input.resource.bytes());
        hash.update(&input.token.bytes());
        let mut id = [0; 16];
        id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        let mut event = crate::definition::RECONCILE_PREFIX.to_vec();
        event.extend(encode_wire(&input, 512)?);
        signal(context, input.resource, run_id, id, event)?;
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO reconciliations(resource_id,token) VALUES(?1,?2)",
            vec![sql::id(input.resource), sql::id(input.token)],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
