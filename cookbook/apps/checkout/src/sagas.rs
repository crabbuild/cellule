use crate::{
    DeliveryOutcome, OrderSpec, Reconcile, Reply, Sagas, model::encode, sql, wire::encode_wire,
};
use cellule_runtime::{
    CellModule, Error,
    identity::RequestId,
    primitives::{
        sql::SqlValue,
        workflow::{
            WorkflowOutcome, WorkflowSignal, WorkflowSignalCommand, WorkflowStart,
            WorkflowStartCommand,
        },
    },
    registry::{Command, CommandContext, CommandResult},
};
fn run(
    context: &CommandContext<'_, '_>,
    id: crate::Id,
) -> cellule_runtime::Result<Option<[u8; 16]>> {
    sql::blob(&context.sql(&sql::batch(
        "SELECT run_id FROM saga_bindings WHERE order_id=?1",
        vec![sql::id(id)],
    ))?)?
    .map(|bytes| {
        bytes
            .try_into()
            .map_err(|_| Error::Identity("invalid bound saga run"))
    })
    .transpose()
}
/// Signed order-start receiver with a permanent immutable business binding.
pub struct StartSaga;
impl Command for StartSaga {
    const MODULE: &'static str = Sagas::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = OrderSpec;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: OrderSpec,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        spec.validate()?;
        let bytes = encode(&spec)?;
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT spec FROM saga_bindings WHERE order_id=?1",
            vec![sql::id(spec.id)],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM saga_bindings", vec![]))?)?
            >= crate::MAX_ORDERS
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let outcome = WorkflowStartCommand::<Sagas>::execute(
            context,
            WorkflowStart {
                workflow_id: spec.id.bytes().to_vec(),
                request_id: RequestId::from_bytes(spec.id.bytes()),
                event: encode_wire(&spec, 2048)?,
            },
        )?;
        let CommandResult::Success(WorkflowOutcome::Applied { run_id, .. }) = outcome else {
            return Err(Error::Command(
                "unbound native checkout run; publication rolled back",
            ));
        };
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO saga_bindings(order_id,spec,run_id) VALUES(?1,?2,?3)",
            vec![
                sql::id(spec.id),
                SqlValue::Blob(bytes),
                SqlValue::Blob(run_id.to_vec()),
            ],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Signed callback receiver; permanent bindings outlive native inbox retention.
pub struct ReplySaga;
impl Command for ReplySaga {
    const MODULE: &'static str = Sagas::NAME;
    const ID: u32 = 12;
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
            "SELECT reply FROM saga_replies WHERE message_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        let Some(run_id) = run(context, reply.call.spec.id)? else {
            return Ok(sql::classify(DeliveryOutcome::NotFound));
        };
        if run_id != reply.call.run_id {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM saga_replies", vec![]))?)?
            >= crate::MAX_MESSAGES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let mut signal_id = [0; 16];
        signal_id.copy_from_slice(&key[..16]);
        let mut event = crate::definition::REPLY_PREFIX.to_vec();
        event.extend(encode_wire(&reply, 4096)?);
        let result = WorkflowSignalCommand::<Sagas>::execute(
            context,
            WorkflowSignal {
                workflow_id: reply.call.spec.id.bytes().to_vec(),
                run_id,
                signal_id,
                event,
            },
        )?;
        if !matches!(
            result,
            CommandResult::Success(WorkflowOutcome::Applied { .. })
        ) {
            return Err(Error::Command(
                "checkout callback not applied; binding rolled back",
            ));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO saga_replies(message_key,reply) VALUES(?1,?2)",
            vec![SqlValue::Blob(key.to_vec()), SqlValue::Blob(bytes)],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Operator reconciliation reuses the permanent external identity with a new native action.
pub struct ReconcileSaga;
impl Command for ReconcileSaga {
    const MODULE: &'static str = Sagas::NAME;
    const ID: u32 = 17;
    const CODEC_VERSION: u32 = 1;
    type Input = Reconcile;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Reconcile,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        let selected = context.sql(&sql::batch(
            "SELECT count(*) FROM reconciliations WHERE order_id=?1 AND token=?2",
            vec![sql::id(input.order), sql::id(input.token)],
        ))?;
        if sql::count(&selected)? == 1 {
            return Ok(CommandResult::Success(DeliveryOutcome::Applied));
        }
        let Some(run_id) = run(context, input.order)? else {
            return Ok(sql::classify(DeliveryOutcome::NotFound));
        };
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM reconciliations WHERE order_id=?1",
            vec![sql::id(input.order)],
        ))?)?
            >= i64::from(crate::MAX_RECONCILIATIONS)
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.checkout.reconcile.v1\0");
        hash.update(&input.order.bytes());
        hash.update(&input.token.bytes());
        let mut signal_id = [0; 16];
        signal_id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        let mut event = crate::definition::RECONCILE_PREFIX.to_vec();
        event.extend(encode_wire(&input, 512)?);
        let result = WorkflowSignalCommand::<Sagas>::execute(
            context,
            WorkflowSignal {
                workflow_id: input.order.bytes().to_vec(),
                run_id,
                signal_id,
                event,
            },
        )?;
        if !matches!(
            result,
            CommandResult::Success(WorkflowOutcome::Applied { .. })
        ) {
            return Err(Error::Command(
                "checkout reconciliation not applied; token rolled back",
            ));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO reconciliations(order_id,token) VALUES(?1,?2)",
            vec![sql::id(input.order), sql::id(input.token)],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
