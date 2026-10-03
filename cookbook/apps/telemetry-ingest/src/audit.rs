use crate::{Audit, AuditOutcome, Completion, DEVICES, MAX_AUDITS, MessageId, sql, wire};
use cellule_runtime::{
    CellModule, CellTarget, Error, Result,
    primitives::sql::{SqlBatch, SqlResultSet, SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn lookup(
    sql: impl Fn(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    message: MessageId,
) -> Result<Option<Completion>> {
    let selected = sql::one(sql(&sql::batch(
        "SELECT completion FROM batches WHERE message_id=?1",
        vec![SqlValue::Blob(message.bytes().to_vec())],
    ))?)?;
    match selected.rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("telemetry batch audit row differs"));
            };
            let value: Completion = wire::decode(bytes, 64 << 10)?;
            value.validate()?;
            if value.message != message {
                return Err(Error::Identity("telemetry audit physical message differs"));
            }
            Ok(Some(value))
        }
        _ => Err(Error::Command(
            "telemetry audit identity uniqueness violated",
        )),
    }
}
/// Permanently publishes exact original batch outcomes before Queue acknowledgment.
pub struct CompleteBatch;
impl Command for CompleteBatch {
    const MODULE: &'static str = Audit::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Completion;
    type Output = AuditOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.validate()?;
        for result in &input.results {
            if let Some(source) = &result.source {
                let target = CellTarget::new(
                    context.target().tenant(),
                    context.target().application(),
                    DEVICES,
                    &sql::partition(&result.event.device)?,
                )?;
                if source.cell != *target.cell_id().as_bytes() {
                    return Err(Error::Identity(
                        "telemetry audit source receipt targets a different device scope",
                    ));
                }
            }
        }
        if let Some(current) = lookup(|batch| context.sql(batch), input.message)? {
            // Recovery replays the original outcomes. A fresh observation may now say Duplicate.
            return Ok(if current.batch == input.batch {
                CommandResult::Success(AuditOutcome::Complete(current))
            } else {
                CommandResult::Rejected(AuditOutcome::Conflict)
            });
        }
        if sql::count(context.sql(&sql::batch("SELECT count(*) FROM batches", vec![]))?)?
            >= MAX_AUDITS as i64
        {
            return Ok(CommandResult::Rejected(AuditOutcome::Capacity));
        }
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO batches(message_id,completion) VALUES(?1,?2)",
            vec![
                SqlValue::Blob(input.message.bytes().to_vec()),
                SqlValue::Blob(wire::encode(&input, 64 << 10)?),
            ],
        ))?)?;
        Ok(CommandResult::Success(AuditOutcome::Complete(input)))
    }
}
/// Looks up complete permanent processing evidence for one physical Queue message.
pub struct GetBatch;
impl Query for GetBatch {
    const MODULE: &'static str = Audit::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = MessageId;
    type Output = Option<Completion>;
    fn execute(context: &mut QueryContext<'_>, message: Self::Input) -> Result<Self::Output> {
        lookup(|batch| context.sql(batch), message)
    }
}
