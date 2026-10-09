use crate::{Request, Runs, StartOutcome, model};
use cellule_runtime::{
    CellModule, Error,
    identity::RequestId,
    primitives::{
        sql::{SqlBatch, SqlStatement, SqlValue},
        workflow::{WorkflowOutcome, WorkflowStart, WorkflowStartCommand},
    },
    registry::{Command, CommandContext, CommandResult},
};
fn batch(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}
/// Permanently binds a caller run ID to the frozen source, transform, deadline, and endpoint.
pub struct Start;
impl Command for Start {
    const MODULE: &'static str = Runs::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = Request;
    type Output = StartOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        request: Request,
    ) -> cellule_runtime::Result<CommandResult<StartOutcome>> {
        request.validate()?;
        let encoded = model::encode(&request)?;
        let results = context.sql(&batch(
            "SELECT request FROM media_bindings WHERE run_id=?1",
            vec![SqlValue::Blob(request.id.to_vec())],
        ))?;
        let [result] = results.as_slice() else {
            return Err(Error::Command("invalid media binding results"));
        };
        match result.rows.as_slice() {
            [] => {}
            [row] => {
                let [SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("invalid media binding row"));
                };
                if original != &encoded {
                    return Ok(CommandResult::Rejected(StartOutcome::Conflict));
                }
                return Ok(CommandResult::Success(StartOutcome::AlreadyBound));
            }
            _ => return Err(Error::Command("media binding uniqueness violated")),
        }
        if request
            .deadline_ms
            .checked_sub(context.now_ms())
            .is_none_or(|window| window > 3_600_000)
        {
            return Err(Error::Command("media Activity deadline exceeds one hour"));
        }
        let counts = context.sql(&batch("SELECT count(*) FROM media_bindings", vec![]))?;
        let [result] = counts.as_slice() else {
            return Err(Error::Command("invalid media binding count result"));
        };
        let [row] = result.rows.as_slice() else {
            return Err(Error::Command("invalid media binding count rows"));
        };
        let [SqlValue::Integer(count)] = row.as_slice() else {
            return Err(Error::Command("invalid media binding count"));
        };
        if *count >= 1024 {
            return Ok(CommandResult::Rejected(StartOutcome::Capacity));
        }
        let inserted = context.sql(&batch(
            "INSERT INTO media_bindings(run_id,request) VALUES(?1,?2)",
            vec![
                SqlValue::Blob(request.id.to_vec()),
                SqlValue::Blob(encoded.clone()),
            ],
        ))?;
        if !matches!(inserted.as_slice(),[result]if result.rows_affected==1) {
            return Err(Error::Command("media binding insertion failed"));
        }
        // Public native delegation shares the SQL transaction with the permanent binding.
        match WorkflowStartCommand::<Runs>::execute(
            context,
            WorkflowStart {
                workflow_id: request.id.to_vec(),
                request_id: RequestId::from_bytes(request.id),
                event: encoded,
            },
        )? {
            CommandResult::Success(WorkflowOutcome::Applied { run_id, .. }) => {
                Ok(CommandResult::Success(StartOutcome::Started(run_id)))
            }
            _ => Err(Error::Command(
                "unbound native media run; binding rolled back",
            )),
        }
    }
}
