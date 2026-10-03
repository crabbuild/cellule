use crate::{CloseRequest, Runs, StartDecision, sql, wire};
use cellule_runtime::{
    CellModule, Error,
    identity::RequestId,
    primitives::{
        sql::SqlValue,
        workflow::{WorkflowOutcome, WorkflowStart, WorkflowStartCommand},
    },
    registry::{Command, CommandContext, CommandResult},
};

/// Permanently binds one period ID to its local Activity endpoint and starts native close.
pub struct StartClose;

impl Command for StartClose {
    const MODULE: &'static str = Runs::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = CloseRequest;
    type Output = StartDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        request: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        request.validate()?;
        let bytes = wire::encode(&request, 4096)?;
        let prior = context.sql(&sql::batch(
            "SELECT request FROM close_bindings WHERE period_id=?1",
            vec![SqlValue::Blob(request.period_id.to_vec())],
        ))?;
        let [result] = prior.as_slice() else {
            return Err(Error::Command(
                "usage-ledger Workflow binding result differs",
            ));
        };
        match result.rows.as_slice() {
            [row] => {
                let [SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("usage-ledger Workflow binding row differs"));
                };
                return Ok(if original == &bytes {
                    CommandResult::Success(StartDecision::Started)
                } else {
                    CommandResult::Rejected(StartDecision::Conflict)
                });
            }
            [] => {}
            _ => return Err(Error::Command("usage-ledger Workflow identity duplicated")),
        }
        let counts = context.sql(&sql::batch("SELECT count(*) FROM close_bindings", vec![]))?;
        let [result] = counts.as_slice() else {
            return Err(Error::Command("usage-ledger Workflow count differs"));
        };
        let [row] = result.rows.as_slice() else {
            return Err(Error::Command("usage-ledger Workflow count row differs"));
        };
        let [SqlValue::Integer(count)] = row.as_slice() else {
            return Err(Error::Command("usage-ledger Workflow count type differs"));
        };
        if *count >= 1024 {
            return Ok(CommandResult::Rejected(StartDecision::Conflict));
        }
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO close_bindings(period_id,request) VALUES(?1,?2)",
            vec![
                SqlValue::Blob(request.period_id.to_vec()),
                SqlValue::Blob(bytes.clone()),
            ],
        ))?)?;
        let result = WorkflowStartCommand::<Runs>::execute(
            context,
            WorkflowStart {
                workflow_id: request.period_id.to_vec(),
                request_id: RequestId::from_bytes(request.period_id),
                event: bytes,
            },
        )?;
        if !matches!(
            result,
            CommandResult::Success(WorkflowOutcome::Applied { .. })
        ) {
            return Err(Error::Command(
                "usage-ledger Workflow failed to start; binding rolled back",
            ));
        }
        Ok(CommandResult::Success(StartDecision::Started))
    }
}
