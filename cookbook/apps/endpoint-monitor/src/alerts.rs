use crate::{
    AlertOutcome, AlertPage, Alerts, Edge, MAX_CHECKS, PageRequest,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
/// Permanent business-edge deduplication in an independent notification SQL inbox.
pub struct RecordAlert;
impl Command for RecordAlert {
    const MODULE: &'static str = Alerts::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Edge;
    type Output = AlertOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        edge: Edge,
    ) -> cellule_runtime::Result<CommandResult<AlertOutcome>> {
        edge.validate()?;
        let bytes = encode(&edge)?;
        let selected = context.sql(&sql::batch(
            "SELECT row_id,edge FROM alerts WHERE edge_key=?1",
            vec![SqlValue::Blob(edge.key.to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => {}
            [row] => {
                let [SqlValue::Integer(id), SqlValue::Blob(original)] = row.as_slice() else {
                    return Err(Error::Command("invalid alert row"));
                };
                return Ok(if original == &bytes {
                    CommandResult::Success(AlertOutcome::Recorded(*id))
                } else {
                    CommandResult::Rejected(AlertOutcome::Conflict)
                });
            }
            _ => return Err(Error::Command("alert identity uniqueness violated")),
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM alerts", vec![]))?)?
            >= MAX_CHECKS
        {
            return Ok(CommandResult::Rejected(AlertOutcome::Capacity));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO alerts(edge_key,monitor,definition,edge) VALUES(?1,?2,?3,?4)",
            vec![
                SqlValue::Blob(edge.key.to_vec()),
                SqlValue::Blob(edge.monitor.bytes().to_vec()),
                SqlValue::Blob(edge.definition.bytes().to_vec()),
                SqlValue::Blob(bytes),
            ],
        ))?)?;
        Ok(CommandResult::Success(AlertOutcome::Recorded(sql::count(
            &context.sql(&sql::batch(
                "SELECT row_id FROM alerts WHERE edge_key=?1",
                vec![SqlValue::Blob(edge.key.to_vec())],
            ))?,
        )?)))
    }
}
/// Bounded independent notification history, with explicit keyset continuation.
pub struct ListAlerts;
impl Query for ListAlerts {
    const MODULE: &'static str = Alerts::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = AlertPage;
    fn execute(
        context: &mut QueryContext<'_>,
        page: PageRequest,
    ) -> cellule_runtime::Result<AlertPage> {
        page.validate()?;
        let selected=context.sql(&sql::batch("SELECT row_id,edge FROM alerts WHERE monitor=?1 AND definition=?2 AND row_id>?3 ORDER BY row_id LIMIT ?4",vec![SqlValue::Blob(page.monitor.bytes().to_vec()),SqlValue::Blob(page.definition.bytes().to_vec()),SqlValue::Integer(page.after),SqlValue::Integer(i64::from(page.limit)+1)]))?;
        let rows = sql::rows(&selected)?;
        let more = rows.len() > page.limit as usize;
        let mut edges = Vec::new();
        let mut cursor = None;
        for row in rows.iter().take(page.limit as usize) {
            let [SqlValue::Integer(id), SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid alert page row"));
            };
            let edge: Edge = decode(bytes)?;
            edge.validate()?;
            edges.push(edge);
            cursor = Some(*id);
        }
        Ok(AlertPage {
            edges,
            next: if more { cursor } else { None },
        })
    }
}
