use crate::{
    Delivery, Inspection, InspectionCursor, InspectionPageRequest, Job, Receiver, RecordOutcome,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
fn enabled(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<bool> {
    match rows {
        [row] => match row.as_slice() {
            [SqlValue::Integer(0)] => Ok(false),
            [SqlValue::Integer(1)] => Ok(true),
            _ => Err(Error::Command("invalid receiver availability")),
        },
        _ => Err(Error::Command("missing receiver availability")),
    }
}
/// Durably applies an external action or records inspection, deduplicating business keys.
pub struct Record;
impl Command for Record {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Delivery;
    type Output = RecordOutcome;
    fn execute(
        c: &mut CommandContext<'_, '_>,
        input: Delivery,
    ) -> cellule_runtime::Result<CommandResult<RecordOutcome>> {
        input.job.validate()?;
        let results = c.sql(&SqlBatch {
            statements: vec![
                statement(
                    "SELECT body FROM deliveries WHERE job=?1 AND dead=?2",
                    vec![
                        SqlValue::Text(input.job.id.clone()),
                        SqlValue::Integer(i64::from(input.dead)),
                    ],
                ),
                statement("SELECT enabled FROM availability WHERE id=1", vec![]),
            ],
        })?;
        let [existing, availability] = results.as_slice() else {
            return Err(Error::Command("invalid receiver read"));
        };
        // Deduplication precedes availability: redelivery of a completed action
        // is safe even if the external service is currently disabled.
        if let [row] = existing.rows.as_slice() {
            let [SqlValue::Text(body)] = row.as_slice() else {
                return Err(Error::Command("invalid stored job body"));
            };
            return Ok(if body == &input.job.body {
                CommandResult::Success(RecordOutcome::Duplicate)
            } else {
                CommandResult::Rejected(RecordOutcome::Conflict)
            });
        }
        if !existing.rows.is_empty() {
            return Err(Error::Command("duplicate receiver key"));
        }
        if !input.dead && !enabled(&availability.rows)? {
            return Ok(CommandResult::Success(RecordOutcome::Unavailable));
        }
        c.sql(&SqlBatch {
            statements: vec![statement(
                "INSERT INTO deliveries(job,dead,body) VALUES(?1,?2,?3)",
                vec![
                    SqlValue::Text(input.job.id),
                    SqlValue::Integer(i64::from(input.dead)),
                    SqlValue::Text(input.job.body),
                ],
            )],
        })?;
        Ok(CommandResult::Success(RecordOutcome::Recorded))
    }
}
/// Application-owned availability switch used to exercise retry and exhaustion.
pub struct SetEnabled;
impl Command for SetEnabled {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = bool;
    type Output = ();
    fn execute(
        c: &mut CommandContext<'_, '_>,
        value: bool,
    ) -> cellule_runtime::Result<CommandResult<()>> {
        c.sql(&SqlBatch {
            statements: vec![statement(
                "UPDATE availability SET enabled=?1 WHERE id=1",
                vec![SqlValue::Integer(i64::from(value))],
            )],
        })?;
        Ok(CommandResult::Success(()))
    }
}
/// Bounded receiver inspection; no user-supplied SQL is exposed.
pub struct Inspect;
impl Query for Inspect {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 2;
    type Input = InspectionPageRequest;
    type Output = Inspection;
    fn execute(
        c: &mut QueryContext<'_>,
        page: InspectionPageRequest,
    ) -> cellule_runtime::Result<Inspection> {
        if !(1..=100).contains(&page.limit) {
            return Err(Error::Command("inspection page limit must be 1..100"));
        }
        if let Some(after) = &page.after {
            crate::model::job_identity(&after.job_id)?;
        }
        let (after, kind) = page.after.as_ref().map_or((String::new(), -1), |cursor| {
            (cursor.job_id.clone(), i64::from(cursor.dead))
        });
        let result = c.sql(&SqlBatch {
            statements: vec![
                statement("SELECT enabled FROM availability WHERE id=1", vec![]),
                statement("SELECT COUNT(*) FROM deliveries WHERE dead=0", vec![]),
                statement("SELECT COUNT(*) FROM deliveries WHERE dead=1", vec![]),
                statement(
                    "SELECT job,body,dead FROM deliveries WHERE job>?1 OR (job=?1 AND dead>?2) ORDER BY job,dead LIMIT ?3",
                    vec![SqlValue::Text(after),SqlValue::Integer(kind),SqlValue::Integer(i64::from(page.limit)+1)],
                ),
            ],
        })?;
        let [availability, delivered, dead, rows] = result.as_slice() else {
            return Err(Error::Command("invalid inspection result"));
        };
        let count = |rows: &[Vec<SqlValue>]| match rows {
            [row] => match row.as_slice() {
                [SqlValue::Integer(value)] if *value >= 0 => Ok(*value),
                _ => Err(Error::Command("invalid inspection count")),
            },
            _ => Err(Error::Command("missing inspection count")),
        };
        let mut rows = rows
            .rows
            .iter()
            .map(|row| match row.as_slice() {
                [
                    SqlValue::Text(id),
                    SqlValue::Text(body),
                    SqlValue::Integer(dead),
                ] if matches!(dead, 0 | 1) => Ok(Delivery {
                    job: Job {
                        id: id.clone(),
                        body: body.clone(),
                    },
                    dead: *dead == 1,
                }),
                _ => Err(Error::Command("invalid inspection row")),
            })
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        for row in &rows {
            row.job.validate()?;
        }
        let more = rows.len() > page.limit as usize;
        rows.truncate(page.limit as usize);
        let next = if more {
            rows.last().map(|row| InspectionCursor {
                job_id: row.job.id.clone(),
                dead: row.dead,
            })
        } else {
            None
        };
        Ok(Inspection {
            enabled: enabled(&availability.rows)?,
            delivered: count(&delivered.rows)?,
            dead: count(&dead.rows)?,
            rows,
            next,
        })
    }
}
