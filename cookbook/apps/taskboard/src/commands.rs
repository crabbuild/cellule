use cellule_runtime::{
    CellModule, Error,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{Command, CommandContext, CommandResult},
};

use crate::{Change, TaskOutcome, Tasks, queries::task_from_row};

/// Atomically changes one task and durably records its business outcome.
pub struct ChangeTask;

impl Command for ChangeTask {
    const MODULE: &'static str = Tasks::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = TaskOutcome;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Change,
    ) -> cellule_runtime::Result<CommandResult<TaskOutcome>> {
        if !input.valid() {
            return Ok(CommandResult::Rejected(TaskOutcome::Invalid));
        }
        let (id, sql, parameters) = match input {
            Change::Create { id, title } => (
                id,
                "INSERT INTO tasks(id, title) VALUES (?1, ?2) ON CONFLICT(id) DO NOTHING",
                vec![SqlValue::Integer(id), SqlValue::Text(title)],
            ),
            Change::Assign {
                id,
                expected_revision,
                assignee,
            } => (
                id,
                "UPDATE tasks SET assignee = ?3, revision = revision + 1 WHERE id = ?1 AND revision = ?2 AND closed = 0",
                vec![
                    SqlValue::Integer(id),
                    SqlValue::Integer(expected_revision),
                    assignee.map_or(SqlValue::Null, SqlValue::Text),
                ],
            ),
            Change::Close {
                id,
                expected_revision,
            } => (
                id,
                "UPDATE tasks SET closed = 1, revision = revision + 1 WHERE id = ?1 AND revision = ?2 AND closed = 0",
                vec![SqlValue::Integer(id), SqlValue::Integer(expected_revision)],
            ),
        };
        // The selected row is read inside the same serialized command transaction.
        // Use affected-row evidence; Cellule deliberately rejects SQL RETURNING.
        let results = context.sql(&SqlBatch {
            statements: vec![
                SqlStatement {
                    sql: sql.into(),
                    parameters,
                },
                SqlStatement {
                    sql: "SELECT id, title, assignee, closed, revision FROM tasks WHERE id = ?1"
                        .into(),
                    parameters: vec![SqlValue::Integer(id)],
                },
            ],
        })?;
        let [changed, selected] = results.as_slice() else {
            return Err(Error::Command("unexpected task mutation result"));
        };
        match (changed.rows_affected, selected.rows.as_slice()) {
            (1, [row]) => Ok(CommandResult::Success(TaskOutcome::Applied(task_from_row(
                row,
            )?))),
            (0, []) => Ok(CommandResult::Rejected(TaskOutcome::NotFound)),
            (0, [_]) => Ok(CommandResult::Rejected(TaskOutcome::Conflict)),
            _ => Err(Error::Command(
                "task mutation violated its row-count invariant",
            )),
        }
    }
}
