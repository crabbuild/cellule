use crate::{Project, ProjectInput, ProjectOutcome, Projects};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

/// Atomic conditional project replacement and durable business outcome.
pub struct ChangeProject;
impl Command for ChangeProject {
    const MODULE: &'static str = Projects::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ProjectInput;
    type Output = ProjectOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: ProjectInput,
    ) -> cellule_runtime::Result<CommandResult<ProjectOutcome>> {
        if input.change.validate().is_err()
            || crate::model::canonical_slug(&input.actor, 48).is_err()
        {
            return Ok(CommandResult::Rejected(ProjectOutcome::Invalid));
        }
        let sql = if input.change.expected_revision.is_none() {
            "INSERT INTO project(singleton,title,description,revision,updated_by) VALUES(1,?1,?2,1,?3) ON CONFLICT(singleton) DO NOTHING"
        } else {
            "UPDATE project SET title=?1,description=?2,updated_by=?3,revision=revision+1 WHERE singleton=1 AND revision=?4"
        };
        let mut parameters = vec![
            SqlValue::Text(input.change.title),
            SqlValue::Text(input.change.description),
            SqlValue::Text(input.actor),
        ];
        if let Some(revision) = input.change.expected_revision {
            parameters.push(SqlValue::Integer(revision));
        }
        let result = context.sql(&SqlBatch {
            statements: vec![
                SqlStatement {
                    sql: sql.into(),
                    parameters,
                },
                SqlStatement {
                    sql: SELECT.into(),
                    parameters: vec![],
                },
            ],
        })?;
        let [changed, selected] = result.as_slice() else {
            return Err(Error::Command("unexpected project mutation shape"));
        };
        match (changed.rows_affected, selected.rows.as_slice()) {
            (1, [row]) => Ok(CommandResult::Success(ProjectOutcome::Applied(
                project_from_row(row)?,
            ))),
            (0, []) => Ok(CommandResult::Rejected(ProjectOutcome::NotFound)),
            (0, [_]) => Ok(CommandResult::Rejected(ProjectOutcome::Conflict)),
            _ => Err(Error::Command("project singleton invariant violated")),
        }
    }
}
const SELECT: &str = "SELECT title,description,revision,updated_by FROM project WHERE singleton=1";
/// Bounded point read of the authorized project document.
pub struct GetProject;
impl Query for GetProject {
    const MODULE: &'static str = Projects::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<Project>;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Option<Project>> {
        let results = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: SELECT.into(),
                parameters: vec![],
            }],
        })?;
        let [selected] = results.as_slice() else {
            return Err(Error::Command("unexpected project read shape"));
        };
        match selected.rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(project_from_row(row)?)),
            _ => Err(Error::Command("project singleton invariant violated")),
        }
    }
}
fn project_from_row(row: &[SqlValue]) -> cellule_runtime::Result<Project> {
    let [
        SqlValue::Text(title),
        SqlValue::Text(description),
        SqlValue::Integer(revision),
        SqlValue::Text(updated_by),
    ] = row
    else {
        return Err(Error::Command("invalid project row"));
    };
    let change = crate::ProjectChange {
        expected_revision: None,
        title: title.clone(),
        description: description.clone(),
    };
    if *revision <= 0
        || change.validate().is_err()
        || crate::model::canonical_slug(updated_by, 48).is_err()
    {
        return Err(Error::Command("invalid stored project document"));
    }
    Ok(Project {
        title: title.clone(),
        description: description.clone(),
        revision: *revision,
        updated_by: updated_by.clone(),
    })
}
