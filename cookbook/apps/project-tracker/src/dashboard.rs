use crate::{
    Dashboard, DashboardPage, DashboardPageRequest, MAX_DASHBOARD_PROJECTS, ProjectKey,
    ProjectSummary, ProjectionOutcome, sql,
};
use cellule_runtime::{
    CellModule, Error, Result,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

fn lookup(
    sql: impl Fn(
        &cellule_runtime::primitives::sql::SqlBatch,
    ) -> Result<Vec<cellule_runtime::primitives::sql::SqlResultSet>>,
    key: &ProjectKey,
) -> Result<Option<ProjectSummary>> {
    let selected = sql::one(sql(&sql::batch(
        &format!(
            "SELECT {} FROM dashboard WHERE project_key=?1",
            sql::SUMMARY_FIELDS
        ),
        vec![SqlValue::Text(key.as_str().into())],
    ))?)?;
    match selected.rows.as_slice() {
        [] => Ok(None),
        [row] => Ok(Some(sql::summary(row)?)),
        _ => Err(Error::Command("dashboard key uniqueness violated")),
    }
}
/// Accepts only monotonic full summaries; reordering and redelivery cannot regress state.
pub struct ProjectDashboard;
impl Command for ProjectDashboard {
    const MODULE: &'static str = Dashboard::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ProjectSummary;
    type Output = ProjectionOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.validate()?;
        match lookup(|batch| context.sql(batch), &input.project)? {
            Some(current) if current.revision > input.revision => {
                return Ok(CommandResult::Success(ProjectionOutcome::Stale));
            }
            Some(current) if current.revision == input.revision => {
                return Ok(if current == input {
                    CommandResult::Success(ProjectionOutcome::Unchanged)
                } else {
                    CommandResult::Rejected(ProjectionOutcome::Conflict)
                });
            }
            None => {
                let selected =
                    sql::one(context.sql(&sql::batch("SELECT count(*) FROM dashboard", vec![]))?)?;
                let [row] = selected.rows.as_slice() else {
                    return Err(Error::Command("dashboard count missing"));
                };
                let [SqlValue::Integer(count)] = row.as_slice() else {
                    return Err(Error::Command("dashboard count type differs"));
                };
                if *count >= MAX_DASHBOARD_PROJECTS as i64 {
                    return Ok(CommandResult::Rejected(ProjectionOutcome::Capacity));
                }
            }
            _ => {}
        }
        sql::changed(context.sql(&sql::batch("INSERT INTO dashboard(project_key,name,revision,issues,open,attachments,digest) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(project_key) DO UPDATE SET name=excluded.name,revision=excluded.revision,issues=excluded.issues,open=excluded.open,attachments=excluded.attachments,digest=excluded.digest",vec![SqlValue::Text(input.project.as_str().into()),SqlValue::Text(input.name),SqlValue::Integer(input.revision),SqlValue::Integer(i64::from(input.issues)),SqlValue::Integer(i64::from(input.open)),SqlValue::Integer(i64::from(input.attachments)),SqlValue::Blob(input.digest.to_vec())]))?)?;
        Ok(CommandResult::Success(ProjectionOutcome::Applied))
    }
}
/// Reads one projected revision; absence can mean that source delivery is still pending.
pub struct LookupProject;
impl Query for LookupProject {
    const MODULE: &'static str = Dashboard::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = ProjectKey;
    type Output = Option<ProjectSummary>;
    fn execute(context: &mut QueryContext<'_>, key: Self::Input) -> Result<Self::Output> {
        lookup(|batch| context.sql(batch), &key)
    }
}
/// Reads current tenant summaries with bounded keyset pagination and dashboard-local receipts.
pub struct ListDashboard;
impl Query for ListDashboard {
    const MODULE: &'static str = Dashboard::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = DashboardPageRequest;
    type Output = DashboardPage;
    fn execute(context: &mut QueryContext<'_>, page: Self::Input) -> Result<Self::Output> {
        if !(1..=16).contains(&page.limit) {
            return Err(Error::Command("dashboard page limit must be 1..16"));
        }
        let selected = sql::one(context.sql(&sql::batch(
            &format!(
                "SELECT {} FROM dashboard WHERE project_key>?1 ORDER BY project_key LIMIT ?2",
                sql::SUMMARY_FIELDS
            ),
            vec![
                SqlValue::Text(page.after.map(String::from).unwrap_or_default()),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?)?;
        let mut projects = selected
            .rows
            .iter()
            .map(|row| sql::summary(row))
            .collect::<Result<Vec<_>>>()?;
        let more = projects.len() > page.limit as usize;
        projects.truncate(page.limit as usize);
        let next = if more {
            projects.last().map(|v| v.project.clone())
        } else {
            None
        };
        Ok(DashboardPage { projects, next })
    }
}
