use cellule_runtime::{
    CellModule, Error,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{Query, QueryContext},
};

use crate::{Page, PageRequest, Task, Tasks};

/// Bounded keyset read of one project's tasks.
pub struct ListTasks;

impl Query for ListTasks {
    const MODULE: &'static str = Tasks::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Page;

    fn execute(context: &mut QueryContext<'_>, page: PageRequest) -> cellule_runtime::Result<Page> {
        if !(1..=100).contains(&page.limit) || page.after.is_some_and(|id| id <= 0) {
            return Err(Error::Command(
                "page requires a positive cursor and a limit from 1 to 100",
            ));
        }
        let results = context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT id, title, assignee, closed, revision FROM tasks WHERE id > ?1 ORDER BY id LIMIT ?2".into(),
            parameters: vec![SqlValue::Integer(page.after.unwrap_or(0)), SqlValue::Integer(i64::from(page.limit) + 1)],
        }] })?;
        let [result] = results.as_slice() else {
            return Err(Error::Command("unexpected task list result"));
        };
        let mut tasks = result
            .rows
            .iter()
            .map(|row| task_from_row(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let has_more = tasks.len() > page.limit as usize;
        tasks.truncate(page.limit as usize);
        let next = if has_more {
            tasks.last().map(|task| task.id)
        } else {
            None
        };
        Ok(Page { tasks, next })
    }
}

pub(crate) fn task_from_row(row: &[SqlValue]) -> cellule_runtime::Result<Task> {
    let [
        SqlValue::Integer(id),
        SqlValue::Text(title),
        assignee,
        SqlValue::Integer(closed),
        SqlValue::Integer(revision),
    ] = row
    else {
        return Err(Error::Command("task row does not match declared schema"));
    };
    let assignee = match assignee {
        SqlValue::Null => None,
        SqlValue::Text(value) => Some(value.clone()),
        _ => {
            return Err(Error::Command(
                "task assignee does not match declared schema",
            ));
        }
    };
    if *id <= 0 || *revision <= 0 || !matches!(*closed, 0 | 1) {
        return Err(Error::Command(
            "task row violates declared domain invariants",
        ));
    }
    Ok(Task {
        id: *id,
        title: title.clone(),
        assignee,
        closed: *closed == 1,
        revision: *revision,
    })
}
