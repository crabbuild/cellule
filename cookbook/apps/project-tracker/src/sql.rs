use crate::{
    Assignee, AttachmentId, AttachmentPublication, Issue, IssueFields, IssueId, IssueStatus,
    PROJECTS, ProjectKey, ProjectState, ProjectSummary, Projects, wire,
};
use cellule_app::CellType;
use cellule_runtime::{
    CatalogRole, CellModule, CellTarget, Error, Result,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
use std::collections::BTreeMap;

pub(crate) fn partition(project: &ProjectKey) -> Result<[u8; 33]> {
    CellType::new(Projects::NAME, "projects", PROJECTS, CatalogRole::Sql, 1)?
        .with_entity_partitions()?
        .entity_partition(project.as_bytes())
}
pub(crate) fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
pub(crate) fn batch(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![statement(sql, parameters)],
    }
}
pub(crate) fn one(results: Vec<SqlResultSet>) -> Result<SqlResultSet> {
    let mut results = results.into_iter();
    let result = results
        .next()
        .ok_or(Error::Command("missing tracker SQL result"))?;
    if results.next().is_some() {
        return Err(Error::Command("unexpected tracker SQL result count"));
    }
    Ok(result)
}
pub(crate) fn changed(results: Vec<SqlResultSet>) -> Result<()> {
    if one(results)?.rows_affected != 1 {
        return Err(Error::Command(
            "tracker SQL write did not change exactly one row",
        ));
    }
    Ok(())
}
pub(crate) fn source(
    sql: impl Fn(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    target: Option<&CellTarget>,
) -> Result<Option<ProjectState>> {
    let results = sql(&SqlBatch {
        statements: vec![
            statement(
                "SELECT project_key, name, revision, effect_id FROM project WHERE singleton=1 LIMIT 2",
                vec![],
            ),
            statement(
                "SELECT issue_id, title, description, assignee, status, revision FROM issues ORDER BY issue_id LIMIT 33",
                vec![],
            ),
            statement(
                "SELECT issue_id, attachment_id, publication FROM attachments ORDER BY issue_id, attachment_id LIMIT 65",
                vec![],
            ),
        ],
    })?;
    let [meta, selected_issues, selected_links] = results.as_slice() else {
        return Err(Error::Command("tracker source result count differs"));
    };
    let row = match meta.rows.as_slice() {
        [] if selected_issues.rows.is_empty() && selected_links.rows.is_empty() => return Ok(None),
        [row] => row,
        _ => {
            return Err(Error::Command(
                "tracker project singleton invariant violated",
            ));
        }
    };
    let [
        SqlValue::Text(key),
        SqlValue::Text(name),
        SqlValue::Integer(revision),
        SqlValue::Blob(effect),
    ] = row.as_slice()
    else {
        return Err(Error::Command("tracker project row differs from schema"));
    };
    let project = ProjectKey::new(key.clone())?;
    if let Some(target) = target
        && target.partition() != partition(&project)?
    {
        return Err(Error::Command(
            "stored tracker aggregate differs from its Cell",
        ));
    }
    let mut indices = BTreeMap::new();
    let mut issues = Vec::with_capacity(selected_issues.rows.len());
    for row in &selected_issues.rows {
        let [
            SqlValue::Text(id),
            SqlValue::Text(title),
            SqlValue::Text(description),
            assignee,
            SqlValue::Integer(status),
            SqlValue::Integer(revision),
        ] = row.as_slice()
        else {
            return Err(Error::Command("tracker issue row differs from schema"));
        };
        let id = IssueId::new(id.clone())?;
        let assignee = match assignee {
            SqlValue::Null => None,
            SqlValue::Text(value) => Some(Assignee::new(value.clone())?),
            _ => return Err(Error::Command("stored tracker assignee has wrong type")),
        };
        let status = match status {
            1 => IssueStatus::Open,
            2 => IssueStatus::Closed,
            _ => return Err(Error::Command("stored tracker issue state invalid")),
        };
        indices.insert(id.clone(), issues.len());
        issues.push(Issue {
            id,
            revision: *revision,
            fields: IssueFields {
                title: title.clone(),
                description: description.clone(),
                assignee,
                status,
            },
            attachments: vec![],
        });
    }
    for row in &selected_links.rows {
        let [
            SqlValue::Text(issue),
            SqlValue::Text(id),
            SqlValue::Blob(bytes),
        ] = row.as_slice()
        else {
            return Err(Error::Command("tracker attachment row differs from schema"));
        };
        let issue = IssueId::new(issue.clone())?;
        let id = AttachmentId::new(id.clone())?;
        let value: AttachmentPublication = wire::decode(bytes, 1024)?;
        if value.descriptor.id != id || value.descriptor.issue != issue {
            return Err(Error::Command(
                "tracker attachment identity differs from stored key",
            ));
        }
        let index = *indices
            .get(&issue)
            .ok_or(Error::Command("tracker attachment has no parent issue"))?;
        issues[index].attachments.push(value);
    }
    let state = ProjectState {
        project,
        name: name.clone(),
        revision: *revision,
        issues,
        effect_id: effect
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("tracker effect identity length differs"))?,
    };
    state.validate()?;
    Ok(Some(state))
}
pub(crate) const SUMMARY_FIELDS: &str =
    "project_key, name, revision, issues, open, attachments, digest";
pub(crate) fn summary(row: &[SqlValue]) -> Result<ProjectSummary> {
    let [
        SqlValue::Text(key),
        SqlValue::Text(name),
        SqlValue::Integer(revision),
        SqlValue::Integer(issues),
        SqlValue::Integer(open),
        SqlValue::Integer(attachments),
        SqlValue::Blob(digest),
    ] = row
    else {
        return Err(Error::Command("tracker dashboard row differs from schema"));
    };
    let value = ProjectSummary {
        project: ProjectKey::new(key.clone())?,
        name: name.clone(),
        revision: *revision,
        issues: u32::try_from(*issues)
            .map_err(|_| Error::Command("tracker issue count invalid"))?,
        open: u32::try_from(*open).map_err(|_| Error::Command("tracker open count invalid"))?,
        attachments: u32::try_from(*attachments)
            .map_err(|_| Error::Command("tracker attachment count invalid"))?,
        digest: digest
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("tracker summary digest length differs"))?,
    };
    value.validate()?;
    Ok(value)
}
