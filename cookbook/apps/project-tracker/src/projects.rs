use crate::{
    AttachmentLink, ChangeOutcome, DASHBOARD, Decision, MAX_ISSUE_ATTACHMENTS, MAX_ISSUES,
    MAX_PROJECT_ATTACHMENTS, ProjectChange, ProjectKey, ProjectMutation, ProjectState,
    ProjectVersion, Projects, sql, wire,
};
use cellule_runtime::{
    CellModule, CellTarget, Error, Result, partition_for_shard,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

fn reject(decision: Decision) -> CommandResult<ChangeOutcome> {
    CommandResult::Rejected(ChangeOutcome {
        decision,
        version: None,
    })
}
fn version(state: &ProjectState) -> Result<ProjectVersion> {
    if state.effect_id == [0; 32] {
        return Err(Error::Command(
            "tracker source has no committed dashboard intent",
        ));
    }
    Ok(ProjectVersion {
        summary: state.summary()?,
        effect_id: state.effect_id,
    })
}
fn next_revision(value: i64) -> Option<i64> {
    value.checked_add(1).filter(|value| *value < i64::MAX)
}
fn publish(context: &mut CommandContext<'_, '_>) -> Result<CommandResult<ChangeOutcome>> {
    let state = sql::source(|batch| context.sql(batch), Some(context.target()))?
        .ok_or(Error::Command("changed tracker aggregate disappeared"))?;
    let summary = state.summary()?;
    let effect_id = context.emit_effect(&EffectCommandIntent {
        target: CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            DASHBOARD,
            &partition_for_shard(0),
        )?,
        command_id: crate::ProjectDashboard::ID,
        codec_version: 1,
        input: wire::encode(&summary, 1024)?,
        expires_at_ms: context
            .now_ms()
            .checked_add(7 * 24 * 60 * 60 * 1000)
            .ok_or(Error::Command("tracker projection expiry overflow"))?,
    })?;
    // The issue/link, aggregate revision, and native intent share this transaction.
    // Returning an error rolls all of them back; business rejections precede writes.
    sql::changed(context.sql(&sql::batch(
        "UPDATE project SET effect_id=?1 WHERE singleton=1",
        vec![SqlValue::Blob(effect_id.to_vec())],
    ))?)?;
    Ok(CommandResult::Success(ChangeOutcome {
        decision: Decision::Applied,
        version: Some(ProjectVersion { summary, effect_id }),
    }))
}
fn field_parameters(fields: &crate::IssueFields) -> Vec<SqlValue> {
    vec![
        SqlValue::Text(fields.title.clone()),
        SqlValue::Text(fields.description.clone()),
        fields
            .assignee
            .as_ref()
            .map_or(SqlValue::Null, |v| SqlValue::Text(v.as_str().into())),
        SqlValue::Integer(match fields.status {
            crate::IssueStatus::Open => 1,
            crate::IssueStatus::Closed => 2,
        }),
    ]
}
/// Changes one project aggregate and atomically emits its complete revisioned summary.
pub struct ChangeProject;
impl Command for ChangeProject {
    const MODULE: &'static str = Projects::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ProjectChange;
    type Output = ChangeOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if input.validate().is_err()
            || context.target().partition() != sql::partition(&input.project)?
        {
            return Ok(reject(Decision::Invalid));
        }
        let state = sql::source(|batch| context.sql(batch), Some(context.target()))?;
        if state.as_ref().is_some_and(|v| v.project != input.project) {
            return Err(Error::Identity(
                "stored tracker key differs from command input",
            ));
        }
        if let ProjectMutation::Create { name } = &input.mutation {
            if state.is_some() {
                return Ok(reject(Decision::Exists));
            }
            sql::changed(context.sql(&sql::batch("INSERT INTO project(singleton, project_key, name, revision, effect_id) VALUES(1, ?1, ?2, 1, ?3)",vec![SqlValue::Text(input.project.as_str().into()),SqlValue::Text(name.clone()),SqlValue::Blob(vec![0;32])]))?)?;
            return publish(context);
        }
        let Some(state) = state else {
            return Ok(reject(Decision::NotFound));
        };
        let Some(revision) = next_revision(state.revision) else {
            return Ok(reject(Decision::Capacity));
        };
        match &input.mutation {
            ProjectMutation::Create { .. } => {
                return Err(Error::Command("tracker creation branch invariant violated"));
            }
            ProjectMutation::Rename {
                expected_revision,
                name,
            } => {
                if *expected_revision != state.revision {
                    return Ok(reject(Decision::Conflict));
                }
                sql::changed(context.sql(&sql::batch(
                    "UPDATE project SET name=?1 WHERE singleton=1",
                    vec![SqlValue::Text(name.clone())],
                ))?)?;
            }
            ProjectMutation::CreateIssue { id, fields } => {
                if state.issues.iter().any(|v| v.id == *id) {
                    return Ok(reject(Decision::Exists));
                }
                if state.issues.len() >= MAX_ISSUES {
                    return Ok(reject(Decision::Capacity));
                }
                let mut parameters = vec![SqlValue::Text(id.as_str().into())];
                parameters.extend(field_parameters(fields));
                sql::changed(context.sql(&sql::batch("INSERT INTO issues(issue_id,title,description,assignee,status,revision) VALUES(?1,?2,?3,?4,?5,1)",parameters))?)?;
            }
            ProjectMutation::EditIssue {
                id,
                expected_revision,
                fields,
            } => {
                let Some(issue) = state.issues.iter().find(|v| v.id == *id) else {
                    return Ok(reject(Decision::NotFound));
                };
                if issue.revision != *expected_revision {
                    return Ok(reject(Decision::Conflict));
                }
                let Some(next) = next_revision(issue.revision) else {
                    return Ok(reject(Decision::Capacity));
                };
                let mut parameters = field_parameters(fields);
                parameters.push(SqlValue::Integer(next));
                parameters.push(SqlValue::Text(id.as_str().into()));
                sql::changed(context.sql(&sql::batch("UPDATE issues SET title=?1, description=?2, assignee=?3, status=?4, revision=?5 WHERE issue_id=?6",parameters))?)?;
            }
        }
        sql::changed(context.sql(&sql::batch(
            "UPDATE project SET revision=?1 WHERE singleton=1",
            vec![SqlValue::Integer(revision)],
        ))?)?;
        publish(context)
    }
}
/// Adds one verified immutable publication to an issue, using its original revision fence.
pub struct LinkAttachment;
impl Command for LinkAttachment {
    const MODULE: &'static str = Projects::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = AttachmentLink;
    type Output = ChangeOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let descriptor = &input.publication.descriptor;
        if input.validate().is_err()
            || context.target().partition() != sql::partition(&descriptor.project)?
        {
            return Ok(reject(Decision::Invalid));
        }
        let Some(state) = sql::source(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(reject(Decision::NotFound));
        };
        if state.project != descriptor.project {
            return Err(Error::Identity(
                "stored tracker key differs from attachment input",
            ));
        }
        let Some(issue) = state.issues.iter().find(|v| v.id == descriptor.issue) else {
            return Ok(reject(Decision::NotFound));
        };
        if let Some(existing) = issue
            .attachments
            .iter()
            .find(|v| v.descriptor.id == descriptor.id)
        {
            return Ok(if existing == &input.publication {
                CommandResult::Success(ChangeOutcome {
                    decision: Decision::Unchanged,
                    version: Some(version(&state)?),
                })
            } else {
                reject(Decision::BindingMismatch)
            });
        }
        if issue.revision != input.expected_revision {
            return Ok(reject(Decision::Conflict));
        }
        if issue.attachments.len() >= MAX_ISSUE_ATTACHMENTS
            || state
                .issues
                .iter()
                .map(|v| v.attachments.len())
                .sum::<usize>()
                >= MAX_PROJECT_ATTACHMENTS
        {
            return Ok(reject(Decision::Capacity));
        }
        let (Some(project_revision), Some(issue_revision)) =
            (next_revision(state.revision), next_revision(issue.revision))
        else {
            return Ok(reject(Decision::Capacity));
        };
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO attachments(issue_id,attachment_id,publication) VALUES(?1,?2,?3)",
            vec![
                SqlValue::Text(descriptor.issue.as_str().into()),
                SqlValue::Text(descriptor.id.as_str().into()),
                SqlValue::Blob(wire::encode(&input.publication, 1024)?),
            ],
        ))?)?;
        sql::changed(context.sql(&sql::batch(
            "UPDATE issues SET revision=?1 WHERE issue_id=?2",
            vec![
                SqlValue::Integer(issue_revision),
                SqlValue::Text(descriptor.issue.as_str().into()),
            ],
        ))?)?;
        sql::changed(context.sql(&sql::batch(
            "UPDATE project SET revision=?1 WHERE singleton=1",
            vec![SqlValue::Integer(project_revision)],
        ))?)?;
        publish(context)
    }
}
/// Reads the complete bounded issue/reference roster at one source commit.
pub struct GetProject;
impl Query for GetProject {
    const MODULE: &'static str = Projects::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ProjectKey;
    type Output = Option<ProjectState>;
    fn execute(context: &mut QueryContext<'_>, key: Self::Input) -> Result<Self::Output> {
        let value = sql::source(|batch| context.sql(batch), None)?;
        if value.as_ref().is_some_and(|v| v.project != key) {
            return Err(Error::Identity("tracker query targets a different project"));
        }
        if value.as_ref().is_some_and(|v| v.effect_id == [0; 32]) {
            return Err(Error::Command(
                "tracker source lacks a committed projection identity",
            ));
        }
        Ok(value)
    }
}
