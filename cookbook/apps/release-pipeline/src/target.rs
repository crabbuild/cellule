use crate::{
    Deployment, MAX_DEPLOYMENTS, MAX_TARGETS, ReleaseId, Selection, TargetAction, TargetName,
    TargetOutcome, TargetRecord, TargetState, TargetWork, Targets,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::{SqlResultSet, SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

fn operation_query(release: ReleaseId) -> cellule_runtime::primitives::sql::SqlBatch {
    sql::batch(
        "SELECT record FROM target_operations WHERE release_id=?1",
        vec![SqlValue::Blob(release.bytes().to_vec())],
    )
}
fn slot_query(name: &TargetName) -> cellule_runtime::primitives::sql::SqlBatch {
    sql::batch(
        "SELECT record FROM target_slots WHERE name=?1",
        vec![SqlValue::Text(name.as_str().into())],
    )
}
fn content_query(release: ReleaseId) -> cellule_runtime::primitives::sql::SqlBatch {
    sql::batch(
        "SELECT payload FROM target_artifacts WHERE release_id=?1",
        vec![SqlValue::Blob(release.bytes().to_vec())],
    )
}
fn content(selection: &Selection, results: &[SqlResultSet]) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = sql::blob(results)?.ok_or(Error::Command("deployed target artifact is missing"))?;
    selection.artifact.verify(selection.release, &bytes)?;
    Ok(bytes)
}
fn operation(
    bytes: Option<Vec<u8>>,
    release: ReleaseId,
) -> cellule_runtime::Result<Option<TargetRecord>> {
    bytes
        .map(|bytes| {
            let value: TargetRecord = decode(&bytes)?;
            value.validate()?;
            if value.deployment.release != release {
                return Err(Error::Identity("target operation identity differs"));
            }
            Ok(value)
        })
        .transpose()
}
fn slot(bytes: Option<Vec<u8>>, name: TargetName) -> cellule_runtime::Result<TargetState> {
    let value = match bytes {
        Some(bytes) => decode(&bytes)?,
        None => TargetState {
            target: name.clone(),
            generation: 0,
            selected: None,
        },
    };
    value.validate()?;
    if value.target != name {
        return Err(Error::Identity("target slot identity differs"));
    }
    Ok(value)
}
fn save_slot(context: &CommandContext<'_, '_>, value: &TargetState) -> cellule_runtime::Result<()> {
    value.validate()?;
    sql::changed(&context.sql(&sql::batch("INSERT INTO target_slots(name,record) VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET record=excluded.record", vec![SqlValue::Text(value.target.as_str().into()),SqlValue::Blob(encode(value)?)]))?)
}
fn save_operation(
    context: &CommandContext<'_, '_>,
    value: &TargetRecord,
) -> cellule_runtime::Result<()> {
    value.validate()?;
    sql::changed(&context.sql(&sql::batch("INSERT INTO target_operations(release_id,record) VALUES(?1,?2) ON CONFLICT(release_id) DO UPDATE SET record=excluded.record", vec![SqlValue::Blob(value.deployment.release.bytes().to_vec()),SqlValue::Blob(encode(value)?)]))?)
}
fn original(deployment: Deployment, result: TargetOutcome) -> TargetRecord {
    TargetRecord {
        deployment,
        outcome: result,
        installed_generation: None,
        previous: None,
        deploys: 0,
        rollbacks: u32::from(result == TargetOutcome::Cancelled),
    }
}
/// Atomic independent target mutation with permanent request binding and conditional compensation.
pub struct ApplyTarget;
impl Command for ApplyTarget {
    const MODULE: &'static str = Targets::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = TargetWork;
    type Output = TargetOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: TargetWork,
    ) -> cellule_runtime::Result<CommandResult<TargetOutcome>> {
        input.validate()?;
        let old = operation(
            sql::blob(&context.sql(&operation_query(input.deployment.release))?)?,
            input.deployment.release,
        )?;
        if let Some(mut value) = old {
            if value.deployment != input.deployment {
                return Ok(CommandResult::Rejected(TargetOutcome::Conflict));
            }
            if value.deploys == 1 {
                content(
                    &Selection {
                        release: value.deployment.release,
                        artifact: value.deployment.artifact.clone(),
                    },
                    &context.sql(&content_query(value.deployment.release))?,
                )?;
            }
            if let Some(previous) = &value.previous {
                content(previous, &context.sql(&content_query(previous.release))?)?;
            }
            if matches!(input.action, TargetAction::Rollback)
                && value.outcome == TargetOutcome::Deployed
            {
                let mut current = slot(
                    sql::blob(&context.sql(&slot_query(&input.deployment.target))?)?,
                    input.deployment.target.clone(),
                )?;
                if let Some(selected) = &current.selected {
                    content(selected, &context.sql(&content_query(selected.release))?)?;
                }
                let selected = Some(Selection {
                    release: input.deployment.release,
                    artifact: input.deployment.artifact.clone(),
                });
                if Some(current.generation) == value.installed_generation
                    && current.selected == selected
                {
                    // Generation and exact selection are one transactional fence.
                    // Even a matching older artifact cannot authorize an ABA rollback.
                    current.generation = current
                        .generation
                        .checked_add(1)
                        .ok_or(Error::Command("target generation overflow"))?;
                    current.selected = value.previous.clone();
                    save_slot(context, &current)?;
                    value.outcome = TargetOutcome::RolledBack;
                    value.rollbacks = 1;
                } else {
                    value.outcome = TargetOutcome::Superseded;
                }
                save_operation(context, &value)?;
            }
            return Ok(CommandResult::Success(value.outcome));
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM target_operations",
            vec![],
        ))?)?
            >= MAX_DEPLOYMENTS
        {
            return Ok(CommandResult::Rejected(TargetOutcome::Capacity));
        }
        let value = match input.action {
            TargetAction::Rollback => {
                // A rollback arriving before a delayed deploy permanently binds
                // the full request and blocks that release from ever installing.
                original(input.deployment, TargetOutcome::Cancelled)
            }
            TargetAction::Deploy(payload) => {
                let bytes = sql::blob(&context.sql(&slot_query(&input.deployment.target))?)?;
                if bytes.is_none()
                    && sql::count(
                        &context.sql(&sql::batch("SELECT count(*) FROM target_slots", vec![]))?,
                    )? >= MAX_TARGETS
                {
                    return Ok(CommandResult::Rejected(TargetOutcome::Capacity));
                }
                let mut current = slot(bytes, input.deployment.target.clone())?;
                if current.generation != input.deployment.expected_generation {
                    original(input.deployment, TargetOutcome::Conflict)
                } else {
                    if let Some(selected) = &current.selected {
                        content(selected, &context.sql(&content_query(selected.release))?)?;
                    }
                    let previous = current.selected.take();
                    current.generation += 1;
                    current.selected = Some(Selection {
                        release: input.deployment.release,
                        artifact: input.deployment.artifact.clone(),
                    });
                    // The installed bytes, slot selection, permanent operation,
                    // and native response outcome publish in this one Cell commit.
                    sql::changed(&context.sql(&sql::batch(
                        "INSERT INTO target_artifacts(release_id,payload) VALUES(?1,?2)",
                        vec![
                            SqlValue::Blob(input.deployment.release.bytes().to_vec()),
                            SqlValue::Blob(payload),
                        ],
                    ))?)?;
                    save_slot(context, &current)?;
                    TargetRecord {
                        deployment: input.deployment,
                        outcome: TargetOutcome::Deployed,
                        installed_generation: Some(current.generation),
                        previous,
                        deploys: 1,
                        rollbacks: 0,
                    }
                }
            }
        };
        save_operation(context, &value)?;
        // Native rejection rolls application SQL back. A generation refusal
        // is acknowledged as a stored domain outcome so its full permanent
        // request binding commits; callers must inspect the domain result.
        Ok(CommandResult::Success(value.outcome))
    }
}
/// Read-only exact permanent operation; absence alone does not prove an unknown mutation failed.
pub struct GetTargetRecord;
impl Query for GetTargetRecord {
    const MODULE: &'static str = Targets::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ReleaseId;
    type Output = Option<TargetRecord>;
    fn execute(
        context: &mut QueryContext<'_>,
        release: ReleaseId,
    ) -> cellule_runtime::Result<Self::Output> {
        release.validate()?;
        let value = operation(
            sql::blob(&context.sql(&operation_query(release))?)?,
            release,
        )?;
        if let Some(record) = &value {
            if record.deploys == 1 {
                content(
                    &Selection {
                        release,
                        artifact: record.deployment.artifact.clone(),
                    },
                    &context.sql(&content_query(release))?,
                )?;
            }
            if let Some(previous) = &record.previous {
                content(previous, &context.sql(&content_query(previous.release))?)?;
            }
        }
        Ok(value)
    }
}
/// Read-only independently committed current target generation and selection.
pub struct GetTargetState;
impl Query for GetTargetState {
    const MODULE: &'static str = Targets::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = TargetName;
    type Output = TargetState;
    fn execute(
        context: &mut QueryContext<'_>,
        name: TargetName,
    ) -> cellule_runtime::Result<Self::Output> {
        name.validate()?;
        let state = slot(sql::blob(&context.sql(&slot_query(&name))?)?, name)?;
        if let Some(selected) = &state.selected {
            content(selected, &context.sql(&content_query(selected.release))?)?;
        }
        Ok(state)
    }
}
/// Exact installed bytes retained even after rollback; rejected and cancelled work has no artifact.
pub struct GetTargetArtifact;
impl Query for GetTargetArtifact {
    const MODULE: &'static str = Targets::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = ReleaseId;
    type Output = Option<Vec<u8>>;
    fn execute(
        context: &mut QueryContext<'_>,
        release: ReleaseId,
    ) -> cellule_runtime::Result<Self::Output> {
        let record = GetTargetRecord::execute(context, release)?;
        match record {
            Some(value) if value.deploys == 1 => Ok(Some(content(
                &Selection {
                    release,
                    artifact: value.deployment.artifact,
                },
                &context.sql(&content_query(release))?,
            )?)),
            _ => Ok(None),
        }
    }
}
