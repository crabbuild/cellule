use crate::{
    Advance, Advanced, DeliveryOutcome, MAX_RESOURCES, Provider, ProviderAction, ProviderPhase,
    ProviderResource, ProviderWork,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn phase(value: ProviderPhase) -> i64 {
    match value {
        ProviderPhase::Creating => 0,
        ProviderPhase::Ready => 1,
        ProviderPhase::Deleting => 2,
        ProviderPhase::Deleted => 3,
    }
}
fn checked(
    bytes: &[u8],
    stored_phase: i64,
    due: &SqlValue,
) -> cellule_runtime::Result<ProviderResource> {
    let value: ProviderResource = decode(bytes)?;
    value.validate()?;
    let expected = match value.due_at_ms {
        Some(value) => SqlValue::Integer(value),
        None => SqlValue::Null,
    };
    if stored_phase != phase(value.phase) || due != &expected {
        return Err(Error::Command(
            "provider lifecycle index differs from record",
        ));
    }
    Ok(value)
}
fn selected(
    results: &[cellule_runtime::primitives::sql::SqlResultSet],
) -> cellule_runtime::Result<Option<ProviderResource>> {
    match sql::rows(results)? {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Blob(bytes), SqlValue::Integer(phase), due] = row.as_slice() else {
                return Err(Error::Command("invalid provider resource row"));
            };
            Ok(Some(checked(bytes, *phase, due)?))
        }
        _ => Err(Error::Command("provider resource uniqueness violated")),
    }
}
fn query(id: crate::Id) -> cellule_runtime::primitives::sql::SqlBatch {
    sql::batch(
        "SELECT record,phase,due_at_ms FROM provider_resources WHERE resource_id=?1",
        vec![sql::id(id)],
    )
}
fn save(context: &CommandContext<'_, '_>, value: &ProviderResource) -> cellule_runtime::Result<()> {
    value.validate()?;
    sql::changed(&context.sql(&sql::batch("INSERT INTO provider_resources(resource_id,record,phase,due_at_ms) VALUES(?1,?2,?3,?4) ON CONFLICT(resource_id) DO UPDATE SET record=excluded.record,phase=excluded.phase,due_at_ms=excluded.due_at_ms",vec![sql::id(value.spec.id),SqlValue::Blob(encode(value)?),SqlValue::Integer(phase(value.phase)),value.due_at_ms.map_or(SqlValue::Null,SqlValue::Integer)]))?)
}
/// Independent provider's permanent create/delete operation and tombstone.
pub struct ApplyProvider;
impl Command for ApplyProvider {
    const MODULE: &'static str = Provider::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ProviderWork;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        work: ProviderWork,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        work.validate()?;
        let old = selected(&context.sql(&query(work.spec.id))?)?;
        if old.as_ref().is_some_and(|value| value.spec != work.spec) {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if old.is_none()
            && sql::count(&context.sql(&sql::batch(
                "SELECT count(*) FROM provider_resources",
                vec![],
            ))?)?
                >= MAX_RESOURCES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let value = match (old, work.action) {
            (Some(value), ProviderAction::Create) => value,
            (Some(mut value), ProviderAction::Delete) => {
                if !matches!(
                    value.phase,
                    ProviderPhase::Deleting | ProviderPhase::Deleted
                ) {
                    value.phase = ProviderPhase::Deleting;
                    value.deletes = 1;
                    value.due_at_ms = Some(
                        context
                            .now_ms()
                            .checked_add(1000)
                            .ok_or(Error::Command("provider cleanup due overflow"))?,
                    );
                }
                value
            }
            (None, action) => ProviderResource::initial(
                work.spec,
                context.now_ms(),
                action == ProviderAction::Delete,
            )?,
        };
        // Deleting before any observed create installs a permanent tombstone.
        // Late or replayed creation can never reopen that provider business key.
        save(context, &value)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Provider-owned asynchronous lifecycle advancement, independent of HTTP polling.
pub struct AdvanceProvider;
impl Command for AdvanceProvider {
    const MODULE: &'static str = Provider::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Advance;
    type Output = Advanced;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Advance,
    ) -> cellule_runtime::Result<CommandResult<Advanced>> {
        if !(1..=16).contains(&input.limit) {
            return Err(Error::Command("invalid provider advancement bound"));
        }
        let results=context.sql(&sql::batch("SELECT resource_id,record,phase,due_at_ms FROM provider_resources WHERE phase IN (0,2) AND due_at_ms<=?1 ORDER BY due_at_ms,resource_id LIMIT ?2",vec![SqlValue::Integer(context.now_ms()),SqlValue::Integer(i64::from(input.limit))]))?;
        let mut count = 0;
        for row in sql::rows(&results)? {
            let [
                SqlValue::Blob(id),
                SqlValue::Blob(bytes),
                SqlValue::Integer(phase),
                due,
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid due provider row"));
            };
            let mut value = checked(bytes, *phase, due)?;
            if id != &value.spec.id.bytes() {
                return Err(Error::Identity("provider row identity differs"));
            }
            value.phase = match value.phase {
                ProviderPhase::Creating => ProviderPhase::Ready,
                ProviderPhase::Deleting => ProviderPhase::Deleted,
                _ => return Err(Error::Command("stable provider resource unexpectedly due")),
            };
            value.due_at_ms = None;
            save(context, &value)?;
            count += 1;
        }
        Ok(CommandResult::Success(Advanced { resources: count }))
    }
}
/// Read-only provider state; polling cannot itself create or delete a resource.
pub struct GetProviderResource;
impl Query for GetProviderResource {
    const MODULE: &'static str = Provider::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::Id;
    type Output = Option<ProviderResource>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: crate::Id,
    ) -> cellule_runtime::Result<Self::Output> {
        let value = selected(&context.sql(&query(id))?)?;
        if value.as_ref().is_some_and(|value| value.spec.id != id) {
            return Err(Error::Identity("provider lookup identity differs"));
        }
        Ok(value)
    }
}
/// Read-only scheduling hint; polling an idle provider publishes no command evidence.
pub struct NextProviderDue;
impl Query for NextProviderDue {
    const MODULE: &'static str = Provider::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = crate::ProviderDue;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Self::Output> {
        let value = selected(&context.sql(&sql::batch(
            "SELECT record,phase,due_at_ms FROM provider_resources WHERE phase IN (0,2) ORDER BY due_at_ms,resource_id LIMIT 1",
            vec![],
        ))?)?;
        Ok(crate::ProviderDue {
            at_ms: value.and_then(|value| value.due_at_ms),
        })
    }
}
