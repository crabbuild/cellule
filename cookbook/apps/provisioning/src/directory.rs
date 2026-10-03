use crate::{
    Call, Change, DeliveryOutcome, Directory, MAX_MESSAGES, MAX_RESOURCES, Outcome, Page,
    Projection, Reply, ReplyValue, Resource, ResourcePage, ResourceStatus,
    model::{decode, encode},
    sql,
    wire::encode_wire,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn read(
    context: &CommandContext<'_, '_>,
    id: crate::Id,
) -> cellule_runtime::Result<Option<Resource>> {
    sql::blob(&context.sql(&sql::batch(
        "SELECT record FROM resources WHERE resource_id=?1",
        vec![sql::id(id)],
    ))?)?
    .map(|bytes| checked(&bytes, id))
    .transpose()
}
fn checked(bytes: &[u8], id: crate::Id) -> cellule_runtime::Result<Resource> {
    let value: Resource = decode(bytes)?;
    value.validate()?;
    if value.spec.id != id {
        return Err(Error::Identity("stored directory resource differs"));
    }
    Ok(value)
}
fn save(context: &CommandContext<'_, '_>, resource: &Resource) -> cellule_runtime::Result<()> {
    resource.validate()?;
    sql::changed(&context.sql(&sql::batch(
        "UPDATE resources SET record=?2 WHERE resource_id=?1",
        vec![sql::id(resource.spec.id), SqlValue::Blob(encode(resource)?)],
    ))?)
}
fn emit<T: cellule_runtime::codec::WireValue>(
    context: &mut CommandContext<'_, '_>,
    command_id: u32,
    input: &T,
    limit: u32,
) -> cellule_runtime::Result<[u8; 32]> {
    context.emit_effect(&EffectCommandIntent {
        target: crate::target(context.target(), crate::FLOWS)?,
        command_id,
        codec_version: 1,
        input: encode_wire(input, limit)?,
        expires_at_ms: context
            .now_ms()
            .checked_add(7 * 24 * 60 * 60 * 1000)
            .ok_or(Error::Command("provisioning Effect expiry overflow"))?,
    })
}
/// Atomic resource request and cancellation/deprovisioning ingress.
pub struct ChangeResource;
impl Command for ChangeResource {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        change: Change,
    ) -> cellule_runtime::Result<CommandResult<Outcome>> {
        match change {
            Change::Request(spec) => {
                spec.validate()?;
                if let Some(original) = read(context, spec.id)? {
                    return Ok(if original.spec == spec {
                        CommandResult::Success(Outcome::Accepted {
                            start_effect: original.start_effect,
                        })
                    } else {
                        CommandResult::Rejected(Outcome::Conflict)
                    });
                }
                if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM resources", vec![]))?)?
                    >= MAX_RESOURCES
                {
                    return Ok(CommandResult::Rejected(Outcome::Capacity));
                }
                let start_effect = emit(context, 11, &spec, 2048)?;
                let resource = Resource {
                    spec,
                    status: ResourceStatus::Requested,
                    delete_requested: false,
                    observed: None,
                    start_effect,
                };
                resource.validate()?;
                sql::changed(&context.sql(&sql::batch(
                    "INSERT INTO resources(resource_id,record) VALUES(?1,?2)",
                    vec![
                        sql::id(resource.spec.id),
                        SqlValue::Blob(encode(&resource)?),
                    ],
                ))?)?;
                Ok(CommandResult::Success(Outcome::Accepted { start_effect }))
            }
            Change::Delete(id) => {
                let Some(mut resource) = read(context, id)? else {
                    return Ok(CommandResult::Rejected(Outcome::NotFound));
                };
                if resource.delete_requested {
                    return Ok(CommandResult::Success(Outcome::DeletionRequested));
                }
                resource.delete_requested = true;
                resource.status = ResourceStatus::Deleting;
                save(context, &resource)?;
                // Cancellation and its signed signal publish atomically. The Flow
                // receiver binds early signals even when start delivery is delayed.
                emit(context, 12, &resource.spec, 2048)?;
                Ok(CommandResult::Success(Outcome::DeletionRequested))
            }
        }
    }
}
/// Signed Workflow projection receiver with an immutable callback intent.
pub struct ProjectResource;
impl Command for ProjectResource {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Call;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        call: Call,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        call.validate()?;
        let bytes = encode(&call)?;
        if let Some(original) = sql::blob(&context.sql(&sql::batch(
            "SELECT call_bytes FROM directory_messages WHERE message_key=?1",
            vec![SqlValue::Blob(call.key().to_vec())],
        ))?)? {
            return Ok(sql::classify(if original == bytes {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        let Some(mut resource) = read(context, call.spec.id)? else {
            return Ok(sql::classify(DeliveryOutcome::NotFound));
        };
        if resource.spec != call.spec {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if resource.status == ResourceStatus::Deleted {
            return Ok(sql::classify(DeliveryOutcome::InvalidState));
        }
        if sql::count(&context.sql(&sql::batch(
            "SELECT count(*) FROM directory_messages",
            vec![],
        ))?)?
            >= MAX_MESSAGES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let value = match &call.projection {
            Projection::Ready(observed) => {
                resource.observed = Some(observed.clone());
                if resource.delete_requested {
                    ReplyValue::DeleteRequested
                } else {
                    resource.status = ResourceStatus::Active;
                    ReplyValue::Published
                }
            }
            Projection::Review(observed) => {
                resource.observed = observed.clone();
                resource.status = ResourceStatus::NeedsReview;
                ReplyValue::Reviewed
            }
            Projection::Deleted(observed) => {
                resource.observed = Some(observed.clone());
                resource.delete_requested = true;
                resource.status = ResourceStatus::Deleted;
                ReplyValue::Deleted
            }
        };
        save(context, &resource)?;
        let reply = Reply {
            call: call.clone(),
            value,
        };
        reply.validate()?;
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO directory_messages(message_key,call_bytes,reply_bytes) VALUES(?1,?2,?3)",
            vec![
                SqlValue::Blob(call.key().to_vec()),
                SqlValue::Blob(bytes),
                SqlValue::Blob(encode(&reply)?),
            ],
        ))?)?;
        emit(context, 13, &reply, 4096)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Current receiver-local resource projection at an optional directory receipt.
pub struct GetResource;
impl Query for GetResource {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::Id;
    type Output = Option<Resource>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: crate::Id,
    ) -> cellule_runtime::Result<Self::Output> {
        sql::blob(&context.sql(&sql::batch(
            "SELECT record FROM resources WHERE resource_id=?1",
            vec![sql::id(id)],
        ))?)?
        .map(|bytes| checked(&bytes, id))
        .transpose()
    }
}
/// Bounded current resource listing with receiver-local continuation.
pub struct ListResources;
impl Query for ListResources {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = Page;
    type Output = ResourcePage;
    fn execute(
        context: &mut QueryContext<'_>,
        page: Page,
    ) -> cellule_runtime::Result<Self::Output> {
        page.validate()?;
        let results = context.sql(&sql::batch(
            "SELECT row_id,record FROM resources WHERE row_id>?1 ORDER BY row_id LIMIT ?2",
            vec![
                SqlValue::Integer(page.after),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?;
        let mut resources = Vec::new();
        for row in sql::rows(&results)? {
            let [SqlValue::Integer(position), SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid resource page row"));
            };
            let value: Resource = decode(bytes)?;
            value.validate()?;
            resources.push((*position, value));
        }
        let more = resources.len() > page.limit as usize;
        resources.truncate(page.limit as usize);
        let next = if more {
            resources.last().map(|value| value.0)
        } else {
            None
        };
        Ok(ResourcePage { resources, next })
    }
}
