use crate::{
    Attributes, Change, ChangeOutcome, DEVICES, DIRECTORY, Device, DeviceKey, Devices, Directory,
    Page, PageRequest, ProjectionOutcome, PublishedDevice,
};
use cellule_app::CellType;
use cellule_runtime::{
    CatalogRole, CellModule, CellTarget, Error,
    codec::{BoundedEncoder, WireValue},
    partition_for_shard,
    primitives::{
        effects::EffectCommandIntent,
        sql::{SqlBatch, SqlStatement, SqlValue},
    },
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

pub(crate) fn entity_partition(key: &DeviceKey) -> cellule_runtime::Result<[u8; 33]> {
    CellType::new(Devices::NAME, "devices", DEVICES, CatalogRole::Sql, 1)?
        .with_entity_partitions()?
        .entity_partition(key.as_bytes())
}
fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}
const FIELDS: &str = "device_key, name, location, enabled, revision";
fn device_from_row(row: &[SqlValue]) -> cellule_runtime::Result<Device> {
    let [
        SqlValue::Text(key),
        SqlValue::Text(name),
        SqlValue::Text(location),
        SqlValue::Integer(enabled),
        SqlValue::Integer(revision),
    ] = row
    else {
        return Err(Error::Command("device row differs from schema"));
    };
    if !matches!(*enabled, 0 | 1) {
        return Err(Error::Command("invalid stored enabled flag"));
    }
    let value = Device {
        key: DeviceKey::new(key.clone())?,
        attributes: Attributes {
            name: name.clone(),
            location: location.clone(),
            enabled: *enabled == 1,
        },
        revision: *revision,
    };
    value.validate()?;
    Ok(value)
}
fn published_from_rows(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<Option<PublishedDevice>> {
    match rows {
        [] => Ok(None),
        [row] => {
            let Some((SqlValue::Blob(effect), fields)) = row.split_last() else {
                return Err(Error::Command("stored device effect identity missing"));
            };
            Ok(Some(PublishedDevice {
                device: device_from_row(fields)?,
                effect_id: effect
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Command("stored device effect identity invalid"))?,
            }))
        }
        _ => Err(Error::Command("device singleton invariant violated")),
    }
}
/// Atomically changes one device and emits a native directory Effect.
pub struct ChangeDevice;
impl Command for ChangeDevice {
    const MODULE: &'static str = Devices::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = ChangeOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Change,
    ) -> cellule_runtime::Result<CommandResult<ChangeOutcome>> {
        if input.validate().is_err()
            || context.target().partition() != entity_partition(&input.key)?
        {
            return Ok(CommandResult::Rejected(ChangeOutcome::Invalid));
        }
        let results = context.sql(&statement(
            &format!("SELECT {FIELDS}, effect_id FROM device WHERE singleton = 1"),
            vec![],
        ))?;
        let [selected] = results.as_slice() else {
            return Err(Error::Command("unexpected device selection"));
        };
        let existing = published_from_rows(&selected.rows)?;
        if existing.as_ref().is_some_and(|v| v.device.key != input.key) {
            return Err(Error::Command(
                "stored device key differs from Cell identity",
            ));
        }
        match &existing {
            None if input.expected_revision != 0 => {
                return Ok(CommandResult::Rejected(ChangeOutcome::NotFound));
            }
            Some(v) if v.device.revision != input.expected_revision => {
                return Ok(CommandResult::Rejected(ChangeOutcome::Conflict));
            }
            _ => {}
        }
        let device = Device {
            key: input.key,
            attributes: input.attributes,
            revision: input
                .expected_revision
                .checked_add(1)
                .ok_or(Error::Command("device revision overflow"))?,
        };
        let mut encoder = BoundedEncoder::new(1024)?;
        device.encode(&mut encoder)?;
        let effect_id = context.emit_effect(&EffectCommandIntent {
            target: CellTarget::new(
                context.target().tenant(),
                context.target().application(),
                DIRECTORY,
                &partition_for_shard(0),
            )?,
            command_id: ProjectDevice::ID,
            codec_version: 1,
            input: encoder.finish(),
            expires_at_ms: context
                .now_ms()
                .checked_add(7 * 24 * 60 * 60 * 1000)
                .ok_or(Error::Command("projection expiry overflow"))?,
        })?;
        // A failure below rolls back both this domain write and its native intent.
        let results=context.sql(&statement("INSERT INTO device(singleton, device_key, name, location, enabled, revision, effect_id) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(singleton) DO UPDATE SET name=excluded.name, location=excluded.location, enabled=excluded.enabled, revision=excluded.revision, effect_id=excluded.effect_id", vec![SqlValue::Text(device.key.as_str().into()), SqlValue::Text(device.attributes.name.clone()), SqlValue::Text(device.attributes.location.clone()), SqlValue::Integer(i64::from(device.attributes.enabled)), SqlValue::Integer(device.revision), SqlValue::Blob(effect_id.to_vec())]))?;
        if !matches!(results.as_slice(), [r] if r.rows_affected==1) {
            return Err(Error::Command("device update row count violated"));
        }
        Ok(CommandResult::Success(ChangeOutcome::Applied(
            PublishedDevice { device, effect_id },
        )))
    }
}
/// Reads authoritative device state from its independent Cell.
pub struct GetDevice;
impl Query for GetDevice {
    const MODULE: &'static str = Devices::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = Option<PublishedDevice>;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Self::Output> {
        let results = context.sql(&statement(
            &format!("SELECT {FIELDS}, effect_id FROM device WHERE singleton=1"),
            vec![],
        ))?;
        let [r] = results.as_slice() else {
            return Err(Error::Command("unexpected device query"));
        };
        published_from_rows(&r.rows)
    }
}
/// Applies monotonic full-state projections, safely accepting duplicates and old revisions.
pub struct ProjectDevice;
impl Command for ProjectDevice {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Device;
    type Output = ProjectionOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        device: Device,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        device.validate()?;
        let selected = context.sql(&statement(
            &format!("SELECT {FIELDS} FROM directory WHERE device_key=?1"),
            vec![SqlValue::Text(device.key.as_str().into())],
        ))?;
        let [selected] = selected.as_slice() else {
            return Err(Error::Command("unexpected projection selection"));
        };
        match selected.rows.as_slice() {
            [row] => {
                let current = device_from_row(row)?;
                if current.revision > device.revision {
                    return Ok(CommandResult::Success(ProjectionOutcome::Stale));
                }
                if current.revision == device.revision {
                    return Ok(if current == device {
                        CommandResult::Success(ProjectionOutcome::Unchanged)
                    } else {
                        CommandResult::Rejected(ProjectionOutcome::Conflict)
                    });
                }
            }
            [] => {
                let count = context.sql(&statement("SELECT count(*) FROM directory", vec![]))?;
                let [count] = count.as_slice() else {
                    return Err(Error::Command("unexpected directory count"));
                };
                let [row] = count.rows.as_slice() else {
                    return Err(Error::Command("missing directory count"));
                };
                let [SqlValue::Integer(count)] = row.as_slice() else {
                    return Err(Error::Command("invalid directory count"));
                };
                if *count >= 1024 {
                    return Ok(CommandResult::Rejected(ProjectionOutcome::Capacity));
                }
            }
            _ => return Err(Error::Command("directory key uniqueness violated")),
        }
        let result=context.sql(&statement("INSERT INTO directory(device_key, name, location, enabled, revision) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(device_key) DO UPDATE SET name=excluded.name, location=excluded.location, enabled=excluded.enabled, revision=excluded.revision", vec![SqlValue::Text(device.key.as_str().into()), SqlValue::Text(device.attributes.name), SqlValue::Text(device.attributes.location), SqlValue::Integer(i64::from(device.attributes.enabled)), SqlValue::Integer(device.revision)]))?;
        if !matches!(result.as_slice(), [r] if r.rows_affected==1) {
            return Err(Error::Command("projection row count violated"));
        }
        Ok(CommandResult::Success(ProjectionOutcome::Applied))
    }
}
/// Current directory lookup; missing rows can still be pending at their source.
pub struct LookupDevice;
impl Query for LookupDevice {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = DeviceKey;
    type Output = Option<Device>;
    fn execute(
        context: &mut QueryContext<'_>,
        key: DeviceKey,
    ) -> cellule_runtime::Result<Self::Output> {
        let selected = context.sql(&statement(
            &format!("SELECT {FIELDS} FROM directory WHERE device_key=?1"),
            vec![SqlValue::Text(key.as_str().into())],
        ))?;
        let [selected] = selected.as_slice() else {
            return Err(Error::Command("unexpected directory lookup"));
        };
        match selected.rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(device_from_row(row)?)),
            _ => Err(Error::Command("directory uniqueness violated")),
        }
    }
}
/// Bounded directory pages; no application-owned source enumeration is inferred.
pub struct ListDirectory;
impl Query for ListDirectory {
    const MODULE: &'static str = Directory::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Page;
    fn execute(context: &mut QueryContext<'_>, page: PageRequest) -> cellule_runtime::Result<Page> {
        if !(1..=100).contains(&page.limit) {
            return Err(Error::Command("directory page limit must be 1..100"));
        }
        let results = context.sql(&statement(
            &format!(
                "SELECT {FIELDS} FROM directory WHERE device_key>?1 ORDER BY device_key LIMIT ?2"
            ),
            vec![
                SqlValue::Text(page.after.map(String::from).unwrap_or_default()),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?;
        let [r] = results.as_slice() else {
            return Err(Error::Command("unexpected directory page"));
        };
        let mut devices = r
            .rows
            .iter()
            .map(|row| device_from_row(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = devices.len() > page.limit as usize;
        devices.truncate(page.limit as usize);
        let next = if more {
            devices.last().map(|v| v.key.clone())
        } else {
            None
        };
        Ok(Page { devices, next })
    }
}
