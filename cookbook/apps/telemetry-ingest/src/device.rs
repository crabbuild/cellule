use crate::{
    Decision, DeviceKey, DeviceOutcome, DeviceState, Devices, Event, MAX_EVENTS, PublishSummary,
    RecordedEvent, Registration, SUMMARIES, Version, Window, sql, wire,
};
use cellule_runtime::{
    CellModule, CellTarget, Error, Result, partition_for_shard,
    primitives::{
        effects::EffectCommandIntent,
        sql::{SqlBatch, SqlResultSet, SqlValue},
    },
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
    shard_for_scope,
};
fn reject(decision: Decision) -> CommandResult<DeviceOutcome> {
    CommandResult::Rejected(DeviceOutcome {
        decision,
        version: None,
    })
}
fn version(state: &DeviceState) -> Result<Version> {
    if state.effect_id == [0; 32] {
        return Err(Error::Command(
            "telemetry source has no committed projection intent",
        ));
    }
    Ok(Version {
        snapshot: state.snapshot()?,
        effect_id: state.effect_id,
    })
}
fn unchanged(state: &DeviceState) -> Result<CommandResult<DeviceOutcome>> {
    Ok(CommandResult::Success(DeviceOutcome {
        decision: Decision::Duplicate,
        version: Some(version(state)?),
    }))
}
fn source(
    sql: impl Fn(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    target: Option<&CellTarget>,
) -> Result<Option<DeviceState>> {
    let selected = sql(&SqlBatch {
        statements: vec![
            sql::statement(
                "SELECT device_key,start_ms,minutes,revision,effect_id FROM device WHERE singleton=1 LIMIT 2",
                vec![],
            ),
            sql::statement(
                "SELECT sequence,at_ms,value_milli,reordered FROM events ORDER BY sequence LIMIT 129",
                vec![],
            ),
        ],
    })?;
    let [meta, rows] = selected.as_slice() else {
        return Err(Error::Command("telemetry source result count differs"));
    };
    let row = match meta.rows.as_slice() {
        [] if rows.rows.is_empty() => return Ok(None),
        [row] => row,
        _ => return Err(Error::Command("telemetry source singleton violated")),
    };
    let [
        SqlValue::Text(key),
        SqlValue::Integer(start),
        SqlValue::Integer(minutes),
        SqlValue::Integer(revision),
        SqlValue::Blob(effect),
    ] = row.as_slice()
    else {
        return Err(Error::Command("telemetry source row differs"));
    };
    let device = DeviceKey::new(key.clone())?;
    if let Some(target) = target
        && target.partition() != sql::partition(&device)?
    {
        return Err(Error::Identity(
            "stored telemetry device differs from its Cell",
        ));
    }
    let mut events = Vec::with_capacity(rows.rows.len());
    for row in &rows.rows {
        let [
            SqlValue::Integer(sequence),
            SqlValue::Integer(at_ms),
            SqlValue::Integer(value),
            SqlValue::Integer(reordered),
        ] = row.as_slice()
        else {
            return Err(Error::Command("telemetry event row differs"));
        };
        let reordered = match reordered {
            0 => false,
            1 => true,
            _ => return Err(Error::Command("invalid telemetry reorder flag")),
        };
        events.push(RecordedEvent {
            event: Event {
                device: device.clone(),
                sequence: *sequence,
                at_ms: *at_ms,
                value_milli: *value,
            },
            reordered,
        });
    }
    let state = DeviceState {
        device,
        window: Window {
            start_ms: *start,
            minutes: u32::try_from(*minutes)
                .map_err(|_| Error::Command("invalid telemetry window width"))?,
        },
        revision: *revision,
        events,
        effect_id: effect
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("telemetry effect identity length differs"))?,
    };
    state.validate()?;
    Ok(Some(state))
}
fn publish(context: &mut CommandContext<'_, '_>) -> Result<CommandResult<DeviceOutcome>> {
    let state = source(|batch| context.sql(batch), Some(context.target()))?
        .ok_or(Error::Command("changed telemetry source disappeared"))?;
    let snapshot = state.snapshot()?;
    let shard = shard_for_scope(SUMMARIES, state.device.as_bytes(), 2)?;
    let effect_id = context.emit_effect(&EffectCommandIntent {
        target: CellTarget::new(
            context.target().tenant(),
            context.target().application(),
            SUMMARIES,
            &partition_for_shard(shard),
        )?,
        command_id: PublishSummary::ID,
        codec_version: 1,
        input: wire::encode(&snapshot, 4096)?,
        expires_at_ms: context
            .now_ms()
            .checked_add(7 * 24 * 60 * 60 * 1000)
            .ok_or(Error::Command("telemetry projection expiry overflow"))?,
    })?;
    // The new event, revision, and native intent commit together. Any error rolls them all back.
    sql::changed(context.sql(&sql::batch(
        "UPDATE device SET effect_id=?1 WHERE singleton=1",
        vec![SqlValue::Blob(effect_id.to_vec())],
    ))?)?;
    Ok(CommandResult::Success(DeviceOutcome {
        decision: Decision::Applied,
        version: Some(Version {
            snapshot,
            effect_id,
        }),
    }))
}
/// Registers an immutable event-time window and emits its initial empty contribution.
pub struct RegisterDevice;
impl Command for RegisterDevice {
    const MODULE: &'static str = Devices::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Registration;
    type Output = DeviceOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if input.window.validate().is_err()
            || context.target().partition() != sql::partition(&input.device)?
        {
            return Ok(reject(Decision::Invalid));
        }
        if let Some(state) = source(|batch| context.sql(batch), Some(context.target()))? {
            return if state.window == input.window {
                unchanged(&state)
            } else {
                Ok(reject(Decision::Conflict))
            };
        }
        sql::changed(context.sql(&sql::batch("INSERT INTO device(singleton,device_key,start_ms,minutes,revision,effect_id) VALUES(1,?1,?2,?3,1,?4)",vec![SqlValue::Text(input.device.as_str().into()),SqlValue::Integer(input.window.start_ms),SqlValue::Integer(i64::from(input.window.minutes)),SqlValue::Blob(vec![0;32])]))?)?;
        publish(context)
    }
}
/// Permanently binds an exact source sequence; lower unique sequences count without replacing latest.
pub struct RecordEvent;
impl Command for RecordEvent {
    const MODULE: &'static str = Devices::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Event;
    type Output = DeviceOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if input.validate().is_err()
            || context.target().partition() != sql::partition(&input.device)?
        {
            return Ok(reject(Decision::Invalid));
        }
        let Some(state) = source(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(reject(Decision::NotRegistered));
        };
        if let Some(row) = state
            .events
            .iter()
            .find(|v| v.event.sequence == input.sequence)
        {
            return if row.event == input {
                unchanged(&state)
            } else {
                Ok(reject(Decision::Conflict))
            };
        }
        if state.window.bucket(input.at_ms).is_none() {
            return Ok(reject(Decision::OutsideWindow));
        }
        if state.events.len() >= MAX_EVENTS {
            return Ok(reject(Decision::Capacity));
        }
        let reordered = state
            .events
            .last()
            .is_some_and(|v| v.event.sequence > input.sequence);
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO events(sequence,at_ms,value_milli,reordered) VALUES(?1,?2,?3,?4)",
            vec![
                SqlValue::Integer(input.sequence),
                SqlValue::Integer(input.at_ms),
                SqlValue::Integer(input.value_milli),
                SqlValue::Integer(i64::from(reordered)),
            ],
        ))?)?;
        sql::changed(context.sql(&sql::batch(
            "UPDATE device SET revision=revision+1 WHERE singleton=1",
            vec![],
        ))?)?;
        publish(context)
    }
}
/// Reads complete bounded history at one device-local commit, including actual latest intent.
pub struct GetDevice;
impl Query for GetDevice {
    const MODULE: &'static str = Devices::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = DeviceKey;
    type Output = Option<DeviceState>;
    fn execute(context: &mut QueryContext<'_>, key: Self::Input) -> Result<Self::Output> {
        let value = source(|batch| context.sql(batch), None)?;
        if let Some(state) = &value {
            if state.device != key {
                return Err(Error::Identity("telemetry read targets a different device"));
            }
            version(state)?;
        }
        Ok(value)
    }
}
