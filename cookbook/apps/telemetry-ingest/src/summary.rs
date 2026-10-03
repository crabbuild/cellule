use crate::{
    Bucket, BucketPage, BucketPageRequest, DeviceKey, DeviceSnapshot, MAX_DEVICES,
    ProjectionOutcome, SUMMARIES, Summaries, sql, wire,
};
use cellule_runtime::{
    CellModule, Error, Result, partition_for_shard,
    primitives::sql::{SqlBatch, SqlResultSet, SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
    shard_for_scope,
};
fn lookup(
    sql: impl Fn(&SqlBatch) -> Result<Vec<SqlResultSet>>,
    key: &DeviceKey,
) -> Result<Option<DeviceSnapshot>> {
    let selected = sql::one(sql(&sql::batch(
        "SELECT snapshot FROM devices WHERE device_key=?1",
        vec![SqlValue::Text(key.as_str().into())],
    ))?)?;
    match selected.rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("telemetry summary row differs"));
            };
            let value: DeviceSnapshot = wire::decode(bytes, 4096)?;
            value.validate()?;
            if value.device != *key {
                return Err(Error::Identity("stored summary device differs"));
            }
            Ok(Some(value))
        }
        _ => Err(Error::Command("telemetry summary key uniqueness violated")),
    }
}
/// Replaces one device's complete contribution atomically; repeats and reordering cannot double counts.
pub struct PublishSummary;
impl Command for PublishSummary {
    const MODULE: &'static str = Summaries::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = DeviceSnapshot;
    type Output = ProjectionOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.validate()?;
        if context.target().partition()
            != partition_for_shard(shard_for_scope(SUMMARIES, input.device.as_bytes(), 2)?)
        {
            return Err(Error::Identity(
                "telemetry summary targets a different shard",
            ));
        }
        match lookup(|batch| context.sql(batch), &input.device)? {
            Some(current) if current.window != input.window => {
                return Ok(CommandResult::Rejected(ProjectionOutcome::Conflict));
            }
            Some(current) if current.revision > input.revision => {
                return Ok(CommandResult::Success(ProjectionOutcome::Stale));
            }
            Some(current) if current.revision == input.revision => {
                return Ok(if current == input {
                    CommandResult::Success(ProjectionOutcome::Duplicate)
                } else {
                    CommandResult::Rejected(ProjectionOutcome::Conflict)
                });
            }
            None if sql::count(
                context.sql(&sql::batch("SELECT count(*) FROM devices", vec![]))?,
            )? >= MAX_DEVICES as i64 =>
            {
                return Ok(CommandResult::Rejected(ProjectionOutcome::Capacity));
            }
            _ => {}
        }
        let key = SqlValue::Text(input.device.as_str().into());
        sql::changed(context.sql(&sql::batch("INSERT INTO devices(device_key,revision,accepted,snapshot) VALUES(?1,?2,?3,?4) ON CONFLICT(device_key) DO UPDATE SET revision=excluded.revision,accepted=excluded.accepted,snapshot=excluded.snapshot",vec![key.clone(),SqlValue::Integer(input.revision),SqlValue::Integer(i64::from(input.accepted)),SqlValue::Blob(wire::encode(&input,4096)?)]))?)?;
        context.sql(&sql::batch(
            "DELETE FROM buckets WHERE device_key=?1",
            vec![key.clone()],
        ))?;
        // Replace, rather than add to, the old contribution in the same receiver transaction.
        for bucket in &input.buckets {
            sql::changed(context.sql(&sql::batch(
                "INSERT INTO buckets(device_key,start_ms,count,sum_milli) VALUES(?1,?2,?3,?4)",
                vec![
                    key.clone(),
                    SqlValue::Integer(bucket.start_ms),
                    SqlValue::Integer(i64::from(bucket.count)),
                    SqlValue::Integer(bucket.sum_milli),
                ],
            ))?)?;
        }
        Ok(CommandResult::Success(ProjectionOutcome::Applied))
    }
}
/// Reads one projected device; absence does not prove source absence or completed delivery.
pub struct GetSummary;
impl Query for GetSummary {
    const MODULE: &'static str = Summaries::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = DeviceKey;
    type Output = Option<DeviceSnapshot>;
    fn execute(context: &mut QueryContext<'_>, key: Self::Input) -> Result<Self::Output> {
        lookup(|batch| context.sql(batch), &key)
    }
}
/// Reads grouped event-time buckets at one independently committed fixed summary shard.
pub struct ListBuckets;
impl Query for ListBuckets {
    const MODULE: &'static str = Summaries::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = BucketPageRequest;
    type Output = BucketPage;
    fn execute(context: &mut QueryContext<'_>, page: Self::Input) -> Result<Self::Output> {
        if !(1..=16).contains(&page.limit)
            || page.after_ms.is_some_and(|v| v < 60000 || v % 60000 != 0)
        {
            return Err(Error::Command("invalid telemetry summary cursor or limit"));
        }
        let results=context.sql(&SqlBatch{statements:vec![sql::statement("SELECT count(*),coalesce(sum(accepted),0) FROM devices",vec![]),sql::statement("SELECT start_ms,sum(count),sum(sum_milli) FROM buckets WHERE start_ms>?1 GROUP BY start_ms ORDER BY start_ms LIMIT ?2",vec![SqlValue::Integer(page.after_ms.unwrap_or(0)),SqlValue::Integer(i64::from(page.limit)+1)])]})?;
        let [meta, rows] = results.as_slice() else {
            return Err(Error::Command("telemetry bucket result count differs"));
        };
        let [row] = meta.rows.as_slice() else {
            return Err(Error::Command("telemetry shard totals missing"));
        };
        let [SqlValue::Integer(devices), SqlValue::Integer(accepted)] = row.as_slice() else {
            return Err(Error::Command("telemetry shard total types differ"));
        };
        if !(0..=MAX_DEVICES as i64).contains(devices)
            || !(0..=MAX_DEVICES as i64 * 128).contains(accepted)
        {
            return Err(Error::Command("telemetry shard totals exceed bounds"));
        }
        let mut buckets = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            let [
                SqlValue::Integer(start),
                SqlValue::Integer(count),
                SqlValue::Integer(sum),
            ] = row.as_slice()
            else {
                return Err(Error::Command("telemetry grouped bucket differs"));
            };
            if !(0..=2048).contains(count)
                || !(-count * crate::MAX_VALUE..=count * crate::MAX_VALUE).contains(sum)
            {
                return Err(Error::Command("telemetry grouped bucket exceeds bounds"));
            }
            buckets.push(Bucket {
                start_ms: *start,
                count: *count as u32,
                sum_milli: *sum,
            });
        }
        let more = buckets.len() > page.limit as usize;
        buckets.truncate(page.limit as usize);
        let next_ms = if more {
            buckets.last().map(|v| v.start_ms)
        } else {
            None
        };
        Ok(BucketPage {
            devices: *devices as u32,
            accepted: *accepted as u32,
            buckets,
            next_ms,
        })
    }
}
