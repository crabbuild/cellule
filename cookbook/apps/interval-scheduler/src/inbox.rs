use crate::{
    Delivery, DeliveryPage, DeliveryPageRequest, Inbox, RecordOutcome, Reminder, ScheduleId,
    model::Definition,
};
use cellule_runtime::{
    CellModule, Error,
    codec::{BoundedDecoder, WireValue},
    primitives::{
        cron::CronInvocation,
        sql::{SqlBatch, SqlStatement, SqlValue},
    },
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
/// Destination command bound as the Cron namespace's only delivery target.
pub struct RecordReminder;
impl Command for RecordReminder {
    const MODULE: &'static str = Inbox::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = CronInvocation;
    type Output = RecordOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: CronInvocation,
    ) -> cellule_runtime::Result<CommandResult<RecordOutcome>> {
        let schedule = ScheduleId::from_bytes(input.schedule_id)?;
        let mut decoder = BoundedDecoder::new(&input.payload, 1024)?;
        let definition = Definition::decode(&mut decoder)?;
        decoder.finish()?;
        let generation = i64::try_from(input.generation)
            .map_err(|_| Error::Command("reminder generation exceeds SQL range"))?;
        let occurrence = i64::try_from(input.occurrence)
            .map_err(|_| Error::Command("reminder occurrence exceeds SQL range"))?;
        if generation <= 0 || occurrence <= 0 || input.scheduled_at_ms < 0 {
            return Err(Error::Command("invalid reminder occurrence"));
        }
        let parameters = vec![
            SqlValue::Text(schedule.to_string()),
            SqlValue::Text(definition.id.to_string()),
            SqlValue::Integer(generation),
            SqlValue::Integer(occurrence),
        ];
        let mut insert = parameters.clone();
        insert.extend([
            SqlValue::Integer(input.scheduled_at_ms),
            SqlValue::Text(definition.reminder.title.clone()),
            SqlValue::Text(definition.reminder.message.clone()),
        ]);
        // The business key is durable beyond inbox retention. Verify immutable
        // content after INSERT OR IGNORE; changed bytes are never silently lost.
        let results=context.sql(&SqlBatch {statements:vec![statement("INSERT OR IGNORE INTO reminders(schedule,definition,generation,occurrence,scheduled_at_ms,title,message) VALUES(?1,?2,?3,?4,?5,?6,?7)",insert),statement("SELECT row_id,schedule,definition,generation,occurrence,scheduled_at_ms,title,message FROM reminders WHERE schedule=?1 AND definition=?2 AND generation=?3 AND occurrence=?4",parameters)]})?;
        let [_, selected] = results.as_slice() else {
            return Err(Error::Command("unexpected reminder result"));
        };
        let [row] = selected.rows.as_slice() else {
            return Err(Error::Command("missing recorded reminder"));
        };
        let stored = delivery_from_row(row)?;
        Ok(
            if stored.scheduled_at_ms == input.scheduled_at_ms
                && stored.reminder == definition.reminder
            {
                CommandResult::Success(RecordOutcome::Recorded { row: stored.row })
            } else {
                CommandResult::Rejected(RecordOutcome::Conflict)
            },
        )
    }
}
/// Typed bounded read of published deliveries for one stable schedule identity.
pub struct ListDeliveries;
impl Query for ListDeliveries {
    const MODULE: &'static str = Inbox::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = DeliveryPageRequest;
    type Output = DeliveryPage;
    fn execute(
        context: &mut QueryContext<'_>,
        page: DeliveryPageRequest,
    ) -> cellule_runtime::Result<DeliveryPage> {
        if !(1..=100).contains(&page.limit) || page.after.is_some_and(|after| after <= 0) {
            return Err(Error::Command(
                "delivery page requires a positive cursor and limit 1..100",
            ));
        }
        let results=context.sql(&SqlBatch {statements:vec![statement("SELECT row_id,schedule,definition,generation,occurrence,scheduled_at_ms,title,message FROM reminders WHERE schedule=?1 AND row_id>?2 ORDER BY row_id LIMIT ?3",vec![SqlValue::Text(page.schedule.to_string()),SqlValue::Integer(page.after.unwrap_or(0)),SqlValue::Integer(i64::from(page.limit)+1)])]})?;
        let [selected] = results.as_slice() else {
            return Err(Error::Command("unexpected reminder page result"));
        };
        let mut deliveries = selected
            .rows
            .iter()
            .map(|row| delivery_from_row(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = deliveries.len() > page.limit as usize;
        deliveries.truncate(page.limit as usize);
        let next = if more {
            deliveries.last().map(|row| row.row)
        } else {
            None
        };
        Ok(DeliveryPage { deliveries, next })
    }
}
fn delivery_from_row(row: &[SqlValue]) -> cellule_runtime::Result<Delivery> {
    let [
        SqlValue::Integer(id),
        SqlValue::Text(schedule),
        SqlValue::Text(definition),
        SqlValue::Integer(generation),
        SqlValue::Integer(occurrence),
        SqlValue::Integer(due),
        SqlValue::Text(title),
        SqlValue::Text(message),
    ] = row
    else {
        return Err(Error::Command("reminder row differs from schema"));
    };
    if *id <= 0 || *generation <= 0 || *occurrence <= 0 || *due < 0 {
        return Err(Error::Command("stored reminder violates domain bounds"));
    }
    let reminder = Reminder {
        title: title.clone(),
        message: message.clone(),
    };
    reminder.validate()?;
    Ok(Delivery {
        row: *id,
        schedule: ScheduleId::parse(schedule)?,
        definition: ScheduleId::parse(definition)?,
        generation: *generation as u64,
        occurrence: *occurrence as u64,
        scheduled_at_ms: *due,
        reminder,
    })
}
