use crate::{
    Change, Definition, MAX_DEFINITIONS, MAX_MONITORS, ScheduleOutcome, Schedules, sql,
    wire::{decode_wire, encode_wire},
};
use cellule_runtime::{
    CellModule, Error, partition_for_shard,
    primitives::{
        cron::{CronCommand, CronMutation, CronMutationOutcome},
        sql::SqlValue,
    },
    registry::{Command, CommandContext, CommandResult},
};
/// Bounded domain Cron mutation, sharing one transaction with immutable definition bindings.
pub struct ChangeSchedule;
impl Command for ChangeSchedule {
    const MODULE: &'static str = Schedules::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = ScheduleOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        change: Change,
    ) -> cellule_runtime::Result<CommandResult<ScheduleOutcome>> {
        // Native Cron checks the original issued timestamp when delegated below.
        // A retained first due time may already have elapsed when dispatch resumes.
        let earliest = match &change {
            Change::Upsert { next_due_ms, .. } | Change::Resume { next_due_ms, .. } => *next_due_ms,
            _ => context.now_ms(),
        };
        change.validate(earliest.min(context.now_ms()))?;
        let monitor = change.monitor().bytes();
        let binding = if let Change::Upsert { definition, .. } = &change {
            let bytes = encode_wire(&change, 4096)?;
            let selected = context.sql(&sql::batch(
                "SELECT change,generation FROM definitions WHERE version=?1",
                vec![SqlValue::Blob(definition.id.bytes().to_vec())],
            ))?;
            match sql::rows(&selected)? {
                [] => {}
                [row] => {
                    let [SqlValue::Blob(original), SqlValue::Integer(generation)] = row.as_slice()
                    else {
                        return Err(Error::Command("invalid definition binding"));
                    };
                    return Ok(if original == &bytes {
                        CommandResult::Success(ScheduleOutcome::Applied(*generation as u64))
                    } else {
                        CommandResult::Rejected(ScheduleOutcome::Conflict)
                    });
                }
                _ => return Err(Error::Command("definition binding uniqueness violated")),
            }
            if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM definitions", vec![]))?)?
                >= MAX_DEFINITIONS
            {
                return Ok(CommandResult::Rejected(ScheduleOutcome::Capacity));
            }
            let known = context.sql(&sql::batch(
                "SELECT monitor FROM monitor_ids WHERE monitor=?1",
                vec![SqlValue::Blob(monitor.to_vec())],
            ))?;
            if sql::rows(&known)?.is_empty() {
                if sql::count(
                    &context.sql(&sql::batch("SELECT count(*) FROM monitor_ids", vec![]))?,
                )? >= MAX_MONITORS
                {
                    return Ok(CommandResult::Rejected(ScheduleOutcome::Capacity));
                }
                sql::changed(&context.sql(&sql::batch(
                    "INSERT INTO monitor_ids(monitor) VALUES(?1)",
                    vec![SqlValue::Blob(monitor.to_vec())],
                ))?)?;
            }
            Some((definition.id.bytes(), bytes))
        } else {
            None
        };
        let mutation = match change {
            Change::Upsert {
                definition,
                interval_ms,
                next_due_ms,
                ..
            } => CronMutation::Upsert {
                schedule_id: monitor,
                target_index: 0,
                target_partition: partition_for_shard(0).to_vec(),
                payload: encode_wire(&definition, 1024)?,
                interval_ms,
                next_due_ms,
            },
            Change::Pause { .. } => CronMutation::Pause {
                schedule_id: monitor,
            },
            Change::Resume { next_due_ms, .. } => CronMutation::Resume {
                schedule_id: monitor,
                next_due_ms,
            },
            Change::Delete { .. } => CronMutation::Delete {
                schedule_id: monitor,
            },
        };
        let native = CronCommand::<Schedules>::execute(context, mutation)?;
        let (outcome, rejected) = match native {
            CommandResult::Success(CronMutationOutcome::Applied { generation }) => {
                (ScheduleOutcome::Applied(generation), false)
            }
            CommandResult::Success(CronMutationOutcome::Deleted) => {
                (ScheduleOutcome::Deleted, false)
            }
            CommandResult::Rejected(CronMutationOutcome::NotFound) => {
                (ScheduleOutcome::NotFound, true)
            }
            _ => return Err(Error::Command("unexpected native schedule mutation result")),
        };
        if let Some((id, bytes)) = binding {
            let ScheduleOutcome::Applied(generation) = outcome else {
                return Err(Error::Command("upsert did not produce a native generation"));
            };
            sql::changed(&context.sql(&sql::batch(
                "INSERT INTO definitions(version,change,generation) VALUES(?1,?2,?3)",
                vec![
                        SqlValue::Blob(id.to_vec()),
                        SqlValue::Blob(bytes),
                        SqlValue::Integer(
                            i64::try_from(generation)
                                .map_err(|_| Error::Command("generation outside SQL range"))?,
                        ),
                    ],
            ))?)?;
        }
        Ok(if rejected {
            CommandResult::Rejected(outcome)
        } else {
            CommandResult::Success(outcome)
        })
    }
}
pub(crate) fn definition(payload: &[u8]) -> cellule_runtime::Result<Definition> {
    let value: Definition = decode_wire(payload, 1024)?;
    value.validate()?;
    Ok(value)
}
