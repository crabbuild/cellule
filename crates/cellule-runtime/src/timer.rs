use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::effects::EffectBatch;
use crate::{
    BoundedEncoder, CellTarget, EffectCommandIntent, Error, NamespaceId, Result, WireValue,
};

mod api;

pub use api::{TimerCommand, TimerModule, TimerNamespace, TimerQueryCommand, register_timer};

/// Version-one Timer schema for a module's `MigrationDescriptor`.
pub const TIMER_SCHEMA_SQL: &str = include_str!("migrations/timer.sql");
const MAX_PAYLOAD_BYTES: usize = 256 * 1_024;
const MAX_PARTITION_BYTES: usize = 1_024;
const MAX_FUTURE_MS: i64 = 5 * 365 * 24 * 60 * 60 * 1_000;
const EFFECT_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_LIST_BYTES: usize = 512 * 1_024;
const MAX_LIST_ITEMS: u32 = 128;

/// Compile-time destination contract for one-shot Timer invocations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerTarget {
    module: &'static str,
    namespace: NamespaceId,
    command_id: u32,
    codec_version: u32,
    input_limit: u32,
}

impl TimerTarget {
    #[must_use]
    pub const fn new(
        module: &'static str,
        namespace: NamespaceId,
        command_id: u32,
        codec_version: u32,
        input_limit: u32,
    ) -> Self {
        Self {
            module,
            namespace,
            command_id,
            codec_version,
            input_limit,
        }
    }

    pub(crate) const fn module(self) -> &'static str {
        self.module
    }
    pub(crate) const fn namespace(self) -> NamespaceId {
        self.namespace
    }
    pub(crate) const fn command_id(self) -> u32 {
        self.command_id
    }
    pub(crate) const fn codec_version(self) -> u32 {
        self.codec_version
    }
    pub(crate) const fn input_limit(self) -> u32 {
        self.input_limit
    }
}

/// Payload delivered exactly once to a destination inbox for one Timer deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerInvocation {
    pub timer_id: [u8; 16],
    pub generation: u64,
    pub scheduled_at_ms: i64,
    pub payload: Vec<u8>,
}

/// Durable one-shot Timer mutation.
///
/// A repeated `Set` for the same `timer_id` replaces the pending deadline and
/// advances the generation, so a destination can recognize a delivery that a
/// superseded generation scheduled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerMutation {
    Set {
        timer_id: [u8; 16],
        target_index: u32,
        target_partition: Vec<u8>,
        payload: Vec<u8>,
        due_at_ms: i64,
    },
    Cancel {
        timer_id: [u8; 16],
    },
}

/// Result of one Timer mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerMutationOutcome {
    Applied { generation: u64 },
    Cancelled,
    NotFound,
}

/// Materialized pending Timer deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerEntry {
    pub timer_id: [u8; 16],
    pub target_index: u32,
    pub target_partition: Vec<u8>,
    pub payload: Vec<u8>,
    pub due_at_ms: i64,
    pub generation: u64,
}

/// Bounded Timer query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerQuery {
    Get { timer_id: [u8; 16] },
    List { after: Option<[u8; 16]>, limit: u32 },
}

/// Result of a Timer query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerQueryResult {
    Get(Option<TimerEntry>),
    List {
        entries: Vec<TimerEntry>,
        next: Option<[u8; 16]>,
    },
}

/// Installs the exact version-one Timer schema.
pub fn install_timer_schema(transaction: &Transaction<'_>) -> Result<()> {
    transaction.execute_batch(TIMER_SCHEMA_SQL)?;
    Ok(())
}

/// Applies one durable Timer mutation.
pub fn timer_mutate(
    transaction: &Transaction<'_>,
    now_ms: i64,
    issued_at_ms: i64,
    targets: &[TimerTarget],
    mutation: &TimerMutation,
) -> Result<TimerMutationOutcome> {
    validate_now(now_ms)?;
    validate_now(issued_at_ms)?;
    match mutation {
        TimerMutation::Set {
            timer_id,
            target_index,
            target_partition,
            payload,
            due_at_ms,
        } => {
            let target = target_at(targets, *target_index)?;
            validate_set(issued_at_ms, target, target_partition, payload, *due_at_ms)?;
            transaction.execute(
                "INSERT INTO timer_entries(timer_id, target_index, target_partition, payload, due_at_ms, generation, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6) ON CONFLICT(timer_id) DO UPDATE SET target_index = excluded.target_index, target_partition = excluded.target_partition, payload = excluded.payload, due_at_ms = excluded.due_at_ms, generation = timer_entries.generation + 1, updated_at_ms = excluded.updated_at_ms",
                (
                    timer_id.as_slice(),
                    i64::from(*target_index),
                    target_partition,
                    payload,
                    *due_at_ms,
                    now_ms,
                ),
            )?;
            Ok(TimerMutationOutcome::Applied {
                generation: entry_generation(transaction, *timer_id)?,
            })
        }
        TimerMutation::Cancel { timer_id } => {
            let changed = transaction.execute(
                "DELETE FROM timer_entries WHERE timer_id = ?1",
                [timer_id.as_slice()],
            )?;
            Ok(if changed == 1 {
                TimerMutationOutcome::Cancelled
            } else {
                TimerMutationOutcome::NotFound
            })
        }
    }
}

/// Reads one or one bounded page of pending Timer deadlines.
pub fn timer_query(connection: &Connection, query: &TimerQuery) -> Result<TimerQueryResult> {
    match query {
        TimerQuery::Get { timer_id } => {
            Ok(TimerQueryResult::Get(load_entry(connection, *timer_id)?))
        }
        TimerQuery::List { after, limit } => {
            if !(1..=MAX_LIST_ITEMS).contains(limit) {
                return Err(Error::Command("timer list limit must be in 1..=128"));
            }
            let after = after.unwrap_or([0; 16]);
            let mut statement = connection.prepare(
                "SELECT timer_id, target_index, target_partition, payload, due_at_ms, generation FROM timer_entries WHERE timer_id > ?1 ORDER BY timer_id LIMIT ?2",
            )?;
            let rows =
                statement.query_map((after.as_slice(), i64::from(*limit) + 1), decode_entry)?;
            let mut entries = Vec::with_capacity(*limit as usize);
            let mut bytes = 0_usize;
            let mut truncated = false;
            for row in rows {
                let entry = row?;
                let entry_bytes = entry
                    .target_partition
                    .len()
                    .checked_add(entry.payload.len())
                    .and_then(|value| value.checked_add(96))
                    .ok_or(Error::Command("timer list byte count overflow"))?;
                if entries.len() == *limit as usize
                    || bytes.saturating_add(entry_bytes) > MAX_LIST_BYTES
                {
                    truncated = true;
                    break;
                }
                bytes += entry_bytes;
                entries.push(entry);
            }
            let next = truncated
                .then(|| entries.last().map(|value| value.timer_id))
                .flatten();
            Ok(TimerQueryResult::List { entries, next })
        }
    }
}

pub(crate) fn timer_fire_due_bounded(
    transaction: &Transaction<'_>,
    effects: &mut EffectBatch,
    source: &CellTarget,
    now_ms: i64,
    targets: &[TimerTarget],
    limit: usize,
) -> Result<usize> {
    let mut processed = 0;
    while processed < limit {
        let row = transaction
            .query_row(
                "SELECT timer_id, target_index, target_partition, payload, due_at_ms, generation FROM timer_entries INDEXED BY timer_due WHERE due_at_ms <= ?1 ORDER BY due_at_ms, timer_id LIMIT 1",
                [now_ms],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((timer_id, target_index, partition, payload, scheduled_at_ms, generation)) = row
        else {
            break;
        };
        let timer_id: [u8; 16] = timer_id
            .try_into()
            .map_err(|_| Error::Command("invalid stored timer ID"))?;
        let target_index = u32::try_from(target_index)
            .map_err(|_| Error::Command("invalid stored timer target index"))?;
        let generation = u64::try_from(generation)
            .map_err(|_| Error::Command("invalid stored timer generation"))?;
        let target = target_at(targets, target_index)?;
        let invocation = TimerInvocation {
            timer_id,
            generation,
            scheduled_at_ms,
            payload,
        };
        let mut encoder = BoundedEncoder::new(target.input_limit())?;
        invocation.encode(&mut encoder)?;
        let destination = CellTarget::new(
            source.tenant(),
            source.application(),
            target.namespace(),
            &partition,
        )?;
        effects.insert_command(
            transaction,
            &EffectCommandIntent {
                target: destination,
                command_id: target.command_id(),
                codec_version: target.codec_version(),
                input: encoder.finish(),
                expires_at_ms: now_ms
                    .checked_add(EFFECT_LIFETIME_MS)
                    .ok_or(Error::Command("timer effect expiry overflow"))?,
            },
        )?;
        if transaction.execute(
            "DELETE FROM timer_entries WHERE timer_id = ?1 AND generation = ?2 AND due_at_ms = ?3",
            (
                timer_id.as_slice(),
                i64::try_from(generation)
                    .map_err(|_| Error::Command("timer generation overflow"))?,
                scheduled_at_ms,
            ),
        )? != 1
        {
            return Err(Error::Command("timer entry changed during serialized fire"));
        }
        processed += 1;
    }
    Ok(processed)
}

fn entry_generation(transaction: &Transaction<'_>, timer_id: [u8; 16]) -> Result<u64> {
    let value = transaction.query_row(
        "SELECT generation FROM timer_entries WHERE timer_id = ?1",
        [timer_id.as_slice()],
        |row| row.get::<_, i64>(0),
    )?;
    u64::try_from(value).map_err(|_| Error::Command("invalid stored timer generation"))
}

fn load_entry(connection: &Connection, timer_id: [u8; 16]) -> Result<Option<TimerEntry>> {
    connection
        .query_row(
            "SELECT timer_id, target_index, target_partition, payload, due_at_ms, generation FROM timer_entries WHERE timer_id = ?1",
            [timer_id.as_slice()],
            decode_entry,
        )
        .optional()
        .map_err(Into::into)
}

fn decode_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<TimerEntry> {
    Ok(TimerEntry {
        timer_id: row
            .get::<_, Vec<u8>>(0)?
            .try_into()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        target_index: u32::try_from(row.get::<_, i64>(1)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        target_partition: row.get(2)?,
        payload: row.get(3)?,
        due_at_ms: row.get(4)?,
        generation: u64::try_from(row.get::<_, i64>(5)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

fn validate_set(
    issued_at_ms: i64,
    target: TimerTarget,
    partition: &[u8],
    payload: &[u8],
    due_at_ms: i64,
) -> Result<()> {
    if partition.len() > MAX_PARTITION_BYTES || payload.len() > MAX_PAYLOAD_BYTES {
        return Err(Error::Command("timer target or payload exceeds limits"));
    }
    if due_at_ms < 0 || due_at_ms > issued_at_ms.saturating_add(MAX_FUTURE_MS) {
        return Err(Error::Command("timer due time is outside five-year window"));
    }
    let wrapper_bytes = payload
        .len()
        .checked_add(64)
        .ok_or(Error::Command("timer invocation size overflow"))?;
    if wrapper_bytes > target.input_limit() as usize {
        return Err(Error::Command(
            "timer invocation exceeds target input limit",
        ));
    }
    Ok(())
}

fn target_at(targets: &[TimerTarget], index: u32) -> Result<TimerTarget> {
    targets
        .get(index as usize)
        .copied()
        .ok_or(Error::Command("timer target index is unavailable"))
}

fn validate_now(now_ms: i64) -> Result<()> {
    if now_ms < 0 {
        return Err(Error::Command("negative timer logical time"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApplicationId, IncarnationId, TenantId};
    use cellule_ltx::rusqlite::Connection;

    const SOURCE_NAMESPACE: NamespaceId = NamespaceId::from_bytes([1; 16]);
    const TARGET_NAMESPACE: NamespaceId = NamespaceId::from_bytes([2; 16]);
    const TARGETS: &[TimerTarget] = &[TimerTarget::new(
        "timer-destination",
        TARGET_NAMESPACE,
        9,
        1,
        1024,
    )];

    fn source_target() -> CellTarget {
        CellTarget::new(
            TenantId::from_bytes([3; 16]),
            ApplicationId::from_bytes([4; 16]),
            SOURCE_NAMESPACE,
            b"timer-shard",
        )
        .unwrap()
    }

    fn install(connection: &Connection) -> CellTarget {
        let source = source_target();
        let transaction = connection.unchecked_transaction().unwrap();
        crate::schema::install_runtime_schema_in(
            &transaction,
            source.cell_id(),
            IncarnationId::from_bytes([5; 16]),
            1,
        )
        .unwrap();
        install_timer_schema(&transaction).unwrap();
        transaction.commit().unwrap();
        source
    }

    fn set_deadline(transaction: &Transaction<'_>, due_at_ms: i64) {
        timer_mutate(
            transaction,
            200,
            10,
            TARGETS,
            &TimerMutation::Set {
                timer_id: [6; 16],
                target_index: 0,
                target_partition: b"destination".to_vec(),
                payload: b"expire".to_vec(),
                due_at_ms,
            },
        )
        .unwrap();
    }

    #[test]
    fn due_deadline_fires_once_and_removes_the_entry() {
        let mut connection = Connection::open_in_memory().unwrap();
        let source = install(&connection);
        let transaction = connection.transaction().unwrap();
        set_deadline(&transaction, 100);
        let mut effects = EffectBatch::new(&transaction, &source, 1, 200).unwrap();
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut effects, &source, 200, TARGETS, 8).unwrap(),
            1
        );
        assert_eq!(
            transaction
                .query_row("SELECT count(*) FROM sys_effects", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert_eq!(
            transaction
                .query_row("SELECT count(*) FROM timer_entries", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut effects, &source, 200, TARGETS, 8).unwrap(),
            0
        );
    }

    #[test]
    fn future_deadline_waits_for_its_tick() {
        let mut connection = Connection::open_in_memory().unwrap();
        let source = install(&connection);
        let transaction = connection.transaction().unwrap();
        set_deadline(&transaction, 300);
        let mut effects = EffectBatch::new(&transaction, &source, 1, 200).unwrap();
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut effects, &source, 200, TARGETS, 8).unwrap(),
            0
        );
        transaction
            .execute("UPDATE sys_meta SET commit_sequence = 1", [])
            .unwrap();
        let mut later = EffectBatch::new(&transaction, &source, 2, 300).unwrap();
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut later, &source, 300, TARGETS, 8).unwrap(),
            1
        );
    }

    #[test]
    fn repeated_set_replaces_the_deadline_and_advances_generation() {
        let mut connection = Connection::open_in_memory().unwrap();
        let source = install(&connection);
        let transaction = connection.transaction().unwrap();
        set_deadline(&transaction, 100);
        let TimerMutationOutcome::Applied { generation } = timer_mutate(
            &transaction,
            210,
            10,
            TARGETS,
            &TimerMutation::Set {
                timer_id: [6; 16],
                target_index: 0,
                target_partition: b"destination".to_vec(),
                payload: b"expire-later".to_vec(),
                due_at_ms: 300,
            },
        )
        .unwrap() else {
            panic!("timer set was not applied");
        };
        assert_eq!(generation, 2);
        let mut effects = EffectBatch::new(&transaction, &source, 1, 200).unwrap();
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut effects, &source, 200, TARGETS, 8).unwrap(),
            0
        );
        let TimerQueryResult::Get(Some(entry)) =
            timer_query(&transaction, &TimerQuery::Get { timer_id: [6; 16] }).unwrap()
        else {
            panic!("timer entry was not retained");
        };
        assert_eq!(entry.due_at_ms, 300);
        assert_eq!(entry.payload, b"expire-later");
    }

    #[test]
    fn cancelled_deadline_never_fires() {
        let mut connection = Connection::open_in_memory().unwrap();
        let source = install(&connection);
        let transaction = connection.transaction().unwrap();
        set_deadline(&transaction, 100);
        assert_eq!(
            timer_mutate(
                &transaction,
                210,
                10,
                TARGETS,
                &TimerMutation::Cancel { timer_id: [6; 16] },
            )
            .unwrap(),
            TimerMutationOutcome::Cancelled
        );
        let mut effects = EffectBatch::new(&transaction, &source, 1, 200).unwrap();
        assert_eq!(
            timer_fire_due_bounded(&transaction, &mut effects, &source, 200, TARGETS, 8).unwrap(),
            0
        );
    }

    #[test]
    fn checked_in_timer_schema_matches_runtime_schema() {
        assert_eq!(
            TIMER_SCHEMA_SQL,
            include_str!("../docs/contracts/timer.sql")
        );
    }
}
