use crate::{
    ALERTS, Check, Checks, Edge, EdgeKind, Health, Inspection, MAX_CHECKS, MonitorState,
    PageRequest, RecordAlert, RecordOutcome, StoredCheck, Ticket,
    model::{decode, edge_key, encode},
    sql,
};
use cellule_runtime::{
    CellModule, CellTarget, Error, partition_for_shard,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn scope(context: &CommandContext<'_, '_>, check: &Check) -> cellule_runtime::Result<()> {
    check.validate()?;
    let source = CellTarget::new(
        context.target().tenant(),
        context.target().application(),
        crate::SCHEDULES,
        &partition_for_shard(0),
    )?;
    if check.ticket.source_cell != *source.cell_id().as_bytes() {
        return Err(Error::Identity("foreign probe Cron source"));
    }
    Ok(())
}
fn state_row(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<Option<MonitorState>> {
    match rows {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid incident state row"));
            };
            Ok(Some(decode(bytes)?))
        }
        _ => Err(Error::Command("incident state uniqueness violated")),
    }
}
fn stored(row: &[SqlValue]) -> cellule_runtime::Result<StoredCheck> {
    let [
        SqlValue::Integer(id),
        SqlValue::Blob(check),
        SqlValue::Blob(outcome),
    ] = row
    else {
        return Err(Error::Command("invalid stored check row"));
    };
    let check: Check = decode(check)?;
    check.validate()?;
    let outcome: RecordOutcome = decode(outcome)?;
    if !matches!(&outcome,RecordOutcome::Recorded{row,..}if row==id&&*id>0) {
        return Err(Error::Command("stored check outcome differs from its row"));
    }
    Ok(StoredCheck {
        row: *id,
        check,
        outcome,
    })
}
/// Atomically records immutable observations, incident changes, and notification intent.
pub struct RecordCheck;
impl Command for RecordCheck {
    const MODULE: &'static str = Checks::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Check;
    type Output = RecordOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        check: Check,
    ) -> cellule_runtime::Result<CommandResult<RecordOutcome>> {
        scope(context, &check)?;
        let key = check.ticket.key();
        let encoded = encode(&check)?;
        let previous = context.sql(&sql::batch(
            "SELECT row_id,check_bytes,outcome FROM checks WHERE check_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?;
        match sql::rows(&previous)? {
            [] => {}
            [row] => {
                let original = stored(row)?;
                return Ok(if original.check == check {
                    CommandResult::Success(original.outcome)
                } else {
                    CommandResult::Rejected(RecordOutcome::Conflict)
                });
            }
            _ => return Err(Error::Command("check identity uniqueness violated")),
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM checks", vec![]))?)?
            >= MAX_CHECKS
        {
            return Ok(CommandResult::Rejected(RecordOutcome::Capacity));
        }
        let parameters = vec![
            SqlValue::Blob(check.ticket.monitor.bytes().to_vec()),
            SqlValue::Blob(check.ticket.definition.id.bytes().to_vec()),
        ];
        let selected = context.sql(&sql::batch(
            "SELECT state FROM monitor_state WHERE monitor=?1 AND definition=?2",
            parameters.clone(),
        ))?;
        let mut state = state_row(sql::rows(&selected)?)?.unwrap_or(MonitorState {
            monitor: check.ticket.monitor,
            definition: check.ticket.definition.id,
            occurrence: 0,
            health: None,
            incident: None,
        });
        let current = check.ticket.occurrence > state.occurrence;
        let mut edge = None;
        // Arrival can be out of source order. Keep every check, but only advance the
        // definition-local watermark; an older down check cannot reopen a recovered incident.
        if current {
            state.occurrence = check.ticket.occurrence;
            if check.probe.health != Health::Unknown {
                let kind = match (check.probe.health, state.incident) {
                    (Health::Down, None) => Some(EdgeKind::Opened),
                    (Health::Up, Some(_)) => Some(EdgeKind::Closed),
                    _ => None,
                };
                state.health = Some(check.probe.health);
                if let Some(kind) = kind {
                    let edge_id = edge_key(&key, kind);
                    let incident = if kind == EdgeKind::Opened {
                        sql::changed(&context.sql(&sql::batch(
                            "INSERT INTO incidents(incident,opening) VALUES(?1,?2)",
                            vec![
                                SqlValue::Blob(edge_id.to_vec()),
                                SqlValue::Blob(key.to_vec()),
                            ],
                        ))?)?;
                        state.incident = Some(edge_id);
                        edge_id
                    } else {
                        let incident = state
                            .incident
                            .ok_or(Error::Command("closing incident missing"))?;
                        sql::changed(&context.sql(&sql::batch(
                            "UPDATE incidents SET closing=?2 WHERE incident=?1 AND closing IS NULL",
                            vec![
                                SqlValue::Blob(incident.to_vec()),
                                SqlValue::Blob(key.to_vec()),
                            ],
                        ))?)?;
                        state.incident = None;
                        incident
                    };
                    let value = Edge {
                        key: edge_id,
                        incident,
                        monitor: state.monitor,
                        definition: state.definition,
                        kind,
                        check: key,
                    };
                    value.validate()?;
                    let effect = context.emit_effect(&EffectCommandIntent {
                        target: CellTarget::new(
                            context.target().tenant(),
                            context.target().application(),
                            ALERTS,
                            &partition_for_shard(0),
                        )?,
                        command_id: RecordAlert::ID,
                        codec_version: 1,
                        input: crate::wire::encode_wire(&value, 2048)?,
                        expires_at_ms: context
                            .now_ms()
                            .checked_add(7 * 24 * 60 * 60 * 1000)
                            .ok_or(Error::Command("notification expiry overflow"))?,
                    })?;
                    sql::changed(&context.sql(&sql::batch(
                        "INSERT INTO edges(edge_key,edge,effect_id) VALUES(?1,?2,?3)",
                        vec![
                            SqlValue::Blob(edge_id.to_vec()),
                            SqlValue::Blob(encode(&value)?),
                            SqlValue::Blob(effect.to_vec()),
                        ],
                    ))?)?;
                    edge = Some(value);
                }
            }
            let mut state_parameters = parameters.clone();
            state_parameters.push(SqlValue::Blob(encode(&state)?));
            sql::changed(&context.sql(&sql::batch("INSERT INTO monitor_state(monitor,definition,state) VALUES(?1,?2,?3) ON CONFLICT(monitor,definition) DO UPDATE SET state=excluded.state",state_parameters))?)?;
        }
        sql::changed(&context.sql(&sql::batch("INSERT INTO checks(check_key,monitor,definition,check_bytes,outcome) VALUES(?1,?2,?3,?4,?5)",vec![SqlValue::Blob(key.to_vec()),parameters[0].clone(),parameters[1].clone(),SqlValue::Blob(encoded),SqlValue::Blob(vec![])]))?)?;
        let row = sql::count(&context.sql(&sql::batch(
            "SELECT row_id FROM checks WHERE check_key=?1",
            vec![SqlValue::Blob(key.to_vec())],
        ))?)?;
        let outcome = RecordOutcome::Recorded { row, edge, current };
        sql::changed(&context.sql(&sql::batch(
            "UPDATE checks SET outcome=?2 WHERE check_key=?1",
            vec![
                SqlValue::Blob(key.to_vec()),
                SqlValue::Blob(encode(&outcome)?),
            ],
        ))?)?;
        Ok(CommandResult::Success(outcome))
    }
}
/// Coherent bounded inspection of checks and definition-local incident state.
pub struct Inspect;
impl Query for Inspect {
    const MODULE: &'static str = Checks::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Inspection;
    fn execute(
        context: &mut QueryContext<'_>,
        page: PageRequest,
    ) -> cellule_runtime::Result<Inspection> {
        page.validate()?;
        let parameters = vec![
            SqlValue::Blob(page.monitor.bytes().to_vec()),
            SqlValue::Blob(page.definition.bytes().to_vec()),
        ];
        let selected = context.sql(&sql::batch(
            "SELECT state FROM monitor_state WHERE monitor=?1 AND definition=?2",
            parameters.clone(),
        ))?;
        let state = state_row(sql::rows(&selected)?)?;
        let mut parameters = parameters;
        parameters.extend([
            SqlValue::Integer(page.after),
            SqlValue::Integer(i64::from(page.limit) + 1),
        ]);
        let selected=context.sql(&sql::batch("SELECT row_id,check_bytes,outcome FROM checks WHERE monitor=?1 AND definition=?2 AND row_id>?3 ORDER BY row_id LIMIT ?4",parameters))?;
        let mut checks = sql::rows(&selected)?
            .iter()
            .map(|row| stored(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = checks.len() > page.limit as usize;
        checks.truncate(page.limit as usize);
        let next = if more {
            checks.last().map(|row| row.row)
        } else {
            None
        };
        Ok(Inspection {
            state,
            checks,
            next,
        })
    }
}
/// Reads one exact permanent check result, independent of Workflow completion visibility.
pub struct ReadCheck;
impl Query for ReadCheck {
    const MODULE: &'static str = Checks::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Ticket;
    type Output = Option<StoredCheck>;
    fn execute(
        context: &mut QueryContext<'_>,
        ticket: Ticket,
    ) -> cellule_runtime::Result<Option<StoredCheck>> {
        ticket.validate()?;
        let selected = context.sql(&sql::batch(
            "SELECT row_id,check_bytes,outcome FROM checks WHERE check_key=?1",
            vec![SqlValue::Blob(ticket.key().to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => Ok(None),
            [row] => Ok(Some(stored(row)?)),
            _ => Err(Error::Command("check uniqueness violated")),
        }
    }
}
