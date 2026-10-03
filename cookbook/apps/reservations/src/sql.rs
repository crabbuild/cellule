use crate::{BuyerKey, DeadlineTicket, EventKey, Hold, HoldId, HoldState, Inventory};
use cellule_runtime::{
    Error,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
pub(crate) const HOLD_FIELDS: &str =
    "id, seat, generation, buyer, deadline_ms, state, start_effect";
pub(crate) fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
pub(crate) fn batch(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![statement(sql, parameters)],
    }
}
pub(crate) fn rows(results: &[SqlResultSet]) -> cellule_runtime::Result<&[Vec<SqlValue>]> {
    let [result] = results else {
        return Err(Error::Command("unexpected reservation SQL result"));
    };
    Ok(&result.rows)
}
pub(crate) fn changed(results: &[SqlResultSet]) -> cellule_runtime::Result<()> {
    if !matches!(results,[result] if result.rows_affected==1) {
        return Err(Error::Command("reservation write invariant violated"));
    }
    Ok(())
}
pub(crate) fn hold_row(row: &[SqlValue], event: &EventKey) -> cellule_runtime::Result<Hold> {
    let [
        SqlValue::Blob(id),
        SqlValue::Integer(seat),
        SqlValue::Integer(generation),
        SqlValue::Text(buyer),
        SqlValue::Integer(deadline_ms),
        SqlValue::Integer(state),
        SqlValue::Blob(effect),
    ] = row
    else {
        return Err(Error::Command("hold row differs from schema"));
    };
    let state = match state {
        0 => HoldState::Held,
        1 => HoldState::Confirmed,
        2 => HoldState::Cancelled,
        3 => HoldState::Expired,
        _ => return Err(Error::Command("invalid stored hold state")),
    };
    let ticket = DeadlineTicket {
        event: event.clone(),
        id: HoldId::from_bytes(
            id.as_slice()
                .try_into()
                .map_err(|_| Error::Command("invalid stored hold identity"))?,
        )?,
        seat: u32::try_from(*seat).map_err(|_| Error::Command("invalid stored seat"))?,
        generation: *generation,
        deadline_ms: *deadline_ms,
    };
    ticket.validate()?;
    if effect.len() != 32 {
        return Err(Error::Command("invalid stored start intent"));
    }
    Ok(Hold {
        ticket,
        buyer: BuyerKey::new(buyer.clone())?,
        state,
        start_effect: effect.clone(),
    })
}
pub(crate) fn hold(
    results: &[SqlResultSet],
    event: &EventKey,
) -> cellule_runtime::Result<Option<Hold>> {
    match rows(results)? {
        [] => Ok(None),
        [row] => Ok(Some(hold_row(row, event)?)),
        _ => Err(Error::Command("hold uniqueness violated")),
    }
}
pub(crate) fn event(results: &[SqlResultSet]) -> cellule_runtime::Result<Option<(EventKey, u32)>> {
    match rows(results)? {
        [] => Ok(None),
        [row] => {
            let [SqlValue::Text(key), SqlValue::Integer(seats)] = row.as_slice() else {
                return Err(Error::Command("event row differs from schema"));
            };
            let seats =
                u32::try_from(*seats).map_err(|_| Error::Command("invalid event seat count"))?;
            if !(1..=crate::MAX_SEATS).contains(&seats) {
                return Err(Error::Command("invalid event seat count"));
            }
            Ok(Some((EventKey::new(key.clone())?, seats)))
        }
        _ => Err(Error::Command("event singleton violated")),
    }
}
pub(crate) fn event_query() -> SqlBatch {
    batch(
        "SELECT event_key, seat_count FROM event WHERE singleton=1",
        vec![],
    )
}
pub(crate) fn hold_query(id: HoldId) -> SqlBatch {
    batch(
        &format!("SELECT {HOLD_FIELDS} FROM holds WHERE id=?1"),
        vec![SqlValue::Blob(id.as_bytes().to_vec())],
    )
}
pub(crate) fn bump_revision(
    context: &mut cellule_runtime::registry::CommandContext<'_, '_>,
) -> cellule_runtime::Result<()> {
    changed(&context.sql(&batch(
        "UPDATE event SET revision=revision+1 WHERE singleton=1 AND revision<9223372036854775807",
        vec![],
    ))?)
}
pub(crate) fn transition(
    context: &mut cellule_runtime::registry::CommandContext<'_, '_>,
    id: HoldId,
    state: HoldState,
) -> cellule_runtime::Result<()> {
    let state = match state {
        HoldState::Confirmed => 1,
        HoldState::Cancelled => 2,
        HoldState::Expired => 3,
        _ => return Err(Error::Command("unsupported hold transition")),
    };
    changed(&context.sql(&batch(
        "UPDATE holds SET state=?1 WHERE id=?2 AND state=0",
        vec![
            SqlValue::Integer(state),
            SqlValue::Blob(id.as_bytes().to_vec()),
        ],
    ))?)?;
    bump_revision(context)
}
pub(crate) fn inventory(results: &[SqlResultSet]) -> cellule_runtime::Result<Option<Inventory>> {
    match rows(results)? {
        [] => Ok(None),
        [row] => {
            let [
                SqlValue::Text(key),
                SqlValue::Integer(seats),
                SqlValue::Integer(revision),
                SqlValue::Integer(held),
                SqlValue::Integer(confirmed),
                SqlValue::Integer(count),
            ] = row.as_slice()
            else {
                return Err(Error::Command("inventory row differs from schema"));
            };
            let available = seats
                .checked_sub(*held)
                .and_then(|v| v.checked_sub(*confirmed))
                .ok_or(Error::Command("inventory arithmetic overflow"))?;
            let value = Inventory {
                event: EventKey::new(key.clone())?,
                seats: u32::try_from(*seats).map_err(|_| Error::Command("invalid seat count"))?,
                held: u32::try_from(*held).map_err(|_| Error::Command("invalid held count"))?,
                confirmed: u32::try_from(*confirmed)
                    .map_err(|_| Error::Command("invalid confirmed count"))?,
                available: u32::try_from(available)
                    .map_err(|_| Error::Command("negative available seats"))?,
                revision: *revision,
                history_count: *count,
            };
            if !(1..=crate::MAX_SEATS).contains(&value.seats)
                || value.revision <= 0
                || !(0..=crate::MAX_HOLDS).contains(count)
            {
                return Err(Error::Command("inventory invariant violated"));
            }
            Ok(Some(value))
        }
        _ => Err(Error::Command("inventory singleton violated")),
    }
}
pub(crate) fn inventory_query() -> SqlBatch {
    batch(
        "SELECT event_key, seat_count, revision, (SELECT count(*) FROM holds WHERE state=0), (SELECT count(*) FROM holds WHERE state=1), (SELECT count(*) FROM holds) FROM event WHERE singleton=1",
        vec![],
    )
}
