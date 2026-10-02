use crate::{Account, CustomerKey, MAX_CREDITS, Reservation, ReservationId, ReservationState};
use cellule_runtime::{
    Error,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};

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
pub(crate) fn account_query() -> SqlBatch {
    batch(
        "SELECT customer, allowance, consumed, reserved, revision, reservation_count FROM quota_account WHERE singleton=1",
        vec![],
    )
}
pub(crate) fn reservation_query(id: ReservationId) -> SqlBatch {
    batch(
        "SELECT id, credits, state FROM reservations WHERE id=?1",
        vec![SqlValue::Blob(id.as_bytes().to_vec())],
    )
}
pub(crate) fn account(results: &[SqlResultSet]) -> cellule_runtime::Result<Option<Account>> {
    let [result] = results else {
        return Err(Error::Command("unexpected quota account result"));
    };
    match result.rows.as_slice() {
        [] => Ok(None),
        [row] => {
            let [
                SqlValue::Text(customer),
                SqlValue::Integer(allowance),
                SqlValue::Integer(consumed),
                SqlValue::Integer(reserved),
                SqlValue::Integer(revision),
                SqlValue::Integer(count),
            ] = row.as_slice()
            else {
                return Err(Error::Command("quota account row differs from schema"));
            };
            let available = allowance
                .checked_sub(*consumed)
                .and_then(|v| v.checked_sub(*reserved))
                .ok_or(Error::Command("quota balance overflow"))?;
            let value = Account {
                customer: CustomerKey::new(customer.clone())?,
                allowance: *allowance,
                consumed: *consumed,
                reserved: *reserved,
                available,
                revision: *revision,
                reservation_count: *count,
            };
            value.validate()?;
            Ok(Some(value))
        }
        _ => Err(Error::Command("quota singleton invariant violated")),
    }
}
pub(crate) fn reservation(
    results: &[SqlResultSet],
) -> cellule_runtime::Result<Option<Reservation>> {
    let [result] = results else {
        return Err(Error::Command("unexpected reservation result"));
    };
    match result.rows.as_slice() {
        [] => Ok(None),
        [row] => Ok(Some(reservation_row(row)?)),
        _ => Err(Error::Command("reservation identity uniqueness violated")),
    }
}
pub(crate) fn reservation_row(row: &[SqlValue]) -> cellule_runtime::Result<Reservation> {
    let [
        SqlValue::Blob(id),
        SqlValue::Integer(credits),
        SqlValue::Integer(state),
    ] = row
    else {
        return Err(Error::Command("reservation row differs from schema"));
    };
    if !(1..=MAX_CREDITS).contains(credits) {
        return Err(Error::Command("stored reservation credits invalid"));
    }
    let state = match state {
        0 => ReservationState::Active,
        1 => ReservationState::Consumed,
        2 => ReservationState::Released,
        _ => return Err(Error::Command("stored reservation state invalid")),
    };
    Ok(Reservation {
        id: ReservationId::from_bytes(
            id.as_slice()
                .try_into()
                .map_err(|_| Error::Command("stored reservation UUID invalid"))?,
        )?,
        credits: *credits,
        state,
    })
}
pub(crate) fn changed(results: &[SqlResultSet], count: usize) -> cellule_runtime::Result<()> {
    if results.len() != count || results.iter().any(|r| r.rows_affected != 1) {
        return Err(Error::Command(
            "quota mutation row-count invariant violated",
        ));
    }
    Ok(())
}
