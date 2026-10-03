use crate::{TICKETS, TicketKey, Tickets};
use cellule_runtime::{
    CatalogRole, CellModule, Error, Result,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
pub(crate) fn partition(key: &TicketKey) -> Result<[u8; 33]> {
    cellule_app::CellType::new(Tickets::NAME, "tickets", TICKETS, CatalogRole::Sql, 1)?
        .with_entity_partitions()?
        .entity_partition(key.as_bytes())
}
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
pub(crate) fn one(results: Vec<SqlResultSet>) -> Result<SqlResultSet> {
    let mut iter = results.into_iter();
    let result = iter
        .next()
        .ok_or(Error::Command("missing support SQL result"))?;
    if iter.next().is_some() {
        return Err(Error::Command("unexpected support SQL result count"));
    }
    Ok(result)
}
pub(crate) fn changed(results: Vec<SqlResultSet>) -> Result<()> {
    if one(results)?.rows_affected != 1 {
        return Err(Error::Command("support SQL write count differs"));
    }
    Ok(())
}
pub(crate) fn count(results: Vec<SqlResultSet>) -> Result<i64> {
    let rows = one(results)?.rows;
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [SqlValue::Integer(n)] if *n >= 0 => Ok(*n),
            _ => Err(Error::Command("invalid support SQL count")),
        },
        _ => Err(Error::Command("missing support SQL count")),
    }
}
