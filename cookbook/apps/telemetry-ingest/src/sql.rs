use crate::{DEVICES, DeviceKey, Devices};
use cellule_app::CellType;
use cellule_runtime::{
    CatalogRole, CellModule, Error, Result,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
pub(crate) fn partition(key: &DeviceKey) -> Result<[u8; 33]> {
    CellType::new(Devices::NAME, "devices", DEVICES, CatalogRole::Sql, 1)?
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
    let value = iter
        .next()
        .ok_or(Error::Command("missing telemetry SQL result"))?;
    if iter.next().is_some() {
        return Err(Error::Command("unexpected telemetry SQL result count"));
    }
    Ok(value)
}
pub(crate) fn changed(results: Vec<SqlResultSet>) -> Result<()> {
    if one(results)?.rows_affected != 1 {
        return Err(Error::Command(
            "telemetry write did not change exactly one row",
        ));
    }
    Ok(())
}
pub(crate) fn count(results: Vec<SqlResultSet>) -> Result<i64> {
    let selected = one(results)?;
    match selected.rows.as_slice() {
        [row] => match row.as_slice() {
            [SqlValue::Integer(value)] if *value >= 0 => Ok(*value),
            _ => Err(Error::Command("invalid telemetry SQL count")),
        },
        _ => Err(Error::Command("missing telemetry SQL count")),
    }
}
