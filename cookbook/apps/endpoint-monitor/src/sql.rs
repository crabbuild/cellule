use cellule_runtime::{
    Error,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
pub(crate) fn batch(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}
pub(crate) fn rows(results: &[SqlResultSet]) -> cellule_runtime::Result<&[Vec<SqlValue>]> {
    let [result] = results else {
        return Err(Error::Command("unexpected monitor SQL results"));
    };
    Ok(&result.rows)
}
pub(crate) fn changed(results: &[SqlResultSet]) -> cellule_runtime::Result<()> {
    if !matches!(results, [result] if result.rows_affected == 1) {
        return Err(Error::Command("monitor SQL write invariant violated"));
    }
    Ok(())
}
pub(crate) fn count(results: &[SqlResultSet]) -> cellule_runtime::Result<i64> {
    let [row] = rows(results)? else {
        return Err(Error::Command("missing monitor count"));
    };
    let [SqlValue::Integer(value)] = row.as_slice() else {
        return Err(Error::Command("invalid monitor count"));
    };
    Ok(*value)
}
