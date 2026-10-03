use crate::{DatasetInfo, Row, Snapshot, Version};
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
        return Err(Error::Command("invalid export SQL result cardinality"));
    };
    Ok(&result.rows)
}
pub(crate) fn changed(results: &[SqlResultSet]) -> cellule_runtime::Result<()> {
    if !matches!(results,[result]if result.rows_affected==1) {
        return Err(Error::Command("export SQL write invariant violated"));
    }
    Ok(())
}
pub(crate) fn count(results: &[SqlResultSet]) -> cellule_runtime::Result<i64> {
    let [row] = rows(results)? else {
        return Err(Error::Command("invalid export count rows"));
    };
    let [SqlValue::Integer(value)] = row.as_slice() else {
        return Err(Error::Command("invalid export count value"));
    };
    Ok(*value)
}
pub(crate) fn row(row: &[SqlValue]) -> cellule_runtime::Result<Row> {
    let [
        SqlValue::Integer(id),
        SqlValue::Text(label),
        SqlValue::Integer(units),
    ] = row
    else {
        return Err(Error::Command("invalid stored export row"));
    };
    let value = Row {
        id: u32::try_from(*id).map_err(|_| Error::Command("invalid export row ID"))?,
        label: label.clone(),
        units: u64::try_from(*units).map_err(|_| Error::Command("invalid export units"))?,
    };
    value.validate()?;
    Ok(value)
}
pub(crate) fn snapshot(row: &[SqlValue]) -> cellule_runtime::Result<Snapshot> {
    let [
        SqlValue::Blob(version),
        SqlValue::Integer(revision),
        SqlValue::Integer(count),
        SqlValue::Blob(digest),
    ] = row
    else {
        return Err(Error::Command("invalid stored export snapshot"));
    };
    let value = Snapshot {
        version: Version(
            version
                .as_slice()
                .try_into()
                .map_err(|_| Error::Command("invalid stored version identity"))?,
        ),
        revision: u64::try_from(*revision)
            .map_err(|_| Error::Command("invalid snapshot revision"))?,
        rows: u32::try_from(*count).map_err(|_| Error::Command("invalid snapshot row count"))?,
        digest: digest
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid snapshot digest"))?,
    };
    value.validate()?;
    Ok(value)
}
pub(crate) fn info(row: &[SqlValue]) -> cellule_runtime::Result<DatasetInfo> {
    let [
        SqlValue::Integer(revision),
        SqlValue::Integer(count),
        SqlValue::Integer(versions),
    ] = row
    else {
        return Err(Error::Command("invalid dataset counters"));
    };
    let value = DatasetInfo {
        revision: u64::try_from(*revision)
            .map_err(|_| Error::Command("invalid dataset revision"))?,
        rows: u32::try_from(*count).map_err(|_| Error::Command("invalid dataset count"))?,
        versions: u32::try_from(*versions)
            .map_err(|_| Error::Command("invalid dataset version count"))?,
    };
    if value.rows > crate::MAX_ROWS || value.versions > crate::MAX_VERSIONS {
        return Err(Error::Command("stored dataset exceeds bounds"));
    }
    Ok(value)
}
