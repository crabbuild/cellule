use cellule_runtime::{
    CellModule, CellTarget, Error, Result,
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

pub(crate) fn account_partition(key: &crate::AccountKey) -> Result<[u8; 33]> {
    cellule_app::CellType::new(
        crate::Accounts::NAME,
        "accounts",
        crate::ACCOUNTS,
        cellule_runtime::CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()?
    .entity_partition(key.as_bytes())
}

pub(crate) fn period_partition(id: &[u8; 16]) -> Result<[u8; 33]> {
    cellule_app::CellType::new(
        crate::Periods::NAME,
        "periods",
        crate::PERIODS,
        cellule_runtime::CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()?
    .entity_partition(id)
}

pub(crate) fn period_target(scope: &CellTarget, id: &[u8; 16]) -> Result<CellTarget> {
    CellTarget::new(
        scope.tenant(),
        scope.application(),
        crate::PERIODS,
        &period_partition(id)?,
    )
}

pub(crate) fn changed(results: impl AsRef<[SqlResultSet]>) -> Result<()> {
    let results = results.as_ref();
    if results.len() != 1 || results[0].rows_affected != 1 {
        return Err(Error::Command(
            "usage-ledger SQL mutation row count differs",
        ));
    }
    Ok(())
}
