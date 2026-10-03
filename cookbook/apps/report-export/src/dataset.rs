use crate::{Change, DataOutcome, Dataset, DatasetInfo, Page, PageRequest, Snapshot, Version, sql};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
/// Conditional draft mutation or atomic immutable version sealing.
pub struct ChangeDataset;
impl Command for ChangeDataset {
    const MODULE: &'static str = Dataset::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = DataOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        change: Change,
    ) -> cellule_runtime::Result<CommandResult<DataOutcome>> {
        change.validate()?;
        let expected = match &change {
            Change::Put {
                expected_revision, ..
            }
            | Change::Delete {
                expected_revision, ..
            }
            | Change::Replace {
                expected_revision, ..
            }
            | Change::Seal {
                expected_revision, ..
            } => *expected_revision,
        };
        if let Change::Seal { version, .. } = &change {
            let selected = context.sql(&sql::batch(
                "SELECT version,revision,row_count,digest FROM snapshots WHERE version=?1",
                vec![SqlValue::Blob(version.0.to_vec())],
            ))?;
            match sql::rows(&selected)? {
                [] => {}
                [row] => {
                    let snapshot = sql::snapshot(row)?;
                    return if snapshot.revision == expected {
                        Ok(CommandResult::Success(DataOutcome::Sealed(snapshot)))
                    } else {
                        Ok(CommandResult::Rejected(DataOutcome::Conflict))
                    };
                }
                _ => return Err(Error::Command("snapshot identity uniqueness violated")),
            }
        }
        let revision = sql::count(&context.sql(&sql::batch(
            "SELECT revision FROM draft_meta WHERE singleton=1",
            vec![],
        ))?)?;
        if revision < 0 {
            return Err(Error::Command("negative draft revision"));
        }
        if revision as u64 != expected {
            return Ok(CommandResult::Rejected(DataOutcome::Conflict));
        }
        match change {
            Change::Put { row, .. } => {
                sql::changed(&context.sql(&sql::batch("INSERT INTO draft_rows(row_id,label,units) VALUES(?1,?2,?3) ON CONFLICT(row_id) DO UPDATE SET label=excluded.label,units=excluded.units",vec![SqlValue::Integer(i64::from(row.id)),SqlValue::Text(row.label),SqlValue::Integer(row.units as i64)]))?)?;
            }
            Change::Delete { id, .. } => {
                let removed = context.sql(&sql::batch(
                    "DELETE FROM draft_rows WHERE row_id=?1",
                    vec![SqlValue::Integer(i64::from(id))],
                ))?;
                let [result] = removed.as_slice() else {
                    return Err(Error::Command("invalid row deletion result"));
                };
                if result.rows_affected == 0 {
                    return Ok(CommandResult::Rejected(DataOutcome::Missing));
                }
                sql::changed(&removed)?;
            }
            Change::Replace { rows, .. } => {
                context.sql(&sql::batch("DELETE FROM draft_rows", vec![]))?;
                for row in rows {
                    sql::changed(&context.sql(&sql::batch(
                        "INSERT INTO draft_rows(row_id,label,units) VALUES(?1,?2,?3)",
                        vec![
                            SqlValue::Integer(i64::from(row.id)),
                            SqlValue::Text(row.label),
                            SqlValue::Integer(row.units as i64),
                        ],
                    ))?)?;
                }
            }
            Change::Seal { version, .. } => {
                if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM snapshots", vec![]))?)?
                    >= i64::from(crate::MAX_VERSIONS)
                {
                    return Ok(CommandResult::Rejected(DataOutcome::Capacity));
                }
                let selected = context.sql(&sql::batch(
                    "SELECT row_id,label,units FROM draft_rows ORDER BY row_id LIMIT 513",
                    vec![],
                ))?;
                let rows = sql::rows(&selected)?
                    .iter()
                    .map(|row| sql::row(row))
                    .collect::<cellule_runtime::Result<Vec<_>>>()?;
                let snapshot = Snapshot {
                    version,
                    revision: expected,
                    rows: rows.len() as u32,
                    digest: crate::dataset_digest(&rows)?,
                };
                snapshot.validate()?;
                sql::changed(&context.sql(&sql::batch(
                    "INSERT INTO snapshots(version,revision,row_count,digest) VALUES(?1,?2,?3,?4)",
                    vec![
                        SqlValue::Blob(version.0.to_vec()),
                        SqlValue::Integer(revision),
                        SqlValue::Integer(i64::from(snapshot.rows)),
                        SqlValue::Blob(snapshot.digest.to_vec()),
                    ],
                ))?)?;
                let copied=context.sql(&sql::batch("INSERT INTO snapshot_rows(version,row_id,label,units) SELECT ?1,row_id,label,units FROM draft_rows",vec![SqlValue::Blob(version.0.to_vec())]))?;
                if !matches!(copied.as_slice(),[result]if result.rows_affected==u64::from(snapshot.rows))
                {
                    return Err(Error::Command("sealed copy row count differs"));
                }
                return Ok(CommandResult::Success(DataOutcome::Sealed(snapshot)));
            }
        }
        sql::changed(&context.sql(&sql::batch(
            "UPDATE draft_meta SET revision=revision+1 WHERE singleton=1 AND revision=?1",
            vec![SqlValue::Integer(revision)],
        ))?)?;
        Ok(CommandResult::Success(DataOutcome::Applied(expected + 1)))
    }
}
/// Coherent current draft revision and bounded version inventory.
pub struct ReadDataset;
impl Query for ReadDataset {
    const MODULE: &'static str = Dataset::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = DatasetInfo;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<DatasetInfo> {
        let selected=context.sql(&sql::batch("SELECT revision,(SELECT count(*) FROM draft_rows),(SELECT count(*) FROM snapshots) FROM draft_meta WHERE singleton=1",vec![]))?;
        let [row] = sql::rows(&selected)? else {
            return Err(Error::Command("invalid dataset metadata row"));
        };
        sql::info(row)
    }
}
/// Reads one permanent sealed version description.
pub struct ReadSnapshot;
impl Query for ReadSnapshot {
    const MODULE: &'static str = Dataset::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = Version;
    type Output = Option<Snapshot>;
    fn execute(
        context: &mut QueryContext<'_>,
        version: Version,
    ) -> cellule_runtime::Result<Option<Snapshot>> {
        version.validate()?;
        let selected = context.sql(&sql::batch(
            "SELECT version,revision,row_count,digest FROM snapshots WHERE version=?1",
            vec![SqlValue::Blob(version.0.to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => Ok(None),
            [row] => Ok(Some(sql::snapshot(row)?)),
            _ => Err(Error::Command("snapshot identity uniqueness violated")),
        }
    }
}
/// Reads an immutable keyset page only when all pinned metadata still agrees.
pub struct ReadPage;
impl Query for ReadPage {
    const MODULE: &'static str = Dataset::NAME;
    const ID: u32 = 5;
    const CODEC_VERSION: u32 = 1;
    type Input = PageRequest;
    type Output = Page;
    fn execute(
        context: &mut QueryContext<'_>,
        input: PageRequest,
    ) -> cellule_runtime::Result<Page> {
        input.snapshot.validate()?;
        if input.after > crate::MAX_ROWS {
            return Err(Error::Command("invalid sealed page cursor"));
        }
        let snapshot = ReadSnapshot::execute(context, input.snapshot.version)?
            .ok_or(Error::Command("sealed dataset version missing"))?;
        if snapshot != input.snapshot {
            return Err(Error::Command("sealed dataset metadata differs from pin"));
        }
        let selected=context.sql(&sql::batch("SELECT row_id,label,units FROM snapshot_rows WHERE version=?1 AND row_id>?2 ORDER BY row_id LIMIT 33",vec![SqlValue::Blob(snapshot.version.0.to_vec()),SqlValue::Integer(i64::from(input.after))]))?;
        let mut rows = sql::rows(&selected)?
            .iter()
            .map(|row| sql::row(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()?;
        let more = rows.len() > crate::PAGE_ROWS as usize;
        rows.truncate(crate::PAGE_ROWS as usize);
        Ok(Page {
            snapshot,
            after: input.after,
            rows,
            more,
        })
    }
}
