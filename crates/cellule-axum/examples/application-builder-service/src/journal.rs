//! Application-owned atomic custody, separate from the Cell's authoritative ledger.

use crate::application::SetTotal;
use cellule_axum::HttpError;
use cellule_runtime::{Error, PreparedCommand, PreparedCommandSnapshot};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Journal(PathBuf);
type EncodedRecord = (Vec<u8>, Vec<u8>);

fn connection(path: &Path) -> Result<Connection, Error> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch("PRAGMA synchronous=FULL;")?;
    Ok(connection)
}

impl Journal {
    pub fn open(path: PathBuf) -> Result<Self, Error> {
        let connection = connection(&path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS evidence (
                cell BLOB NOT NULL, request BLOB NOT NULL,
                snapshot BLOB NOT NULL, input BLOB NOT NULL,
                PRIMARY KEY(cell, request)
            );",
        )?;
        Ok(Self(path))
    }

    pub async fn retain(&self, prepared: &PreparedCommand<SetTotal>) -> Result<(), HttpError> {
        let path = self.0.clone();
        let cell = prepared.evidence().target().cell_id().as_bytes().to_vec();
        let request = prepared
            .evidence()
            .identity()
            .request_id
            .as_bytes()
            .to_vec();
        let snapshot = prepared
            .snapshot()
            .to_bytes()
            .map_err(HttpError::internal)?;
        let input = prepared.input_bytes().to_vec();
        tokio::task::spawn_blocking(move || -> Result<(), Error> {
            let mut connection = connection(&path)?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let existing: Option<EncodedRecord> = transaction
                .query_row(
                    "SELECT snapshot, input FROM evidence WHERE cell=?1 AND request=?2",
                    params![cell, request],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            match existing {
                Some((old_snapshot, old_input))
                    if old_snapshot != snapshot || old_input != input =>
                {
                    return Err(Error::RequestConflict);
                }
                Some(_) => (),
                None => {
                    transaction.execute(
                        "INSERT INTO evidence VALUES (?1, ?2, ?3, ?4)",
                        params![cell, request, snapshot, input],
                    )?;
                }
            }
            // Header and exact encoded input become durable together, before dispatch.
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(HttpError::internal)?
        .map_err(HttpError::from)
    }

    pub async fn load(
        &self,
        cell: [u8; 32],
        request: Uuid,
    ) -> Result<Option<(PreparedCommandSnapshot, Vec<u8>)>, HttpError> {
        let path = self.0.clone();
        let record =
            tokio::task::spawn_blocking(move || -> Result<Option<EncodedRecord>, Error> {
                Ok(connection(&path)?
                    .query_row(
                        "SELECT snapshot, input FROM evidence WHERE cell=?1 AND request=?2",
                        params![cell.as_slice(), request.as_bytes().as_slice()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?)
            })
            .await
            .map_err(HttpError::internal)??;
        record
            .map(|(header, input)| {
                Ok((
                    PreparedCommandSnapshot::from_bytes(&header).map_err(HttpError::internal)?,
                    input,
                ))
            })
            .transpose()
    }
}
