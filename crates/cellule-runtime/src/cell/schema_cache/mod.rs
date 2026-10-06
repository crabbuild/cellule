//! Connection-owned schema capabilities, credited only after transaction commit.

use rusqlite::Connection;

use crate::Result;
use crate::primitives::{blob, cron, kv, queue, workflow};

const TABLES: [&str; 7] = [
    kv::KV_TABLE,
    queue::QUEUE_TABLE,
    workflow::WORKFLOW_TABLE,
    blob::BLOB_TABLE,
    cron::CRON_TABLE,
    "capacity_reservations",
    "capacity_total",
];

/// A fixed-size observation of the optional runtime schemas, not their contents.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SchemaCapabilities {
    installed: u8,
}

impl SchemaCapabilities {
    pub(crate) fn discover(connection: &Connection) -> Result<Self> {
        let mut statement =
            connection.prepare("SELECT name FROM main.sqlite_schema WHERE type = 'table'")?;
        let mut rows = statement.query([])?;
        let mut capabilities = Self::default();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            if let Some(index) = TABLES.iter().position(|table| *table == name) {
                capabilities.installed |= 1 << index;
            }
        }
        Ok(capabilities)
    }

    pub(crate) fn contains(self, table: &str) -> bool {
        TABLES
            .iter()
            .position(|candidate| *candidate == table)
            .is_some_and(|index| self.installed & (1 << index) != 0)
    }

    pub(crate) fn capacity_tables(self) -> u32 {
        (self.installed >> 5).count_ones()
    }

    #[cfg(test)]
    pub(crate) fn len(self) -> usize {
        self.installed.count_ones() as usize
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SchemaObservation {
    version: i64,
    pub(crate) capabilities: SchemaCapabilities,
}

/// Lives with one executor/connection and never retains an uncommitted schema.
#[derive(Default)]
pub(crate) struct SchemaCache {
    committed: Option<SchemaObservation>,
}

impl SchemaCache {
    pub(crate) fn observe(&self, connection: &Connection) -> Result<SchemaObservation> {
        let version = connection
            .prepare_cached("PRAGMA main.schema_version")?
            .query_row([], |row| row.get(0))?;
        if let Some(observation) = self.committed.filter(|value| value.version == version) {
            return Ok(observation);
        }
        Ok(SchemaObservation {
            version,
            capabilities: SchemaCapabilities::discover(connection)?,
        })
    }

    pub(crate) fn record_commit(&mut self, observation: SchemaObservation) {
        // Rolled-back DDL can reuse a schema cookie for different later DDL.
        // Only the outer transaction's successful commit makes this reusable.
        self.committed = Some(observation);
    }
}

#[cfg(test)]
mod tests;
