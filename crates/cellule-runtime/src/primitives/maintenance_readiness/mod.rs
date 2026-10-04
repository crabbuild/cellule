//! Primitive obligations at a foreground-closed maintenance barrier.
//!
//! Pending durable work can travel in an exact root after source claims close.
//! A live or malformed lease retains its current owner. This snapshot does not
//! prove actor quiescence, object coverage, or release, and never changes rows.

use rusqlite::Connection;

use crate::cell::catalog::CatalogRole;
use crate::{Error, Result};

/// An obligation that cannot yet move under planned maintenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceWorkBlocker {
    /// An outstanding external Effect lease needs completion or normal expiry.
    EffectLease,
    /// A Queue delivery lease needs completion or normal expiry.
    QueueLease,
    /// An Activity lease needs completion or normal expiry.
    ActivityLease,
    /// Complete Blob stream/upload/pin coverage has not been established.
    BlobInventory,
}

impl MaintenanceWorkBlocker {
    const fn bit(self) -> u8 {
        match self {
            Self::EffectLease => 1,
            Self::QueueLease => 2,
            Self::ActivityLease => 4,
            Self::BlobInventory => 8,
        }
    }
}

/// Bounded read-only inventory; it grants no execution or movement authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceWorkInventory {
    bits: u8,
}

impl MaintenanceWorkInventory {
    /// Checks one obligation, without collapsing multiple simultaneous leases.
    #[must_use]
    pub const fn has_blocker(self, blocker: MaintenanceWorkBlocker) -> bool {
        self.bits & blocker.bit() != 0
    }

    /// True only for this primitive snapshot. Source foreground closure and all
    /// actor/publication/role barriers are additionally required for release.
    #[must_use]
    pub const fn is_transferable(self) -> bool {
        self.bits == 0
    }
}

pub(crate) fn inspect(
    connection: &Connection,
    role: CatalogRole,
    now_ms: i64,
) -> Result<MaintenanceWorkInventory> {
    if now_ms < 0 {
        return Err(Error::Command("invalid maintenance inspection time"));
    }
    let mut inventory = MaintenanceWorkInventory::default();
    if exists(
        connection,
        "SELECT EXISTS(SELECT 1 FROM sys_effects INDEXED BY sys_effects_leases WHERE state = 1 AND (lease_until_ms IS NULL OR lease_until_ms < 0 OR token IS NULL OR length(token) != 16 OR lease_until_ms > ?1) LIMIT 1)",
        now_ms,
    )? {
        inventory.bits |= MaintenanceWorkBlocker::EffectLease.bit();
    }
    if role == CatalogRole::Queue
        && exists(
            connection,
            "SELECT EXISTS(SELECT 1 FROM queue_messages INDEXED BY queue_leases WHERE state = 1 AND (lease_until_ms IS NULL OR lease_until_ms < 0 OR token IS NULL OR length(token) != 16 OR lease_until_ms > ?1) LIMIT 1)",
            now_ms,
        )?
    {
        inventory.bits |= MaintenanceWorkBlocker::QueueLease.bit();
    }
    if role == CatalogRole::Workflow
        && exists(
            connection,
            "SELECT EXISTS(SELECT 1 FROM workflow_activities INDEXED BY activities_leases WHERE state = 1 AND (lease_until_ms IS NULL OR lease_until_ms < 0 OR token IS NULL OR length(token) != 16 OR lease_until_ms > ?1) LIMIT 1)",
            now_ms,
        )?
    {
        inventory.bits |= MaintenanceWorkBlocker::ActivityLease.bit();
    }
    if role == CatalogRole::Blob {
        // Actor queue closure alone cannot inventory external upload/stream and
        // pin owners. Keep this class explicit until those owners join the scan.
        inventory.bits |= MaintenanceWorkBlocker::BlobInventory.bit();
    }
    Ok(inventory)
}

fn exists(connection: &Connection, sql: &str, now_ms: i64) -> Result<bool> {
    match connection.query_row(sql, [now_ms], |row| row.get::<_, i64>(0))? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::Command("invalid maintenance existence result")),
    }
}

#[cfg(test)]
mod tests;
