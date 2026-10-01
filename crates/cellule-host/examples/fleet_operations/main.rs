//! Reference embedding. SQLite journal inspection is the first executable
//! scenario; the fleet driver and leased three-node scenarios follow here.

mod journal;
#[cfg(test)]
mod reconciler_tests;

use cellule_host::fleet::FleetJournal;
use cellule_runtime::fleet::operations::{FleetProfile, FleetScope};
use cellule_runtime::identity::{ApplicationId, Digest};
use journal::{JournalResult, SqliteJournal};

#[tokio::main]
async fn main() -> JournalResult<()> {
    let mut args = std::env::args_os().skip(1);
    let command = args.next();
    let database = args.next();
    if command.as_deref() != Some(std::ffi::OsStr::new("inspect-journal"))
        || database.is_none()
        || args.next().is_some()
    {
        return Err(std::io::Error::other(
            "usage: fleet_operations inspect-journal <database-path>",
        )
        .into());
    }
    let database = database.ok_or_else(|| std::io::Error::other("database path is absent"))?;
    let scope = FleetScope {
        fleet: Digest::from_bytes([200; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    };
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let journal =
        SqliteJournal::open(database.into(), scope, FleetProfile::default(), now_ms).await?;
    let result = journal.load_snapshot(scope).await;
    let close = journal.close().await;
    let snapshot = result?;
    close?;
    println!(
        "journal revision={} registry revision={} bootstrapped={} scheduling={} unresolved={} restore_bytes={}",
        snapshot.head().revision(),
        snapshot.registry().revision(),
        snapshot.registry().bootstrap_revision().is_some(),
        snapshot.registry().scheduling_enabled(),
        snapshot.head().attempts().len(),
        snapshot.head().reserved_restore_bytes()
    );
    Ok(())
}
