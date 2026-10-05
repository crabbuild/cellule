//! Reference embedding with a durable journal and real leased-node overload.

mod journal;
#[cfg(test)]
mod reconciler_tests;
mod scenario;

use cellule_host::fleet::FleetJournal;
use cellule_runtime::fleet::operations::{FleetProfile, FleetScope};
use cellule_runtime::identity::{ApplicationId, Digest};
use journal::{JournalResult, SqliteJournal};

#[tokio::main]
async fn main() -> JournalResult<()> {
    let mut args = std::env::args_os().skip(1);
    let command = args.next();
    if matches!(command.as_deref(), Some(value) if value == "overload" || value == "controller-restart" || value == "balance" || value == "maintenance" || value == "maintenance-reader" || value == "maintenance-follower" || value == "receiver-loss")
        && args.next().is_none()
    {
        let summary = if command.as_deref() == Some(std::ffi::OsStr::new("controller-restart")) {
            scenario::controller_restart().await?
        } else if command.as_deref() == Some(std::ffi::OsStr::new("balance")) {
            scenario::count_balance().await?
        } else if command.as_deref() == Some(std::ffi::OsStr::new("maintenance")) {
            scenario::maintenance().await?
        } else if command.as_deref() == Some(std::ffi::OsStr::new("maintenance-reader")) {
            scenario::maintenance_reader().await?
        } else if command.as_deref() == Some(std::ffi::OsStr::new("maintenance-follower")) {
            scenario::maintenance_follower().await?
        } else if command.as_deref() == Some(std::ffi::OsStr::new("receiver-loss")) {
            scenario::receiver_loss().await?
        } else {
            scenario::overload().await?
        };
        println!(
            "released={} activated={} retired={} receipt_checks={} max_inflight={} max_restore_bytes={} joined_nodes={} boot_retirements={} receiver_nodes={} lost_release_replies={} controller_epoch={} expired_receiver_cleanups={} blocker_count={} final_counts={:?} maintenance_completed={} maintenance_boot_withdrawn={} receiver_process_closures={} lost_activation_replies={} routed_activation_replays={}",
            summary.released,
            summary.activated,
            summary.retired,
            summary.receipt_checks,
            summary.max_inflight,
            summary.max_restore_bytes,
            summary.joined_nodes,
            summary.boot_retirements,
            summary.receiver_nodes,
            summary.lost_release_replies,
            summary.controller_epoch,
            summary.expired_receiver_cleanups,
            summary.blockers.len(),
            summary.final_counts,
            summary.maintenance_completed,
            summary.maintenance_boot_withdrawn,
            summary.receiver_process_closures,
            summary.lost_activation_replies,
            summary.routed_activation_replays
        );
        println!("blockers={:?}", summary.blockers);
        return Ok(());
    }
    let database = args.next();
    if command.as_deref() != Some(std::ffi::OsStr::new("inspect-journal"))
        || database.is_none()
        || args.next().is_some()
    {
        return Err(std::io::Error::other(
            "usage: fleet_operations overload | controller-restart | balance | maintenance | maintenance-reader | maintenance-follower | receiver-loss | inspect-journal <database-path>",
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
