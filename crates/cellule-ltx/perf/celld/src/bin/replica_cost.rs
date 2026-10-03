//! Celld's native L0 upload protocol against the same provider/workload as
//! replica-cost. This does not implement Cellule's authenticated root protocol.
#[path = "../../../payload.rs"]
mod fixture;

use celld_ltx::{Db, ObjectStoreClient, ObjectStoreConfig, Pos, Replica, TXID};
use rusqlite::Connection;
use serde::Serialize;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

#[derive(Clone, Serialize)]
struct Sample {
    command: usize,
    command_total_us: u64,
    commit_us: u64,
    capture_us: u64,
    elapsed_us: u64,
    prune_us: u64,
    objects: u64,
    bytes: u64,
}

#[derive(Serialize)]
struct Report {
    implementation: &'static str,
    source: &'static str,
    sqlite_version: &'static str,
    object_prefix: String,
    payload_bytes: usize,
    payload_pattern: &'static str,
    sync_parent: bool,
    measured_commands: usize,
    restored_rows: usize,
    restore_us: u64,
    restore_plan_us: u64,
    restore_download_us: u64,
    restore_apply_us: u64,
    samples: Vec<Sample>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mut options = args.iter();
    while let Some(option) = options.next() {
        match option.as_str() {
            "--random-payload" | "--sync-parent" => {}
            "--commands" | "--warmup" | "--payload-bytes" | "--endpoint" | "--bucket" => {
                if options.next().is_none_or(|value| value.starts_with("--")) {
                    return Err(format!("{option} needs a value").into());
                }
            }
            _ => return Err(format!("unknown option: {option}").into()),
        }
    }
    let commands: usize = option(&args, "--commands", "128")?.parse()?;
    let warmup: usize = option(&args, "--warmup", "8")?.parse()?;
    let payload_bytes: usize = option(&args, "--payload-bytes", "4096")?.parse()?;
    if commands == 0 || warmup >= commands || payload_bytes == 0 {
        return Err("need positive payload-bytes and commands greater than warmup".into());
    }
    let random = args.iter().any(|arg| arg == "--random-payload");
    let sync_parent = args.iter().any(|arg| arg == "--sync-parent");
    let endpoint = option(&args, "--endpoint", "http://127.0.0.1:9000")?;
    let bucket = option(&args, "--bucket", "cellule-ltx-cost")?;
    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let prefix = format!("celld-cost/{}-{run}", std::process::id());
    let config = ObjectStoreConfig {
        bucket,
        path: prefix.clone(),
        region: "us-east-1".into(),
        endpoint,
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
        force_path_style: true,
        ..Default::default()
    };
    let client = ObjectStoreClient::new(config.clone());
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.sqlite");
    let db = Db::open(&source)?;
    let mut replica = Replica::new(db, client);
    // The benchmark owns a fresh prefix; skip discovery of an already-known
    // empty lineage, as the embedding Celld engine does under its fencing.
    replica.seed_pos(Pos::ZERO);
    let mut writer = Connection::open(&source)?;
    writer.busy_timeout(Duration::from_secs(1))?;
    writer.pragma_update(None, "wal_autocheckpoint", 0)?;
    writer.pragma_update(None, "synchronous", "FULL")?;
    writer.execute_batch("CREATE TABLE payload(id INTEGER PRIMARY KEY, value BLOB NOT NULL)")?;
    capture(&mut replica, sync_parent, true)?;
    replica.sync().await?;

    let mut samples = Vec::with_capacity(commands - warmup);
    for command in 0..commands {
        let value = payload(command, payload_bytes, random);
        let command_started = Instant::now();
        let started = command_started;
        let tx = writer.transaction()?;
        tx.execute("INSERT INTO payload VALUES(?1, ?2)", (command + 1, value))?;
        tx.commit()?;
        let commit_us = elapsed_us(started);
        let started = Instant::now();
        capture(&mut replica, sync_parent, false)?;
        let capture_us = elapsed_us(started);
        let db = replica.db_mut().ok_or("attached database missing")?;
        let txid = db.pos()?.txid;
        let bytes = std::fs::metadata(db.ltx_path(0, txid, txid))?.len();
        let started = Instant::now();
        replica.sync().await?;
        let preparation_us = elapsed_us(started);
        if replica.pos().txid != txid {
            return Err("upload did not advance to the captured endpoint".into());
        }
        if command >= warmup {
            samples.push(Sample {
                command,
                command_total_us: elapsed_us(command_started),
                commit_us,
                capture_us,
                elapsed_us: preparation_us,
                prune_us: 0,
                objects: 1,
                bytes,
            });
        }
    }
    // Remove every local cut before restoring from a newly constructed client.
    // Celld retains cuts during the measured loop; no remote authority is run.
    let db = replica.into_db().ok_or("attached database missing")?;
    let metadata = db.meta_path().to_owned();
    db.close()?;
    drop(writer);
    std::fs::remove_file(&source)?;
    std::fs::remove_dir_all(metadata)?;
    let restored = directory.path().join("restored.sqlite");
    let client = ObjectStoreClient::new(config);
    let started = Instant::now();
    let timing = celld_ltx::replica::restore_timed_with_download_slots(
        &client,
        &restored,
        TXID::ZERO,
        Arc::new(Semaphore::new(1)),
    )
    .await?;
    let restore_us = elapsed_us(started);
    let connection = Connection::open(&restored)?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err("restore failed integrity_check".into());
    }
    let mut statement = connection.prepare("SELECT id, value FROM payload ORDER BY id")?;
    let mut rows = statement.query([])?;
    let mut restored_rows = 0;
    while let Some(row) = rows.next()? {
        let id: usize = row.get(0)?;
        let value: Vec<u8> = row.get(1)?;
        if id != restored_rows + 1 || value != payload(restored_rows, payload_bytes, random) {
            return Err("restore changed a committed payload".into());
        }
        restored_rows += 1;
    }
    if restored_rows != commands {
        return Err("restore lost committed rows".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            implementation: "celld-ltx Replica L0",
            source: "10cb1303dac710dcb3b557e318e08c855261f68b",
            sqlite_version: rusqlite::version(),
            object_prefix: prefix,
            payload_bytes,
            payload_pattern: if random {
                "xorshift64-command-seeded"
            } else {
                "periodic-251"
            },
            sync_parent,
            measured_commands: samples.len(),
            restored_rows,
            restore_us,
            restore_plan_us: timing.plan_us,
            restore_download_us: timing.download_us,
            restore_apply_us: timing.apply_us,
            samples,
        })?
    );
    Ok(())
}

fn capture(
    replica: &mut Replica<ObjectStoreClient>,
    sync: bool,
    first: bool,
) -> Result<(), Box<dyn Error>> {
    let db = replica.db_mut().ok_or("attached database missing")?;
    db.sync()?;
    if sync {
        let meta = db.meta_path();
        std::fs::File::open(meta.join("ltx/0"))?.sync_all()?;
        if first {
            std::fs::File::open(meta.join("ltx"))?.sync_all()?;
            std::fs::File::open(meta)?.sync_all()?;
            std::fs::File::open(meta.parent().ok_or("metadata parent missing")?)?.sync_all()?;
        }
    }
    Ok(())
}

fn payload(seed: usize, bytes: usize, random: bool) -> Vec<u8> {
    if random {
        fixture::high_entropy(seed, bytes)
    } else {
        (0..bytes)
            .map(|offset| ((seed * 131 + offset) % 251) as u8)
            .collect()
    }
}

fn option(args: &[String], name: &str, default: &str) -> Result<String, Box<dyn Error>> {
    match args.iter().position(|arg| arg == name) {
        Some(index) => Ok(args.get(index + 1).ok_or("option value missing")?.clone()),
        None => Ok(default.into()),
    }
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}
