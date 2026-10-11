//! Application SQL and durable request ledger, executed by production celld storage.
use anyhow::{anyhow, bail, Context, Result};
use celld::storage;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn exec(scope: &str, query: &str, binds: &[Value]) -> Result<Vec<Vec<Value>>> {
    storage::sql_exec(scope, query, binds)
        .map(|(_, rows, _)| rows)
        .map_err(|error| anyhow!(error))
}

pub fn migrate(scope: &str) -> Result<()> {
    exec(
        scope,
        "CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY,total_cents INTEGER NOT NULL)",
        &[],
    )?;
    exec(scope, "CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,input TEXT NOT NULL,response TEXT NOT NULL)", &[])?;
    exec(scope, "CREATE TABLE IF NOT EXISTS metadata(id INTEGER PRIMARY KEY,incarnation TEXT NOT NULL,sequence INTEGER NOT NULL)", &[])?;
    exec(
        scope,
        "INSERT OR IGNORE INTO metadata VALUES(1,?1,0)",
        &[json!(super::ownership::random_id()?)],
    )?;
    Ok(())
}

pub fn read(scope: &str, id: i64) -> Result<Value> {
    let metadata = exec(
        scope,
        "SELECT incarnation,sequence FROM metadata WHERE id=1",
        &[],
    )?;
    let row = metadata.first().context("missing metadata")?;
    let orders = exec(
        scope,
        "SELECT id,total_cents FROM orders WHERE id=?1",
        &[json!(id)],
    )?;
    let output = orders
        .first()
        .map(|order| json!({"id":order[0],"total_cents":order[1]}));
    Ok(
        json!({"output":output,"receipt":{"cell":scope,"incarnation":row[0],"commit_sequence":row[1]}}),
    )
}

pub fn write(scope: &str, body: &Value) -> Result<Value> {
    let id = body["id"].as_i64().context("invalid order id")?;
    let total = body["total_cents"]
        .as_i64()
        .context("invalid order total")?;
    let request = body["request_id"].as_str().context("invalid request id")?;
    let input = format!("{:x}", Sha256::digest(serde_json::to_vec(body)?));
    let saved = exec(
        scope,
        "SELECT input,response FROM requests WHERE id=?1",
        &[json!(request)],
    )?;
    if let Some(row) = saved.first() {
        if row[0].as_str() != Some(&input) {
            bail!("request identity conflict");
        }
        return Ok(serde_json::from_str(
            row[1].as_str().context("invalid saved response")?,
        )?);
    }
    let expires = body["expires_at_ms"].as_u64().context("invalid expiry")?;
    anyhow::ensure!(super::ownership::now()? < expires, "expired request");
    storage::transaction_control(scope, "start", false, "").map_err(|error| anyhow!(error))?;
    let result = (|| {
        exec(
            scope,
            "INSERT INTO orders VALUES(?1,?2)",
            &[json!(id), json!(total)],
        )?;
        exec(
            scope,
            "UPDATE metadata SET sequence=sequence+1 WHERE id=1",
            &[],
        )?;
        let reply = read(scope, id)?;
        exec(
            scope,
            "INSERT INTO requests VALUES(?1,?2,?3)",
            &[
                json!(request),
                json!(input),
                json!(serde_json::to_string(&reply)?),
            ],
        )?;
        storage::transaction_control(scope, "commit", false, "").map_err(|error| anyhow!(error))?;
        Ok(reply)
    })();
    if result.is_err() {
        storage::transaction_control(scope, "rollback", false, "")
            .map_err(|error| anyhow!(error))?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;

    #[test]
    fn request_ledger_survives_reopen_and_rejects_conflict_and_expiry() -> Result<()> {
        let directory = std::env::temp_dir().join(super::super::ownership::random_id()?);
        std::fs::create_dir(&directory)?;
        let path = directory.join("cell.sqlite");
        let scope = "native-ledger-test";
        let local = storage::Cells::default();
        let _turn = local.install();
        celld::native_http_open(scope, path.to_str().context("test path")?, 1, None)?;
        migrate(scope)?;
        let initial = read(scope, 1)?;
        ensure!(
            initial["output"].is_null() && initial["receipt"]["commit_sequence"] == 0,
            "initial state"
        );
        let body = json!({"id":1,"total_cents":112,"request_id":"native-test-request","issued_at_ms":0,"expires_at_ms":u64::MAX});
        let committed = write(scope, &body)?;
        ensure!(
            committed["receipt"]["commit_sequence"] == 1,
            "first sequence"
        );
        ensure!(
            write(scope, &body)? == committed,
            "exact retry changed reply"
        );
        let mut conflict = body.clone();
        conflict["total_cents"] = json!(113);
        ensure!(write(scope, &conflict).is_err(), "conflict accepted");
        let expired = json!({"id":2,"total_cents":125,"request_id":"expired","issued_at_ms":0,"expires_at_ms":1});
        ensure!(write(scope, &expired).is_err(), "expiry accepted");
        ensure!(
            read(scope, 1)? == committed && read(scope, 2)?["output"].is_null(),
            "rejection mutated data"
        );
        storage::close(scope);
        celld::native_http_open(scope, path.to_str().context("test path")?, 2, None)?;
        migrate(scope)?;
        ensure!(
            read(scope, 1)? == committed && write(scope, &body)? == committed,
            "reopen changed data or receipt"
        );
        storage::close(scope);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
