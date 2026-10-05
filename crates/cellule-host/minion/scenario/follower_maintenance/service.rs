//! Resume through canonical acquisition and check exact durable command evidence.
use super::*;

pub(super) async fn resume(
    root: &tempfile::TempDir,
    nodes: &[Arc<CellNode>],
    boots: &[startup::BootOwner],
    inputs: &Inputs,
) -> JournalResult<StoredOutcome> {
    let cell = inputs
        .records
        .values()
        .next()
        .ok_or_else(|| invalid("follower maintenance Cell is absent"))?;
    let idle = cell
        .authority
        .load(cell.target.cell_id())
        .await?
        .ok_or_else(|| invalid("drained writer authority is absent"))?;
    let resumed = nodes[0]
        .runtime()
        .acquire_idle_restored(
            cell.catalog.clone(),
            cell.replica.clone(),
            cell.authority.clone(),
            idle,
            root.path().join("resumed-writer.sqlite"),
            owner(0),
        )
        .await?;
    if resumed
        .resolve(
            inputs.acknowledged.identity,
            inputs.acknowledged.digest,
            clock()?,
            64,
        )
        .await?
        != Resolution::Committed(inputs.acknowledged.outcome.clone())
        || resumed
            .query(64, 64, |tx| {
                let value: i64 = tx.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await?
            != inputs.acknowledged.value.to_be_bytes()
    {
        return Err(invalid(
            "resumed writer lost the original acknowledged command",
        ));
    }
    let now = clock()?;
    let renewed_identity = MutationIdentity {
        request_id: RequestId::from_bytes([2; 16]),
        issued_at_ms: now,
        expires_at_ms: now
            .checked_add(120_000)
            .ok_or_else(|| invalid("renewed receipt deadline overflow"))?,
    };
    let renewed_digest = Digest::from_bytes([2; 32]);
    let renewed_outcome = resumed
        .execute(renewed_identity, renewed_digest, now, 64, 64, |tx| {
            tx.execute_batch("UPDATE counter SET value = 29")?;
            Ok(HandlerOutcome::Success(29i64.to_be_bytes().to_vec()))
        })
        .await?;
    if resumed
        .resolve(renewed_identity, renewed_digest, clock()?, 64)
        .await?
        != Resolution::Committed(renewed_outcome.clone())
    {
        return Err(invalid(
            "replacement ensemble lost its acknowledged command",
        ));
    }
    // Object proof may win before authoritative follower activation. A real
    // installed ensemble is checked independently by follower evacuation below.
    let leader = boots[0]
        .directory
        .load_if_live(session(0), clock()?)
        .await?
        .ok_or_else(|| invalid("replacement leader is absent"))?;
    if leader
        .advertisement()
        .log()
        .is_none_or(|log| log.epoch() != 2 || log.members() != [node_id(2), node_id(3)])
    {
        return Err(invalid(
            "replacement ensemble changed during resumed service",
        ));
    }
    Ok(renewed_outcome)
}

pub(super) async fn readback(
    root: &tempfile::TempDir,
    inputs: &Inputs,
    renewed: &StoredOutcome,
) -> JournalResult<()> {
    let record = inputs
        .records
        .values()
        .next()
        .ok_or_else(|| invalid("follower maintenance Cell is absent"))?;
    // Restore only a canonical root covering both acknowledgements. Follower
    // proof can precede asynchronous object publication, so wait for real CAS.
    let (control, published) = tokio::time::timeout_at(deadline(), async {
        loop {
            let control = record
                .authority
                .load(record.target.cell_id())
                .await?
                .ok_or_else(|| invalid("follower maintenance authority is absent"))?;
            let published = control
                .value()
                .ltx_root()
                .ok_or_else(|| invalid("follower maintenance canonical root is absent"))?;
            if published.commit_sequence >= renewed.commit_sequence() {
                return Ok::<_, JournalError>((control, published));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    if control.value().owner.as_ref() != Some(&owner(0))
        || control.value().incarnation != record.incarnation
        || published.commit_sequence < inputs.acknowledged.outcome.commit_sequence()
    {
        return Err(invalid(
            "follower maintenance changed the canonical writer or lost its receipt prefix",
        ));
    }
    let destination = root.path().join("follower-maintenance-readback.sqlite");
    if record
        .replica
        .open_root(&published)
        .await?
        .restore(&destination)
        .await?
        != published.position
    {
        return Err(invalid("follower maintenance restore position differs"));
    }
    let acknowledged = &inputs.acknowledged;
    let request = acknowledged.identity.request_id;
    let expected = acknowledged.value;
    let (value, result, digest, expires, stored) = tokio::task::spawn_blocking(move || {
        let database = rusqlite::Connection::open_with_flags(
            destination,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let value =
            database.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
        let (result, digest, expires, outcome, sequence) = database.query_row(
            "SELECT result, operation_digest, expires_at_ms, outcome, commit_sequence FROM sys_requests WHERE request_id=?1",
            [request.as_bytes().as_slice()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, u64>(4)?)),
        )?;
        let stored = match outcome {
            1 => StoredOutcome::Success { result: result.clone(), commit_sequence: sequence },
            2 => StoredOutcome::Rejected { result: result.clone(), commit_sequence: sequence },
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        Ok::<_, rusqlite::Error>((value, result, digest, expires, stored))
    })
    .await??;
    if value != expected
        || result != expected.to_be_bytes()
        || digest != acknowledged.digest.as_bytes()
        || expires != acknowledged.identity.expires_at_ms
        || stored != acknowledged.outcome
    {
        return Err(invalid(
            "follower maintenance lost acknowledged value or original stored outcome",
        ));
    }
    Ok(())
}
