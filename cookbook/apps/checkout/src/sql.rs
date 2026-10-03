use cellule_runtime::{
    Error,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
pub(crate) fn batch(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}
pub(crate) fn rows(results: &[SqlResultSet]) -> cellule_runtime::Result<&[Vec<SqlValue>]> {
    let [result] = results else {
        return Err(Error::Command("unexpected checkout SQL results"));
    };
    Ok(&result.rows)
}
pub(crate) fn changed(results: &[SqlResultSet]) -> cellule_runtime::Result<()> {
    if !matches!(results, [result] if result.rows_affected == 1) {
        return Err(Error::Command("checkout SQL write invariant violated"));
    }
    Ok(())
}
pub(crate) fn count(results: &[SqlResultSet]) -> cellule_runtime::Result<i64> {
    let [row] = rows(results)? else {
        return Err(Error::Command("missing checkout count"));
    };
    let [SqlValue::Integer(value)] = row.as_slice() else {
        return Err(Error::Command("invalid checkout count"));
    };
    Ok(*value)
}

pub(crate) fn blob(results: &[SqlResultSet]) -> cellule_runtime::Result<Option<Vec<u8>>> {
    match rows(results)? {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [SqlValue::Blob(bytes)] => Ok(Some(bytes.clone())),
            _ => Err(Error::Command("invalid checkout blob row")),
        },
        _ => Err(Error::Command("checkout uniqueness violated")),
    }
}
pub(crate) fn id(value: crate::Id) -> SqlValue {
    SqlValue::Blob(value.bytes().to_vec())
}
pub(crate) fn emit_reply(
    context: &mut cellule_runtime::registry::CommandContext<'_, '_>,
    call: crate::Call,
    value: crate::ReplyValue,
) -> cellule_runtime::Result<()> {
    let reply = crate::Reply { call, value };
    context.emit_effect(&cellule_runtime::primitives::effects::EffectCommandIntent {
        target: crate::target(context.target(), crate::SAGAS)?,
        command_id: 12,
        codec_version: 1,
        input: crate::wire::encode_wire(&reply, 4096)?,
        expires_at_ms: context
            .now_ms()
            .checked_add(7 * 24 * 60 * 60 * 1000)
            .ok_or(Error::Command("checkout reply expiry overflow"))?,
    })?;
    Ok(())
}
pub(crate) fn previous(
    context: &cellule_runtime::registry::CommandContext<'_, '_>,
    table: &str,
    call: &crate::Call,
) -> cellule_runtime::Result<Option<crate::DeliveryOutcome>> {
    let result = context.sql(&batch(
        &format!("SELECT call_bytes FROM {table} WHERE message_key=?1"),
        vec![SqlValue::Blob(call.key().to_vec())],
    ))?;
    let bytes = crate::model::encode(call)?;
    Ok(blob(&result)?.map(|original| {
        if bytes == original {
            crate::DeliveryOutcome::Applied
        } else {
            crate::DeliveryOutcome::Conflict
        }
    }))
}
pub(crate) fn record_reply(
    context: &mut cellule_runtime::registry::CommandContext<'_, '_>,
    table: &str,
    call: crate::Call,
    value: crate::ReplyValue,
) -> cellule_runtime::Result<()> {
    let reply = crate::Reply {
        call: call.clone(),
        value,
    };
    changed(&context.sql(&batch(
        &format!("INSERT INTO {table}(message_key,call_bytes,reply_bytes) VALUES(?1,?2,?3)"),
        vec![
            SqlValue::Blob(call.key().to_vec()),
            SqlValue::Blob(crate::model::encode(&call)?),
            SqlValue::Blob(crate::model::encode(&reply)?),
        ],
    ))?)?;
    emit_reply(context, call, value)
}
pub(crate) fn classify(
    outcome: crate::DeliveryOutcome,
) -> cellule_runtime::registry::CommandResult<crate::DeliveryOutcome> {
    if outcome == crate::DeliveryOutcome::Applied {
        cellule_runtime::registry::CommandResult::Success(outcome)
    } else {
        cellule_runtime::registry::CommandResult::Rejected(outcome)
    }
}
