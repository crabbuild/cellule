use crate::{DeliveryTicket, Endpoint, Key, ReceiverRecord, Subscription, model::decode_wire};
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
        return Err(Error::Command("unexpected webhook SQL results"));
    };
    Ok(&result.rows)
}
pub(crate) fn changed(results: &[SqlResultSet]) -> cellule_runtime::Result<()> {
    if !matches!(results,[result] if result.rows_affected==1) {
        return Err(Error::Command("webhook write invariant violated"));
    }
    Ok(())
}
pub(crate) fn count(results: &[SqlResultSet]) -> cellule_runtime::Result<i64> {
    let [row] = rows(results)? else {
        return Err(Error::Command("missing webhook count"));
    };
    let [SqlValue::Integer(count)] = row.as_slice() else {
        return Err(Error::Command("invalid webhook count"));
    };
    Ok(*count)
}
pub(crate) const SUB_FIELDS: &str = "subscription_key,topic,endpoint,enabled,revision";
pub(crate) fn subscription(row: &[SqlValue]) -> cellule_runtime::Result<Subscription> {
    let [
        SqlValue::Text(id),
        SqlValue::Text(topic),
        SqlValue::Text(endpoint),
        SqlValue::Integer(enabled),
        SqlValue::Integer(revision),
    ] = row
    else {
        return Err(Error::Command("invalid stored subscription row"));
    };
    if !matches!(enabled, 0 | 1) || *revision <= 0 {
        return Err(Error::Command(
            "invalid stored subscription revision or flag",
        ));
    }
    Ok(Subscription {
        id: Key::new(id.clone())?,
        topic: Key::new(topic.clone())?,
        endpoint: Endpoint::new(endpoint.clone())?,
        enabled: *enabled == 1,
        revision: *revision,
    })
}
pub(crate) fn receiver_record(row: &[SqlValue]) -> cellule_runtime::Result<ReceiverRecord> {
    let [
        SqlValue::Blob(ticket),
        SqlValue::Integer(requests),
        SqlValue::Integer(applied),
    ] = row
    else {
        return Err(Error::Command("invalid stored receiver record"));
    };
    if !(1..=20).contains(requests) || !matches!(applied, 0 | 1) {
        return Err(Error::Command("invalid receiver counter or applied flag"));
    }
    let ticket: DeliveryTicket = decode_wire(ticket, 4096)?;
    Ok(ReceiverRecord {
        ticket,
        requests: *requests as u32,
        applied: *applied == 1,
    })
}
