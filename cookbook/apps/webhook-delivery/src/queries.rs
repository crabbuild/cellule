use crate::{EventId, Feed, PublishedEvent, Receiver, ReceiverRecord, model::decode_wire, sql};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Query, QueryContext},
};
pub(crate) struct ReadEvent;
impl Query for ReadEvent {
    const MODULE: &'static str = Feed::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = EventId;
    type Output = Option<PublishedEvent>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: EventId,
    ) -> cellule_runtime::Result<Self::Output> {
        let selected = context.sql(&sql::batch(
            "SELECT record FROM published_events WHERE event_id=?1",
            vec![SqlValue::Blob(id.bytes().to_vec())],
        ))?;
        match sql::rows(&selected)? {
            [] => Ok(None),
            [row] => {
                let [SqlValue::Blob(bytes)] = row.as_slice() else {
                    return Err(Error::Command("invalid stored source event"));
                };
                let event: PublishedEvent = decode_wire(bytes, 32768)?;
                if event.id != id {
                    return Err(Error::Command("stored source event differs"));
                }
                Ok(Some(event))
            }
            _ => Err(Error::Command("event uniqueness violated")),
        }
    }
}
pub(crate) struct ListSubscriptions;
impl Query for ListSubscriptions {
    const MODULE: &'static str = Feed::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = crate::SubscriptionList;
    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<Self::Output> {
        let selected = context.sql(&sql::batch(
            &format!(
                "SELECT {} FROM subscriptions ORDER BY subscription_key LIMIT 17",
                sql::SUB_FIELDS
            ),
            vec![],
        ))?;
        let rows = sql::rows(&selected)?;
        if rows.len() > crate::MAX_SUBSCRIBERS {
            return Err(Error::Command("subscriber bound violated"));
        }
        Ok(crate::SubscriptionList {
            subscriptions: rows
                .iter()
                .map(|row| sql::subscription(row))
                .collect::<cellule_runtime::Result<Vec<_>>>()?,
        })
    }
}
pub(crate) struct ReadReceiver;
impl Query for ReadReceiver {
    const MODULE: &'static str = Receiver::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Option<ReceiverRecord>;
    fn execute(
        context: &mut QueryContext<'_>,
        key: Vec<u8>,
    ) -> cellule_runtime::Result<Self::Output> {
        if key.len() != 32 {
            return Err(Error::Command("invalid receiver delivery key"));
        }
        let selected = context.sql(&sql::batch(
            "SELECT ticket,requests,applied FROM received_deliveries WHERE delivery_key=?1",
            vec![SqlValue::Blob(key.clone())],
        ))?;
        match sql::rows(&selected)? {
            [] => Ok(None),
            [row] => {
                let record = sql::receiver_record(row)?;
                if record.ticket.key().as_slice() != key {
                    return Err(Error::Command("stored receiver key differs"));
                }
                Ok(Some(record))
            }
            _ => Err(Error::Command("receiver uniqueness violated")),
        }
    }
}
