use crate::{
    Call, DeliveryOutcome, MAX_MESSAGES, MAX_ORDERS, Operation, Order, OrderChange, OrderOutcome,
    OrderResult, OrderStatus, Orders, OrdersPage, Page, ReplyValue,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn read(context: &CommandContext<'_, '_>, id: crate::Id) -> cellule_runtime::Result<Option<Order>> {
    sql::blob(&context.sql(&sql::batch(
        "SELECT order_bytes FROM orders WHERE order_id=?1",
        vec![sql::id(id)],
    ))?)?
    .map(|bytes| {
        let value: Order = decode(&bytes)?;
        value.validate()?;
        if value.spec.id != id {
            return Err(Error::Identity("stored order identity differs"));
        }
        Ok(value)
    })
    .transpose()
}
fn save(context: &CommandContext<'_, '_>, order: &Order) -> cellule_runtime::Result<()> {
    sql::changed(&context.sql(&sql::batch(
        "UPDATE orders SET order_bytes=?2 WHERE order_id=?1",
        vec![sql::id(order.spec.id), SqlValue::Blob(encode(order)?)],
    ))?)
}
/// Public atomic placement and cancellation intent.
pub struct ChangeOrder;
impl Command for ChangeOrder {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = OrderChange;
    type Output = OrderOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        change: OrderChange,
    ) -> cellule_runtime::Result<CommandResult<OrderOutcome>> {
        match change {
            OrderChange::Place(spec) => {
                spec.validate()?;
                if let Some(old) = read(context, spec.id)? {
                    return Ok(if old.spec == spec {
                        CommandResult::Success(OrderOutcome::Accepted {
                            start_effect: old.start_effect,
                        })
                    } else {
                        CommandResult::Rejected(OrderOutcome::Conflict)
                    });
                }
                if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM orders", vec![]))?)?
                    >= MAX_ORDERS
                {
                    return Ok(CommandResult::Rejected(OrderOutcome::Capacity));
                }
                let start_effect = context.emit_effect(&EffectCommandIntent {
                    target: crate::target(context.target(), crate::SAGAS)?,
                    command_id: 11,
                    codec_version: 1,
                    input: crate::wire::encode_wire(&spec, 2048)?,
                    expires_at_ms: context
                        .now_ms()
                        .checked_add(7 * 24 * 60 * 60 * 1000)
                        .ok_or(Error::Command("checkout start expiry overflow"))?,
                })?;
                let order = Order {
                    spec,
                    status: OrderStatus::Pending,
                    cancel_requested: false,
                    start_effect,
                };
                sql::changed(&context.sql(&sql::batch(
                    "INSERT INTO orders(order_id,order_bytes) VALUES(?1,?2)",
                    vec![sql::id(order.spec.id), SqlValue::Blob(encode(&order)?)],
                ))?)?;
                Ok(CommandResult::Success(OrderOutcome::Accepted {
                    start_effect,
                }))
            }
            OrderChange::Cancel(id) => {
                let Some(mut order) = read(context, id)? else {
                    return Ok(CommandResult::Rejected(OrderOutcome::NotFound));
                };
                if order.cancel_requested {
                    return Ok(CommandResult::Success(OrderOutcome::CancellationRequested));
                }
                if matches!(
                    order.status,
                    OrderStatus::Committing | OrderStatus::Finished(_)
                ) {
                    return Ok(CommandResult::Rejected(OrderOutcome::TooLate));
                }
                order.cancel_requested = true;
                save(context, &order)?;
                Ok(CommandResult::Success(OrderOutcome::CancellationRequested))
            }
        }
    }
}
/// Internal signed saga receiver, atomically publishing its immutable response intent.
pub struct OrderStep;
impl Command for OrderStep {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Call;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        call: Call,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        call.validate()?;
        if let Some(old) = sql::previous(context, "order_messages", &call)? {
            return Ok(sql::classify(old));
        }
        let Some(mut order) = read(context, call.spec.id)? else {
            return Ok(sql::classify(DeliveryOutcome::NotFound));
        };
        if order.spec != call.spec {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM order_messages", vec![]))?)?
            >= MAX_MESSAGES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let value = match call.operation {
            Operation::Decide => {
                if !matches!(
                    order.status,
                    OrderStatus::Pending | OrderStatus::NeedsReview
                ) {
                    return Ok(sql::classify(DeliveryOutcome::InvalidState));
                }
                if order.cancel_requested {
                    ReplyValue::Cancelled
                } else {
                    order.status = OrderStatus::Committing;
                    ReplyValue::Accepted
                }
            }
            Operation::Review => {
                if !matches!(
                    order.status,
                    OrderStatus::Pending | OrderStatus::NeedsReview
                ) {
                    return Ok(sql::classify(DeliveryOutcome::InvalidState));
                }
                order.status = OrderStatus::NeedsReview;
                ReplyValue::ReviewRecorded
            }
            Operation::Finish(planned) => {
                if (planned == OrderResult::Cancelled && !order.cancel_requested)
                    || matches!(order.status, OrderStatus::Finished(_))
                    || (planned == OrderResult::Fulfilled)
                        != (order.status == OrderStatus::Committing)
                {
                    return Ok(sql::classify(DeliveryOutcome::InvalidState));
                }
                let result = if order.cancel_requested {
                    OrderResult::Cancelled
                } else {
                    planned
                };
                order.status = OrderStatus::Finished(result);
                ReplyValue::Finished(result)
            }
            _ => return Ok(sql::classify(DeliveryOutcome::InvalidState)),
        };
        save(context, &order)?;
        sql::record_reply(context, "order_messages", call, value)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Current durable order at a receipt from this SQL Cell.
pub struct GetOrder;
impl Query for GetOrder {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::Id;
    type Output = Option<Order>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: crate::Id,
    ) -> cellule_runtime::Result<Self::Output> {
        sql::blob(&context.sql(&sql::batch(
            "SELECT order_bytes FROM orders WHERE order_id=?1",
            vec![sql::id(id)],
        ))?)?
        .map(|bytes| {
            let order: Order = decode(&bytes)?;
            order.validate()?;
            if order.spec.id != id {
                return Err(Error::Identity("stored checkout order differs"));
            }
            Ok(order)
        })
        .transpose()
    }
}
/// Bounded current-order listing; row positions are local to this receiver.
pub struct ListOrders;
impl Query for ListOrders {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = Page;
    type Output = OrdersPage;
    fn execute(
        context: &mut QueryContext<'_>,
        page: Page,
    ) -> cellule_runtime::Result<Self::Output> {
        page.validate()?;
        let selected = context.sql(&sql::batch(
            "SELECT row_id,order_bytes FROM orders WHERE row_id>?1 ORDER BY row_id LIMIT ?2",
            vec![
                SqlValue::Integer(page.after),
                SqlValue::Integer(i64::from(page.limit) + 1),
            ],
        ))?;
        let mut orders = Vec::new();
        for row in sql::rows(&selected)? {
            let [SqlValue::Integer(id), SqlValue::Blob(bytes)] = row.as_slice() else {
                return Err(Error::Command("invalid order page row"));
            };
            let order: Order = decode(bytes)?;
            order.validate()?;
            orders.push((*id, order));
        }
        let more = orders.len() > page.limit as usize;
        orders.truncate(page.limit as usize);
        let next = if more {
            orders.last().map(|x| x.0)
        } else {
            None
        };
        Ok(OrdersPage { orders, next })
    }
}
