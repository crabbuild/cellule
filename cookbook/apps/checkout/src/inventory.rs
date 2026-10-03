use crate::{
    Call, DeliveryOutcome, Inventory, MAX_MESSAGES, MAX_ORDERS, MAX_PRODUCTS, Operation,
    ReplyValue, Reservation, ReservationStatus, Seed, Stock,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
fn stock(
    results: &[cellule_runtime::primitives::sql::SqlResultSet],
) -> cellule_runtime::Result<Option<Stock>> {
    match sql::rows(results)? {
        [] => Ok(None),
        [row] => {
            let [
                SqlValue::Text(sku),
                SqlValue::Integer(total),
                SqlValue::Integer(available),
                SqlValue::Integer(held),
                SqlValue::Integer(sold),
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid stock row"));
            };
            let value = Stock {
                sku: sku.clone(),
                total: u32::try_from(*total).map_err(|_| Error::Command("invalid stock total"))?,
                available: u32::try_from(*available)
                    .map_err(|_| Error::Command("invalid available stock"))?,
                held: u32::try_from(*held).map_err(|_| Error::Command("invalid held stock"))?,
                sold: u32::try_from(*sold).map_err(|_| Error::Command("invalid sold stock"))?,
            };
            value.validate()?;
            Ok(Some(value))
        }
        _ => Err(Error::Command("stock uniqueness violated")),
    }
}
fn stock_query(sku: &str) -> cellule_runtime::primitives::sql::SqlBatch {
    sql::batch(
        "SELECT sku,total,available,held,sold FROM products WHERE sku=?1",
        vec![SqlValue::Text(sku.into())],
    )
}
/// Permanent seed; replay cannot replenish inventory consumed by orders.
pub struct SeedStock;
impl Command for SeedStock {
    const MODULE: &'static str = Inventory::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Seed;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        seed: Seed,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        seed.validate()?;
        if let Some(old) = stock(&context.sql(&stock_query(&seed.sku))?)? {
            return Ok(sql::classify(if old.total == seed.units {
                DeliveryOutcome::Applied
            } else {
                DeliveryOutcome::Conflict
            }));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM products", vec![]))?)?
            >= MAX_PRODUCTS
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        sql::changed(&context.sql(&sql::batch(
            "INSERT INTO products(sku,total,available,held,sold) VALUES(?1,?2,?2,0,0)",
            vec![
                SqlValue::Text(seed.sku),
                SqlValue::Integer(i64::from(seed.units)),
            ],
        ))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Atomic stock reservation/settlement and its signed callback intent.
pub struct StockStep;
impl Command for StockStep {
    const MODULE: &'static str = Inventory::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Call;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        call: Call,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        call.validate()?;
        if !matches!(
            call.operation,
            Operation::Reserve | Operation::Commit | Operation::Release
        ) {
            return Ok(sql::classify(DeliveryOutcome::InvalidState));
        }
        if let Some(old) = sql::previous(context, "stock_messages", &call)? {
            return Ok(sql::classify(old));
        }
        if sql::count(&context.sql(&sql::batch("SELECT count(*) FROM stock_messages", vec![]))?)?
            >= MAX_MESSAGES
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let original = sql::blob(&context.sql(&sql::batch(
            "SELECT reservation FROM reservations WHERE order_id=?1",
            vec![sql::id(call.spec.id)],
        ))?)?;
        let mut reservation = original
            .as_ref()
            .map(|bytes| decode::<Reservation>(bytes))
            .transpose()?;
        if let Some(old) = &reservation {
            old.validate()?;
        }
        if reservation
            .as_ref()
            .is_some_and(|old| old.spec != call.spec || old.run_id != call.run_id)
        {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if reservation.is_none()
            && sql::count(&context.sql(&sql::batch("SELECT count(*) FROM reservations", vec![]))?)?
                >= MAX_ORDERS
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let sku = SqlValue::Text(call.spec.sku.clone());
        let quantity = SqlValue::Integer(i64::from(call.spec.quantity));
        let value = match call.operation {
            Operation::Reserve => match reservation.as_ref().map(|x| x.status) {
                Some(ReservationStatus::Held) => ReplyValue::Held,
                Some(ReservationStatus::Unavailable) => ReplyValue::Unavailable,
                Some(_) => return Ok(sql::classify(DeliveryOutcome::InvalidState)),
                None => {
                    let available = stock(&context.sql(&stock_query(&call.spec.sku))?)?
                        .is_some_and(|s| s.available >= call.spec.quantity);
                    if available {
                        sql::changed(&context.sql(&sql::batch("UPDATE products SET available=available-?2,held=held+?2 WHERE sku=?1 AND available>=?2",vec![sku.clone(),quantity.clone()]))?)?;
                    }
                    reservation = Some(Reservation {
                        spec: call.spec.clone(),
                        run_id: call.run_id,
                        status: if available {
                            ReservationStatus::Held
                        } else {
                            ReservationStatus::Unavailable
                        },
                    });
                    if available {
                        ReplyValue::Held
                    } else {
                        ReplyValue::Unavailable
                    }
                }
            },
            Operation::Commit => {
                let Some(old) = reservation.as_mut() else {
                    return Ok(sql::classify(DeliveryOutcome::NotFound));
                };
                match old.status {
                    ReservationStatus::Held => {
                        sql::changed(&context.sql(&sql::batch("UPDATE products SET held=held-?2,sold=sold+?2 WHERE sku=?1 AND held>=?2",vec![sku.clone(),quantity.clone()]))?)?;
                        old.status = ReservationStatus::Committed;
                    }
                    ReservationStatus::Committed => {}
                    _ => return Ok(sql::classify(DeliveryOutcome::InvalidState)),
                };
                ReplyValue::Committed
            }
            Operation::Release => {
                match reservation.as_mut() {
                    Some(old) => match old.status {
                        ReservationStatus::Held => {
                            sql::changed(&context.sql(&sql::batch("UPDATE products SET held=held-?2,available=available+?2 WHERE sku=?1 AND held>=?2",vec![sku,quantity]))?)?;
                            old.status = ReservationStatus::Released;
                        }
                        ReservationStatus::Released | ReservationStatus::Unavailable => {}
                        ReservationStatus::Committed => {
                            return Ok(sql::classify(DeliveryOutcome::InvalidState));
                        }
                    },
                    None => {
                        reservation = Some(Reservation {
                            spec: call.spec.clone(),
                            run_id: call.run_id,
                            status: ReservationStatus::Released,
                        })
                    }
                };
                ReplyValue::Released
            }
            _ => return Err(Error::Command("unsupported stock operation")),
        };
        let current = reservation.ok_or(Error::Command(
            "reservation transition did not bind identity",
        ))?;
        sql::changed(&context.sql(&sql::batch("INSERT INTO reservations(order_id,reservation) VALUES(?1,?2) ON CONFLICT(order_id) DO UPDATE SET reservation=excluded.reservation",vec![sql::id(call.spec.id),SqlValue::Blob(encode(&current)?)]))?)?;
        sql::record_reply(context, "stock_messages", call, value)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Coherent local stock counters.
pub struct GetStock;
impl Query for GetStock {
    const MODULE: &'static str = Inventory::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::StockQuery;
    type Output = Option<Stock>;
    fn execute(
        context: &mut QueryContext<'_>,
        seed: crate::StockQuery,
    ) -> cellule_runtime::Result<Self::Output> {
        crate::validate_sku(&seed.sku)?;
        stock(&context.sql(&stock_query(&seed.sku))?)
    }
}
/// Permanent per-order reservation, including releases and unavailable tombstones.
pub struct GetReservation;
impl Query for GetReservation {
    const MODULE: &'static str = Inventory::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::Id;
    type Output = Option<Reservation>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: crate::Id,
    ) -> cellule_runtime::Result<Self::Output> {
        sql::blob(&context.sql(&sql::batch(
            "SELECT reservation FROM reservations WHERE order_id=?1",
            vec![sql::id(id)],
        ))?)?
        .map(|bytes| {
            let value: Reservation = decode(&bytes)?;
            value.validate()?;
            if value.spec.id != id {
                return Err(Error::Identity("stored reservation identity differs"));
            }
            Ok(value)
        })
        .transpose()
    }
}
