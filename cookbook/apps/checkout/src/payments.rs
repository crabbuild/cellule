use crate::{
    DeliveryOutcome, MAX_ORDERS, Payment, PaymentAction, PaymentPolicy, PaymentStatus, PaymentWork,
    Payments,
    model::{decode, encode},
    sql,
};
use cellule_runtime::{
    CellModule, Error,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};
/// Included external simulator's atomic idempotent authorization and void tombstone.
pub struct ApplyPayment;
impl Command for ApplyPayment {
    const MODULE: &'static str = Payments::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = PaymentWork;
    type Output = DeliveryOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        work: PaymentWork,
    ) -> cellule_runtime::Result<CommandResult<DeliveryOutcome>> {
        work.spec.validate()?;
        let original = sql::blob(&context.sql(&sql::batch(
            "SELECT payment FROM payments WHERE order_id=?1",
            vec![sql::id(work.spec.id)],
        ))?)?
        .map(|bytes| decode::<Payment>(&bytes))
        .transpose()?;
        if let Some(old) = &original {
            old.validate()?;
        }
        if original.as_ref().is_some_and(|x| x.spec != work.spec) {
            return Ok(sql::classify(DeliveryOutcome::Conflict));
        }
        if original.is_none()
            && sql::count(&context.sql(&sql::batch("SELECT count(*) FROM payments", vec![]))?)?
                >= MAX_ORDERS
        {
            return Ok(sql::classify(DeliveryOutcome::Capacity));
        }
        let payment = match (original, work.action) {
            (Some(mut old), PaymentAction::Void) => {
                if old.status != PaymentStatus::Voided {
                    old.status = PaymentStatus::Voided;
                    old.voids = 1;
                }
                old
            }
            (Some(old), PaymentAction::Authorize) => old,
            (None, PaymentAction::Void) => Payment {
                spec: work.spec,
                status: PaymentStatus::Voided,
                authorizations: 0,
                voids: 1,
            },
            (None, PaymentAction::Authorize) => Payment {
                spec: work.spec.clone(),
                status: if work.spec.payment_policy == PaymentPolicy::Approve {
                    PaymentStatus::Authorized
                } else {
                    PaymentStatus::Declined
                },
                authorizations: u32::from(work.spec.payment_policy == PaymentPolicy::Approve),
                voids: 0,
            },
        };
        // A void with no visible authorization creates a permanent tombstone. A
        // delayed authorize arriving later cannot reopen the business identity.
        sql::changed(&context.sql(&sql::batch("INSERT INTO payments(order_id,payment) VALUES(?1,?2) ON CONFLICT(order_id) DO UPDATE SET payment=excluded.payment",vec![sql::id(payment.spec.id),SqlValue::Blob(encode(&payment)?)]))?)?;
        Ok(CommandResult::Success(DeliveryOutcome::Applied))
    }
}
/// Current external payment business record; absence alone cannot rule out an in-flight call.
pub struct GetPayment;
impl Query for GetPayment {
    const MODULE: &'static str = Payments::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = crate::Id;
    type Output = Option<Payment>;
    fn execute(
        context: &mut QueryContext<'_>,
        id: crate::Id,
    ) -> cellule_runtime::Result<Self::Output> {
        sql::blob(&context.sql(&sql::batch(
            "SELECT payment FROM payments WHERE order_id=?1",
            vec![sql::id(id)],
        ))?)?
        .map(|bytes| {
            let value: Payment = decode(&bytes)?;
            value.validate()?;
            if value.spec.id != id {
                return Err(Error::Identity("stored payment identity differs"));
            }
            Ok(value)
        })
        .transpose()
    }
}
