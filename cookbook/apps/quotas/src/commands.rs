use crate::{
    Account, Action, Change, Credits, Decision, MAX_RESERVATIONS, NAMESPACE, Outcome, Reservation,
    ReservationState, sql,
};
use cellule_app::CellType;
use cellule_runtime::{
    CatalogRole, CellModule, Error,
    primitives::sql::{SqlBatch, SqlValue},
    registry::{Command, CommandContext, CommandResult},
};

/// Serialized local accounting with durable reservation business identities.
pub struct ChangeQuota;
fn decision(
    value: Decision,
    account: Option<Account>,
    reservation: Option<Reservation>,
) -> CommandResult<Outcome> {
    let output = Outcome {
        decision: value,
        account,
        reservation,
    };
    if value.success() {
        CommandResult::Success(output)
    } else {
        CommandResult::Rejected(output)
    }
}
fn increment(account: &mut Account) -> cellule_runtime::Result<()> {
    account.revision = account
        .revision
        .checked_add(1)
        .ok_or(Error::Command("quota revision overflow"))?;
    account.available = account
        .allowance
        .checked_sub(account.consumed)
        .and_then(|v| v.checked_sub(account.reserved))
        .ok_or(Error::Command("quota balance overflow"))?;
    account.validate()?;
    Ok(())
}
fn account_update(
    account: &Account,
    previous_revision: i64,
) -> cellule_runtime::primitives::sql::SqlStatement {
    sql::statement(
        "UPDATE quota_account SET allowance=?1, consumed=?2, reserved=?3, revision=?4, reservation_count=?5 WHERE singleton=1 AND revision=?6",
        vec![
            SqlValue::Integer(account.allowance),
            SqlValue::Integer(account.consumed),
            SqlValue::Integer(account.reserved),
            SqlValue::Integer(account.revision),
            SqlValue::Integer(account.reservation_count),
            SqlValue::Integer(previous_revision),
        ],
    )
}
impl Command for ChangeQuota {
    const MODULE: &'static str = Credits::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Change;
    type Output = Outcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Change,
    ) -> cellule_runtime::Result<CommandResult<Outcome>> {
        let partition = CellType::new(Credits::NAME, "customers", NAMESPACE, CatalogRole::Sql, 1)?
            .with_entity_partitions()?
            .entity_partition(input.customer.as_bytes())?;
        if input.action.validate().is_err() || context.target().partition() != partition {
            return Ok(decision(Decision::Invalid, None, None));
        }
        let existing = sql::account(&context.sql(&sql::account_query())?)?;
        if existing
            .as_ref()
            .is_some_and(|v| v.customer != input.customer)
        {
            return Err(Error::Command(
                "stored quota customer differs from Cell identity",
            ));
        }
        if let Action::Open { allowance } = input.action {
            if existing.is_some() {
                return Ok(decision(Decision::AlreadyOpen, existing, None));
            }
            sql::changed(&context.sql(&sql::batch("INSERT INTO quota_account(singleton, customer, allowance, consumed, reserved, revision, reservation_count) VALUES (1,?1,?2,0,0,1,0)",vec![SqlValue::Text(input.customer.as_str().into()),SqlValue::Integer(allowance)]))?,1)?;
            return Ok(decision(
                Decision::Opened,
                sql::account(&context.sql(&sql::account_query())?)?,
                None,
            ));
        }
        let Some(mut account) = existing else {
            return Ok(decision(Decision::NotFound, None, None));
        };
        let previous_revision = account.revision;
        if let Action::SetAllowance {
            expected_revision,
            allowance,
        } = input.action
        {
            if expected_revision != account.revision {
                return Ok(decision(Decision::Conflict, Some(account), None));
            }
            if allowance < account.consumed || allowance - account.consumed < account.reserved {
                return Ok(decision(Decision::Overcommitted, Some(account), None));
            }
            if allowance == account.allowance {
                return Ok(decision(Decision::Unchanged, Some(account), None));
            }
            account.allowance = allowance;
            increment(&mut account)?;
            sql::changed(
                &context.sql(&SqlBatch {
                    statements: vec![account_update(&account, previous_revision)],
                })?,
                1,
            )?;
            return Ok(decision(Decision::AllowanceChanged, Some(account), None));
        }
        let id = match &input.action {
            Action::Reserve { id, .. } | Action::Consume { id } | Action::Release { id } => *id,
            _ => return Err(Error::Command("invalid quota action dispatch")),
        };
        let existing = sql::reservation(&context.sql(&sql::reservation_query(id))?)?;
        match input.action {
            Action::Reserve { credits, .. } => {
                // Business IDs remain permanent after request-receipt expiry. Never
                // recreate a released or consumed hold under a repeated identity.
                if let Some(reservation) = existing {
                    let result = if reservation.credits == credits {
                        Decision::ExistingReservation
                    } else {
                        Decision::Conflict
                    };
                    return Ok(decision(result, Some(account), Some(reservation)));
                }
                if account.reservation_count >= MAX_RESERVATIONS {
                    return Ok(decision(Decision::Capacity, Some(account), None));
                }
                if credits > account.available {
                    return Ok(decision(Decision::Insufficient, Some(account), None));
                }
                account.reserved = account
                    .reserved
                    .checked_add(credits)
                    .ok_or(Error::Command("reserved credits overflow"))?;
                account.reservation_count = account
                    .reservation_count
                    .checked_add(1)
                    .ok_or(Error::Command("reservation count overflow"))?;
                increment(&mut account)?;
                let reservation = Reservation {
                    id,
                    credits,
                    state: ReservationState::Active,
                };
                // Both writes share the command savepoint. Any row-count or constraint
                // failure rolls back counters and the permanent business identity.
                sql::changed(
                    &context.sql(&SqlBatch {
                        statements: vec![
                            sql::statement(
                                "INSERT INTO reservations(id, credits, state) VALUES (?1,?2,0)",
                                vec![
                                    SqlValue::Blob(id.as_bytes().to_vec()),
                                    SqlValue::Integer(credits),
                                ],
                            ),
                            account_update(&account, previous_revision),
                        ],
                    })?,
                    2,
                )?;
                Ok(decision(
                    Decision::Reserved,
                    Some(account),
                    Some(reservation),
                ))
            }
            Action::Consume { .. } | Action::Release { .. } => {
                let Some(mut reservation) = existing else {
                    return Ok(decision(Decision::NotFound, Some(account), None));
                };
                let consuming = matches!(input.action, Action::Consume { .. });
                let state = if consuming {
                    ReservationState::Consumed
                } else {
                    ReservationState::Released
                };
                if reservation.state == state {
                    return Ok(decision(
                        if consuming {
                            Decision::AlreadyConsumed
                        } else {
                            Decision::AlreadyReleased
                        },
                        Some(account),
                        Some(reservation),
                    ));
                }
                if reservation.state != ReservationState::Active {
                    return Ok(decision(Decision::Closed, Some(account), Some(reservation)));
                }
                account.reserved = account
                    .reserved
                    .checked_sub(reservation.credits)
                    .ok_or(Error::Command("reserved credit underflow"))?;
                if consuming {
                    account.consumed = account
                        .consumed
                        .checked_add(reservation.credits)
                        .ok_or(Error::Command("consumed credit overflow"))?;
                }
                increment(&mut account)?;
                reservation.state = state;
                let state = if consuming { 1 } else { 2 };
                sql::changed(
                    &context.sql(&SqlBatch {
                        statements: vec![
                            sql::statement(
                                "UPDATE reservations SET state=?1 WHERE id=?2 AND state=0",
                                vec![
                                    SqlValue::Integer(state),
                                    SqlValue::Blob(id.as_bytes().to_vec()),
                                ],
                            ),
                            account_update(&account, previous_revision),
                        ],
                    })?,
                    2,
                )?;
                Ok(decision(
                    if consuming {
                        Decision::Consumed
                    } else {
                        Decision::Released
                    },
                    Some(account),
                    Some(reservation),
                ))
            }
            _ => Err(Error::Command("invalid reservation action dispatch")),
        }
    }
}
