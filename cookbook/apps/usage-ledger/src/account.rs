use crate::{
    AccountBinding, AccountKey, AccountProgress, AccountSnapshot, Accounts, CloseAccount,
    MAX_EVENTS_PER_ACCOUNT, Projection, UsageDecision, UsageEvent, model::checked_total, sql, wire,
};
use cellule_runtime::{
    CellModule, Error, Result,
    primitives::{effects::EffectCommandIntent, sql::SqlValue},
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

#[derive(Clone, Copy)]
enum AccountState {
    Prepared,
    Open,
    Closed,
}

impl AccountState {
    fn from_sql(value: i64) -> Result<Self> {
        match value {
            0 => Ok(Self::Prepared),
            1 => Ok(Self::Open),
            2 => Ok(Self::Closed),
            _ => Err(Error::Command("invalid usage-ledger account state")),
        }
    }
}

struct AccountRow {
    key: AccountKey,
    period_id: [u8; 16],
    start_ms: i64,
    end_ms: i64,
    state: AccountState,
    snapshot: Option<AccountSnapshot>,
    events: Vec<UsageEvent>,
}

fn account_row(
    query: impl Fn(
        &cellule_runtime::primitives::sql::SqlBatch,
    ) -> Result<Vec<cellule_runtime::primitives::sql::SqlResultSet>>,
    target: Option<&cellule_runtime::CellTarget>,
) -> Result<Option<AccountRow>> {
    let results = query(&sql::batch(
        "SELECT account_key,period_id,start_ms,end_ms,state,close_snapshot FROM account WHERE singleton=1",
        vec![],
    ))?;
    let [meta] = results.as_slice() else {
        return Err(Error::Command("usage-ledger account query result differs"));
    };
    let row = match meta.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(Error::Command("usage-ledger account singleton violated")),
    };
    let [
        SqlValue::Text(account),
        SqlValue::Blob(period_id),
        SqlValue::Integer(start_ms),
        SqlValue::Integer(end_ms),
        SqlValue::Integer(state),
        snapshot,
    ] = row.as_slice()
    else {
        return Err(Error::Command(
            "usage-ledger account row differs from schema",
        ));
    };
    let key = AccountKey::new(account.clone())?;
    if let Some(target) = target
        && target.partition() != sql::account_partition(&key)?
    {
        return Err(Error::Identity(
            "usage-ledger account differs from entity Cell",
        ));
    }
    let snapshot = match snapshot {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => Some(wire::decode::<AccountSnapshot>(bytes, 64 << 10)?),
        _ => return Err(Error::Command("usage-ledger close snapshot column differs")),
    };
    let mut events = Vec::new();
    let result = query(&sql::batch(
        "SELECT event_bytes FROM usage_events ORDER BY event_id LIMIT ?1",
        vec![SqlValue::Integer((MAX_EVENTS_PER_ACCOUNT + 1) as i64)],
    ))?;
    let [rows] = result.as_slice() else {
        return Err(Error::Command("usage-ledger event query result differs"));
    };
    for row in &rows.rows {
        let [SqlValue::Blob(bytes)] = row.as_slice() else {
            return Err(Error::Command("usage-ledger event row differs from schema"));
        };
        events.push(wire::decode::<UsageEvent>(bytes, 2048)?);
    }
    if events.len() > MAX_EVENTS_PER_ACCOUNT {
        return Err(Error::Command(
            "usage-ledger event capacity invariant violated",
        ));
    }
    let row = AccountRow {
        key,
        period_id: period_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid usage-ledger period identity"))?,
        start_ms: *start_ms,
        end_ms: *end_ms,
        state: AccountState::from_sql(*state)?,
        snapshot,
        events,
    };
    if row.period_id == [0; 16] || row.start_ms >= row.end_ms {
        return Err(Error::Command(
            "stored usage-ledger account binding is invalid",
        ));
    }
    if let Some(snapshot) = &row.snapshot {
        snapshot.validate()?;
        if snapshot.account != row.key || snapshot.period_id != row.period_id {
            return Err(Error::Identity(
                "stored usage-ledger snapshot binding differs",
            ));
        }
    }
    Ok(Some(row))
}

fn progress(row: &AccountRow) -> Result<AccountProgress> {
    Ok(AccountProgress {
        account: row.key.clone(),
        period_id: row.period_id,
        closed: matches!(row.state, AccountState::Closed),
        event_count: u32::try_from(row.events.len())
            .map_err(|_| Error::Command("usage-ledger event count overflow"))?,
        total_microcredits: checked_total(&row.events)?,
    })
}

/// Permanently binds an account Cell to one explicit period before period creation.
pub struct BindAccount;

impl Command for BindAccount {
    const MODULE: &'static str = Accounts::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = AccountBinding;
    type Output = AccountProgress;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.spec.validate()?;
        let Some(_) = input
            .spec
            .accounts
            .iter()
            .find(|value| **value == input.account)
        else {
            return Ok(CommandResult::Rejected(AccountProgress {
                account: input.account,
                period_id: input.spec.id,
                closed: false,
                event_count: 0,
                total_microcredits: 0,
            }));
        };
        let expected = cellule_app::CellType::new(
            Accounts::NAME,
            "accounts",
            crate::ACCOUNTS,
            cellule_runtime::CatalogRole::Sql,
            1,
        )?
        .with_entity_partitions()?
        .entity_partition(input.account.as_bytes())?;
        if context.target().partition() != expected {
            return Err(Error::Identity("usage-ledger account target differs"));
        }
        if let Some(existing) = account_row(|batch| context.sql(batch), Some(context.target()))? {
            return if existing.key == input.account
                && existing.period_id == input.spec.id
                && existing.start_ms == input.spec.start_ms
                && existing.end_ms == input.spec.end_ms
            {
                Ok(CommandResult::Success(progress(&existing)?))
            } else {
                Ok(CommandResult::Rejected(progress(&existing)?))
            };
        }
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO account(singleton,account_key,period_id,start_ms,end_ms,state,close_snapshot) VALUES(1,?1,?2,?3,?4,0,NULL)",
            vec![
                SqlValue::Text(input.account.as_str().into()),
                SqlValue::Blob(input.spec.id.to_vec()),
                SqlValue::Integer(input.spec.start_ms),
                SqlValue::Integer(input.spec.end_ms),
            ],
        ))?)?;
        let row = account_row(|batch| context.sql(batch), Some(context.target()))?
            .ok_or(Error::Command("usage-ledger account insert disappeared"))?;
        Ok(CommandResult::Success(progress(&row)?))
    }
}

/// Activates a prepared account and emits its readiness proof to the period Cell.
pub struct ActivateAccount;

impl Command for ActivateAccount {
    const MODULE: &'static str = Accounts::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = AccountBinding;
    type Output = AccountProgress;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.spec.validate()?;
        let Some(mut row) = account_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(AccountProgress {
                account: input.account,
                period_id: input.spec.id,
                closed: false,
                event_count: 0,
                total_microcredits: 0,
            }));
        };
        if row.key != input.account
            || row.period_id != input.spec.id
            || row.start_ms != input.spec.start_ms
            || row.end_ms != input.spec.end_ms
        {
            return Ok(CommandResult::Rejected(progress(&row)?));
        }
        if matches!(row.state, AccountState::Prepared) {
            sql::changed(context.sql(&sql::batch(
                "UPDATE account SET state=1 WHERE singleton=1 AND state=0",
                vec![],
            ))?)?;
            row.state = AccountState::Open;
        }
        Ok(CommandResult::Success(progress(&row)?))
    }
}

/// Durable source admission and transactional outbox publication for one usage event.
pub struct RecordUsage;

impl Command for RecordUsage {
    const MODULE: &'static str = Accounts::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = UsageEvent;
    type Output = UsageDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        event: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        event.validate()?;
        let expected = cellule_app::CellType::new(
            Accounts::NAME,
            "accounts",
            crate::ACCOUNTS,
            cellule_runtime::CatalogRole::Sql,
            1,
        )?
        .with_entity_partitions()?
        .entity_partition(event.account.as_bytes())?;
        if context.target().partition() != expected {
            return Err(Error::Identity("usage-ledger event account target differs"));
        }
        let Some(row) = account_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(UsageDecision::NotBound));
        };
        if row.key != event.account || row.period_id != event.period_id {
            return Ok(CommandResult::Rejected(UsageDecision::NotBound));
        }
        if event.occurred_at_ms < row.start_ms || event.occurred_at_ms >= row.end_ms {
            return Ok(CommandResult::Rejected(UsageDecision::OutsidePeriod));
        }
        let encoded = wire::encode(&event, 2048)?;
        let existing = context.sql(&sql::batch(
            "SELECT event_bytes FROM usage_events WHERE event_id=?1",
            vec![SqlValue::Blob(event.id.to_vec())],
        ))?;
        let [result] = existing.as_slice() else {
            return Err(Error::Command("usage-ledger identity result differs"));
        };
        if let [row] = result.rows.as_slice() {
            let [SqlValue::Blob(original)] = row.as_slice() else {
                return Err(Error::Command("usage-ledger stored event differs"));
            };
            return Ok(if original == &encoded {
                CommandResult::Success(UsageDecision::Duplicate)
            } else {
                CommandResult::Rejected(UsageDecision::Conflict)
            });
        }
        if matches!(row.state, AccountState::Closed) {
            return Ok(CommandResult::Rejected(UsageDecision::Closed));
        }
        if !matches!(row.state, AccountState::Open) {
            return Ok(CommandResult::Rejected(UsageDecision::NotBound));
        }
        if row.events.len() >= MAX_EVENTS_PER_ACCOUNT {
            return Ok(CommandResult::Rejected(UsageDecision::Capacity));
        }
        let target = sql::period_target(context.target(), &event.period_id)?;
        context.emit_effect(&EffectCommandIntent {
            target,
            command_id: crate::period::ProjectUsage::ID,
            codec_version: 1,
            input: wire::encode_value(
                &Projection {
                    event: event.clone(),
                },
                4096,
            )?,
            expires_at_ms: context
                .now_ms()
                .checked_add(7 * 24 * 60 * 60 * 1000)
                .ok_or(Error::Command("usage-ledger projection expiry overflow"))?,
        })?;
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO usage_events(event_id,event_bytes) VALUES(?1,?2)",
            vec![SqlValue::Blob(event.id.to_vec()), SqlValue::Blob(encoded)],
        ))?)?;
        Ok(CommandResult::Success(UsageDecision::Accepted))
    }
}

/// Raises the account-local close fence and persists the complete accepted source set.
pub struct CloseUsage;

impl Command for CloseUsage {
    const MODULE: &'static str = Accounts::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = CloseAccount;
    type Output = AccountSnapshot;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let Some(mut row) = account_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Err(Error::Command(
                "cannot close an unbound usage-ledger account",
            ));
        };
        if row.key != input.account || row.period_id != input.period_id {
            return Err(Error::Identity("usage-ledger close binding differs"));
        }
        if let Some(snapshot) = row.snapshot {
            return Ok(CommandResult::Success(snapshot));
        }
        if !matches!(row.state, AccountState::Open) {
            return Err(Error::Command("usage-ledger account is not open"));
        }
        let snapshot = AccountSnapshot {
            account: row.key.clone(),
            period_id: row.period_id,
            total_microcredits: checked_total(&row.events)?,
            digest: crate::wire::events_digest(&row.events)?,
            events: row.events.clone(),
        };
        snapshot.validate()?;
        let encoded = wire::encode(&snapshot, 64 << 10)?;
        sql::changed(context.sql(&sql::batch(
            "UPDATE account SET state=2,close_snapshot=?1 WHERE singleton=1 AND state=1 AND close_snapshot IS NULL",
            vec![SqlValue::Blob(encoded)],
        ))?)?;
        row.state = AccountState::Closed;
        row.snapshot = Some(snapshot.clone());
        Ok(CommandResult::Success(snapshot))
    }
}

/// Receipt-gated account status query.
pub struct GetAccount;

impl Query for GetAccount {
    const MODULE: &'static str = Accounts::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = AccountKey;
    type Output = Option<AccountProgress>;

    fn execute(context: &mut QueryContext<'_>, key: Self::Input) -> Result<Self::Output> {
        account_row(|batch| context.sql(batch), None)?
            .filter(|row| row.key == key)
            .map(|row| progress(&row))
            .transpose()
    }
}
