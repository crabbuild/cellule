use crate::{
    AccountKey, AccountReady, AccountSnapshot, LedgerReport, MAX_ACCOUNTS, PeriodDecision,
    PeriodIdentity, PeriodSpec, PeriodStatus, PeriodView, Periods, Projection, ReconcileAccount,
    UsageEvent,
    model::{checked_total, event_order},
    sql, wire,
};
use cellule_runtime::{
    CellModule, Error, Result,
    primitives::sql::SqlValue,
    registry::{Command, CommandContext, CommandResult, Query, QueryContext},
};

struct PeriodRow {
    spec: PeriodSpec,
    state: PeriodStatus,
    ready: u32,
    reconciled: u32,
    projected: u32,
    report: Option<LedgerReport>,
}

fn period_row(
    query: impl Fn(
        &cellule_runtime::primitives::sql::SqlBatch,
    ) -> Result<Vec<cellule_runtime::primitives::sql::SqlResultSet>>,
    target: Option<&cellule_runtime::CellTarget>,
) -> Result<Option<PeriodRow>> {
    let results = query(&sql::batch(
        "SELECT spec,state,sealed_report FROM period WHERE singleton=1",
        vec![],
    ))?;
    let [meta] = results.as_slice() else {
        return Err(Error::Command("usage-ledger period query result differs"));
    };
    let row = match meta.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(Error::Command("usage-ledger period singleton violated")),
    };
    let [SqlValue::Blob(spec), SqlValue::Integer(state), report] = row.as_slice() else {
        return Err(Error::Command(
            "usage-ledger period row differs from schema",
        ));
    };
    let spec: PeriodSpec = wire::decode(spec, 4096)?;
    spec.validate()?;
    if let Some(target) = target
        && target.partition() != sql::period_partition(&spec.id)?
    {
        return Err(Error::Identity(
            "usage-ledger period differs from entity Cell",
        ));
    }
    let state = status(*state)?;
    let report = match report {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => {
            let report: LedgerReport = wire::decode(bytes, crate::MAX_WIRE_BYTES)?;
            report.validate()?;
            if report.period_id != spec.id {
                return Err(Error::Identity("stored usage-ledger report period differs"));
            }
            Some(report)
        }
        _ => return Err(Error::Command("usage-ledger report column differs")),
    };
    let values = query(&sql::batch(
        "SELECT SUM(ready),SUM(reconciled IS NOT NULL) FROM period_members",
        vec![],
    ))?;
    let [members] = values.as_slice() else {
        return Err(Error::Command("usage-ledger member count result differs"));
    };
    let [member] = members.rows.as_slice() else {
        return Err(Error::Command("usage-ledger member count row differs"));
    };
    let [ready, reconciled] = member.as_slice() else {
        return Err(Error::Command("usage-ledger member counts differ"));
    };
    let ready = nullable_count(ready)?;
    let reconciled = nullable_count(reconciled)?;
    let events = query(&sql::batch("SELECT count(*) FROM projected_events", vec![]))?;
    let [event_rows] = events.as_slice() else {
        return Err(Error::Command("usage-ledger event count result differs"));
    };
    let [event_row] = event_rows.rows.as_slice() else {
        return Err(Error::Command("usage-ledger event count row differs"));
    };
    let [SqlValue::Integer(projected)] = event_row.as_slice() else {
        return Err(Error::Command("usage-ledger event count differs"));
    };
    let row = PeriodRow {
        spec,
        state,
        ready,
        reconciled,
        projected: u32::try_from(*projected)
            .map_err(|_| Error::Command("usage-ledger event count overflow"))?,
        report,
    };
    if row.spec.accounts.len() > MAX_ACCOUNTS
        || row.ready as usize > row.spec.accounts.len()
        || row.reconciled as usize > row.spec.accounts.len()
    {
        return Err(Error::Command("usage-ledger member counts exceed roster"));
    }
    Ok(Some(row))
}

fn status(value: i64) -> Result<PeriodStatus> {
    match value {
        0 => Ok(PeriodStatus::Opening),
        1 => Ok(PeriodStatus::Open),
        2 => Ok(PeriodStatus::Closing),
        3 => Ok(PeriodStatus::Sealed),
        _ => Err(Error::Command("invalid usage-ledger period state")),
    }
}

fn nullable_count(value: &SqlValue) -> Result<u32> {
    match value {
        SqlValue::Integer(value) => {
            u32::try_from(*value).map_err(|_| Error::Command("usage-ledger member count overflow"))
        }
        SqlValue::Null => Ok(0),
        _ => Err(Error::Command("usage-ledger member count type differs")),
    }
}

fn view(row: PeriodRow) -> PeriodView {
    PeriodView {
        spec: row.spec,
        status: row.state,
        ready_accounts: row.ready,
        reconciled_accounts: row.reconciled,
        projected_events: row.projected,
        report: row.report,
    }
}

fn classify(value: PeriodDecision) -> CommandResult<PeriodDecision> {
    match value {
        PeriodDecision::Conflict | PeriodDecision::InvalidState | PeriodDecision::Capacity => {
            CommandResult::Rejected(value)
        }
        _ => CommandResult::Success(value),
    }
}

/// Permanently creates a period roster in the Opening state.
pub struct CreatePeriod;

impl Command for CreatePeriod {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = PeriodSpec;
    type Output = PeriodDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        spec: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        spec.validate()?;
        let expected = cellule_app::CellType::new(
            Periods::NAME,
            "periods",
            crate::PERIODS,
            cellule_runtime::CatalogRole::Sql,
            1,
        )?
        .with_entity_partitions()?
        .entity_partition(&spec.id)?;
        if context.target().partition() != expected {
            return Err(Error::Identity("usage-ledger period target differs"));
        }
        let encoded = wire::encode(&spec, 4096)?;
        if let Some(existing) = period_row(|batch| context.sql(batch), Some(context.target()))? {
            return Ok(classify(
                if wire::encode(&existing.spec, 4096)? == encoded {
                    PeriodDecision::Existing
                } else {
                    PeriodDecision::Conflict
                },
            ));
        }
        let mut statements = vec![sql::statement(
            "INSERT INTO period(singleton,spec,state,sealed_report) VALUES(1,?1,0,NULL)",
            vec![SqlValue::Blob(encoded)],
        )];
        for account in &spec.accounts {
            statements.push(sql::statement(
                "INSERT INTO period_members(account_key,ready,reconciled) VALUES(?1,0,NULL)",
                vec![SqlValue::Text(account.as_str().into())],
            ));
        }
        let result = context.sql(&cellule_runtime::primitives::sql::SqlBatch { statements })?;
        if result.len() != spec.accounts.len() + 1
            || result.iter().any(|value| value.rows_affected != 1)
        {
            return Err(Error::Command(
                "usage-ledger roster insert invariant violated",
            ));
        }
        Ok(CommandResult::Success(PeriodDecision::Created))
    }
}

/// Receives account activation through a signed Effect and opens after the full roster is ready.
pub struct ConfirmReady;

impl Command for ConfirmReady {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = AccountReady;
    type Output = PeriodDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let Some(row) = period_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        };
        if input.period_id != row.spec.id || !row.spec.accounts.contains(&input.account) {
            return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
        }
        let existing = context.sql(&sql::batch(
            "SELECT ready FROM period_members WHERE account_key=?1",
            vec![SqlValue::Text(input.account.as_str().into())],
        ))?;
        let [result] = existing.as_slice() else {
            return Err(Error::Command("usage-ledger member query result differs"));
        };
        let already = match result.rows.as_slice() {
            [member] => matches!(member.as_slice(), [SqlValue::Integer(1)]),
            _ => return Err(Error::Command("usage-ledger roster member is absent")),
        };
        if !already {
            if row.state != PeriodStatus::Opening {
                return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
            }
            sql::changed(context.sql(&sql::batch(
                "UPDATE period_members SET ready=1 WHERE account_key=?1 AND ready=0",
                vec![SqlValue::Text(input.account.as_str().into())],
            ))?)?;
        }
        let count = period_row(|batch| context.sql(batch), Some(context.target()))?
            .ok_or(Error::Command("usage-ledger period disappeared"))?;
        if count.ready as usize == count.spec.accounts.len() && count.state == PeriodStatus::Opening
        {
            sql::changed(context.sql(&sql::batch(
                "UPDATE period SET state=1 WHERE singleton=1 AND state=0",
                vec![],
            ))?)?;
        }
        Ok(CommandResult::Success(PeriodDecision::Ready))
    }
}

/// Applies the account's eventual usage projection with permanent exact payload identity.
pub struct ProjectUsage;

impl Command for ProjectUsage {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = Projection;
    type Output = PeriodDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let event = input.event;
        event.validate()?;
        let Some(row) = period_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        };
        if event.period_id != row.spec.id
            || !row.spec.accounts.contains(&event.account)
            || event.occurred_at_ms < row.spec.start_ms
            || event.occurred_at_ms >= row.spec.end_ms
        {
            return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
        }
        let encoded = wire::encode(&event, 2048)?;
        let existing = context.sql(&sql::batch(
            "SELECT event_bytes FROM projected_events WHERE account_key=?1 AND event_id=?2",
            vec![
                SqlValue::Text(event.account.as_str().into()),
                SqlValue::Blob(event.id.to_vec()),
            ],
        ))?;
        let [result] = existing.as_slice() else {
            return Err(Error::Command("usage-ledger projection query differs"));
        };
        if let [projected] = result.rows.as_slice() {
            let [SqlValue::Blob(original)] = projected.as_slice() else {
                return Err(Error::Command("usage-ledger projection row differs"));
            };
            return Ok(classify(if original == &encoded {
                PeriodDecision::Projected
            } else {
                PeriodDecision::Conflict
            }));
        }
        if !matches!(
            row.state,
            PeriodStatus::Opening | PeriodStatus::Open | PeriodStatus::Closing
        ) {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        }
        sql::changed(context.sql(&sql::batch(
            "INSERT INTO projected_events(account_key,event_id,event_bytes) VALUES(?1,?2,?3)",
            vec![
                SqlValue::Text(event.account.as_str().into()),
                SqlValue::Blob(event.id.to_vec()),
                SqlValue::Blob(encoded),
            ],
        ))?)?;
        Ok(CommandResult::Success(PeriodDecision::Projected))
    }
}

/// Globally announces close; account-local commands then establish the actual admission fences.
pub struct BeginClose;

impl Command for BeginClose {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = PeriodIdentity;
    type Output = PeriodDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        identity: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let Some(row) = period_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        };
        if row.spec.id != identity.0 {
            return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
        }
        match row.state {
            PeriodStatus::Open => {
                sql::changed(context.sql(&sql::batch(
                    "UPDATE period SET state=2 WHERE singleton=1 AND state=1",
                    vec![],
                ))?)?;
                Ok(CommandResult::Success(PeriodDecision::Closing))
            }
            PeriodStatus::Closing | PeriodStatus::Sealed => {
                Ok(CommandResult::Success(PeriodDecision::Closing))
            }
            PeriodStatus::Opening => Ok(CommandResult::Rejected(PeriodDecision::InvalidState)),
        }
    }
}

/// Repairs missing Effect projections from each account's complete source snapshot.
pub struct ReconcileAccountCommand;

impl Command for ReconcileAccountCommand {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 5;
    const CODEC_VERSION: u32 = 1;
    type Input = ReconcileAccount;
    type Output = PeriodDecision;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        input.spec.validate()?;
        input.snapshot.validate()?;
        let Some(row) = period_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        };
        if row.spec != input.spec
            || input.snapshot.period_id != row.spec.id
            || !row.spec.accounts.contains(&input.snapshot.account)
            || input.snapshot.events.iter().any(|event| {
                event.occurred_at_ms < row.spec.start_ms || event.occurred_at_ms >= row.spec.end_ms
            })
        {
            return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
        }
        let snapshot_bytes = wire::encode(&input.snapshot, 64 << 10)?;
        let previous = context.sql(&sql::batch(
            "SELECT reconciled FROM period_members WHERE account_key=?1",
            vec![SqlValue::Text(input.snapshot.account.as_str().into())],
        ))?;
        let [result] = previous.as_slice() else {
            return Err(Error::Command(
                "usage-ledger reconcile member query differs",
            ));
        };
        let [member] = result.rows.as_slice() else {
            return Err(Error::Identity(
                "usage-ledger reconcile account is outside roster",
            ));
        };
        let [stored] = member.as_slice() else {
            return Err(Error::Command("usage-ledger reconcile row differs"));
        };
        if let SqlValue::Blob(original) = stored {
            return Ok(classify(if original == &snapshot_bytes {
                PeriodDecision::Reconciled
            } else {
                PeriodDecision::Conflict
            }));
        }
        if !matches!(row.state, PeriodStatus::Closing) {
            return Ok(CommandResult::Rejected(PeriodDecision::InvalidState));
        }
        let selected = context.sql(&sql::batch(
            "SELECT event_id,event_bytes FROM projected_events WHERE account_key=?1 ORDER BY event_id",
            vec![SqlValue::Text(input.snapshot.account.as_str().into())],
        ))?;
        let [result] = selected.as_slice() else {
            return Err(Error::Command("usage-ledger projection set query differs"));
        };
        for existing in &result.rows {
            let [SqlValue::Blob(event_id), SqlValue::Blob(bytes)] = existing.as_slice() else {
                return Err(Error::Command("usage-ledger projected event row differs"));
            };
            let Some(source) = input
                .snapshot
                .events
                .iter()
                .find(|event| event.id.as_slice() == event_id)
            else {
                return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
            };
            if wire::encode(source, 2048)? != *bytes {
                return Ok(CommandResult::Rejected(PeriodDecision::Conflict));
            }
        }
        let mut statements = Vec::new();
        for event in &input.snapshot.events {
            let encoded = wire::encode(event, 2048)?;
            let existing = result.rows.iter().any(|row| {
                matches!(row.as_slice(), [SqlValue::Blob(id), _] if id.as_slice() == event.id)
            });
            if !existing {
                statements.push(sql::statement(
                    "INSERT INTO projected_events(account_key,event_id,event_bytes) VALUES(?1,?2,?3)",
                    vec![
                        SqlValue::Text(event.account.as_str().into()),
                        SqlValue::Blob(event.id.to_vec()),
                        SqlValue::Blob(encoded),
                    ],
                ));
            }
        }
        statements.push(sql::statement(
            "UPDATE period_members SET reconciled=?1 WHERE account_key=?2 AND reconciled IS NULL",
            vec![
                SqlValue::Blob(snapshot_bytes),
                SqlValue::Text(input.snapshot.account.as_str().into()),
            ],
        ));
        let statement_count = statements.len();
        let results = context.sql(&cellule_runtime::primitives::sql::SqlBatch { statements })?;
        let Some((last, preceding)) = results.split_last() else {
            return Err(Error::Command(
                "usage-ledger reconciliation returned no results",
            ));
        };
        if results.len() != statement_count
            || preceding.iter().any(|result| result.rows_affected != 1)
            || last.rows_affected != 1
        {
            return Err(Error::Command(
                "usage-ledger reconciliation row count differs",
            ));
        }
        Ok(CommandResult::Success(PeriodDecision::Reconciled))
    }
}

/// Seals one deterministic report after every roster account's full set reconciles.
pub struct SealPeriod;

impl Command for SealPeriod {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 6;
    const CODEC_VERSION: u32 = 1;
    type Input = PeriodIdentity;
    type Output = LedgerReport;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        identity: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let Some(row) = period_row(|batch| context.sql(batch), Some(context.target()))? else {
            return Err(Error::Command("cannot seal absent usage-ledger period"));
        };
        if row.spec.id != identity.0 {
            return Err(Error::Identity("usage-ledger seal period differs"));
        }
        if let Some(report) = row.report {
            return Ok(CommandResult::Success(report));
        }
        if row.state != PeriodStatus::Closing || row.reconciled as usize != row.spec.accounts.len()
        {
            return Err(Error::Command(
                "usage-ledger cannot seal before every account reconciles",
            ));
        }
        let selected = context.sql(&sql::batch(
            "SELECT account_key,event_bytes FROM projected_events ORDER BY account_key,event_id",
            vec![],
        ))?;
        let [result] = selected.as_slice() else {
            return Err(Error::Command("usage-ledger report query differs"));
        };
        let mut events = Vec::with_capacity(result.rows.len());
        for value in &result.rows {
            let [SqlValue::Text(account), SqlValue::Blob(bytes)] = value.as_slice() else {
                return Err(Error::Command("usage-ledger report event row differs"));
            };
            let event: UsageEvent = wire::decode(bytes, 2048)?;
            if event.account != AccountKey::new(account.clone())?
                || event.period_id != row.spec.id
                || event.occurred_at_ms < row.spec.start_ms
                || event.occurred_at_ms >= row.spec.end_ms
            {
                return Err(Error::Identity(
                    "usage-ledger projected source binding differs",
                ));
            }
            events.push(event);
        }
        if events
            .windows(2)
            .any(|v| event_order(&v[0], &v[1]) != std::cmp::Ordering::Less)
            || events.len() > row.spec.accounts.len() * crate::MAX_EVENTS_PER_ACCOUNT
        {
            return Err(Error::Command(
                "usage-ledger report event order or count differs",
            ));
        }
        // Each reconciled account's digest, count, and total are checked against the complete
        // projected set. A partial inbox can never be mistaken for a finished period.
        let member_rows = context.sql(&sql::batch(
            "SELECT account_key,reconciled FROM period_members ORDER BY account_key",
            vec![],
        ))?;
        let [members] = member_rows.as_slice() else {
            return Err(Error::Command("usage-ledger report member result differs"));
        };
        for member in &members.rows {
            let [SqlValue::Text(account), SqlValue::Blob(snapshot_bytes)] = member.as_slice()
            else {
                return Err(Error::Command("unreconciled usage-ledger member remains"));
            };
            let snapshot: AccountSnapshot = wire::decode(snapshot_bytes, 64 << 10)?;
            let projected: Vec<_> = events
                .iter()
                .filter(|event| event.account.as_str() == account)
                .cloned()
                .collect();
            if snapshot.account.as_str() != account
                || projected != snapshot.events
                || checked_total(&projected)? != snapshot.total_microcredits
                || crate::wire::events_digest(&projected)? != snapshot.digest
            {
                return Err(Error::Identity(
                    "usage-ledger account projection is incomplete",
                ));
            }
        }
        let mut report = LedgerReport {
            period_id: row.spec.id,
            start_ms: row.spec.start_ms,
            end_ms: row.spec.end_ms,
            accounts: row.spec.accounts,
            event_count: u32::try_from(events.len())
                .map_err(|_| Error::Command("usage-ledger report event count overflow"))?,
            total_microcredits: checked_total(&events)?,
            events,
            digest: [0; 32],
            sealed_at_ms: context.now_ms(),
        };
        report.digest = crate::wire::report_digest(&report)?;
        report.validate()?;
        sql::changed(context.sql(&sql::batch(
            "UPDATE period SET state=3,sealed_report=?1 WHERE singleton=1 AND state=2 AND sealed_report IS NULL",
            vec![SqlValue::Blob(wire::encode(&report, crate::MAX_WIRE_BYTES)?)],
        ))?)?;
        Ok(CommandResult::Success(report))
    }
}

/// Reads status at a receipt boundary; absence stays distinct from a partial projection.
pub struct GetPeriod;

impl Query for GetPeriod {
    const MODULE: &'static str = Periods::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = PeriodIdentity;
    type Output = Option<PeriodView>;

    fn execute(context: &mut QueryContext<'_>, id: Self::Input) -> Result<Self::Output> {
        period_row(|batch| context.sql(batch), None)?
            .filter(|row| row.spec.id == id.0)
            .map(|row| Ok(view(row)))
            .transpose()
    }
}
