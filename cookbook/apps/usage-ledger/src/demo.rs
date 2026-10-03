use crate::emit;
use cellule_cookbook_support::{new_identity, now_ms};
use cellule_cookbook_usage_ledger::{
    AccountKey, CloseClient, CloseRequest, PeriodSpec, PeriodStatus, StatementFiles, UsageDecision,
    UsageEvent,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    path::Path,
    time::{Duration, Instant},
};

const PLAN_FILE: &str = "usage-ledger-demo-plan.json";
const MAX_PLAN_BYTES: usize = 64 << 10;
const ACCOUNTS: [&str; 2] = ["demo-alpha", "demo-beta"];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u8,
    tenant: String,
    endpoint: String,
    spec: PeriodSpec,
    events: Vec<UsageEvent>,
}

impl Plan {
    fn validate(&self) -> Result<(), crate::BoxError> {
        self.spec.validate()?;
        if self.version != 1
            || self.tenant.len() > 64
            || self.spec.accounts.len() != ACCOUNTS.len()
            || self.events.len() != 4
            || self.endpoint.is_empty()
        {
            return Err("invalid retained usage-ledger demo plan".into());
        }
        let accounts = ACCOUNTS
            .iter()
            .map(|value| AccountKey::new(*value))
            .collect::<Result<Vec<_>, _>>()?;
        if self.spec.accounts != accounts {
            return Err("retained demo roster differs from the declared journey".into());
        }
        let mut event_ids = Vec::new();
        for event in &self.events {
            event.validate()?;
            if event.period_id != self.spec.id
                || !self.spec.accounts.contains(&event.account)
                || event.occurred_at_ms < self.spec.start_ms
                || event.occurred_at_ms >= self.spec.end_ms
                || event_ids.contains(&event.id)
            {
                return Err("retained usage-ledger event differs from its period".into());
            }
            event_ids.push(event.id);
        }
        Ok(())
    }
}

pub(crate) async fn start_service(
    state: &Path,
    tenant: &str,
) -> Result<cellule_cookbook_usage_ledger::Service, crate::BoxError> {
    cellule_cookbook_usage_ledger::Service::start(state.to_path_buf(), tenant).await
}

pub(crate) async fn run(state: &Path) -> Result<(), crate::BoxError> {
    fs::create_dir_all(state)?;
    let path = state.join(PLAN_FILE);
    let plan = load_or_create_plan(&path, state)?;
    plan.validate()?;
    let service = start_service(state, &plan.tenant).await?;
    if service.endpoint != plan.endpoint {
        let shutdown = service.node.shutdown().await;
        shutdown?;
        return Err("usage-ledger adapter endpoint changed from the retained plan".into());
    }
    let result = run_plan(&service, &plan, &path).await;
    let drained = service.node.shutdown().await;
    result?;
    drained?;
    Ok(())
}

async fn run_plan(
    service: &cellule_cookbook_usage_ledger::Service,
    plan: &Plan,
    path: &Path,
) -> Result<(), crate::BoxError> {
    emit(&serde_json::json!({
        "event": "demo_plan",
        "path": path,
        "tenant": plan.tenant,
        "period_id": hex(&plan.spec.id),
        "accounts": plan.spec.accounts,
        "event_ids": plan.events.iter().map(|event| hex(&event.id)).collect::<Vec<_>>(),
    }))?;

    let period = service.open_period(plan.spec.clone()).await?;
    service.spawn_effects(&plan.spec.accounts[0]).await?;
    for event in &plan.events {
        let account = crate::AccountClient::new(service.handle.clone(), event.account.clone())?;
        let result = account.record(new_identity()?, event.clone()).await?;
        if !matches!(
            result.output,
            UsageDecision::Accepted | UsageDecision::Duplicate
        ) {
            return Err(format!("usage event was not accepted: {:?}", result.output).into());
        }
    }

    let immediately_projected = plan
        .events
        .iter()
        .filter(|event| event.account == plan.spec.accounts[0])
        .count();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let observed = period
            .get(None)
            .await?
            .output
            .ok_or("usage-ledger period disappeared")?;
        if observed.projected_events as usize >= immediately_projected {
            emit(&serde_json::json!({
                "event": "projection_before_close",
                "period_id": hex(&plan.spec.id),
                "expected_projected": immediately_projected,
                "actual_projected": observed.projected_events,
                "source_event_count": plan.events.len(),
                "delayed_account": plan.spec.accounts[1],
            }))?;
            break;
        }
        if Instant::now() >= deadline {
            return Err("active account Effects did not reach the period Cell".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let close = CloseClient::new(service.handle.clone())?;
    let started = close
        .start(
            new_identity()?,
            CloseRequest {
                period_id: plan.spec.id,
                endpoint: plan.endpoint.clone(),
            },
        )
        .await?;
    if started.output != cellule_cookbook_usage_ledger::StartDecision::Started {
        return Err("usage-ledger close Workflow identity conflicts with retained inputs".into());
    }
    let workflow_deadline = Instant::now() + Duration::from_secs(300);
    let completed = loop {
        let view = close
            .get(plan.spec.id, None)
            .await?
            .output
            .ok_or("usage-ledger close Workflow has not started")?;
        if view.status == "completed" {
            break view;
        }
        if view.status == "failed" {
            return Err(format!(
                "usage-ledger close Workflow failed: {}",
                view.state
                    .failure
                    .unwrap_or_else(|| "no Activity diagnostic".into())
            )
            .into());
        }
        if Instant::now() >= workflow_deadline {
            return Err("usage-ledger close Workflow did not complete within five minutes".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let completion = completed
        .state
        .completion
        .ok_or("completed usage-ledger Workflow has no report receipt")?;
    completion.report.validate()?;
    completion.artifact.validate()?;
    let sealed = period
        .get(None)
        .await?
        .output
        .ok_or("sealed period disappeared")?;
    if sealed.status != PeriodStatus::Sealed
        || sealed.report.as_ref() != Some(&completion.report)
        || completion.report.events.len() != plan.events.len()
    {
        return Err("usage-ledger sealed set differs from retained source events".into());
    }
    emit(&serde_json::json!({
        "event": "period_sealed_before_delayed_effects",
        "period_id": hex(&plan.spec.id),
        "report_digest": hex(&completion.report.digest),
        "source_event_count": plan.events.len(),
        "sealed_event_count": completion.report.event_count,
        "projected_event_count": sealed.projected_events,
        "total_microcredits": completion.report.total_microcredits,
        "delayed_account": plan.spec.accounts[1],
        "artifact": completion.artifact,
    }))?;

    // The second account's event Effects have remained pending since their SQL commits.
    // Delivering after the close proves snapshot reconciliation made the statement complete.
    service.spawn_effects(&plan.spec.accounts[1]).await?;
    let projection_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let current = period
            .get(None)
            .await?
            .output
            .ok_or("sealed period disappeared")?;
        if current.projected_events == completion.report.event_count {
            break;
        }
        if Instant::now() >= projection_deadline {
            return Err("late usage Effects did not settle as exact period duplicates".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let bytes = StatementFiles::new(service.handle.clone())
        .read(&completion.artifact, None)
        .await?
        .output
        .ok_or("published usage-ledger CSV is absent")?;
    let text = std::str::from_utf8(&bytes)?;
    if text.lines().count() != plan.events.len() + 1
        || !text
            .starts_with("period_id,account,event_id,occurred_at_ms,category,amount_microcredits\n")
    {
        return Err("published usage-ledger statement rows differ from the sealed report".into());
    }
    let statement_path = path
        .parent()
        .ok_or("usage-ledger demo plan has no parent directory")?
        .join("usage-ledger-statement.csv");
    write_atomic(&statement_path, &bytes)?;
    emit(&serde_json::json!({
        "event": "demo_complete",
        "period_id": hex(&plan.spec.id),
        "report_digest": hex(&completion.report.digest),
        "source_event_count": plan.events.len(),
        "reconciled_event_count": completion.report.event_count,
        "projected_after_late_effects": plan.events.len(),
        "total_microcredits": completion.report.total_microcredits,
        "artifact": completion.artifact,
        "csv_digest_blake3": hex(blake3::hash(&bytes).as_bytes()),
        "statement_path": statement_path,
    }))?;
    Ok(())
}

fn load_or_create_plan(path: &Path, state: &Path) -> Result<Plan, crate::BoxError> {
    if path.exists() {
        let metadata = fs::metadata(path)?;
        if metadata.len() as usize > MAX_PLAN_BYTES {
            return Err("retained usage-ledger plan exceeds 64 KiB".into());
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(path)?
            .take((MAX_PLAN_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        let plan: Plan = serde_json::from_slice(&bytes)?;
        return Ok(plan);
    }
    let endpoint = match std::env::var("CELLULE_USAGE_LEDGER_ADAPTER_ENDPOINT") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19031/".into(),
        Err(source) => return Err(source.into()),
    };
    let state_label = state.to_string_lossy();
    let path_hash = blake3::hash(state_label.as_bytes()).to_hex().to_string();
    let tenant = format!("demo-{}", &path_hash[..12]);
    let start = now_ms()?
        .checked_sub(60_000)
        .ok_or("usage-ledger demo time overflow")?;
    let end = start
        .checked_add(24 * 60 * 60 * 1000)
        .ok_or("usage-ledger demo time overflow")?;
    let accounts = ACCOUNTS
        .iter()
        .map(|value| AccountKey::new(*value))
        .collect::<Result<Vec<_>, _>>()?;
    let period_id = *uuid::Uuid::now_v7().as_bytes();
    let mut events = Vec::with_capacity(4);
    for (index, (account_index, category, amount)) in [
        (0, "compute", 1500),
        (0, "storage", 500),
        (1, "compute", 2500),
        (1, "network", 750),
    ]
    .into_iter()
    .enumerate()
    {
        events.push(UsageEvent {
            period_id,
            id: *uuid::Uuid::now_v7().as_bytes(),
            account: accounts[account_index].clone(),
            occurred_at_ms: start + (index as i64 + 1) * 1000,
            amount_microcredits: amount,
            category: category.into(),
        });
    }
    let plan = Plan {
        version: 1,
        tenant,
        endpoint,
        spec: PeriodSpec {
            id: period_id,
            start_ms: start,
            end_ms: end,
            accounts,
        },
        events,
    };
    plan.validate()?;
    let bytes = serde_json::to_vec(&plan)?;
    if bytes.len() > MAX_PLAN_BYTES {
        return Err("usage-ledger demo plan exceeds 64 KiB".into());
    }
    let temporary = path.with_extension("json.tmp");
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(state)?.sync_all()?;
    Ok(plan)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), crate::BoxError> {
    let temporary = path.with_extension("csv.tmp");
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    let parent = path
        .parent()
        .ok_or("statement output has no parent directory")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}
