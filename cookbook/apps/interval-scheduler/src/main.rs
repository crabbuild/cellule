//! Application ingress for persistent, retained fixed-interval reminder operations.
use cellule_cookbook_interval_scheduler::{
    Change, DeliveryOptions, DeliveryPageRequest, DeliveryProgress, IntervalScheduler, Reminder,
    ScheduleId, SchedulerClient, open, spawn_delivery,
};
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, shutdown_signal,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
    primitives::cron::CronMutationOutcome,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x83; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x84; 16]);
const HELP: &str = "Cellule interval scheduler\n\n  demo STATE_DIRECTORY\n  prepare CHANGE_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE\n  resolve STATE_DIRECTORY MUTATION_FILE\n  get STATE_DIRECTORY SCHEDULE_UUID\n  list STATE_DIRECTORY SHARD [AFTER_UUID] [LIMIT]\n  deliveries STATE_DIRECTORY SCHEDULE_UUID [AFTER_ROW] [LIMIT]\n  serve STATE_DIRECTORY [SECONDS] [AFTER_PUBLICATION_MS] [lose-reply]\n\nPreparation accepts upsert/resume with start_in_ms and freezes the absolute due time.\nUse '-' for an absent cursor. Serving progresses native ticks and signed delivery.\nRetain mutation files unchanged; an expired outcome does not prove absence.";
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Ingress {
    Upsert {
        id: ScheduleId,
        reminder: Reminder,
        interval_ms: u64,
        start_in_ms: u64,
    },
    Pause {
        id: ScheduleId,
    },
    Resume {
        id: ScheduleId,
        start_in_ms: u64,
    },
    Delete {
        id: ScheduleId,
    },
}
impl Ingress {
    fn freeze(self, issued_at_ms: i64) -> Result<Change> {
        let due = |delay: u64| -> Result<i64> {
            issued_at_ms
                .checked_add(i64::try_from(delay)?)
                .ok_or_else(|| "due timestamp overflow".into())
        };
        Ok(match self {
            Self::Upsert {
                id,
                reminder,
                interval_ms,
                start_in_ms,
            } => Change::Upsert {
                id,
                reminder,
                interval_ms,
                next_due_ms: due(start_in_ms)?,
            },
            Self::Pause { id } => Change::Pause { id },
            Self::Resume { id, start_in_ms } => Change::Resume {
                id,
                next_due_ms: due(start_in_ms)?,
            },
            Self::Delete { id } => Change::Delete { id },
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    change: Change,
}
impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        let id = uuid::Uuid::parse_str(&self.request_id)?;
        if self.version != 1 || id.is_nil() || id.to_string() != self.request_id {
            return Err("unsupported or noncanonical retained mutation".into());
        }
        self.change.validate(self.issued_at_ms)?;
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(*id.as_bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err("reminder JSON exceeds 4 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn prepare(input: &Path, output: &Path) -> Result<()> {
    let ingress: Ingress = read_json(input)?;
    let identity = new_identity()?;
    let change = ingress.freeze(identity.issued_at_ms)?;
    change.validate(identity.issued_at_ms)?;
    let record = MutationFile {
        version: 1,
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        change,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&record)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::File::open(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    println!(
        "{}",
        serde_json::json!({"prepared":output,"request_id":record.request_id,"schedule":record.change.id()})
    );
    Ok(())
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(LocalNode::start(
        cellule_cookbook_interval_scheduler::compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/interval-scheduler"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
fn outcome(value: CronMutationOutcome) -> serde_json::Value {
    match value {
        CronMutationOutcome::Applied { generation } => {
            serde_json::json!({"status":"applied","generation":generation})
        }
        CronMutationOutcome::Deleted => serde_json::json!({"status":"deleted"}),
        CronMutationOutcome::NotFound => serde_json::json!({"status":"not_found"}),
    }
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
async fn apply(client: &SchedulerClient, record: MutationFile, resolve: bool) -> Result<()> {
    let identity = record.identity()?;
    let prepared = client.prepare(identity, record.change).await?;
    if resolve {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 16)?;
                let value = CronMutationOutcome::decode(&mut decoder)?;
                decoder.finish()?;
                println!(
                    "{}",
                    serde_json::json!({"resolution":"committed","outcome":outcome(value),"commit_sequence":stored.commit_sequence()})
                );
            }
            Resolution::Absent => println!("{}", serde_json::json!({"resolution":"absent"})),
            Resolution::Unknown => {
                return Err("outcome unknown; retain the original mutation file".into());
            }
            Resolution::Expired => {
                return Err("outcome expired; expiry does not prove it never ran".into());
            }
        }
        return Ok(());
    }
    match prepared.execute().await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::json!({"outcome":outcome(result.output),"receipt":receipt(result.receipt)})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(result)) => {
            println!(
                "{}",
                serde_json::json!({"outcome":outcome(result.output),"receipt":receipt(result.receipt)})
            );
            Err("reminder change durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
fn progress(value: DeliveryProgress) -> Result<()> {
    println!(
        "{}",
        serde_json::json!({"event":"destination_published","progress":value})
    );
    std::io::stdout().flush()?;
    Ok(())
}
async fn serve(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<IntervalScheduler>,
    seconds: Option<u64>,
    delay: u64,
    lose_reply: bool,
) -> Result<()> {
    let (sender, mut progress_events) = tokio::sync::mpsc::channel(64);
    spawn_delivery(
        node,
        handle,
        DeliveryOptions {
            after_publication: Duration::from_millis(delay),
            drop_reply_once: lose_reply,
            progress: Some(sender),
        },
    )
    .await?;
    println!("{}", serde_json::json!({"event":"ready"}));
    std::io::stdout().flush()?;
    let deadline = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::select! {
            event=progress_events.recv()=>{match event {Some(value)=>progress(value)?,None=>return Err("delivery workers stopped unexpectedly".into())}},
            ()=tokio::time::sleep(Duration::from_millis(100))=>{
                if !node.is_ready() {return Err("delivery failure closed readiness".into());}
                if deadline.is_some_and(|deadline|tokio::time::Instant::now()>=deadline) {return Ok(());}
            }
        }
    }
}
async fn wait_occurrences(
    node: &LocalNode,
    client: &SchedulerClient,
    id: ScheduleId,
    greater_than: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let schedule = client
            .get(id, None)
            .await?
            .output
            .ok_or("schedule disappeared")?;
        if schedule.occurrence > greater_than {
            return Ok(());
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err("scheduler did not publish the due occurrence".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn wait_deliveries(
    node: &LocalNode,
    client: &SchedulerClient,
    id: ScheduleId,
    count: u64,
) -> Result<cellule_cookbook_interval_scheduler::DeliveryPage> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let page = client
            .deliveries(
                DeliveryPageRequest {
                    schedule: id,
                    after: None,
                    limit: 100,
                },
                None,
            )
            .await?
            .output;
        if page.deliveries.len() as u64 == count {
            return Ok(page);
        }
        if page.deliveries.len() as u64 > count
            || !node.is_ready()
            || tokio::time::Instant::now() >= deadline
        {
            return Err("source/destination occurrence reconciliation failed".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<IntervalScheduler>,
) -> Result<()> {
    let client = SchedulerClient::new(handle.clone());
    let id = ScheduleId::from_bytes(*uuid::Uuid::now_v7().as_bytes())?;
    let identity = new_identity()?;
    let due = identity
        .issued_at_ms
        .checked_add(200)
        .ok_or("demo due overflow")?;
    let change = Change::Upsert {
        id,
        reminder: Reminder {
            title: "Daily check".into(),
            message: "Inspect the durable reminder inbox".into(),
        },
        interval_ms: 1000,
        next_due_ms: due,
    };
    let created = client.change(identity, change.clone()).await?;
    if client.change(identity, change).await? != created {
        return Err("upsert replay changed its receipt".into());
    }
    spawn_delivery(
        node,
        handle,
        DeliveryOptions {
            drop_reply_once: true,
            ..DeliveryOptions::default()
        },
    )
    .await?;
    wait_occurrences(node, &client, id, 1).await?;
    let paused = client.change(new_identity()?, Change::Pause { id }).await?;
    let state = client
        .get(id, Some(paused.receipt))
        .await?
        .output
        .ok_or("paused schedule missing")?;
    let delivered = wait_deliveries(node, &client, id, state.occurrence).await?;
    for item in &delivered.deliveries {
        let expected = due
            .checked_add(
                i64::try_from(item.occurrence.checked_sub(1).ok_or("invalid occurrence")?)?
                    .checked_mul(1000)
                    .ok_or("interval overflow")?,
            )
            .ok_or("scheduled time overflow")?;
        if item.scheduled_at_ms != expected {
            return Err("fixed-interval occurrence timestamp drifted".into());
        }
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    if client
        .get(id, None)
        .await?
        .output
        .ok_or("paused schedule missing")?
        .occurrence
        != state.occurrence
    {
        return Err("paused schedule continued firing".into());
    }
    let resume = new_identity()?;
    client
        .change(
            resume,
            Change::Resume {
                id,
                next_due_ms: resume.issued_at_ms + 200,
            },
        )
        .await?;
    wait_occurrences(node, &client, id, state.occurrence).await?;
    let paused = client.change(new_identity()?, Change::Pause { id }).await?;
    let state = client
        .get(id, Some(paused.receipt))
        .await?
        .output
        .ok_or("resumed schedule missing")?;
    let delivered = wait_deliveries(node, &client, id, state.occurrence).await?;
    let deleted = client
        .change(new_identity()?, Change::Delete { id })
        .await?;
    if client
        .get(id, Some(deleted.receipt))
        .await?
        .output
        .is_some()
    {
        return Err("deleted schedule remained visible".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","schedule":id,"published_occurrences":state.occurrence,"deliveries":delivered,"source_receipt":receipt(deleted.receipt),"checks":["retained-replay","owned-ticks","fixed-interval","signed-inbox-delivery","lost-reply-resolution","pause","resume","delete","source-destination-reconciliation"]})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    let handle = open(node, TENANT, APPLICATION).await?;
    let client = SchedulerClient::new(handle.clone());
    match args {
        [op, _] if op == "demo" => demo(node, handle).await,
        [op, _, path] if op == "apply" || op == "resolve" => {
            apply(&client, read_json(Path::new(path))?, op == "resolve").await
        }
        [op, _, id] if op == "get" => {
            let observed = client.get(ScheduleId::parse(id)?, None).await?;
            println!(
                "{}",
                serde_json::json!({"schedule":observed.output,"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, shard, rest @ ..] if op == "list" && rest.len() <= 2 => {
            let after = rest
                .first()
                .filter(|s| s.as_str() != "-")
                .map(|s| ScheduleId::parse(s))
                .transpose()?;
            let limit = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(20);
            let observed = client.list(shard.parse()?, after, limit, None).await?;
            println!(
                "{}",
                serde_json::json!({"page":observed.output,"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, id, rest @ ..] if op == "deliveries" && rest.len() <= 2 => {
            let after = rest
                .first()
                .filter(|s| s.as_str() != "-")
                .map(|s| s.parse())
                .transpose()?;
            let limit = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(20);
            let observed = client
                .deliveries(
                    DeliveryPageRequest {
                        schedule: ScheduleId::parse(id)?,
                        after,
                        limit,
                    },
                    None,
                )
                .await?;
            println!(
                "{}",
                serde_json::json!({"page":observed.output,"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "serve" && rest.len() <= 3 => {
            let seconds = rest.first().map(|s| s.parse::<u64>()).transpose()?;
            if seconds.is_some_and(|s| s == 0 || s > 3600) {
                return Err("serving seconds must be 1..3600".into());
            }
            let delay = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(0);
            let lose_reply = match rest.get(2).map(String::as_str) {
                None => false,
                Some("lose-reply") => true,
                _ => return Err("fault flag must be lose-reply".into()),
            };
            serve(node, handle, seconds, delay, lose_reply).await
        }
        _ => Err(HELP.into()),
    }
}
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| format!("logging initialization: {error}"))?;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || matches!(args.first().map(String::as_str), Some("help" | "--help")) {
        println!("{HELP}");
        return Ok(());
    }
    if let [op, input, output] = args.as_slice()
        && op == "prepare"
    {
        return prepare(Path::new(input), Path::new(output));
    }
    let node = start(args.get(1).ok_or(HELP)?.into()).await?;
    let result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal {Ok(()) if args[0]=="serve"=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain mutation evidence").into()),Err(error)=>Err(error.into())},
        result=operation(&node,&args)=>result
    };
    let drained = node.shutdown().await;
    result?;
    drained?;
    Ok(())
}
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("interval-scheduler: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
