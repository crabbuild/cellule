//! Persistent CLI, local synthetic HTTP target, and owned monitor worker lifecycle.
mod target;
use cellule_cookbook_endpoint_monitor::{
    Change, Definition, DeliveryOptions, EdgeKind, Health, Id, MonitorApplication, MonitorClient,
    PageRequest, ScheduleOutcome, Ticket, open, spawn_workers,
};
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, now_ms, shutdown_signal,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
type Result<T> = std::result::Result<T, cellule_cookbook_endpoint_monitor::BoxError>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x65; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x66; 16]);
const HELP: &str = "Cellule endpoint monitor\n\n  demo STATE\n  prepare CHANGE_JSON MUTATION_FILE\n  apply STATE MUTATION_FILE\n  resolve STATE MUTATION_FILE\n  get STATE MONITOR_UUID\n  inspect STATE MONITOR_UUID DEFINITION_UUID [AFTER_ROW] [LIMIT]\n  alerts STATE MONITOR_UUID DEFINITION_UUID [AFTER_ROW] [LIMIT]\n  workflow STATE TICKET_JSON\n  serve STATE [SECONDS] [AFTER_PUBLICATION_MS] [lose-reply]\n  target-state STATE_FILE up|down\n  target PORT STATE_FILE [SECONDS]\n\nUpsert/resume preparation freezes start_in_ms into an absolute next_due_ms.\nA new definition UUID is required for each replacement or recreated lifetime.\nRetain prepared files unchanged. Unknown/expired outcomes do not prove absence.";
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Ingress {
    Upsert {
        monitor: Id,
        definition: Definition,
        interval_ms: u64,
        start_in_ms: u64,
    },
    Pause {
        monitor: Id,
    },
    Resume {
        monitor: Id,
        start_in_ms: u64,
    },
    Delete {
        monitor: Id,
    },
}
impl Ingress {
    fn freeze(self, issued: i64) -> Result<Change> {
        let due = |delay: u64| -> Result<i64> {
            issued
                .checked_add(i64::try_from(delay)?)
                .ok_or_else(|| "due timestamp overflow".into())
        };
        Ok(match self {
            Self::Upsert {
                monitor,
                definition,
                interval_ms,
                start_in_ms,
            } => Change::Upsert {
                monitor,
                definition,
                interval_ms,
                next_due_ms: due(start_in_ms)?,
            },
            Self::Pause { monitor } => Change::Pause { monitor },
            Self::Resume {
                monitor,
                start_in_ms,
            } => Change::Resume {
                monitor,
                next_due_ms: due(start_in_ms)?,
            },
            Self::Delete { monitor } => Change::Delete { monitor },
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
        if self.version != 1
            || id.is_nil()
            || id.to_string() != self.request_id
            || self
                .expires_at_ms
                .checked_sub(self.issued_at_ms)
                .is_none_or(|window| !(1..=300000).contains(&window))
        {
            return Err("invalid retained monitor identity".into());
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
        return Err("monitor JSON exceeds 4 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn write(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(parent(path))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    let retained = if replace {
        temporary.persist(path)?
    } else {
        temporary.persist_noclobber(path)?
    };
    retained.sync_all()?;
    std::fs::File::open(parent(path))?.sync_all()?;
    Ok(())
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
    let mut bytes = serde_json::to_vec_pretty(&record)?;
    bytes.push(b'\n');
    write(output, &bytes, false)?;
    println!(
        "{}",
        serde_json::json!({"prepared":output,"request_id":record.request_id,"monitor":record.change.monitor()})
    );
    Ok(())
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
        Err(source) => return Err(source.into()),
    };
    Ok(LocalNode::start(
        cellule_cookbook_endpoint_monitor::compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/endpoint-monitor/cells"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn apply(client: &MonitorClient, record: MutationFile, resolve: bool) -> Result<()> {
    let prepared = client.prepare(record.identity()?, record.change).await?;
    if resolve {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 128)?;
                let outcome = ScheduleOutcome::decode(&mut decoder)?;
                decoder.finish()?;
                println!(
                    "{}",
                    serde_json::json!({"resolution":"committed","outcome":outcome,"commit_sequence":stored.commit_sequence()})
                );
            }
            Resolution::Absent => println!("{}", serde_json::json!({"resolution":"absent"})),
            Resolution::Unknown => {
                println!(
                    "{}",
                    serde_json::json!({"resolution":"unknown","absence_proven":false})
                );
                return Err("retain original monitor request evidence".into());
            }
            Resolution::Expired => println!(
                "{}",
                serde_json::json!({"resolution":"expired","absence_proven":false})
            ),
        }
        return Ok(());
    }
    match prepared.execute().await {
        Ok(value) => {
            println!(
                "{}",
                serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(value)) => {
            println!(
                "{}",
                serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)})
            );
            Err("monitor schedule durably rejected".into())
        }
        Err(source) => Err(source.into()),
    }
}
async fn serve(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<MonitorApplication>,
    seconds: u64,
    delay: u64,
    lose_reply: bool,
) -> Result<()> {
    let (sender, mut events) = tokio::sync::mpsc::channel(64);
    spawn_workers(
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        tokio::select! {
            event = events.recv() => {
                let value = event.ok_or("monitor observers stopped unexpectedly")?;
                println!("{}", serde_json::json!({"event":"check_recorded","progress":value}));
                std::io::stdout().flush()?;
            },
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                if !node.is_ready() {
                    return Err("monitor worker failure closed readiness".into());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Ok(());
                }
            },
        }
    }
}
async fn wait(
    client: &MonitorClient,
    node: &LocalNode,
    monitor: Id,
    definition: Id,
    health: Health,
    edges: usize,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let page = PageRequest {
            monitor,
            definition,
            after: 0,
            limit: 100,
        };
        let state = client.inspect(page.clone(), None).await?.output.state;
        let alerts = client.alerts(page, None).await?.output;
        if state
            .as_ref()
            .is_some_and(|state| state.health == Some(health))
            && alerts.edges.len() == edges
        {
            return Ok(());
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err(format!("monitor did not settle: expected {health:?} and {edges} edges; observed state {state:?}, {} edges", alerts.edges.len()).into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<MonitorApplication>,
    client: &MonitorClient,
    monitor: Id,
) -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let state_file = temporary.path().join("target.txt");
    write(&state_file, b"up\n", false)?;
    let listener = target::bind(0).await?;
    let address = target::address(&listener)?;
    let target_file = state_file.clone();
    node.spawn_worker(move |cancel| async move {
        let _temporary = temporary;
        target::serve(listener, target_file, cancel).await
    })?;
    let version = Id::from_bytes(*uuid::Uuid::now_v7().as_bytes())?;
    let identity = new_identity()?;
    let change = Change::Upsert {
        monitor,
        definition: Definition {
            id: version,
            label: "Local API".into(),
            endpoint: format!("http://{address}/probe"),
        },
        interval_ms: 5000,
        next_due_ms: identity.issued_at_ms + 200,
    };
    let original = client
        .prepare(identity, change.clone())
        .await?
        .execute()
        .await?;
    if client.prepare(identity, change).await?.execute().await? != original {
        return Err("retained schedule replay changed".into());
    }
    spawn_workers(
        node,
        handle,
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await?;
    wait(client, node, monitor, version, Health::Up, 0).await?;
    write(&state_file, b"down\n", true)?;
    wait(client, node, monitor, version, Health::Down, 1).await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    write(&state_file, b"up\n", true)?;
    wait(client, node, monitor, version, Health::Up, 2).await?;
    client
        .prepare(new_identity()?, Change::Pause { monitor })
        .await?
        .execute()
        .await?;
    let page = PageRequest {
        monitor,
        definition: version,
        after: 0,
        limit: 100,
    };
    let published = client
        .schedule(monitor, None)
        .await?
        .output
        .ok_or("paused monitor disappeared")?
        .occurrence;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let inspection = loop {
        let inspection = client.inspect(page.clone(), None).await?.output;
        if inspection.next.is_none() && inspection.checks.len() as u64 == published {
            break inspection;
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err("paused source and completed checks did not reconcile".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let alerts = client.alerts(page, None).await?.output;
    if alerts.edges.len() != 2
        || alerts.edges[0].kind != EdgeKind::Opened
        || alerts.edges[1].kind != EdgeKind::Closed
        || alerts.edges[0].incident != alerts.edges[1].incident
    {
        return Err("incident notifications differ from the one outage".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","monitor":monitor,"definition":version,"inspection":inspection,"alerts":alerts,"checks":["retained-schedule-replay","native-Cron","HTTP-Activity","atomic-incident-edges","notification-intent","signed-inbox-delivery","actual-lost-reply-resolution","one-outage-one-incident","pause"]})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String], demo_monitor: Option<Id>) -> Result<()> {
    let handle = node.application_handle::<MonitorApplication>(TENANT)?;
    let client = open(node, &handle).await?;
    match args {
        [op, _] if op == "demo" => {
            demo(
                node,
                handle,
                &client,
                demo_monitor.ok_or("missing demo scope")?,
            )
            .await
        }
        [op, _, file] if op == "apply" || op == "resolve" => {
            apply(&client, read_json(Path::new(file))?, op == "resolve").await
        }
        [op, _, monitor] if op == "get" => {
            let observed = client
                .schedule(Id::try_from(monitor.clone())?, None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"schedule":observed.output,"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, monitor, definition, rest @ ..]
            if (op == "inspect" || op == "alerts") && rest.len() <= 2 =>
        {
            let page = PageRequest {
                monitor: Id::try_from(monitor.clone())?,
                definition: Id::try_from(definition.clone())?,
                after: rest
                    .first()
                    .map(|value| value.parse())
                    .transpose()?
                    .unwrap_or(0),
                limit: rest
                    .get(1)
                    .map(|value| value.parse())
                    .transpose()?
                    .unwrap_or(20),
            };
            if op == "inspect" {
                let observed = client.inspect(page, None).await?;
                println!(
                    "{}",
                    serde_json::json!({"inspection":observed.output,"receipt":receipt(observed.receipt)})
                );
            } else {
                let observed = client.alerts(page, None).await?;
                println!(
                    "{}",
                    serde_json::json!({"alerts":observed.output,"receipt":receipt(observed.receipt)})
                );
            }
            Ok(())
        }
        [op, _, file] if op == "workflow" => {
            let ticket: Ticket = read_json(Path::new(file))?;
            let observed = client.workflow(&ticket, None).await?;
            println!(
                "{}",
                serde_json::json!({"workflow":observed.output,"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "serve" && rest.len() <= 3 => {
            let seconds = rest
                .first()
                .map(|value| value.parse::<u64>())
                .transpose()?
                .unwrap_or(3600);
            if !(1..=3600).contains(&seconds) {
                return Err("serving seconds must be 1..3600".into());
            }
            let delay = rest
                .get(1)
                .map(|value| value.parse::<u64>())
                .transpose()?
                .unwrap_or(0);
            let lose_reply = match rest.get(2).map(String::as_str) {
                None => false,
                Some("lose-reply") => true,
                _ => return Err("fault must be lose-reply".into()),
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
        .try_init()?;
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
    if let [op, file, status] = args.as_slice()
        && op == "target-state"
    {
        return match status.as_str() {
            "up" => write(Path::new(file), b"up\n", true),
            "down" => write(Path::new(file), b"down\n", true),
            _ => Err("target state must be up or down".into()),
        };
    }
    if let [op, port, file, rest @ ..] = args.as_slice()
        && op == "target"
        && rest.len() <= 1
    {
        let seconds = rest
            .first()
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(3600);
        if !(1..=3600).contains(&seconds) {
            return Err("target seconds must be 1..3600".into());
        }
        let listener = target::bind(port.parse()?).await?;
        let address = target::address(&listener)?;
        let cancel = CancellationToken::new();
        let serving = target::serve(listener, file.into(), cancel.clone());
        tokio::pin!(serving);
        println!(
            "{}",
            serde_json::json!({"event":"target_ready","address":address})
        );
        std::io::stdout().flush()?;
        let result: Result<()> = tokio::select! {result=&mut serving=>return result.map_err(Into::into),signal=shutdown_signal()=>signal.map_err(Into::into),()=tokio::time::sleep(Duration::from_secs(seconds))=>Ok(())};
        cancel.cancel();
        let joined = serving.await;
        result?;
        joined?;
        return Ok(());
    }
    if let [op, _, file] = args.as_slice()
        && op == "resolve"
    {
        let record: MutationFile = read_json(Path::new(file))?;
        let identity = record.identity()?;
        if now_ms()? > identity.expires_at_ms {
            println!(
                "{}",
                serde_json::json!({"resolution":"expired","absence_proven":false})
            );
            return Ok(());
        }
    }
    let demo_monitor = if args.first().is_some_and(|op| op == "demo") {
        Some(Id::from_bytes(*uuid::Uuid::now_v7().as_bytes())?)
    } else {
        None
    };
    let node = start(args.get(1).ok_or(HELP)?.into()).await?;
    let mut result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal{Ok(())if args[0]=="serve"=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain original request").into()),Err(source)=>Err(source.into())},
        result=operation(&node,&args,demo_monitor)=>result,
    };
    if let Some(monitor) = demo_monitor {
        // A cancelled caller cannot withdraw accepted SQL publication. Pause through
        // the same writer before node drain; FIFO command dispatch follows any
        // already accepted upsert, and already published probes remain recoverable.
        let cleanup: Result<()> = async {
            let handle = node.application_handle::<MonitorApplication>(TENANT)?;
            let client = MonitorClient::new(handle);
            match client
                .prepare(new_identity()?, Change::Pause { monitor })
                .await?
                .execute()
                .await
            {
                Ok(_) => Ok(()),
                Err(InvocationError::Rejected(value))
                    if value.output == ScheduleOutcome::NotFound =>
                {
                    Ok(())
                }
                Err(source) => Err(source.into()),
            }
        }
        .await;
        if let Err(source) = cleanup {
            if result.is_ok() {
                result = Err(source);
            } else {
                tracing::error!(error=%source,"demo pause failed; retained scheduling outcome requires inspection");
            }
        }
    }
    let drained = node.shutdown().await;
    result?;
    drained?;
    Ok(())
}
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            eprintln!("endpoint-monitor: {source}");
            let mut cause = source.source();
            while let Some(source) = cause {
                eprintln!("  caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
