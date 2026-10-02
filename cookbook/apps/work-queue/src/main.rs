//! Persistent queue CLI; ingress and local authorization belong to this binary.
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, now_ms, shutdown_signal,
};
use cellule_cookbook_work_queue::{
    DEAD, DeadLetters, Inspect, InspectionCursor, InspectionPageRequest, JOBS, Job, Jobs, Producer,
    RECEIVER, Receiver, SetEnabled, WorkQueue, WorkerOptions, WorkerProgress, spawn_workers,
    target,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, PreparedCommand, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
    primitives::queue::{QueueControlAction, QueueControlCommand, QueueSendOutcome},
    registry::Command,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const TENANT: TenantId = TenantId::from_bytes([0x74; 16]);
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x75; 16]);
const HELP: &str = "Cellule work queue\n\n  prepare OPERATION_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE\n  resolve STATE_DIRECTORY MUTATION_FILE\n  inspect STATE_DIRECTORY [CURSOR_JSON|-] [LIMIT]\n  info STATE_DIRECTORY\n  worker STATE_DIRECTORY [SECONDS] [BEFORE_ACK_MS]\n  demo STATE_DIRECTORY\n\nOperations: submit {job:{id,body},available_at_ms}, pause/resume {shard},\nredrive {shard,limit}, enabled {value}. Tag each JSON object with operation.\nWorker defaults to serving until interruption. Retain mutation files unchanged.";
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Submit { job: Job, available_at_ms: i64 },
    Pause { shard: u32 },
    Resume { shard: u32 },
    Redrive { shard: u32, limit: u32 },
    Enabled { value: bool },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    operation: Operation,
}
impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        let id = uuid::Uuid::parse_str(&self.request_id)?;
        if self.version != 1 || id.is_nil() {
            return Err("unsupported or invalid retained mutation".into());
        }
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
        return Err("mutation JSON exceeds 4 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn prepare(input: &Path, output: &Path) -> Result<()> {
    let operation: Operation = read_json(input)?;
    match &operation {
        Operation::Submit { job, .. } => {
            job.validate()?;
        }
        Operation::Pause { shard }
        | Operation::Resume { shard }
        | Operation::Redrive { shard, .. } => {
            target(TENANT, APPLICATION, JOBS, *shard)?;
        }
        Operation::Enabled { .. } => {}
    }
    let identity = new_identity()?;
    let record = MutationFile {
        version: 1,
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        operation,
    };
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
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
        serde_json::json!({"prepared":output,"request_id":record.request_id})
    );
    Ok(())
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(LocalNode::start(
        cellule_cookbook_work_queue::compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/work-queue"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn open(node: &LocalNode) -> Result<ApplicationHandle<WorkQueue>> {
    let handle = node.application_handle::<WorkQueue>(TENANT)?;
    for shard in 0..2 {
        node.open_cell(&target(TENANT, APPLICATION, JOBS, shard)?, &Jobs)
            .await?;
    }
    node.open_cell(&target(TENANT, APPLICATION, DEAD, 0)?, &DeadLetters)
        .await?;
    node.open_cell(&target(TENANT, APPLICATION, RECEIVER, 0)?, &Receiver)
        .await?;
    Ok(handle)
}
async fn finish<C: Command>(
    handle: &ApplicationHandle<WorkQueue>,
    prepared: PreparedCommand<C>,
    resolve: bool,
) -> Result<()>
where
    C::Output: std::fmt::Debug + Send + Sync,
{
    if resolve {
        match handle.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 1 << 20)?;
                let outcome = C::Output::decode(&mut decoder)?;
                decoder.finish()?;
                println!(
                    "{}",
                    serde_json::json!({"resolution":"committed","outcome":format!("{outcome:?}"),"commit_sequence":stored.commit_sequence()})
                );
            }
            Resolution::Absent => println!("{}", serde_json::json!({"resolution":"absent"})),
            Resolution::Unknown => return Err("outcome unknown; preserve original mutation".into()),
            Resolution::Expired => {
                return Err("outcome expired; expiry does not prove absence".into());
            }
        }
        return Ok(());
    }
    match prepared.execute().await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::json!({"status":"committed","outcome":format!("{:?}",result.output),"commit_sequence":result.receipt.commit_sequence})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(result)) => {
            println!(
                "{}",
                serde_json::json!({"status":"rejected","outcome":format!("{:?}",result.output),"commit_sequence":result.receipt.commit_sequence})
            );
            Err("operation durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
async fn apply(
    handle: &ApplicationHandle<WorkQueue>,
    record: MutationFile,
    resolve: bool,
) -> Result<()> {
    let identity = record.identity()?;
    match record.operation {
        Operation::Submit {
            job,
            available_at_ms,
        } => {
            finish(
                handle,
                Producer::new(handle.clone())
                    .prepare(identity, &job, available_at_ms)
                    .await?,
                resolve,
            )
            .await
        }
        Operation::Enabled { value } => {
            finish(
                handle,
                handle
                    .prepare_command::<SetEnabled>(
                        &target(TENANT, APPLICATION, RECEIVER, 0)?,
                        identity,
                        value,
                    )
                    .await?,
                resolve,
            )
            .await
        }
        operation => {
            let (shard, action) = match operation {
                Operation::Pause { shard } => (shard, QueueControlAction::Pause),
                Operation::Resume { shard } => (shard, QueueControlAction::Resume),
                Operation::Redrive { shard, limit } => {
                    (shard, QueueControlAction::Redrive { limit })
                }
                _ => return Err("invalid control operation".into()),
            };
            finish(
                handle,
                handle
                    .prepare_command::<QueueControlCommand<Jobs>>(
                        &target(TENANT, APPLICATION, JOBS, shard)?,
                        identity,
                        action,
                    )
                    .await?,
                resolve,
            )
            .await
        }
    }
}
fn progress(event: WorkerProgress) {
    let WorkerProgress::ReceiverPublished {
        job,
        attempt,
        outcome,
        dead,
    } = event;
    println!(
        "{}",
        serde_json::json!({"event":"receiver_published","job":job,"attempt":attempt,"outcome":format!("{outcome:?}"),"dead":dead})
    );
    // The crash scenario kills this process after reading this exact checkpoint.
    if let Err(error) = std::io::stdout().flush() {
        tracing::error!(%error,"progress flush failed");
    }
}
async fn worker(
    node: &LocalNode,
    handle: ApplicationHandle<WorkQueue>,
    seconds: Option<u64>,
    delay: u64,
) -> Result<()> {
    let (sender, mut events) = tokio::sync::mpsc::channel(64);
    spawn_workers(
        node,
        handle,
        TENANT,
        APPLICATION,
        WorkerOptions {
            before_ack: Duration::from_millis(delay),
            progress: Some(sender),
        },
    )
    .await?;
    println!("{}", serde_json::json!({"event":"ready"}));
    std::io::stdout().flush()?;
    let deadline = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::select! {
            event=events.recv()=>{match event {Some(event)=>progress(event),None=>return Err("workers stopped unexpectedly".into())}},
            ()=tokio::time::sleep(Duration::from_millis(100))=>{
                if !node.is_ready() {return Err("worker failure closed node readiness".into());}
                if deadline.is_some_and(|deadline|tokio::time::Instant::now()>=deadline) {return Ok(());}
            }
        }
    }
}
async fn demo(node: &LocalNode, handle: ApplicationHandle<WorkQueue>) -> Result<()> {
    let receiver = target(TENANT, APPLICATION, RECEIVER, 0)?;
    let baseline = handle
        .query::<Inspect>(&receiver, None, InspectionPageRequest::default())
        .await?
        .output;
    if !baseline.enabled {
        return Err("demo requires an enabled receiver; apply an enabled mutation first".into());
    }
    let mut baseline_acked = 0;
    for shard in 0..2 {
        baseline_acked += handle
            .queue::<Jobs>()?
            .info(shard, None)
            .await?
            .output
            .acked;
    }
    let job = Job {
        id: uuid::Uuid::now_v7().to_string(),
        body: "Deliver a durable report".into(),
    };
    let producer = Producer::new(handle.clone());
    let identity = new_identity()?;
    let available = now_ms()?;
    let sent = producer.send(identity, &job, available).await?;
    if !matches!(sent.output, QueueSendOutcome::Sent { .. }) {
        return Err("job was not sent".into());
    }
    let replay = producer.send(identity, &job, available).await?;
    if sent != replay {
        return Err("producer replay changed".into());
    }
    spawn_workers(
        node,
        handle.clone(),
        TENANT,
        APPLICATION,
        WorkerOptions::default(),
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let inspection = handle
            .query::<Inspect>(&receiver, None, InspectionPageRequest::default())
            .await?
            .output;
        let mut acked = 0;
        for shard in 0..2 {
            acked += handle
                .queue::<Jobs>()?
                .info(shard, None)
                .await?
                .output
                .acked;
        }
        if inspection.delivered > baseline.delivered && acked > baseline_acked {
            println!(
                "{}",
                serde_json::json!({"scenario":"passed","job":job,"inspection":inspection,"checks":["producer-replay","leased-delivery","receiver-idempotency","owned-worker-drain"]})
            );
            return Ok(());
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err("demo workers did not complete before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    let handle = open(node).await?;
    match args {
        [op, _] if op == "demo" => demo(node, handle).await,
        [op, _, path] if op == "apply" || op == "resolve" => {
            apply(&handle, read_json(Path::new(path))?, op == "resolve").await
        }
        [op, _, rest @ ..] if op == "inspect" && rest.len() <= 2 => {
            let after = match rest.first().map(String::as_str) {
                None | Some("-") => None,
                Some(value) if value.len() <= 128 => {
                    Some(serde_json::from_str::<InspectionCursor>(value)?)
                }
                Some(_) => return Err("inspection cursor exceeds 128 bytes".into()),
            };
            let limit = rest
                .get(1)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(100);
            let page = InspectionPageRequest { after, limit };
            println!(
                "{}",
                serde_json::to_string(
                    &handle
                        .query::<Inspect>(&target(TENANT, APPLICATION, RECEIVER, 0)?, None, page)
                        .await?
                        .output
                )?
            );
            Ok(())
        }
        [op, _] if op == "info" => {
            let mut rows = Vec::new();
            for shard in 0..2 {
                let v = handle.queue::<Jobs>()?.info(shard, None).await?.output;
                rows.push(serde_json::json!({"shard":shard,"paused":v.paused,"ready":v.ready,"leased":v.leased,"acked":v.acked,"dead":v.dead}));
            }
            println!("{}", serde_json::json!({"shards":rows}));
            Ok(())
        }
        [op, _, rest @ ..] if op == "worker" && rest.len() <= 2 => {
            let seconds = rest.first().map(|s| s.parse::<u64>()).transpose()?;
            if seconds.is_some_and(|s| s == 0 || s > 3600) {
                return Err("worker seconds must be 1..3600".into());
            }
            let delay = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(0);
            worker(node, handle, seconds, delay).await
        }
        _ => Err(HELP.into()),
    }
}
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|e| format!("logging initialization: {e}"))?;
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
    let state = args.get(1).ok_or(HELP)?;
    let node = start(state.into()).await?;
    let result = tokio::select! {
        biased;
        signal=shutdown_signal()=>match signal {
            Ok(()) if args[0]=="worker"=>Ok(()),
            Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain mutation evidence").into()),
            Err(error)=>Err(error.into()),
        },
        result=operation(&node,&args)=>result,
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
            eprintln!("work-queue: {error}");
            let mut cause = error.source();
            while let Some(source) = cause {
                eprintln!("  caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
