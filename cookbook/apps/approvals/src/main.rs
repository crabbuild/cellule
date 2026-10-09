//! Local authenticated-persona CLI, providers, and lifecycle; reusable domain code is in the library.
use cellule_cookbook_approvals::{
    ApprovalApplication, ApprovalClient, ApprovalView, Choice, Control, Employee, HistoryRequest,
    Phase, Purchase, PurchaseId, Reminder, compile, open, read_mail, spawn_reminders,
};
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, now_ms, shutdown_signal,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, PreparedCommand, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
    primitives::workflow::WorkflowOutcome,
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
const TENANT: TenantId = TenantId::from_bytes([0x93; 16]);
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x94; 16]);
const HELP: &str = "Cellule approvals\n\n  prepare PRINCIPAL MAILBOX_DIRECTORY OPERATION_JSON MUTATION_FILE\n  apply STATE_DIRECTORY PRINCIPAL MUTATION_FILE\n  resolve STATE_DIRECTORY PRINCIPAL MUTATION_FILE\n  get STATE_DIRECTORY PRINCIPAL PURCHASE_UUID\n  history STATE_DIRECTORY PRINCIPAL PURCHASE_UUID RUN_UUID [AFTER|-] [LIMIT]\n  serve STATE_DIRECTORY [SECONDS]\n  demo STATE_DIRECTORY\n\nLocal personas: alice, bob, carol, dana; the OS user controls this simulation.\nOperations: submit/restart {id,title,units,approvers,timeout_ms,remind_in_ms,publication_delay_ms?},\nvote {id,run_id,signal_id,choice}, pause/resume/cancel {id,run_id}. Restart also needs run_id.\nRetain mutation files unchanged; deadlines and mailbox configuration are frozen by prepare.";
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Ingress {
    Submit {
        id: PurchaseId,
        title: String,
        units: u64,
        approvers: Vec<Employee>,
        timeout_ms: u64,
        remind_in_ms: u64,
        #[serde(default)]
        publication_delay_ms: u64,
    },
    Vote {
        id: PurchaseId,
        run_id: String,
        signal_id: String,
        choice: Choice,
    },
    Pause {
        id: PurchaseId,
        run_id: String,
    },
    Resume {
        id: PurchaseId,
        run_id: String,
    },
    Cancel {
        id: PurchaseId,
        run_id: String,
    },
    Restart {
        id: PurchaseId,
        run_id: String,
        title: String,
        units: u64,
        approvers: Vec<Employee>,
        timeout_ms: u64,
        remind_in_ms: u64,
        #[serde(default)]
        publication_delay_ms: u64,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Submit {
        purchase: Purchase,
    },
    Vote {
        id: PurchaseId,
        run_id: String,
        signal_id: String,
        choice: Choice,
    },
    Pause {
        id: PurchaseId,
        run_id: String,
    },
    Resume {
        id: PurchaseId,
        run_id: String,
    },
    Cancel {
        id: PurchaseId,
        run_id: String,
    },
    Restart {
        purchase: Purchase,
        run_id: String,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    principal: Employee,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    operation: Operation,
}
fn uuid_bytes(value: &str) -> Result<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value)?;
    if id.is_nil() || id.to_string() != value {
        return Err("UUID must be lowercase, hyphenated, and nonzero".into());
    }
    Ok(*id.as_bytes())
}
fn principal(value: &str) -> Result<Employee> {
    if !matches!(value, "alice" | "bob" | "carol" | "dana") {
        return Err("unknown local authenticated persona".into());
    }
    Ok(Employee::parse(value)?)
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(8193)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 8192 {
        return Err("mutation JSON exceeds 8 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        if self.version != 1 {
            return Err("unsupported retained mutation version".into());
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(uuid_bytes(&self.request_id)?),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
fn prepare(actor: Employee, mailbox: &Path, input: &Path, output: &Path) -> Result<()> {
    let input: Ingress = read_json(input)?;
    let identity = new_identity()?;
    let purchase = |id,
                    title,
                    units,
                    approvers,
                    timeout_ms: u64,
                    remind_in_ms: u64,
                    publication_delay_ms|
     -> Result<Purchase> {
        if !(1000..=7 * 24 * 60 * 60 * 1000).contains(&timeout_ms) || remind_in_ms >= timeout_ms {
            return Err(
                "timeout must be 1000 ms through seven days, with an earlier reminder".into(),
            );
        }
        std::fs::create_dir_all(mailbox)?;
        let root = std::fs::canonicalize(mailbox)?;
        let value = Purchase {
            id,
            requester: actor.clone(),
            approvers,
            title,
            units,
            deadline_ms: identity
                .issued_at_ms
                .checked_add(timeout_ms as i64)
                .ok_or("deadline overflow")?,
            remind_at_ms: identity
                .issued_at_ms
                .checked_add(remind_in_ms as i64)
                .ok_or("reminder overflow")?,
            mailbox: root.to_str().ok_or("mailbox path is not UTF-8")?.into(),
            publication_delay_ms,
        };
        value.validate()?;
        Ok(value)
    };
    let operation = match input {
        Ingress::Submit {
            id,
            title,
            units,
            approvers,
            timeout_ms,
            remind_in_ms,
            publication_delay_ms,
        } => Operation::Submit {
            purchase: purchase(
                id,
                title,
                units,
                approvers,
                timeout_ms,
                remind_in_ms,
                publication_delay_ms,
            )?,
        },
        Ingress::Restart {
            id,
            run_id,
            title,
            units,
            approvers,
            timeout_ms,
            remind_in_ms,
            publication_delay_ms,
        } => {
            uuid_bytes(&run_id)?;
            Operation::Restart {
                purchase: purchase(
                    id,
                    title,
                    units,
                    approvers,
                    timeout_ms,
                    remind_in_ms,
                    publication_delay_ms,
                )?,
                run_id,
            }
        }
        Ingress::Vote {
            id,
            run_id,
            signal_id,
            choice,
        } => {
            uuid_bytes(&run_id)?;
            uuid_bytes(&signal_id)?;
            Operation::Vote {
                id,
                run_id,
                signal_id,
                choice,
            }
        }
        Ingress::Pause { id, run_id } => {
            uuid_bytes(&run_id)?;
            Operation::Pause { id, run_id }
        }
        Ingress::Resume { id, run_id } => {
            uuid_bytes(&run_id)?;
            Operation::Resume { id, run_id }
        }
        Ingress::Cancel { id, run_id } => {
            uuid_bytes(&run_id)?;
            Operation::Cancel { id, run_id }
        }
    };
    let record = MutationFile {
        version: 1,
        principal: actor,
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
            .filter(|path| !path.as_os_str().is_empty())
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
        compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/approvals"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn resolution(value: Resolution) -> Result<()> {
    match value {
        Resolution::Committed(outcome) => {
            let mut decoder = BoundedDecoder::new(outcome.result(), 64)?;
            let value = WorkflowOutcome::decode(&mut decoder)?;
            decoder.finish()?;
            println!(
                "{}",
                serde_json::json!({"resolution":"committed","outcome":format!("{value:?}"),"commit_sequence":outcome.commit_sequence()})
            );
        }
        Resolution::Absent => println!("{}", serde_json::json!({"resolution":"absent"})),
        Resolution::Unknown => return Err("outcome unknown; retain mutation evidence".into()),
        Resolution::Expired => return Err("outcome expired; expiry does not prove absence".into()),
    }
    Ok(())
}
async fn finish<C: Command<Output = WorkflowOutcome>>(
    client: &ApprovalClient,
    prepared: PreparedCommand<C>,
    resolve: bool,
) -> Result<()> {
    if resolve {
        return resolution(client.resolve(prepared.evidence()).await?);
    }
    match prepared.execute().await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::json!({"status":"committed","outcome":format!("{:?}",result.output),"receipt":receipt(result.receipt)})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(result)) => {
            println!(
                "{}",
                serde_json::json!({"status":"rejected","outcome":format!("{:?}",result.output),"receipt":receipt(result.receipt)})
            );
            Err("operation durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
async fn apply(
    client: &ApprovalClient,
    actor: &Employee,
    record: MutationFile,
    resolve: bool,
) -> Result<()> {
    if &record.principal != actor {
        return Err("retained principal differs from authenticated persona".into());
    }
    let identity = record.identity()?;
    if resolve {
        let value = match record.operation {
            Operation::Submit { purchase } => {
                let prepared = client.prepare_submit(identity, purchase).await?;
                client.resolve(prepared.evidence()).await?
            }
            Operation::Vote {
                id,
                run_id,
                signal_id,
                choice,
            } => {
                client
                    .resolve_vote(
                        identity,
                        id,
                        uuid_bytes(&run_id)?,
                        uuid_bytes(&signal_id)?,
                        choice,
                    )
                    .await?
            }
            Operation::Pause { id, run_id } => {
                client
                    .resolve_control(identity, id, uuid_bytes(&run_id)?, Control::Pause)
                    .await?
            }
            Operation::Resume { id, run_id } => {
                client
                    .resolve_control(identity, id, uuid_bytes(&run_id)?, Control::Resume)
                    .await?
            }
            Operation::Cancel { id, run_id } => {
                client
                    .resolve_cancel(
                        identity,
                        id,
                        uuid_bytes(&run_id)?,
                        *identity.request_id.as_bytes(),
                    )
                    .await?
            }
            Operation::Restart { purchase, run_id } => {
                client
                    .resolve_control(
                        identity,
                        purchase.id,
                        uuid_bytes(&run_id)?,
                        Control::Restart { purchase },
                    )
                    .await?
            }
        };
        return resolution(value);
    }
    match record.operation {
        Operation::Submit { purchase } => {
            finish(
                client,
                client.prepare_submit(identity, purchase).await?,
                resolve,
            )
            .await
        }
        Operation::Vote {
            id,
            run_id,
            signal_id,
            choice,
        } => {
            finish(
                client,
                client
                    .prepare_vote(
                        identity,
                        id,
                        uuid_bytes(&run_id)?,
                        uuid_bytes(&signal_id)?,
                        choice,
                    )
                    .await?,
                resolve,
            )
            .await
        }
        Operation::Pause { id, run_id } => {
            finish(
                client,
                client
                    .prepare_control(identity, id, uuid_bytes(&run_id)?, Control::Pause)
                    .await?,
                resolve,
            )
            .await
        }
        Operation::Resume { id, run_id } => {
            finish(
                client,
                client
                    .prepare_control(identity, id, uuid_bytes(&run_id)?, Control::Resume)
                    .await?,
                resolve,
            )
            .await
        }
        Operation::Cancel { id, run_id } => {
            finish(
                client,
                client
                    .prepare_cancel(
                        identity,
                        id,
                        uuid_bytes(&run_id)?,
                        *identity.request_id.as_bytes(),
                    )
                    .await?,
                resolve,
            )
            .await
        }
        Operation::Restart { purchase, run_id } => {
            finish(
                client,
                client
                    .prepare_control(
                        identity,
                        purchase.id,
                        uuid_bytes(&run_id)?,
                        Control::Restart { purchase },
                    )
                    .await?,
                resolve,
            )
            .await
        }
    }
}
async fn serve(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<ApprovalApplication>,
    seconds: Option<u64>,
) -> Result<()> {
    spawn_reminders(node, handle)?;
    println!("{}", serde_json::json!({"event":"ready"}));
    std::io::stdout().flush()?;
    let deadline =
        seconds.map(|seconds| tokio::time::Instant::now() + Duration::from_secs(seconds));
    loop {
        if !node.is_ready() {
            return Err("reminder worker failure closed readiness".into());
        }
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn wait(
    client: &ApprovalClient,
    id: PurchaseId,
    node: &LocalNode,
    predicate: impl Fn(&ApprovalView) -> bool,
) -> Result<ApprovalView> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(view) = client.get(id, None).await?.output
            && predicate(&view)
        {
            return Ok(view);
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err("approval did not reach the expected state before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<ApprovalApplication>,
    state: &Path,
) -> Result<()> {
    let alice = ApprovalClient::new(handle.clone(), principal("alice")?);
    let bob = ApprovalClient::new(handle.clone(), principal("bob")?);
    let carol = ApprovalClient::new(handle.clone(), principal("carol")?);
    let mailbox = state
        .parent()
        .unwrap_or(Path::new("."))
        .join("approvals-demo-mailbox");
    std::fs::create_dir_all(&mailbox)?;
    let id = PurchaseId::parse(&uuid::Uuid::now_v7().to_string())?;
    let now = now_ms()?;
    let purchase = Purchase {
        id,
        requester: principal("alice")?,
        approvers: vec![principal("bob")?, principal("carol")?],
        title: "Team workstations".into(),
        units: 2400,
        deadline_ms: now + 60_000,
        remind_at_ms: now,
        mailbox: std::fs::canonicalize(&mailbox)?
            .to_str()
            .ok_or("mailbox path is not UTF-8")?
            .into(),
        publication_delay_ms: 0,
    };
    let identity = new_identity()?;
    let submitted = alice
        .prepare_submit(identity, purchase.clone())
        .await?
        .execute()
        .await?;
    if alice
        .prepare_submit(identity, purchase)
        .await?
        .execute()
        .await?
        != submitted
    {
        return Err("submission replay changed".into());
    }
    spawn_reminders(node, handle)?;
    let reminded = wait(&alice, id, node, |view| {
        matches!(view.state.reminder, Reminder::Delivered { .. })
    })
    .await?;
    let Reminder::Delivered { receipt } = &reminded.state.reminder else {
        return Err("reminder receipt missing".into());
    };
    let record = read_mail(&mailbox, receipt)?;
    if record.purchase_id != id || record.run_id != reminded.run_id {
        return Err("mailbox delivery scope changed".into());
    }
    let signal = *uuid::Uuid::now_v7().as_bytes();
    let voted = bob
        .prepare_vote(
            new_identity()?,
            id,
            reminded.run_id,
            signal,
            Choice::Approve,
        )
        .await?
        .execute()
        .await?;
    let duplicate = bob
        .prepare_vote(
            new_identity()?,
            id,
            reminded.run_id,
            signal,
            Choice::Approve,
        )
        .await?
        .execute()
        .await?;
    if !matches!(duplicate.output, WorkflowOutcome::Duplicate { .. }) {
        return Err("signal was not deduplicated".into());
    }
    let before = alice
        .get(id, Some(voted.receipt))
        .await?
        .output
        .ok_or("purchase disappeared")?;
    if before.state.votes.len() != 1 {
        return Err("duplicate vote changed business state".into());
    }
    let accepted = carol
        .prepare_vote(
            new_identity()?,
            id,
            reminded.run_id,
            *uuid::Uuid::now_v7().as_bytes(),
            Choice::Approve,
        )
        .await?
        .execute()
        .await?;
    let final_view = alice
        .get(id, Some(accepted.receipt))
        .await?
        .output
        .ok_or("purchase disappeared")?;
    if final_view.state.phase != Phase::Approved || final_view.status != "completed" {
        return Err("purchase was not approved".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","purchase":id,"view":final_view,"mail":record,"checks":["retained-submit-replay","owned-timers","owned-activity","durable-mailbox","signal-deduplication","two-human-decisions","receipt-bound-read","worker-drain"]})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    let handle = open(node, TENANT, APPLICATION).await?;
    match args {
        [op, state] if op == "demo" => demo(node, handle, Path::new(state)).await,
        [op, _, rest @ ..] if op == "serve" && rest.len() <= 1 => {
            let seconds = rest.first().map(|value| value.parse::<u64>()).transpose()?;
            if seconds.is_some_and(|value| value == 0 || value > 3600) {
                return Err("serve duration must be 1..3600 seconds".into());
            }
            serve(node, handle, seconds).await
        }
        [op, _, actor, path] if op == "apply" || op == "resolve" => {
            let actor = principal(actor)?;
            let client = ApprovalClient::new(handle, actor.clone());
            apply(
                &client,
                &actor,
                read_json(Path::new(path))?,
                op == "resolve",
            )
            .await
        }
        [op, _, actor, id] if op == "get" => {
            let client = ApprovalClient::new(handle, principal(actor)?);
            let result = client.get(PurchaseId::parse(id)?, None).await?;
            println!(
                "{}",
                serde_json::json!({"view":result.output,"receipt":receipt(result.receipt)})
            );
            Ok(())
        }
        [op, _, actor, id, run, rest @ ..] if op == "history" && rest.len() <= 2 => {
            let client = ApprovalClient::new(handle, principal(actor)?);
            let after = rest
                .first()
                .filter(|value| value.as_str() != "-")
                .map(|value| value.parse())
                .transpose()?;
            let limit = rest
                .get(1)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(10);
            let result = client
                .history(
                    HistoryRequest {
                        id: PurchaseId::parse(id)?,
                        run_id: uuid_bytes(run)?,
                        after,
                        limit,
                    },
                    None,
                )
                .await?;
            println!(
                "{}",
                serde_json::json!({"page":result.output,"receipt":receipt(result.receipt)})
            );
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()?;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || matches!(args.first().map(String::as_str), Some("help" | "--help")) {
        println!("{HELP}");
        return Ok(());
    }
    if let [op, actor, mailbox, input, output] = args.as_slice()
        && op == "prepare"
    {
        return prepare(
            principal(actor)?,
            Path::new(mailbox),
            Path::new(input),
            Path::new(output),
        );
    }
    let state = args.get(1).ok_or(HELP)?;
    let node = start(state.into()).await?;
    let result = tokio::select! {
        biased;
        signal = shutdown_signal() => match signal {
            Ok(()) if args[0] == "serve" => Ok(()),
            Ok(()) => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "operation interrupted; retain mutation evidence",
            ).into()),
            Err(error) => Err(error.into()),
        },
        result = operation(&node, &args) => result,
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
            eprintln!("approvals: {error}");
            let mut cause = error.source();
            while let Some(source) = cause {
                eprintln!("  caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
