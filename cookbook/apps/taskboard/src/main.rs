//! Persistent taskboard CLI. Mutation files are retained before dispatch.

use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::ExitCode,
};

use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, new_identity};
use cellule_cookbook_taskboard::{
    Change, PageRequest, ProjectKey, TaskOutcome, Taskboard, TaskboardClient, Tasks,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId, identity::RequestId,
};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x31; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x41; 16]);
const MAX_FILE_BYTES: u64 = 4096;
const HELP: &str = "Cellule taskboard\n\n\
  cellule-cookbook-taskboard demo STATE_DIRECTORY\n\
  cellule-cookbook-taskboard prepare PROJECT CHANGE_JSON MUTATION_FILE\n\
  cellule-cookbook-taskboard apply STATE_DIRECTORY PROJECT MUTATION_FILE\n\
  cellule-cookbook-taskboard resolve STATE_DIRECTORY PROJECT MUTATION_FILE\n\
  cellule-cookbook-taskboard list STATE_DIRECTORY PROJECT [AFTER_ID] [LIMIT]\n\n\
Start cookbook local storage with scripts/local-storage.sh.\n\
CELLULE_COOKBOOK_ENDPOINT defaults to http://127.0.0.1:19000.\n\
Prepare retains one logical mutation identity; keep that file for retries and resolution.\n\
Project slugs are lowercase ASCII. List limits are from 1 through 100.";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    project: String,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    change: Change,
}

impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        if self.version != 1 {
            return Err("unsupported mutation-file version".into());
        }
        let id = uuid::Uuid::parse_str(&self.request_id)?;
        if id.is_nil() {
            return Err("request identity cannot be zero".into());
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(*id.as_bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    use std::io::Read as _;
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err("input JSON exceeds 4096 bytes".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn prepare(project: &str, input: &Path, output: &Path) -> Result<()> {
    ProjectKey::new(project)?;
    let change = read_json(input)?;
    let identity = new_identity()?;
    let mutation = MutationFile {
        version: 1,
        project: project.into(),
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        change,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&mutation)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"prepared": output, "request_id": mutation.request_id})
    );
    Ok(())
}

fn receipt_json(receipt: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell": format!("{:?}", receipt.cell),
        "incarnation": format!("{:?}", receipt.incarnation), "commit_sequence": receipt.commit_sequence})
}

async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(LocalNode::start(
        cellule_cookbook_taskboard::compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("taskboard-v1"),
            application_id: APPLICATION,
        },
    )
    .await?)
}

async fn client(node: &LocalNode, project: &str) -> Result<TaskboardClient> {
    // This CLI's principal is the local OS user and its tenant is fixed by the
    // embedding application. A remote HTTP adapter must authenticate first.
    let client = TaskboardClient::new(
        node.application_handle::<Taskboard>(TENANT)?,
        &ProjectKey::new(project)?,
    )?;
    node.open_cell(client.target(), &Tasks).await?;
    Ok(client)
}

async fn demo(node: &LocalNode) -> Result<()> {
    let project = format!("demo-{}", uuid::Uuid::now_v7().simple());
    let client = client(node, &project).await?;
    let id = cellule_cookbook_support::now_ms()?;
    let prepared = client
        .prepare(
            new_identity()?,
            Change::Create {
                id,
                title: "Ship a durable taskboard".into(),
            },
        )
        .await?;
    let evidence = prepared.evidence().clone();
    // Deliberately discard the successful reply to teach outcome recovery.
    // The integration test injects reply loss at the transport boundary.
    let _discarded = prepared.execute().await?;
    let Resolution::Committed(outcome) = client.resolve(&evidence).await? else {
        return Err("discarded reply did not resolve to a durable outcome".into());
    };
    let assigned = client
        .change(
            new_identity()?,
            Change::Assign {
                id,
                expected_revision: 1,
                assignee: Some("alice".into()),
            },
        )
        .await?;
    let closed = client
        .change(
            new_identity()?,
            Change::Close {
                id,
                expected_revision: 2,
            },
        )
        .await?;
    let observed = client
        .list(
            Some(closed.receipt),
            PageRequest {
                after: Some(id - 1),
                limit: 1,
            },
        )
        .await?;
    let [task] = observed.output.tasks.as_slice() else {
        return Err("demo must observe one task".into());
    };
    if !task.closed || task.revision != 3 || task.assignee.as_deref() != Some("alice") {
        return Err("observed task differs from the durable journey".into());
    }
    if !matches!(assigned.output, TaskOutcome::Applied(_)) {
        return Err("assignment was not applied".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario": "passed", "project": project, "resolved_sequence": outcome.commit_sequence(),
        "task": task, "receipt": receipt_json(observed.receipt)})
    );
    Ok(())
}

async fn run_operation(node: &LocalNode, args: &[String]) -> Result<()> {
    match args {
        [operation, _state] if operation == "demo" => demo(node).await,
        [operation, _state, project, file] if operation == "apply" || operation == "resolve" => {
            let mutation: MutationFile = read_json(Path::new(file))?;
            if mutation.project != *project {
                return Err("mutation file is bound to another project".into());
            }
            let client = client(node, project).await?;
            let prepared = client
                .prepare(mutation.identity()?, mutation.change)
                .await?;
            if operation == "resolve" {
                let resolved = client.resolve(prepared.evidence()).await?;
                match resolved {
                    Resolution::Committed(outcome) => {
                        use cellule_runtime::codec::{BoundedDecoder, WireValue};
                        let mut decoder = BoundedDecoder::new(outcome.result(), 1024)?;
                        let output = TaskOutcome::decode(&mut decoder)?;
                        decoder.finish()?;
                        println!(
                            "{}",
                            serde_json::json!({"resolution": "committed", "outcome": output,
                            "commit_sequence": outcome.commit_sequence()})
                        );
                        Ok(())
                    }
                    Resolution::Absent => {
                        println!("{}", serde_json::json!({"resolution": "absent"}));
                        Ok(())
                    }
                    Resolution::Unknown => {
                        Err("outcome unknown; retain the original mutation file".into())
                    }
                    Resolution::Expired => {
                        Err("outcome expired; expiry does not prove that it never ran".into())
                    }
                }
            } else {
                match prepared.execute().await {
                    Ok(committed) => {
                        println!(
                            "{}",
                            serde_json::json!({"outcome": committed.output, "receipt": receipt_json(committed.receipt)})
                        );
                        Ok(())
                    }
                    Err(InvocationError::Rejected(committed)) => {
                        println!(
                            "{}",
                            serde_json::json!({"outcome": committed.output, "receipt": receipt_json(committed.receipt)})
                        );
                        Err("command was durably rejected".into())
                    }
                    Err(error) => Err(error.into()),
                }
            }
        }
        [operation, _state, project, rest @ ..] if operation == "list" && rest.len() <= 2 => {
            let after = rest.first().map(|value| value.parse()).transpose()?;
            let limit = rest
                .get(1)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(20);
            let observed = client(node, project)
                .await?
                .list(None, PageRequest { after, limit })
                .await?;
            println!(
                "{}",
                serde_json::json!({"page": observed.output, "receipt": receipt_json(observed.receipt)})
            );
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}

async fn run(args: Vec<String>) -> Result<()> {
    match args.as_slice() {
        [] => {
            println!("{HELP}");
            return Ok(());
        }
        [help] if help == "help" || help == "--help" => {
            println!("{HELP}");
            return Ok(());
        }
        [operation, project, input, output] if operation == "prepare" => {
            return prepare(project, Path::new(input), Path::new(output));
        }
        _ => {}
    }
    let state = args.get(1).ok_or(HELP)?;
    let node = start(PathBuf::from(state)).await?;
    let result = tokio::select! {
        biased;
        interrupted = cellule_cookbook_support::shutdown_signal() => match interrupted {
            Ok(()) => Err("interrupted; retain mutation files for outcome resolution".into()),
            Err(error) => Err(error.into()),
        },
        result = run_operation(&node, &args) => result,
    };
    // Always drain, including validation/JSON/command failures. If both fail,
    // preserve the primary error and record the independent cleanup cause.
    let shutdown = node.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup, "taskboard drain failed");
            }
            Err(error)
        }
        (Ok(()), Err(error)) => Err(error.into()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(std::env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("taskboard: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
