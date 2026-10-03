//! Persistent settings CLI with retained conditional-edit records.

use cellule_cookbook_settings::{
    Edit, Expected, Organization, Preference, Preferences, Settings, SettingsClient,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId, identity::RequestId,
    primitives::kv::KvAtomicOutcome,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x32; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x42; 16]);
const MAX_FILE_BYTES: u64 = 16 << 10;
const HELP: &str = "Cellule organization settings\n\n\
  cellule-cookbook-settings demo STATE_DIRECTORY\n\
  cellule-cookbook-settings prepare ORGANIZATION EDITS_JSON MUTATION_FILE\n\
  cellule-cookbook-settings apply STATE_DIRECTORY ORGANIZATION MUTATION_FILE\n\
  cellule-cookbook-settings resolve STATE_DIRECTORY ORGANIZATION MUTATION_FILE\n\
  cellule-cookbook-settings get STATE_DIRECTORY ORGANIZATION KEY\n\
  cellule-cookbook-settings list STATE_DIRECTORY ORGANIZATION [PREFIX] [AFTER_KEY] [LIMIT]\n\n\
Use '-' for an empty prefix or absent continuation. Each edit requires absent or a version.\n\
Keep mutation files unchanged for retry and resolution; expiry is an absolute Unix timestamp.";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    organization: String,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    edits: Vec<Edit>,
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
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err("input JSON exceeds 16 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn prepare(organization: &str, input: &Path, output: &Path) -> Result<()> {
    Organization::new(organization)?;
    let edits = read_json(input)?;
    let identity = new_identity()?;
    let record = MutationFile {
        version: 1,
        organization: organization.into(),
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        edits,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&record)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"prepared": output, "request_id": record.request_id})
    );
    Ok(())
}
fn receipt(receipt: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell": format!("{:?}", receipt.cell),
        "incarnation": format!("{:?}", receipt.incarnation), "commit_sequence": receipt.commit_sequence})
}
fn outcome(value: &KvAtomicOutcome) -> serde_json::Value {
    match value {
        KvAtomicOutcome::Applied(results) => {
            serde_json::json!({"status": "applied", "edits": results.iter()
            .map(|result| serde_json::json!({"key": String::from_utf8_lossy(&result.key), "deleted": result.deleted}))
            .collect::<Vec<_>>() })
        }
        KvAtomicOutcome::PreconditionFailed { key } => {
            serde_json::json!({"status": "conflict", "key": String::from_utf8_lossy(key)})
        }
    }
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(LocalNode::start(
        cellule_cookbook_settings::compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/settings"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn client(node: &LocalNode, organization: &str) -> Result<SettingsClient> {
    // The local OS principal selects one fixed tenant. Network embeddings must
    // authenticate and derive their tenant before binding a domain capability.
    let client = SettingsClient::new(
        node.application_handle::<Settings>(TENANT)?,
        &Organization::new(organization)?,
    )?;
    node.open_cell(client.target(), &Preferences).await?;
    Ok(client)
}
fn edit(key: &str, expected: Expected, value: Option<Preference>) -> Edit {
    Edit {
        key: key.into(),
        expected,
        value,
        expires_at_ms: None,
    }
}
async fn demo(node: &LocalNode) -> Result<()> {
    let organization = format!("demo-{}", uuid::Uuid::now_v7().simple());
    let client = client(node, &organization).await?;
    let created = client
        .edit(
            new_identity()?,
            vec![
                edit(
                    "theme",
                    Expected::Absent,
                    Some(Preference::Text("dark".into())),
                ),
                edit(
                    "notifications.email",
                    Expected::Absent,
                    Some(Preference::Boolean(true)),
                ),
            ],
        )
        .await?;
    let setting = client
        .get("theme", Some(created.receipt))
        .await?
        .output
        .ok_or("created preference missing")?;
    let first_id = new_identity()?;
    let second_id = new_identity()?;
    let first = vec![edit(
        "theme",
        Expected::Version(setting.version),
        Some(Preference::Text("light".into())),
    )];
    let second = vec![edit(
        "theme",
        Expected::Version(setting.version),
        Some(Preference::Text("system".into())),
    )];
    let (first_result, second_result) = tokio::join!(
        client.edit(first_id, first.clone()),
        client.edit(second_id, second.clone())
    );
    let (loser_id, loser, rejected) = match (first_result, second_result) {
        (Ok(_), Err(InvocationError::Rejected(rejected))) => (second_id, second, rejected),
        (Err(InvocationError::Rejected(rejected)), Ok(_)) => (first_id, first, rejected),
        _ => {
            return Err(
                "concurrent editors must produce one winner and one durable conflict".into(),
            );
        }
    };
    match client.edit(loser_id, loser).await {
        Err(InvocationError::Rejected(replayed)) if replayed == rejected => {}
        _ => return Err("losing edit must replay the original durable rejection".into()),
    }
    let current = client
        .get("theme", None)
        .await?
        .output
        .ok_or("updated preference missing")?;
    client
        .edit(
            new_identity()?,
            vec![edit("theme", Expected::Version(current.version), None)],
        )
        .await?;
    client
        .edit(
            new_identity()?,
            vec![edit(
                "theme",
                Expected::Absent,
                Some(Preference::Text("recreated".into())),
            )],
        )
        .await?;
    if !matches!(
        client
            .edit(
                new_identity()?,
                vec![edit(
                    "theme",
                    Expected::Version(current.version),
                    Some(Preference::Text("stale".into()))
                )]
            )
            .await,
        Err(InvocationError::Rejected(_))
    ) {
        return Err("old version must not overwrite recreated value".into());
    }
    let expiry = now_ms()?.checked_add(500).ok_or("expiry overflow")?;
    let expiring = client
        .edit(
            new_identity()?,
            vec![Edit {
                expires_at_ms: Some(expiry),
                ..edit(
                    "feature.temporary",
                    Expected::Absent,
                    Some(Preference::Boolean(true)),
                )
            }],
        )
        .await?;
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let observed = client.list("", None, 20, Some(expiring.receipt)).await?;
            if observed.receipt.commit_sequence > expiring.receipt.commit_sequence
                && !observed
                    .output
                    .settings
                    .iter()
                    .any(|setting| setting.key == "feature.temporary")
            {
                return Ok::<_, cellule_cookbook_settings::ReadError>(observed);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await??;
    println!(
        "{}",
        serde_json::json!({"scenario": "passed", "organization": organization,
        "checks": ["atomic-bundle", "competing-editors", "durable-conflict-replay", "delete-recreate-version", "supervised-expiry"],
        "page": observed.output, "receipt": receipt(observed.receipt)})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    match args {
        [op, _] if op == "demo" => demo(node).await,
        [op, _, organization, path] if op == "apply" || op == "resolve" => {
            let record: MutationFile = read_json(Path::new(path))?;
            if record.organization != *organization {
                return Err("mutation belongs to another organization".into());
            }
            let identity = record.identity()?;
            let client = client(node, organization).await?;
            let prepared = client.prepare(identity, record.edits).await?;
            if op == "resolve" {
                let resolution = client.resolve(prepared.evidence()).await?;
                let value = match resolution {
                    Resolution::Committed(stored) => {
                        use cellule_runtime::codec::{BoundedDecoder, WireValue};
                        let mut decoder = BoundedDecoder::new(stored.result(), 8 << 10)?;
                        let output = KvAtomicOutcome::decode(&mut decoder)?;
                        decoder.finish()?;
                        serde_json::json!({"resolution": "committed", "outcome": outcome(&output), "commit_sequence": stored.commit_sequence()})
                    }
                    Resolution::Absent => serde_json::json!({"resolution": "absent"}),
                    Resolution::Unknown => {
                        return Err("outcome unknown; retain the original mutation file".into());
                    }
                    Resolution::Expired => {
                        return Err(
                            "outcome expired; expiry does not prove that it never ran".into()
                        );
                    }
                };
                println!("{value}");
                return Ok(());
            }
            match prepared.execute().await {
                Ok(committed) => {
                    println!(
                        "{}",
                        serde_json::json!({"outcome": outcome(&committed.output), "receipt": receipt(committed.receipt)})
                    );
                    Ok(())
                }
                Err(InvocationError::Rejected(rejected)) => {
                    println!(
                        "{}",
                        serde_json::json!({"outcome": outcome(&rejected.output), "receipt": receipt(rejected.receipt)})
                    );
                    Err("edit bundle was durably rejected".into())
                }
                Err(error) => Err(error.into()),
            }
        }
        [op, _, organization, key] if op == "get" => {
            let observed = client(node, organization).await?.get(key, None).await?;
            println!(
                "{}",
                serde_json::json!({"setting": observed.output, "receipt": receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, organization, rest @ ..] if op == "list" && rest.len() <= 3 => {
            let prefix = rest
                .first()
                .map(String::as_str)
                .filter(|value| *value != "-")
                .unwrap_or("");
            let after = rest
                .get(1)
                .map(String::as_str)
                .filter(|value| *value != "-");
            let limit = rest
                .get(2)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(20);
            let observed = client(node, organization)
                .await?
                .list(prefix, after, limit, None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"page": observed.output, "receipt": receipt(observed.receipt)})
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
        [op, organization, input, output] if op == "prepare" => {
            return prepare(organization, Path::new(input), Path::new(output));
        }
        _ => {}
    }
    let node = start(PathBuf::from(args.get(1).ok_or(HELP)?)).await?;
    let result = tokio::select! {
        biased;
        interrupted = cellule_cookbook_support::shutdown_signal() => match interrupted {
            Ok(()) => Err("interrupted; retain mutation files for outcome resolution".into()),
            Err(error) => Err(error.into()),
        },
        result = operation(&node, &args) => result,
    };
    let shutdown = node.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup, "settings drain failed");
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
            eprintln!("settings: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
