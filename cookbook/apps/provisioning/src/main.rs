//! Persistent lifecycle CLI and independently owned local resource provider.
mod provider_server;
#[cfg(test)]
mod tests;
use cellule_cookbook_provisioning::*;
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, now_ms, shutdown_signal,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, PreparedCommand, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
    registry::Command,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
type Result<T> = std::result::Result<T, BoxError>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x84; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x85; 16]);
const HELP: &str = "Cellule provisioning\n\n  demo STATE\n  prepare INPUT_JSON MUTATION_FILE\n  apply STATE MUTATION_FILE\n  resolve STATE MUTATION_FILE\n  resource STATE RESOURCE_UUID\n  resources STATE [AFTER_ROW] [LIMIT]\n  workflow STATE RESOURCE_UUID\n  serve STATE [SECONDS]\n  provider-server STATE PORT FAULT_FILE [SECONDS]\n  provider STATE RESOURCE_UUID\n  provider-state FAULT_FILE up|down|drop-create-reply|fail-delete-once\n\nPrepared operations: request, delete, reconcile. Preserve original files.\nDeletion is an intent; completion requires the provider tombstone and directory callback.\nUnknown or expired evidence does not prove absence. Provider credentials stay in the environment.";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Input {
    Request { spec: Spec },
    Delete { resource: Id },
    Reconcile { input: Reconcile },
}
impl Input {
    fn validate(&self) -> Result<()> {
        if let Self::Request { spec } = self {
            spec.validate()?;
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    request_id: Id,
    issued_at_ms: i64,
    expires_at_ms: i64,
    input: Input,
}
impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        self.input.validate()?;
        if self.version != 1
            || self.issued_at_ms < 0
            || self
                .expires_at_ms
                .checked_sub(self.issued_at_ms)
                .is_none_or(|window| !(1..=300000).contains(&window))
        {
            return Err("invalid retained provisioning identity".into());
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(self.request_id.bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err("provisioning JSON exceeds 4096 bytes".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn write(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    let retained = if replace {
        temporary.persist(path)?
    } else {
        temporary.persist_noclobber(path)?
    };
    retained.sync_all()?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
fn prepare(input: &Path, output: &Path) -> Result<()> {
    let input: Input = read(input)?;
    input.validate()?;
    let identity = new_identity()?;
    let record = MutationFile {
        version: 1,
        request_id: Id::from_bytes(*identity.request_id.as_bytes())?,
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        input,
    };
    record.identity()?;
    let mut bytes = serde_json::to_vec_pretty(&record)?;
    bytes.push(b'\n');
    write(output, &bytes, false)?;
    println!(
        "{}",
        serde_json::json!({"prepared":output,"request_id":record.request_id})
    );
    Ok(())
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
async fn start(state: PathBuf, provider: bool) -> Result<LocalNode> {
    let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
        Err(source) => return Err(source.into()),
    };
    Ok(LocalNode::start(
        compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from(if provider {
                "cookbook/provisioning-provider/cells"
            } else {
                "cookbook/provisioning/cells"
            }),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn dispatch<C: Command>(
    client: &ProvisioningClient,
    prepared: PreparedCommand<C>,
    resolve: bool,
) -> Result<()>
where
    C::Output: Serialize + Sync,
{
    if resolve {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 4096)?;
                let outcome = C::Output::decode(&mut decoder)?;
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
                return Err("retain original provisioning evidence".into());
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
            Err("resource operation durably rejected".into())
        }
        Err(source) => Err(source.into()),
    }
}
async fn apply(client: &ProvisioningClient, record: MutationFile, resolve: bool) -> Result<()> {
    let identity = record.identity()?;
    match record.input {
        Input::Request { spec } => {
            dispatch(
                client,
                client.prepare(identity, Change::Request(spec)).await?,
                resolve,
            )
            .await
        }
        Input::Delete { resource } => {
            dispatch(
                client,
                client.prepare(identity, Change::Delete(resource)).await?,
                resolve,
            )
            .await
        }
        Input::Reconcile { input } => {
            dispatch(
                client,
                client.prepare_reconcile(identity, input).await?,
                resolve,
            )
            .await
        }
    }
}
fn seconds(value: Option<&String>) -> Result<u64> {
    let value = value
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(3600);
    if !(1..=3600).contains(&value) {
        return Err("serving duration must be 1..3600 seconds".into());
    }
    Ok(value)
}
async fn serving(node: &LocalNode, seconds: u64) -> Result<()> {
    println!("{}", serde_json::json!({"event":"ready"}));
    std::io::stdout().flush()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline {
        if !node.is_ready() {
            return Err("resource worker failure closed readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}
async fn wait(
    client: &ProvisioningClient,
    node: &LocalNode,
    id: Id,
    phase: Phase,
) -> Result<FlowView> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let flow = client.workflow(id, None).await?.output;
        let resource = client.resource(id, None).await?.output;
        let expected = match phase {
            Phase::Active => ResourceStatus::Active,
            Phase::Completed => ResourceStatus::Deleted,
            Phase::NeedsReview => ResourceStatus::NeedsReview,
            _ => return Err("unsupported wait phase".into()),
        };
        if flow
            .as_ref()
            .is_some_and(|value| value.state.phase == phase)
            && resource
                .as_ref()
                .is_some_and(|value| value.status == expected)
        {
            return flow.ok_or_else(|| "observed resource Flow absent".into());
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "resource did not settle: resource={resource:?}, workflow={flow:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    provider_node: &LocalNode,
    state: &Path,
) -> Result<()> {
    let provider_handle = provider_node.application_handle::<ProvisioningApplication>(TENANT)?;
    let provider = ProvisioningClient::new(provider_handle);
    provider_node
        .open_cell(&provider.target(PROVIDER)?, &Provider)
        .await?;
    let fixture = state.join("demo-provider-fault.txt");
    write(&fixture, b"drop-create-reply\n", true)?;
    let address =
        provider_server::install(provider_node, provider.clone(), 0, Some(fixture.clone())).await?;
    spawn_provider_lifecycle(provider_node, provider.clone())?;
    let make = || -> Result<Spec> {
        Ok(Spec {
            id: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes())?,
            name: "demo-volume".into(),
            capacity_mib: 16,
            provider_endpoint: format!("http://{address}/"),
        })
    };
    let primary = make()?;
    let original = client
        .prepare(new_identity()?, Change::Request(primary.clone()))
        .await?
        .execute()
        .await?;
    spawn_workers(node, handle).await?;
    let active = wait(client, node, primary.id, Phase::Active).await?;
    if active.status != "running" {
        return Err("active resource lifetime must remain running".into());
    }
    let allocated = provider
        .provider(primary.id, None)
        .await?
        .output
        .ok_or("provider allocation absent")?;
    if allocated.creates != 1 || allocated.phase != ProviderPhase::Ready {
        return Err("provider allocation repeated or unready".into());
    }
    write(&fixture, b"fail-delete-once\n", true)?;
    client
        .prepare(new_identity()?, Change::Delete(primary.id))
        .await?
        .execute()
        .await?;
    let cleaned = wait(client, node, primary.id, Phase::Completed).await?;
    if cleaned.state.cleanup_attempts < 2 {
        return Err("demo did not exercise cleanup retry".into());
    }
    let tombstone = provider
        .provider(primary.id, None)
        .await?
        .output
        .ok_or("cleanup tombstone absent")?;
    if tombstone.creates != 1 || tombstone.deletes != 1 || tombstone.phase != ProviderPhase::Deleted
    {
        return Err("provider cleanup did not apply exactly once".into());
    }
    let replay = client
        .prepare(new_identity()?, Change::Request(primary.clone()))
        .await?
        .execute()
        .await?;
    if replay.output != original.output {
        return Err("resource replay changed original start intent".into());
    }
    let cancelled = make()?;
    client
        .prepare(new_identity()?, Change::Request(cancelled.clone()))
        .await?
        .execute()
        .await?;
    client
        .prepare(new_identity()?, Change::Delete(cancelled.id))
        .await?
        .execute()
        .await?;
    let cancelled_flow = wait(client, node, cancelled.id, Phase::Completed).await?;
    let cancelled_provider = provider
        .provider(cancelled.id, None)
        .await?
        .output
        .ok_or("cancelled resource tombstone absent")?;
    if cancelled_provider.creates > 1
        || cancelled_provider.deletes != 1
        || cancelled_provider.phase != ProviderPhase::Deleted
    {
        return Err("cancelled resource leaked or recreated".into());
    }
    write(&fixture, b"down\n", true)?;
    let review = make()?;
    client
        .prepare(new_identity()?, Change::Request(review.clone()))
        .await?
        .execute()
        .await?;
    let unresolved = wait(client, node, review.id, Phase::NeedsReview).await?;
    if unresolved.state.stage_attempts != MAX_STAGE_ATTEMPTS {
        return Err("demo automatic reconciliation budget differs".into());
    }
    write(&fixture, b"up\n", true)?;
    client
        .prepare_reconcile(
            new_identity()?,
            Reconcile {
                resource: review.id,
                token: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes())?,
            },
        )
        .await?
        .execute()
        .await?;
    wait(client, node, review.id, Phase::Active).await?;
    client
        .prepare(new_identity()?, Change::Delete(review.id))
        .await?
        .execute()
        .await?;
    let reconciled = wait(client, node, review.id, Phase::Completed).await?;
    let reviewed_provider = provider
        .provider(review.id, None)
        .await?
        .output
        .ok_or("reconciled resource tombstone absent")?;
    if reviewed_provider.creates != 1
        || reviewed_provider.deletes != 1
        || reconciled.state.reconciliations != 1
    {
        return Err("operator reconciliation changed permanent provider application counts".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","resource":primary,"active":active,"cleaned":cleaned,"tombstone":tombstone,"cancelled":cancelled_flow,"cancelled_provider":cancelled_provider,"reviewed":reconciled,"reviewed_provider":reviewed_provider,"checks":["permanent-provider-keys","actual-lost-create-reply","asynchronous-provider-lifecycle","read-only-polling","active-directory-details","cleanup-retry","deletion-once","early-cancellation","bounded-automatic-review","manual-reconciliation","permanent-request-replay"]})
    );
    Ok(())
}
async fn operation(
    node: &LocalNode,
    provider_node: Option<&LocalNode>,
    args: &[String],
) -> Result<()> {
    let handle = node.application_handle::<ProvisioningApplication>(TENANT)?;
    if matches!(
        args.first().map(String::as_str),
        Some("provider-server" | "provider")
    ) {
        let client = ProvisioningClient::new(handle);
        node.open_cell(&client.target(PROVIDER)?, &Provider).await?;
        return match args {
            [op, _, port, fault, rest @ ..] if op == "provider-server" && rest.len() <= 1 => {
                let address = provider_server::install(
                    node,
                    client.clone(),
                    port.parse()?,
                    Some(fault.into()),
                )
                .await?;
                spawn_provider_lifecycle(node, client)?;
                println!(
                    "{}",
                    serde_json::json!({"event":"provider_ready","address":address})
                );
                serving(node, seconds(rest.first())?).await
            }
            [op, _, id] if op == "provider" => {
                let value = client.provider(Id::try_from(id.clone())?, None).await?;
                println!(
                    "{}",
                    serde_json::json!({"provider":value.output,"receipt":receipt(value.receipt)})
                );
                Ok(())
            }
            _ => Err(HELP.into()),
        };
    }
    let client = open(node, &handle).await?;
    match args {
        [op, _] if op == "demo" => {
            demo(
                node,
                handle,
                &client,
                provider_node.ok_or("demo provider missing")?,
                Path::new(&args[1]),
            )
            .await
        }
        [op, _, file] if op == "apply" || op == "resolve" => {
            apply(&client, read(Path::new(file))?, op == "resolve").await
        }
        [op, _, id] if op == "resource" => {
            let value = client.resource(Id::try_from(id.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"resource":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, id] if op == "workflow" => {
            let value = client.workflow(Id::try_from(id.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"workflow":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "resources" && rest.len() <= 2 => {
            let page = Page {
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
            let value = client.resources(page, None).await?;
            println!(
                "{}",
                serde_json::json!({"resources":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "serve" && rest.len() <= 1 => {
            spawn_workers(node, handle).await?;
            serving(node, seconds(rest.first())?).await
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
    if let [op, file, mode] = args.as_slice()
        && op == "provider-state"
    {
        if !matches!(
            mode.as_str(),
            "up" | "down" | "drop-create-reply" | "fail-delete-once"
        ) {
            return Err("invalid provider simulator state".into());
        }
        return write(Path::new(file), format!("{mode}\n").as_bytes(), true);
    }
    if let [op, _, file] = args.as_slice()
        && op == "resolve"
    {
        let record: MutationFile = read(Path::new(file))?;
        if now_ms()? > record.identity()?.expires_at_ms {
            println!(
                "{}",
                serde_json::json!({"resolution":"expired","absence_proven":false})
            );
            return Ok(());
        }
    }
    let state = PathBuf::from(args.get(1).ok_or(HELP)?);
    let provider = matches!(
        args.first().map(String::as_str),
        Some("provider-server" | "provider")
    );
    let node = start(state.clone(), provider).await?;
    let provider_node = if args[0] == "demo" {
        match start(state.join("provider"), true).await {
            Ok(value) => Some(value),
            Err(source) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(error=%cleanup,"resource node drain also failed after provider startup failure");
                }
                return Err(source);
            }
        }
    } else {
        None
    };
    let mut result = tokio::select! {
        biased;
        signal = shutdown_signal() => match signal {
            Ok(()) if matches!(args[0].as_str(), "serve" | "provider-server") => Ok(()),
            Ok(()) => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "operation interrupted; retain original request",
            ).into()),
            Err(source) => Err(source.into()),
        },
        result = operation(&node, provider_node.as_ref(), &args) => result,
    };
    if let Err(source) = node.shutdown().await {
        if result.is_ok() {
            result = Err(source.into());
        } else {
            tracing::error!(error=%source,"resource node drain also failed");
        }
    }
    if let Some(node) = provider_node
        && let Err(source) = node.shutdown().await
    {
        if result.is_ok() {
            result = Err(source.into());
        } else {
            tracing::error!(error=%source,"provider node drain also failed");
        }
    }
    result
}
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            tracing::error!(error=%source,"provisioning failed");
            let mut cause = source.source();
            while let Some(source) = cause {
                tracing::error!(cause=%source,"provisioning source error");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
