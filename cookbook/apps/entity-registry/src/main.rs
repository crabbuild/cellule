//! Persistent local CLI ingress. Shell access is the development authorization boundary.
use cellule_cookbook_entity_registry::{
    Attributes, Change, ChangeOutcome, DeliveryOptions, DeviceClient, DeviceKey, Devices,
    Directory, DirectoryClient, EntityRegistry, PageRequest, ProjectionState, compile,
    spawn_delivery,
};
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, shutdown_signal,
};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId,
    codec::{BoundedDecoder, WireValue},
    control::authority::CellAuthority,
    identity::RequestId,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0xb3; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xb4; 16]);
const PREFIX: &str = "cookbook/entity-registry";
const HELP: &str = "Cellule entity registry\n\n  demo STATE_DIRECTORY\n  prepare CHANGE_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE\n  resolve STATE_DIRECTORY MUTATION_FILE\n  get STATE_DIRECTORY DEVICE_KEY\n  progress STATE_DIRECTORY DEVICE_KEY\n  lookup STATE_DIRECTORY DEVICE_KEY\n  list STATE_DIRECTORY [AFTER_KEY] [LIMIT]\n  serve STATE_DIRECTORY ROSTER_JSON [SECONDS] [AFTER_PUBLICATION_MS] [lose-reply]\n\nChange JSON: key, expected_revision (0 registers), attributes { name, location, enabled }.\nRoster JSON: array of 1..3 distinct canonical device keys. Retain mutation files unchanged.\nDevice receipts and directory receipts are separate; missing directory rows may be pending.";
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
            || self.issued_at_ms < 0
            || self.expires_at_ms.checked_sub(self.issued_at_ms) != Some(300_000)
        {
            return Err("unsupported or invalid retained mutation".into());
        }
        self.change.validate()?;
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
        return Err("input exceeds 4 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn prepare(input: &Path, output: &Path) -> Result<()> {
    let change: Change = read_json(input)?;
    change.validate()?;
    let identity = new_identity()?;
    let record = MutationFile {
        version: 1,
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        change,
    };
    record.identity()?;
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
        serde_json::json!({"prepared":output,"key":record.change.key,"request_id":record.request_id})
    );
    Ok(())
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn store() -> Result<cellule_store::Store> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(local_s3_store(&endpoint, "cellule-cookbook")?)
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    Ok(LocalNode::start(
        compile()?,
        store()?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from(PREFIX),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn ownership(target: &cellule_runtime::CellTarget) -> Result<serde_json::Value> {
    let layout = cellule_ltx::CellStorageLayout::new(
        store()?,
        object_store::path::Path::from(PREFIX),
        *APPLICATION.as_bytes(),
    );
    let authority = CellAuthority::new(layout)
        .load(target.cell_id())
        .await?
        .ok_or("device authority missing")?;
    let value = authority.value();
    Ok(
        serde_json::json!({"cell":format!("{:?}",value.cell),"epoch":value.epoch,"owner_session":value.owner.as_ref().map(|o|format!("{:?}",o.session)),"incarnation":format!("{:?}",value.incarnation)}),
    )
}
async fn apply(client: &DeviceClient, record: MutationFile, resolving: bool) -> Result<()> {
    let identity = record.identity()?;
    let prepared = client.prepare(identity, record.change).await?;
    if resolving {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 1024)?;
                let outcome = ChangeOutcome::decode(&mut decoder)?;
                decoder.finish()?;
                println!(
                    "{}",
                    serde_json::json!({"resolution":"committed","outcome":outcome,"commit_sequence":stored.commit_sequence()})
                );
            }
            Resolution::Absent => println!("{}", serde_json::json!({"resolution":"absent"})),
            Resolution::Unknown => {
                return Err("outcome unknown; retain original mutation file".into());
            }
            Resolution::Expired => {
                return Err("outcome expired; expiry does not prove absence".into());
            }
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
            Err("device mutation durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
async fn serve(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<EntityRegistry>,
    keys: Vec<DeviceKey>,
    seconds: Option<u64>,
    delay: u64,
    lose: bool,
) -> Result<()> {
    if seconds.is_some_and(|s| s == 0 || s > 3600) {
        return Err("serving duration must be 1..3600 seconds".into());
    }
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    spawn_delivery(
        node,
        node,
        handle.clone(),
        handle,
        &keys,
        DeliveryOptions {
            after_publication: Duration::from_millis(delay),
            drop_reply_once: lose,
            progress: Some(sender),
        },
    )
    .await?;
    println!("{}", serde_json::json!({"event":"ready","roster":keys}));
    std::io::stdout().flush()?;
    let deadline = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::select! {
            event=receiver.recv()=>{
                let event=event.ok_or("projection workers stopped unexpectedly")?;
                println!("{}",serde_json::json!({"event":"destination_published","progress":event})); std::io::stdout().flush()?;
            },
            ()=tokio::time::sleep(Duration::from_millis(100))=>{
                if !node.is_ready() { return Err("projection failure closed readiness".into()); }
                if deadline.is_some_and(|d|tokio::time::Instant::now()>=d) { return Ok(()); }
            }
        }
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<EntityRegistry>,
) -> Result<()> {
    let key = DeviceKey::new(format!("demo-{}", uuid::Uuid::now_v7()))?;
    let device = DeviceClient::new(handle.clone(), key.clone())?;
    node.open_cell(device.target(), &Devices).await?;
    let directory = DirectoryClient::new(handle.clone())?;
    node.open_cell(directory.target(), &Directory).await?;
    let identity = new_identity()?;
    let change = Change {
        key: key.clone(),
        expected_revision: 0,
        attributes: Attributes {
            name: "Weather sensor".into(),
            location: "Roof".into(),
            enabled: true,
        },
    };
    let published = device.change(identity, change.clone()).await?;
    if device.change(identity, change).await? != published {
        return Err("retained replay changed source outcome".into());
    }
    if directory.lookup(key.clone(), None).await?.output.is_some()
        || device
            .progress(Some(published.receipt))
            .await?
            .output
            .ok_or("source missing")?
            .state
            != ProjectionState::Pending
    {
        return Err("delayed directory publication was not visibly pending".into());
    }
    let updated = device
        .change(
            new_identity()?,
            Change {
                key: key.clone(),
                expected_revision: 1,
                attributes: Attributes {
                    name: "Roof weather sensor".into(),
                    location: "Roof".into(),
                    enabled: false,
                },
            },
        )
        .await?;
    spawn_delivery(
        node,
        node,
        handle.clone(),
        handle,
        std::slice::from_ref(&key),
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if device
            .progress(Some(updated.receipt))
            .await?
            .output
            .ok_or("source missing")?
            .state
            == ProjectionState::Delivered
        {
            break;
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err("projection did not settle".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let projected = directory.lookup(key, None).await?;
    if projected
        .output
        .as_ref()
        .is_none_or(|v| v.revision != 2 || v.attributes.enabled)
    {
        return Err("directory did not converge to revision two".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","device":projected.output,"source_receipt":receipt(updated.receipt),"directory_receipt":receipt(projected.receipt),"ownership":ownership(device.target()).await?,"checks":["stable-entity-key","retained-replay","pending-directory","conditional-update","signed-native-effects","lost-reply-resolution","monotonic-projection"]})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    let handle = node.application_handle::<EntityRegistry>(TENANT)?;
    match args {
        [op, _] if op == "demo" => demo(node, handle).await,
        [op, _, file] if op == "apply" || op == "resolve" => {
            let record: MutationFile = read_json(Path::new(file))?;
            record.identity()?;
            let client = DeviceClient::new(handle, record.change.key.clone())?;
            node.open_cell(client.target(), &Devices).await?;
            apply(&client, record, op == "resolve").await
        }
        [op, _, key] if op == "get" || op == "progress" => {
            let client = DeviceClient::new(handle, DeviceKey::new(key.clone())?)?;
            node.open_cell(client.target(), &Devices).await?;
            let (value, position) = if op == "get" {
                let read = client.get(None).await?;
                (serde_json::to_value(read.output)?, read.receipt)
            } else {
                let read = client.progress(None).await?;
                (serde_json::to_value(read.output)?, read.receipt)
            };
            println!(
                "{}",
                serde_json::json!({"device":value,"receipt":receipt(position),"ownership":ownership(client.target()).await?})
            );
            Ok(())
        }
        [op, _, key] if op == "lookup" => {
            let client = DirectoryClient::new(handle)?;
            node.open_cell(client.target(), &Directory).await?;
            let read = client.lookup(DeviceKey::new(key.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"device":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "list" && rest.len() <= 2 => {
            let after = rest
                .first()
                .filter(|v| v.as_str() != "-")
                .map(|s| DeviceKey::new(s.clone()))
                .transpose()?;
            let limit = rest.get(1).map(|v| v.parse()).transpose()?.unwrap_or(20);
            let client = DirectoryClient::new(handle)?;
            node.open_cell(client.target(), &Directory).await?;
            let read = client.list(PageRequest { after, limit }, None).await?;
            println!(
                "{}",
                serde_json::json!({"page":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, roster, rest @ ..] if op == "serve" && rest.len() <= 3 => {
            let keys: Vec<DeviceKey> = read_json(Path::new(roster))?;
            let seconds = rest.first().map(|v| v.parse()).transpose()?;
            let delay = rest.get(1).map(|v| v.parse()).transpose()?.unwrap_or(0);
            let lose = match rest.get(2).map(String::as_str) {
                None => false,
                Some("lose-reply") => true,
                _ => return Err("fault flag must be lose-reply".into()),
            };
            serve(node, handle, keys, seconds, delay, lose).await
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
    let node = start(args.get(1).ok_or(HELP)?.into()).await?;
    let result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal {Ok(()) if args[0]=="serve"=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain mutation evidence").into()),Err(e)=>Err(e.into())},
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
            eprintln!("entity-registry: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
