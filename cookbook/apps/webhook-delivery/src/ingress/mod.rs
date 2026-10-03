mod demo;
mod http;
use cellule_cookbook_support::{
    LocalNode, NodeConfig, local_s3_store, new_identity, now_ms, shutdown_signal,
};
use cellule_cookbook_webhook_delivery::{
    Action, Endpoint, EventId, Key, Outcome, ReceiverMode, ReceiverPolicy, WebhookApplication,
    WebhookClient, compile, open, spawn_fanout, spawn_http_activities,
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
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x24; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x25; 16]);
const HELP: &str = "Cellule webhook delivery\n\n  demo STATE_DIRECTORY\n  prepare ACTION_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE [AFTER_PUBLICATION_MS]\n  resolve STATE_DIRECTORY MUTATION_FILE\n  subscriptions STATE_DIRECTORY\n  event STATE_DIRECTORY EVENT_UUID\n  delivery STATE_DIRECTORY DELIVERY_KEY_HEX\n  received STATE_DIRECTORY DELIVERY_KEY_HEX\n  policy STATE_DIRECTORY SUBSCRIBER good|drop_once|transient_once|terminal\n  serve STATE_DIRECTORY [SECONDS] [AFTER_RECEIVER_PUBLICATION_MS]\n\nAction JSON: {kind:subscribe,id,topic,endpoint,enabled,expected_revision} or {kind:publish,id:EVENT_UUID,topic,payload}. Shell access authorizes administration. HTTP accepts only the configured receiver bearer and exact immutable delivery key. CELLULE_WEBHOOK_PORT defaults to 19020; freeze that endpoint before publishing. Retain mutation files unchanged; expired evidence never proves absence.";
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Input {
    Subscribe {
        id: Key,
        topic: Key,
        endpoint: Endpoint,
        enabled: bool,
        expected_revision: i64,
    },
    Publish {
        id: String,
        topic: Key,
        payload: String,
    },
}
impl Input {
    fn action(self) -> Result<Action> {
        match self {
            Self::Subscribe {
                id,
                topic,
                endpoint,
                enabled,
                expected_revision,
            } => Ok(Action::Subscribe {
                id,
                topic,
                endpoint,
                enabled,
                expected_revision,
            }),
            Self::Publish { id, topic, payload } => Ok(Action::Publish {
                id: event_id(&id)?,
                topic,
                payload,
            }),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationFile {
    version: u8,
    request_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    action: Action,
}
impl MutationFile {
    fn identity(&self) -> Result<MutationIdentity> {
        let id = canonical_uuid(&self.request_id)?;
        if self.version != 1
            || self.issued_at_ms < 0
            || self.expires_at_ms.checked_sub(self.issued_at_ms) != Some(300_000)
        {
            return Err("unsupported or invalid retained mutation evidence".into());
        }
        // Validate canonical operation bytes before provider acquisition or publication.
        let mut encoder = cellule_runtime::codec::BoundedEncoder::new(4096)?;
        self.action.encode(&mut encoder)?;
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(*id.as_bytes()),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
fn canonical_uuid(value: &str) -> Result<uuid::Uuid> {
    let id = uuid::Uuid::parse_str(value)?;
    if id.is_nil() || id.to_string() != value {
        return Err("identity must be a canonical nonzero UUID".into());
    }
    Ok(id)
}
fn event_id(value: &str) -> Result<EventId> {
    Ok(EventId::from_bytes(*canonical_uuid(value)?.as_bytes())?)
}
fn delivery_key(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("delivery key must be 64 lowercase hex digits".into());
    }
    let mut key = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        key[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(key)
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err("input exceeds 32 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn print(value: serde_json::Value) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn prepare(input: &Path, output: &Path) -> Result<()> {
    let action = read_json::<Input>(input)?.action()?;
    let identity = new_identity()?;
    let record = MutationFile {
        version: 1,
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        action,
    };
    record.identity()?;
    let bytes = serde_json::to_vec_pretty(&record)?;
    if bytes.len() > 32767 {
        return Err("prepared evidence exceeds 32 KiB".into());
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::File::open(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    print(serde_json::json!({"prepared":output,"request_id":record.request_id}))
}
async fn start(state: PathBuf) -> Result<LocalNode> {
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
            storage_prefix: object_store::path::Path::from("cookbook/webhook-delivery"),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn client(node: &LocalNode) -> Result<WebhookClient> {
    Ok(open(node, node.application_handle::<WebhookApplication>(TENANT)?).await?)
}
fn delivery_keys(
    event: Option<&cellule_cookbook_webhook_delivery::PublishedEvent>,
) -> Vec<serde_json::Value> {
    event.map(|event| event.deliveries.iter().map(|delivery| {
        serde_json::json!({"subscription": delivery.ticket.subscription, "key": delivery.ticket.key_hex()})
    }).collect()).unwrap_or_default()
}
async fn apply(
    client: &WebhookClient,
    record: MutationFile,
    resolving: bool,
    delay: u64,
) -> Result<()> {
    let identity = record.identity()?;
    let prepared = client.prepare(identity, record.action).await?;
    if resolving {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 32768)?;
                let outcome = Outcome::decode(&mut decoder)?;
                decoder.finish()?;
                print(
                    serde_json::json!({"resolution":"committed","outcome":outcome,"delivery_keys":delivery_keys(outcome.event.as_ref()),"commit_sequence":stored.commit_sequence()}),
                )?;
            }
            Resolution::Absent => {
                print(serde_json::json!({"resolution":"absent","absence_proven":true}))?
            }
            Resolution::Unknown => {
                return Err("outcome unknown; retain original mutation file".into());
            }
            Resolution::Expired => {
                print(serde_json::json!({"resolution":"expired","absence_proven":false}))?
            }
        }
        return Ok(());
    }
    let (rejected, value) = match prepared.execute().await {
        Ok(value) => (false, value),
        Err(InvocationError::Rejected(value)) => (true, *value),
        Err(source) => return Err(source.into()),
    };
    if delay > 0 {
        print(
            serde_json::json!({"event":"source_published","outcome":value.output,"delivery_keys":delivery_keys(value.output.event.as_ref()),"receipt":receipt(value.receipt)}),
        )?;
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    print(
        serde_json::json!({"outcome":value.output,"delivery_keys":delivery_keys(value.output.event.as_ref()),"receipt":receipt(value.receipt)}),
    )?;
    if rejected {
        Err("source action durably rejected".into())
    } else {
        Ok(())
    }
}
async fn serving(node: &LocalNode, seconds: Option<u64>, delay: u64) -> Result<()> {
    let port = match std::env::var("CELLULE_WEBHOOK_PORT") {
        Ok(value) => value.parse::<u16>()?,
        Err(std::env::VarError::NotPresent) => 19020,
        Err(source) => return Err(source.into()),
    };
    if port == 0 || port == 80 {
        return Err("serving receiver requires an explicit nonzero nondefault port".into());
    }
    let client = client(node).await?;
    let (sender, mut progress) = tokio::sync::mpsc::channel(32);
    let address = http::install(
        node,
        client,
        port,
        http::Options {
            after_publication: Duration::from_millis(delay),
            progress: Some(sender),
        },
    )
    .await?;
    let handle = node.application_handle::<WebhookApplication>(TENANT)?;
    spawn_fanout(node, handle.clone()).await?;
    spawn_http_activities(node, handle)?;
    print(serde_json::json!({"event":"ready","receiver":format!("http://{address}/deliver")}))?;
    let deadline = seconds.map(|value| tokio::time::Instant::now() + Duration::from_secs(value));
    loop {
        tokio::select! {Some(value)=progress.recv()=>print(serde_json::json!({"event":"receiver_published","progress":value}))?,()=tokio::time::sleep(Duration::from_millis(100))=>{}}
        if !node.is_ready() {
            return Err("webhook node lost readiness".into());
        }
        if deadline.is_some_and(|value| tokio::time::Instant::now() >= value) {
            return Ok(());
        }
    }
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    match args {
        [op, _] if op == "demo" => demo::run(node).await,
        [op, _, file, rest @ ..]
            if (op == "apply" && rest.len() <= 1) || (op == "resolve" && rest.is_empty()) =>
        {
            let record: MutationFile = read_json(Path::new(file))?;
            record.identity()?;
            let delay = rest
                .first()
                .map(|value| value.parse::<u64>())
                .transpose()?
                .unwrap_or(0);
            if delay > 10000 {
                return Err("source publication delay must be at most ten seconds".into());
            }
            apply(&client(node).await?, record, op == "resolve", delay).await
        }
        [op, _] if op == "subscriptions" => {
            let value = client(node).await?.subscriptions(None).await?;
            print(
                serde_json::json!({"subscriptions":value.output.subscriptions,"receipt":receipt(value.receipt)}),
            )
        }
        [op, _, id] if op == "event" => {
            let value = client(node).await?.event(event_id(id)?, None).await?;
            print(
                serde_json::json!({"event":value.output,"delivery_keys":delivery_keys(value.output.as_ref()),"receipt":receipt(value.receipt)}),
            )
        }
        [op, _, key] if op == "delivery" || op == "received" => {
            let c = client(node).await?;
            let key = delivery_key(key)?;
            if op == "delivery" {
                let value = c.delivery(key, None).await?;
                let uncertainty = value
                    .output
                    .as_ref()
                    .map(|view| view.state.may_have_applied());
                print(
                    serde_json::json!({"delivery":value.output,"may_have_applied":uncertainty,"receipt":receipt(value.receipt)}),
                )
            } else {
                let value = c.received(key, None).await?;
                print(serde_json::json!({"received":value.output,"receipt":receipt(value.receipt)}))
            }
        }
        [op, _, subscription, mode] if op == "policy" => {
            let mode = match mode.as_str() {
                "good" => ReceiverMode::Good,
                "drop_once" => ReceiverMode::DropOnce,
                "transient_once" => ReceiverMode::TransientOnce,
                "terminal" => ReceiverMode::Terminal,
                _ => return Err("unknown receiver mode".into()),
            };
            let value = client(node)
                .await?
                .policy(
                    new_identity()?,
                    ReceiverPolicy {
                        subscription: Key::new(subscription.clone())?,
                        mode,
                    },
                )
                .await?;
            print(serde_json::json!({"policy":"published","receipt":receipt(value.receipt)}))
        }
        [op, _, rest @ ..] if op == "serve" && rest.len() <= 2 => {
            let seconds = rest.first().map(|value| value.parse::<u64>()).transpose()?;
            let delay = rest
                .get(1)
                .map(|value| value.parse::<u64>())
                .transpose()?
                .unwrap_or(0);
            if seconds.is_some_and(|value| value == 0 || value > 3600) || delay > 10000 {
                return Err("serving duration must be 1..3600 seconds and publication delay at most ten seconds".into());
            }
            serving(node, seconds, delay).await
        }
        _ => Err(HELP.into()),
    }
}
pub(crate) fn print_error(error: &(dyn std::error::Error + 'static)) {
    let mut current = Some(error);
    let mut first = true;
    while let Some(error) = current {
        let prefix = if first {
            "webhook-delivery: "
        } else {
            "  caused by: "
        };
        if error.downcast_ref::<std::env::VarError>().is_some() {
            eprintln!("{prefix}configured environment variable could not be read");
        } else {
            eprintln!("{prefix}{error}");
        }
        first = false;
        current = error.source();
    }
}
pub(crate) async fn run() -> Result<()> {
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
    if let [op, _, file] = args.as_slice()
        && op == "resolve"
    {
        let record: MutationFile = read_json(Path::new(file))?;
        if record.identity()?.expires_at_ms <= now_ms()? {
            return print(
                serde_json::json!({"resolution":"expired","absence_proven":false,"request_id":record.request_id}),
            );
        }
    }
    let node = start(args.get(1).ok_or(HELP)?.into()).await?;
    let result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal{Ok(()) if args[0]=="serve"=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain mutation evidence").into()),Err(source)=>Err(source.into())},
        result=operation(&node,&args)=>result,
    };
    let drained = node.shutdown().await;
    match (result, drained) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(error=%cleanup,"webhook drain failed after operation failure");
            }
            Err(source)
        }
        (Ok(()), Err(source)) => Err(source.into()),
    }
}
#[cfg(test)]
mod tests;
