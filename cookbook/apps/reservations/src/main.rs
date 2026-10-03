//! Persistent local CLI ingress. Shell access is the development authorization boundary.
use cellule_cookbook_reservations::{
    Action, BuyerKey, Change, Decision, DeliveryOptions, EventKey, HoldId, HoldState, Outcome,
    PageRequest, ReservationClient, Reservations, compile, spawn_deadlines,
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
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0xf3; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xf4; 16]);
const PREFIX: &str = "cookbook/reservations";
const HELP: &str = "Cellule event reservations\n\n  demo STATE_DIRECTORY\n  prepare CHANGE_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE [AFTER_PUBLICATION_MS]\n  resolve STATE_DIRECTORY MUTATION_FILE\n  race STATE_DIRECTORY HOLD_MUTATION_A HOLD_MUTATION_B\n  inventory STATE_DIRECTORY EVENT\n  hold STATE_DIRECTORY EVENT HOLD_UUID\n  deadline STATE_DIRECTORY EVENT HOLD_UUID\n  list STATE_DIRECTORY EVENT [AFTER_UUID] [LIMIT]\n  serve STATE_DIRECTORY EVENT [SECONDS] [AFTER_EXPIRATION_PUBLICATION_MS] [keep|drop]\n\nChange JSON: event and action { operation: initialize/hold/confirm/cancel, ... }.\nAuthorize event and buyer before dispatch. Permanent hold IDs and exact seat generations prevent obsolete timeout releases. Retain prepared evidence unchanged; expired evidence never proves absence.";
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
        self.change.action.validate()?;
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
    change.action.validate()?;
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
        serde_json::json!({"prepared":output,"event_key":record.change.event,"request_id":record.request_id})
    );
    Ok(())
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
async fn start(state: PathBuf) -> Result<LocalNode> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(LocalNode::start(
        compile()?,
        local_s3_store(&endpoint, "cellule-cookbook")?,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from(PREFIX),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn client(node: &LocalNode, event_key: EventKey) -> Result<ReservationClient> {
    let client = ReservationClient::new(
        node.application_handle::<Reservations>(TENANT)?,
        event_key.clone(),
    )?;
    spawn_deadlines(
        node,
        node.application_handle::<Reservations>(TENANT)?,
        event_key,
        DeliveryOptions::default(),
    )
    .await?;
    Ok(client)
}
async fn apply(
    client: &ReservationClient,
    record: MutationFile,
    resolving: bool,
    delay: u64,
) -> Result<()> {
    let identity = record.identity()?;
    let prepared = client.prepare(identity, record.change.action).await?;
    if resolving {
        match client.resolve(prepared.evidence()).await? {
            Resolution::Committed(stored) => {
                let mut decoder = BoundedDecoder::new(stored.result(), 1024)?;
                let outcome = Outcome::decode(&mut decoder)?;
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
            if delay > 0 {
                println!(
                    "{}",
                    serde_json::json!({"event":"published","outcome":value.output,"receipt":receipt(value.receipt)})
                );
                std::io::stdout().flush()?;
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
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
            Err("reservation action durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
async fn race(node: &LocalNode, a: MutationFile, b: MutationFile) -> Result<()> {
    let ia = a.identity()?;
    let ib = b.identity()?;
    if a.change.event != b.change.event
        || ia.request_id == ib.request_id
        || !matches!((&a.change.action,&b.change.action),(Action::Hold{id:a,seat:sa,..},Action::Hold{id:b,seat:sb,..}) if a!=b&&sa==sb)
    {
        return Err(
            "race requires distinct hold and mutation IDs for the same event and seat".into(),
        );
    }
    let client = client(node, a.change.event).await?;
    let a = client.prepare(ia, a.change.action).await?;
    let b = client.prepare(ib, b.change.action).await?;
    let (a, b) = tokio::join!(a.execute(), b.execute());
    let render = |result: std::result::Result<
        cellule_runtime::Committed<Outcome>,
        InvocationError<Outcome>,
    >|
     -> Result<serde_json::Value> {
        let (rejected, value) = match result {
            Ok(v) => (false, v),
            Err(InvocationError::Rejected(v)) => (true, *v),
            Err(e) => return Err(e.into()),
        };
        Ok(
            serde_json::json!({"rejected":rejected,"outcome":value.output,"receipt":receipt(value.receipt)}),
        )
    };
    println!("{}", serde_json::json!({"results":[render(a)?,render(b)?]}));
    Ok(())
}
async fn serve(
    node: &LocalNode,
    event_key: EventKey,
    seconds: Option<u64>,
    delay: u64,
    drop_reply: bool,
) -> Result<()> {
    if seconds.is_some_and(|s| s == 0 || s > 3600) || delay > 10000 {
        return Err(
            "serving seconds must be 1..3600 and checkpoint delay at most ten seconds".into(),
        );
    }
    let (sender, mut progress) = tokio::sync::mpsc::channel(32);
    spawn_deadlines(
        node,
        node.application_handle::<Reservations>(TENANT)?,
        event_key.clone(),
        DeliveryOptions {
            after_publication: Duration::from_millis(delay),
            drop_reply_once: drop_reply,
            progress: Some(sender),
        },
    )
    .await?;
    println!(
        "{}",
        serde_json::json!({"event":"ready","event_key":event_key})
    );
    std::io::stdout().flush()?;
    let deadline = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::select! {
            Some(value)=progress.recv()=>{println!("{}",serde_json::json!({"event":"expiration_published","progress":value}));std::io::stdout().flush()?;},
            ()=tokio::time::sleep(Duration::from_millis(100))=>{},
        }
        if !node.is_ready() {
            return Err("reservation node lost readiness".into());
        }
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            return Ok(());
        }
    }
}
fn hold_id() -> Result<HoldId> {
    Ok(HoldId::from_bytes(*uuid::Uuid::now_v7().as_bytes())?)
}
async fn wait_state(
    node: &LocalNode,
    client: &ReservationClient,
    id: HoldId,
    state: HoldState,
) -> Result<cellule_cookbook_reservations::Hold> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if !node.is_ready() {
            return Err("deadline coordinator lost readiness".into());
        }
        let read = client
            .hold(id, None)
            .await?
            .output
            .ok_or("hold disappeared")?;
        if read.state == state {
            return Ok(read);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("hold did not reach expected state".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn demo(node: &LocalNode) -> Result<()> {
    let event_key = EventKey::new(format!("demo-{}", uuid::Uuid::now_v7()))?;
    let handle = node.application_handle::<Reservations>(TENANT)?;
    let c = ReservationClient::new(handle.clone(), event_key.clone())?;
    let (progress_sender, mut progress) = tokio::sync::mpsc::channel(32);
    spawn_deadlines(
        node,
        handle,
        event_key.clone(),
        DeliveryOptions {
            drop_reply_once: true,
            progress: Some(progress_sender),
            ..Default::default()
        },
    )
    .await?;
    c.change(new_identity()?, Action::Initialize { seats: 3 })
        .await?;
    let buyer = BuyerKey::new("alice")?;
    let other = BuyerKey::new("bob")?;
    let ids = [hold_id()?, hold_id()?];
    let identities = [new_identity()?, new_identity()?];
    let deadline = now_ms()? + 10000;
    let actions = [
        Action::Hold {
            id: ids[0],
            seat: 1,
            buyer: buyer.clone(),
            deadline_ms: deadline,
        },
        Action::Hold {
            id: ids[1],
            seat: 1,
            buyer: other.clone(),
            deadline_ms: deadline,
        },
    ];
    let (a, b) = tokio::join!(
        c.change(identities[0], actions[0].clone()),
        c.change(identities[1], actions[1].clone())
    );
    let (held, loser) = match (a, b) {
        (Ok(v), Err(InvocationError::Rejected(r))) | (Err(InvocationError::Rejected(r)), Ok(v))
            if r.output.decision == Decision::Occupied =>
        {
            (v.output.hold.ok_or("hold missing")?, r)
        }
        _ => return Err("seat race did not have one winner".into()),
    };
    let confirm = Action::Confirm {
        id: held.ticket.id,
        generation: held.ticket.generation,
        buyer: held.buyer.clone(),
    };
    let identity = new_identity()?;
    let confirmed = c.change(identity, confirm.clone()).await?;
    if c.change(identity, confirm.clone()).await? != confirmed {
        return Err("confirmation replay changed".into());
    }
    if c.change(new_identity()?, confirm).await?.output.decision != Decision::AlreadyConfirmed {
        return Err("fresh confirmation allocated twice".into());
    }
    let old = hold_id()?;
    let old_deadline = now_ms()? + 1200;
    let cancelled = c
        .change(
            new_identity()?,
            Action::Hold {
                id: old,
                seat: 2,
                buyer: buyer.clone(),
                deadline_ms: old_deadline,
            },
        )
        .await?
        .output
        .hold
        .ok_or("hold missing")?;
    c.change(
        new_identity()?,
        Action::Cancel {
            id: old,
            generation: cancelled.ticket.generation,
            buyer: buyer.clone(),
        },
    )
    .await?;
    let newer = hold_id()?;
    let replacement = c
        .change(
            new_identity()?,
            Action::Hold {
                id: newer,
                seat: 2,
                buyer: buyer.clone(),
                deadline_ms: now_ms()? + 15000,
            },
        )
        .await?
        .output
        .hold
        .ok_or("replacement missing")?;
    let expiring = hold_id()?;
    c.change(
        new_identity()?,
        Action::Hold {
            id: expiring,
            seat: 3,
            buyer: buyer.clone(),
            deadline_ms: now_ms()? + 700,
        },
    )
    .await?;
    wait_state(node, &c, expiring, HoldState::Expired).await?;
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if !node.is_ready() {
            return Err("deadline coordinator lost readiness".into());
        }
        let value = tokio::time::timeout_at(wait_until, progress.recv())
            .await?
            .ok_or("deadline observer closed")?;
        if value.ticket.id == old {
            if value.outcome != cellule_cookbook_reservations::ExpirationOutcome::Unchanged {
                return Err("old timeout changed its cancelled hold".into());
            }
            break;
        }
    }
    if c.hold(newer, None).await?.output.as_ref().is_none_or(|v| {
        v.state != HoldState::Held || v.ticket.generation != replacement.ticket.generation
    }) {
        return Err("old timeout released a newer generation".into());
    }
    let final_id = hold_id()?;
    let final_hold = c
        .change(
            new_identity()?,
            Action::Hold {
                id: final_id,
                seat: 3,
                buyer: buyer.clone(),
                deadline_ms: now_ms()? + 15000,
            },
        )
        .await?
        .output
        .hold
        .ok_or("replacement missing")?;
    for hold in [replacement, final_hold] {
        c.change(
            new_identity()?,
            Action::Confirm {
                id: hold.ticket.id,
                generation: hold.ticket.generation,
                buyer: buyer.clone(),
            },
        )
        .await?;
    }
    let page = c
        .list(
            PageRequest {
                after: None,
                limit: 100,
            },
            None,
        )
        .await?;
    let inventory = page.output.inventory.as_ref().ok_or("inventory missing")?;
    if inventory.available != 0
        || inventory.held != 0
        || inventory.confirmed != 3
        || page.output.holds.len() != 5
    {
        return Err("inventory does not reconcile".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","event_key":event_key,"inventory":inventory,"holds":page.output.holds,"receipt":receipt(page.receipt),"rejected":loser.output,"checks":["same-seat-contention","retained-confirmation-replay","fresh-confirmation-idempotency","autonomous-workflow-expiration","generation-fenced-old-timeout","permanent-hold-history","coherent-inventory"]})
    );
    Ok(())
}
async fn operation(node: &LocalNode, args: &[String]) -> Result<()> {
    match args {
        [op, _] if op == "demo" => demo(node).await,
        [op, _, file, rest @ ..]
            if (op == "apply" && rest.len() <= 1) || (op == "resolve" && rest.is_empty()) =>
        {
            let record: MutationFile = read_json(Path::new(file))?;
            record.identity()?;
            let delay = rest
                .first()
                .map(|s| s.parse::<u64>())
                .transpose()?
                .unwrap_or(0);
            if delay > 10_000 {
                return Err("publication checkpoint delay must be at most 10 seconds".into());
            }
            let client = client(node, record.change.event.clone()).await?;
            apply(&client, record, op == "resolve", delay).await
        }
        [op, _, a, b] if op == "race" => {
            race(node, read_json(Path::new(a))?, read_json(Path::new(b))?).await
        }
        [op, _, event_key] if op == "inventory" => {
            let read = client(node, EventKey::new(event_key.clone())?)
                .await?
                .inventory(None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"inventory":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, event_key, id] if op == "hold" || op == "deadline" => {
            let c = client(node, EventKey::new(event_key.clone())?).await?;
            let hold = c.hold(HoldId::parse(id)?, None).await?;
            if op == "hold" {
                println!(
                    "{}",
                    serde_json::json!({"hold":hold.output,"receipt":receipt(hold.receipt)})
                );
            } else {
                let view = match hold.output {
                    Some(h) => c.deadline(&h.ticket).await?,
                    None => None,
                };
                println!("{}", serde_json::json!({"deadline":view}));
            }
            Ok(())
        }
        [op, _, event_key, rest @ ..] if op == "list" && rest.len() <= 2 => {
            let after = rest
                .first()
                .filter(|s| s.as_str() != "-")
                .map(|s| HoldId::parse(s))
                .transpose()?;
            let limit = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(20);
            let read = client(node, EventKey::new(event_key.clone())?)
                .await?
                .list(PageRequest { after, limit }, None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"page":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, event_key, rest @ ..] if op == "serve" && rest.len() <= 3 => {
            serve(
                node,
                EventKey::new(event_key.clone())?,
                rest.first().map(|s| s.parse()).transpose()?,
                rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(0),
                match rest.get(2).map(String::as_str) {
                    None | Some("keep") => false,
                    Some("drop") => true,
                    _ => return Err("reply option must be keep or drop".into()),
                },
            )
            .await
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
    if let [op, _, file] = args.as_slice()
        && op == "resolve"
    {
        let record: MutationFile = read_json(Path::new(file))?;
        if record.identity()?.expires_at_ms <= now_ms()? {
            println!(
                "{}",
                serde_json::json!({"resolution":"expired","absence_proven":false,"request_id":record.request_id})
            );
            return Ok(());
        }
    }
    let node = start(args.get(1).ok_or(HELP)?.into()).await?;
    let result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal {Ok(()) if args[0]=="serve"=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain mutation evidence").into()),Err(e)=>Err(e.into())},
        result=operation(&node,&args)=>result,
    };
    let drained = node.shutdown().await;
    match (result, drained) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup,"reservation drain failed after operation failure");
            }
            Err(error)
        }
        (Ok(()), Err(error)) => Err(error.into()),
    }
}
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("reservations: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
