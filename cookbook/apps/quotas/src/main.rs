//! Persistent local CLI ingress. Shell access is the development authorization boundary.
use cellule_cookbook_quotas::{
    Action, Change, Credits, CustomerKey, Decision, Outcome, PageRequest, QuotaClient, Quotas,
    ReservationId, ReservationState, compile,
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
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0xd2; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xd3; 16]);
const PREFIX: &str = "cookbook/quotas";
const HELP: &str = "Cellule credit quotas\n\n  demo STATE_DIRECTORY\n  prepare CHANGE_JSON MUTATION_FILE\n  apply STATE_DIRECTORY MUTATION_FILE [AFTER_PUBLICATION_MS]\n  resolve STATE_DIRECTORY MUTATION_FILE\n  race STATE_DIRECTORY RESERVE_MUTATION_A RESERVE_MUTATION_B\n  account STATE_DIRECTORY CUSTOMER\n  list STATE_DIRECTORY CUSTOMER [AFTER_UUID] [LIMIT]\n  serve STATE_DIRECTORY CUSTOMER [SECONDS]\n\nChange JSON: customer and action { operation: open/set_allowance/reserve/consume/release, ... }.\nReserve business IDs are permanent; consume and release are mutually exclusive terminal operations.\nRetain prepared files unchanged; expired outcome evidence never proves absence.";
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
        serde_json::json!({"prepared":output,"customer":record.change.customer,"request_id":record.request_id})
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
async fn client(node: &LocalNode, customer: CustomerKey) -> Result<QuotaClient> {
    let client = QuotaClient::new(node.application_handle::<Quotas>(TENANT)?, customer)?;
    node.open_cell(client.target(), &Credits).await?;
    Ok(client)
}
async fn apply(
    client: &QuotaClient,
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
            Err("quota action durably rejected".into())
        }
        Err(error) => Err(error.into()),
    }
}
async fn race(node: &LocalNode, a: MutationFile, b: MutationFile) -> Result<()> {
    let ia = a.identity()?;
    let ib = b.identity()?;
    if a.change.customer != b.change.customer
        || ia.request_id == ib.request_id
        || !matches!((&a.change.action,&b.change.action),(Action::Reserve{id:a,..},Action::Reserve{id:b,..}) if a!=b)
    {
        return Err(
            "race requires distinct reservation and request IDs for the same customer".into(),
        );
    }
    let client = client(node, a.change.customer).await?;
    let a = client.prepare(ia, a.change.action).await?;
    let b = client.prepare(ib, b.change.action).await?;
    let (a, b) = tokio::join!(a.execute(), b.execute());
    let render = |value: std::result::Result<
        cellule_runtime::Committed<Outcome>,
        InvocationError<Outcome>,
    >|
     -> Result<serde_json::Value> {
        let (rejected, value) = match value {
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
async fn serve(node: &LocalNode, customer: CustomerKey, seconds: Option<u64>) -> Result<()> {
    if seconds.is_some_and(|s| s == 0 || s > 3600) {
        return Err("serving seconds must be 1..3600".into());
    }
    let client = client(node, customer).await?;
    println!(
        "{}",
        serde_json::json!({"event":"ready","cell":format!("{:?}",client.target().cell_id())})
    );
    std::io::stdout().flush()?;
    let deadline = seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !node.is_ready() {
            return Err("quota node lost readiness".into());
        }
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            return Ok(());
        }
    }
}
fn reservation_id() -> Result<ReservationId> {
    Ok(ReservationId::from_bytes(*uuid::Uuid::now_v7().as_bytes())?)
}
async fn demo(node: &LocalNode) -> Result<()> {
    let customer = CustomerKey::new(format!("demo-{}", uuid::Uuid::now_v7()))?;
    let client = client(node, customer.clone()).await?;
    client
        .change(new_identity()?, Action::Open { allowance: 100 })
        .await?;
    let ids = [reservation_id()?, reservation_id()?];
    let mutations = [new_identity()?, new_identity()?];
    let (a, b) = tokio::join!(
        client.change(
            mutations[0],
            Action::Reserve {
                id: ids[0],
                credits: 75
            }
        ),
        client.change(
            mutations[1],
            Action::Reserve {
                id: ids[1],
                credits: 75
            }
        )
    );
    let (winner, loser, rejected) = match (a, b) {
        (Ok(v), Err(InvocationError::Rejected(e))) if v.output.decision == Decision::Reserved => {
            (0, 1, e)
        }
        (Err(InvocationError::Rejected(e)), Ok(v)) if v.output.decision == Decision::Reserved => {
            (1, 0, e)
        }
        _ => return Err("reservation race did not have one winner".into()),
    };
    if rejected.output.decision != Decision::Insufficient {
        return Err("loser did not report insufficient credits".into());
    }
    let release = new_identity()?;
    let returned = client
        .change(release, Action::Release { id: ids[winner] })
        .await?;
    if client
        .change(release, Action::Release { id: ids[winner] })
        .await?
        != returned
    {
        return Err("release replay changed its original outcome".into());
    }
    let again = client
        .change(new_identity()?, Action::Release { id: ids[winner] })
        .await?;
    if again.output.decision != Decision::AlreadyReleased
        || again.output.account != returned.output.account
    {
        return Err("fresh release returned credits twice".into());
    }
    let Err(InvocationError::Rejected(replayed)) = client
        .change(
            mutations[loser],
            Action::Reserve {
                id: ids[loser],
                credits: 75,
            },
        )
        .await
    else {
        return Err("original rejection changed after credits became available".into());
    };
    if replayed != rejected {
        return Err("durable rejection changed".into());
    }
    let terminal = client
        .change(
            new_identity()?,
            Action::Reserve {
                id: ids[winner],
                credits: 75,
            },
        )
        .await?;
    if terminal.output.decision != Decision::ExistingReservation
        || terminal
            .output
            .reservation
            .as_ref()
            .is_none_or(|r| r.state != ReservationState::Released)
    {
        return Err("released business ID was reused".into());
    }
    let consume = reservation_id()?;
    client
        .change(
            new_identity()?,
            Action::Reserve {
                id: consume,
                credits: 25,
            },
        )
        .await?;
    let spent = client
        .change(new_identity()?, Action::Consume { id: consume })
        .await?;
    let repeated = client
        .change(new_identity()?, Action::Consume { id: consume })
        .await?;
    if repeated.output.decision != Decision::AlreadyConsumed
        || repeated.output.account != spent.output.account
    {
        return Err("consumption was duplicated".into());
    }
    let Err(InvocationError::Rejected(closed)) = client
        .change(new_identity()?, Action::Release { id: consume })
        .await
    else {
        return Err("consumed reservation was refunded".into());
    };
    if closed.output.decision != Decision::Closed {
        return Err("terminal consumption did not reject release".into());
    }
    let page = client
        .list(
            PageRequest {
                after: None,
                limit: 100,
            },
            Some(repeated.receipt),
        )
        .await?;
    let account = page.output.account.as_ref().ok_or("account missing")?;
    if account.consumed != 25
        || account.reserved != 0
        || account.available != 75
        || page.output.reservations.len() != 2
    {
        return Err("quota accounting did not reconcile".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","customer":customer,"account":account,"reservations":page.output.reservations,"receipt":receipt(page.receipt),"checks":["concurrent-reservations","allowance-invariant","retained-release-replay","fresh-release-idempotency","durable-insufficient-rejection","permanent-business-identities","consumption-once","terminal-release-refusal","coherent-account-page"]})
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
            let client = client(node, record.change.customer.clone()).await?;
            apply(&client, record, op == "resolve", delay).await
        }
        [op, _, a, b] if op == "race" => {
            race(node, read_json(Path::new(a))?, read_json(Path::new(b))?).await
        }
        [op, _, customer] if op == "account" => {
            let read = client(node, CustomerKey::new(customer.clone())?)
                .await?
                .account(None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"account":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, customer, rest @ ..] if op == "list" && rest.len() <= 2 => {
            let after = rest
                .first()
                .filter(|s| s.as_str() != "-")
                .map(|s| ReservationId::parse(s))
                .transpose()?;
            let limit = rest.get(1).map(|s| s.parse()).transpose()?.unwrap_or(20);
            let read = client(node, CustomerKey::new(customer.clone())?)
                .await?
                .list(PageRequest { after, limit }, None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"page":read.output,"receipt":receipt(read.receipt)})
            );
            Ok(())
        }
        [op, _, customer, rest @ ..] if op == "serve" && rest.len() <= 1 => {
            serve(
                node,
                CustomerKey::new(customer.clone())?,
                rest.first().map(|s| s.parse()).transpose()?,
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
                tracing::error!(%cleanup,"quota drain failed after operation failure");
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
            eprintln!("quotas: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
