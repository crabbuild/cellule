//! Persistent checkout CLI and independently owned synthetic payment receiver.
mod payment_server;
use cellule_cookbook_checkout::*;
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
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x75; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x76; 16]);
const HELP: &str = "Cellule checkout\n\n  demo STATE\n  prepare INPUT_JSON MUTATION_FILE\n  apply STATE MUTATION_FILE\n  resolve STATE MUTATION_FILE\n  order STATE ORDER_UUID\n  stock STATE SKU\n  reservation STATE ORDER_UUID\n  saga STATE ORDER_UUID\n  orders STATE [AFTER_ROW] [LIMIT]\n  serve STATE [SECONDS]\n  payment-server STATE PORT FAULT_FILE [SECONDS]\n  payment STATE ORDER_UUID\n  payment-state FAULT_FILE up|down|drop-authorize-reply\n\nPrepared operations: place, cancel, seed, reconcile. Preserve original files.\nCancellation is an intent; inspect saga, payment, and stock settlement separately.\nUnknown or expired evidence does not prove absence. Payment credentials stay in the environment.";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Input {
    Place { spec: OrderSpec },
    Cancel { order: Id },
    Seed { seed: Seed },
    Reconcile { input: Reconcile },
}
impl Input {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Place { spec } => spec.validate()?,
            Self::Seed { seed } => seed.validate()?,
            _ => {}
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
            return Err("invalid retained checkout identity".into());
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
        return Err("checkout JSON exceeds 4096 bytes".into());
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
async fn start(state: PathBuf, payment: bool) -> Result<LocalNode> {
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
            storage_prefix: object_store::path::Path::from(if payment {
                "cookbook/checkout-payments/cells"
            } else {
                "cookbook/checkout/cells"
            }),
            application_id: APPLICATION,
        },
    )
    .await?)
}
async fn dispatch<C: Command>(
    client: &CheckoutClient,
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
                return Err("retain original checkout evidence".into());
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
            Err("checkout operation durably rejected".into())
        }
        Err(source) => Err(source.into()),
    }
}
async fn apply(client: &CheckoutClient, record: MutationFile, resolve: bool) -> Result<()> {
    let identity = record.identity()?;
    match record.input {
        Input::Place { spec } => {
            dispatch(
                client,
                client
                    .prepare_order(identity, OrderChange::Place(spec))
                    .await?,
                resolve,
            )
            .await
        }
        Input::Cancel { order } => {
            dispatch(
                client,
                client
                    .prepare_order(identity, OrderChange::Cancel(order))
                    .await?,
                resolve,
            )
            .await
        }
        Input::Seed { seed } => {
            dispatch(client, client.prepare_seed(identity, seed).await?, resolve).await
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
            return Err("checkout worker failure closed readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}
async fn wait(
    client: &CheckoutClient,
    node: &LocalNode,
    id: Id,
    result: OrderResult,
) -> Result<SagaView> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    loop {
        let order = client.order(id, None).await?.output;
        let saga = client.saga(id, None).await?.output;
        if order
            .as_ref()
            .is_some_and(|value| value.status == OrderStatus::Finished(result))
            && saga.as_ref().is_some_and(|value| {
                value.status == "completed" && value.state.result == Some(result)
            })
        {
            return saga.ok_or_else(|| "completed saga absent".into());
        }
        if !node.is_ready() || tokio::time::Instant::now() >= deadline {
            return Err(format!("checkout did not settle: order={order:?}, saga={saga:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn demo(
    node: &LocalNode,
    handle: cellule_app::ApplicationHandle<CheckoutApplication>,
    client: &CheckoutClient,
    payment_node: &LocalNode,
) -> Result<()> {
    let payment_handle = payment_node.application_handle::<CheckoutApplication>(TENANT)?;
    let payments = CheckoutClient::new(payment_handle);
    payment_node
        .open_cell(&payments.target(PAYMENTS)?, &Payments)
        .await?;
    let address = payment_server::install(payment_node, payments.clone(), 0, None).await?;
    client
        .prepare_seed(
            new_identity()?,
            Seed {
                sku: "demo-widget".into(),
                units: 10000,
            },
        )
        .await?
        .execute()
        .await?;
    let make = |policy| -> Result<OrderSpec> {
        Ok(OrderSpec {
            id: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes())?,
            sku: "demo-widget".into(),
            quantity: 1,
            amount: 100,
            payment_endpoint: format!("http://{address}/"),
            payment_policy: policy,
        })
    };
    let fulfilled = make(PaymentPolicy::Approve)?;
    let declined = make(PaymentPolicy::Decline)?;
    let cancelled = make(PaymentPolicy::Approve)?;
    let mut unavailable = make(PaymentPolicy::Approve)?;
    unavailable.sku = "absent-widget".into();
    for spec in [&fulfilled, &declined, &cancelled, &unavailable] {
        client
            .prepare_order(new_identity()?, OrderChange::Place(spec.clone()))
            .await?
            .execute()
            .await?;
    }
    client
        .prepare_order(new_identity()?, OrderChange::Cancel(cancelled.id))
        .await?
        .execute()
        .await?;
    spawn_workers(node, handle).await?;
    let mut outcomes = Vec::new();
    for (spec, result) in [
        (&fulfilled, OrderResult::Fulfilled),
        (&declined, OrderResult::Declined),
        (&cancelled, OrderResult::Cancelled),
        (&unavailable, OrderResult::OutOfStock),
    ] {
        let saga = wait(client, node, spec.id, result).await?;
        let order = client
            .order(spec.id, None)
            .await?
            .output
            .ok_or("demo order missing")?;
        let stock = client
            .reservation(spec.id, None)
            .await?
            .output
            .ok_or("demo reservation missing")?;
        let payment = payments.payment(spec.id, None).await?.output;
        let expected = match result {
            OrderResult::Fulfilled => ReservationStatus::Committed,
            OrderResult::OutOfStock => ReservationStatus::Unavailable,
            _ => ReservationStatus::Released,
        };
        if stock.status != expected {
            return Err("demo inventory settlement differs".into());
        }
        match (&payment, result) {
            (Some(value), OrderResult::Fulfilled)
                if value.status == PaymentStatus::Authorized
                    && value.authorizations == 1
                    && value.voids == 0 => {}
            (Some(value), OrderResult::Cancelled)
                if value.status == PaymentStatus::Voided
                    && value.authorizations == 1
                    && value.voids == 1 => {}
            (Some(value), OrderResult::Declined)
                if value.status == PaymentStatus::Declined && value.authorizations == 0 => {}
            (None, OrderResult::OutOfStock) => {}
            _ => return Err("demo external payment settlement differs".into()),
        }
        let replay = client
            .prepare_order(new_identity()?, OrderChange::Place(spec.clone()))
            .await?
            .execute()
            .await?;
        if replay.output
            != (OrderOutcome::Accepted {
                start_effect: order.start_effect,
            })
        {
            return Err("demo placement replay changed original intent".into());
        }
        outcomes.push(
            serde_json::json!({"order":order,"reservation":stock,"payment":payment,"saga":saga}),
        );
    }
    let inventory = client
        .stock("demo-widget".into(), None)
        .await?
        .output
        .ok_or("demo inventory absent")?;
    inventory.validate()?;
    if inventory.held != 0 {
        return Err("demo leaked inventory holds".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","orders":outcomes,"inventory":inventory,"checks":["native-saga","signed-callbacks","payment-Activities","fulfilled","declined","out-of-stock","authorized-then-voided","released-once","permanent-placement-replay"]})
    );
    Ok(())
}
async fn operation(
    node: &LocalNode,
    payment_node: Option<&LocalNode>,
    args: &[String],
) -> Result<()> {
    let handle = node.application_handle::<CheckoutApplication>(TENANT)?;
    if matches!(
        args.first().map(String::as_str),
        Some("payment-server" | "payment")
    ) {
        let client = CheckoutClient::new(handle);
        node.open_cell(&client.target(PAYMENTS)?, &Payments).await?;
        return match args {
            [op, _, port, fault, rest @ ..] if op == "payment-server" && rest.len() <= 1 => {
                let address =
                    payment_server::install(node, client, port.parse()?, Some(fault.into()))
                        .await?;
                println!(
                    "{}",
                    serde_json::json!({"event":"payment_ready","address":address})
                );
                serving(node, seconds(rest.first())?).await
            }
            [op, _, id] if op == "payment" => {
                let value = client.payment(Id::try_from(id.clone())?, None).await?;
                println!(
                    "{}",
                    serde_json::json!({"payment":value.output,"receipt":receipt(value.receipt)})
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
                payment_node.ok_or("demo simulator missing")?,
            )
            .await
        }
        [op, _, file] if op == "apply" || op == "resolve" => {
            apply(&client, read(Path::new(file))?, op == "resolve").await
        }
        [op, _, id] if op == "order" => {
            let value = client.order(Id::try_from(id.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"order":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, sku] if op == "stock" => {
            let value = client.stock(sku.clone(), None).await?;
            println!(
                "{}",
                serde_json::json!({"stock":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, id] if op == "reservation" => {
            let value = client.reservation(Id::try_from(id.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"reservation":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, id] if op == "saga" => {
            let value = client.saga(Id::try_from(id.clone())?, None).await?;
            println!(
                "{}",
                serde_json::json!({"saga":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        [op, _, rest @ ..] if op == "orders" && rest.len() <= 2 => {
            let page = Page {
                after: rest.first().map(|v| v.parse()).transpose()?.unwrap_or(0),
                limit: rest.get(1).map(|v| v.parse()).transpose()?.unwrap_or(20),
            };
            let value = client.orders(page, None).await?;
            println!(
                "{}",
                serde_json::json!({"orders":value.output,"receipt":receipt(value.receipt)})
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
        && op == "payment-state"
    {
        if !matches!(mode.as_str(), "up" | "down" | "drop-authorize-reply") {
            return Err("invalid simulator state".into());
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
    let payment = matches!(
        args.first().map(String::as_str),
        Some("payment-server" | "payment")
    );
    let node = start(state.clone(), payment).await?;
    let payment_node = if args[0] == "demo" {
        match start(state.join("payments"), true).await {
            Ok(value) => Some(value),
            Err(source) => {
                node.shutdown().await?;
                return Err(source);
            }
        }
    } else {
        None
    };
    let mut result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal{Ok(()) if matches!(args[0].as_str(),"serve"|"payment-server")=>Ok(()),Ok(())=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"operation interrupted; retain original request").into()),Err(source)=>Err(source.into())},
        result=operation(&node,payment_node.as_ref(),&args)=>result,
    };
    if let Err(source) = node.shutdown().await {
        if result.is_ok() {
            result = Err(source.into());
        } else {
            tracing::error!(error=%source,"checkout drain also failed");
        }
    }
    if let Some(node) = payment_node
        && let Err(source) = node.shutdown().await
    {
        if result.is_ok() {
            result = Err(source.into());
        } else {
            tracing::error!(error=%source,"payment simulator drain also failed");
        }
    }
    result
}
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            tracing::error!(error=%source,"checkout failed");
            let mut cause = source.source();
            while let Some(source) = cause {
                tracing::error!(cause=%source,"checkout source error");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests;
