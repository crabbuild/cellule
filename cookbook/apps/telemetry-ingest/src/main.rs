//! Local shell ingress owns tenant authorization, provider configuration, retained evidence, and drain.
mod assembly;
mod demo;
mod files;
use assembly::Service;
use cellule_cookbook_support::{new_identity, now_ms, shutdown_signal};
use cellule_cookbook_telemetry_ingest::*;
use cellule_runtime::{
    Committed, InvocationError, Receipt, Resolution,
    codec::{BoundedDecoder, WireValue},
    primitives::queue::QueueSendOutcome,
};
use files::{Input, InputFile, RequestFile, Roster};
use serde::{Deserialize, Serialize};
use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
const HELP: &str = "Cellule telemetry ingest\n\n  demo STATE\n  prepare INPUT_JSON REQUEST_FILE\n  apply STATE REQUEST_FILE\n  resolve STATE REQUEST_FILE\n  device STATE TENANT DEVICE\n  lookup STATE TENANT DEVICE\n  progress STATE TENANT DEVICE\n  effect STATE TENANT DEVICE EFFECT_HEX\n  batch STATE TENANT MESSAGE_UUID\n  summaries STATE TENANT SHARD [AFTER_MS LIMIT]\n  info STATE TENANT\n  serve STATE ROSTER_JSON SECONDS [CONTROLS_JSON]\n\nInput JSON: {tenant,operation:{type:register,device,window:{start_ms,minutes}}},\n{tenant,operation:{type:record,event:{device,sequence,at_ms,value_milli}}}, or\n{tenant,operation:{type:submit,batch:{id,events:[1..8 exact events]}}}.\nRoster JSON: {tenant,devices:[1..2 distinct canonical keys]}.\nShell access authorizes one tenant. Device, Queue, audit, and summary receipts are independent.\nRetain request files unchanged; expired evidence proves no absence.\nAll unique in-window events count. Lower sequences never replace the latest reading.\nQueue claim order is not an application FIFO guarantee.";
pub(crate) fn emit<T: Serialize>(value: &T) -> Result<(), BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 128 << 10 {
        return Err("telemetry output exceeds 128 KiB".into());
    }
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
fn receipt(value: Receipt) -> SourceReceipt {
    value.into()
}
fn queue_output(value: QueueSendOutcome) -> Result<serde_json::Value, BoxError> {
    match value {
        QueueSendOutcome::Sent { message_id } => Ok(
            serde_json::json!({"decision":"sent","message":MessageId::parse(&uuid::Uuid::from_bytes(message_id).to_string())?}),
        ),
        QueueSendOutcome::ProducerConflict => {
            Ok(serde_json::json!({"decision":"producer_conflict"}))
        }
    }
}
fn domain(
    value: Result<Committed<DeviceOutcome>, InvocationError<DeviceOutcome>>,
) -> Result<(), BoxError> {
    match value {
        Ok(value) => {
            emit(&serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)}))
        }
        Err(source) => {
            if let InvocationError::Rejected(value) = &source {
                emit(
                    &serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)}),
                )?;
            }
            Err(source.into())
        }
    }
}
pub(crate) async fn apply(service: &Service, record: &RequestFile) -> Result<(), BoxError> {
    record.validate()?;
    let identity = record.identity.native()?;
    match &record.operation {
        Input::Register { device, window } => domain(
            service
                .device(device.clone())
                .await?
                .register(identity, *window)
                .await,
        ),
        Input::Record { event } => domain(
            service
                .device(event.device.clone())
                .await?
                .record(identity, event.clone())
                .await,
        ),
        Input::Submit { batch } => match service
            .producer()
            .await?
            .send(identity, batch, record.available_at_ms)
            .await
        {
            Ok(value) => emit(
                &serde_json::json!({"outcome":queue_output(value.output.clone())?,"receipt":receipt(value.receipt)}),
            ),
            Err(source) => {
                if let InvocationError::Rejected(value) = &source {
                    emit(
                        &serde_json::json!({"outcome":queue_output(value.output.clone())?,"receipt":receipt(value.receipt)}),
                    )?;
                }
                Err(source.into())
            }
        },
    }
}
fn resolved(value: Resolution, input: &Input) -> Result<(), BoxError> {
    match value {
        Resolution::Committed(value) => {
            let mut decoder = BoundedDecoder::new(value.result(), 4096)?;
            let output = match input {
                Input::Submit { .. } => queue_output(QueueSendOutcome::decode(&mut decoder)?)?,
                _ => serde_json::to_value(DeviceOutcome::decode(&mut decoder)?)?,
            };
            decoder.finish()?;
            emit(
                &serde_json::json!({"resolution":"committed","outcome":output,"commit_sequence":value.commit_sequence()}),
            )
        }
        Resolution::Absent => emit(&serde_json::json!({"resolution":"absent"})),
        Resolution::Expired => {
            emit(&serde_json::json!({"resolution":"expired"}))?;
            Err("expired original request evidence proves no absence; inspect permanent domain state and audits".into())
        }
        Resolution::Unknown => {
            emit(&serde_json::json!({"resolution":"unknown"}))?;
            Err("unknown outcome; retain original request file".into())
        }
    }
}
async fn resolve(service: &Service, record: RequestFile) -> Result<(), BoxError> {
    record.validate()?;
    let identity = record.identity.native()?;
    let resolution = match &record.operation {
        Input::Register { device, window } => {
            let client = service.device(device.clone()).await?;
            let prepared = client
                .prepare_registration(
                    identity,
                    Registration {
                        device: device.clone(),
                        window: *window,
                    },
                )
                .await?;
            client.resolve(prepared.evidence()).await?
        }
        Input::Record { event } => {
            let client = service.device(event.device.clone()).await?;
            let prepared = client.prepare_event(identity, event.clone()).await?;
            client.resolve(prepared.evidence()).await?
        }
        Input::Submit { batch } => {
            let client = service.producer().await?;
            let prepared = client
                .prepare(identity, batch, record.available_at_ms)
                .await?;
            client.resolve(prepared.evidence()).await?
        }
    };
    resolved(resolution, &record.operation)
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Controls {
    batch: Option<BatchId>,
    effect: Option<String>,
    after_event_ms: u64,
    before_ack_ms: u64,
    after_summary_ms: u64,
    drop_reply: bool,
}
fn effect_hex(text: &str) -> Result<[u8; 32], BoxError> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err("telemetry effect requires 64 lowercase hexadecimal digits".into());
    }
    let value = *blake3::Hash::from_hex(text)?.as_bytes();
    if value == [0; 32] {
        return Err("zero telemetry effect identity".into());
    }
    Ok(value)
}
impl Controls {
    fn validate(&self) -> Result<(), BoxError> {
        if self.after_event_ms > 10000
            || self.before_ack_ms > 10000
            || self.after_summary_ms > 10000
        {
            return Err("telemetry fault delays must be at most ten seconds each".into());
        }
        if let Some(effect) = &self.effect {
            effect_hex(effect)?;
        }
        Ok(())
    }
}
enum Operation {
    Apply(RequestFile),
    Resolve(RequestFile),
    Device(DeviceKey),
    Lookup(DeviceKey),
    Progress(DeviceKey),
    Effect(DeviceKey, [u8; 32]),
    Batch(MessageId),
    Summaries(u32, BucketPageRequest),
    Info,
    Serve(Roster, u64, Controls),
    Demo,
}
struct Invocation {
    state: PathBuf,
    tenant: String,
    operation: Operation,
}
fn parse(args: &[String]) -> Result<Invocation, BoxError> {
    let (state, tenant, operation) = match args {
        [op, state] if op == "demo" => (state.clone(), "cookbook-demo".into(), Operation::Demo),
        [op, state, path] if op == "apply" || op == "resolve" => {
            let record: RequestFile = files::load(Path::new(path))?;
            record.validate()?;
            (
                state.clone(),
                record.tenant.clone(),
                if op == "apply" {
                    Operation::Apply(record)
                } else {
                    Operation::Resolve(record)
                },
            )
        }
        [op, state, tenant, key] if matches!(op.as_str(), "device" | "lookup" | "progress") => {
            let key = DeviceKey::new(key.clone())?;
            (
                state.clone(),
                tenant.clone(),
                match op.as_str() {
                    "device" => Operation::Device(key),
                    "lookup" => Operation::Lookup(key),
                    _ => Operation::Progress(key),
                },
            )
        }
        [op, state, tenant, key, id] if op == "effect" => (
            state.clone(),
            tenant.clone(),
            Operation::Effect(DeviceKey::new(key.clone())?, effect_hex(id)?),
        ),
        [op, state, tenant, id] if op == "batch" => (
            state.clone(),
            tenant.clone(),
            Operation::Batch(MessageId::parse(id)?),
        ),
        [op, state, tenant] if op == "info" => (state.clone(), tenant.clone(), Operation::Info),
        [op, state, tenant, shard, rest @ ..] if op == "summaries" => {
            let shard = shard.parse()?;
            if shard >= 2 {
                return Err("telemetry summary shard must be 0 or 1".into());
            }
            let (after_ms, limit) = match rest {
                [] => (None, 16),
                [after, limit] => (Some(after.parse()?), limit.parse()?),
                _ => return Err(HELP.into()),
            };
            if !(1..=16).contains(&limit)
                || after_ms.is_some_and(|v: i64| v < 60000 || v % 60000 != 0)
            {
                return Err("invalid telemetry summary page".into());
            }
            (
                state.clone(),
                tenant.clone(),
                Operation::Summaries(shard, BucketPageRequest { after_ms, limit }),
            )
        }
        [op, state, path, seconds, rest @ ..] if op == "serve" => {
            let roster: Roster = files::load(Path::new(path))?;
            roster.validate()?;
            let seconds = seconds.parse()?;
            if !(1..=3600).contains(&seconds) {
                return Err("telemetry serve lifetime must be 1..3600 seconds".into());
            }
            let controls = match rest {
                [] => Controls::default(),
                [path] => files::load::<Controls>(Path::new(path))?,
                _ => return Err(HELP.into()),
            };
            controls.validate()?;
            (
                state.clone(),
                roster.tenant.clone(),
                Operation::Serve(roster, seconds, controls),
            )
        }
        _ => return Err(HELP.into()),
    };
    assembly::tenant(&tenant)?;
    Ok(Invocation {
        state: state.into(),
        tenant,
        operation,
    })
}
async fn serve(
    service: &Service,
    roster: Roster,
    seconds: u64,
    controls: Controls,
) -> Result<(), BoxError> {
    let (tx, mut events) = tokio::sync::mpsc::channel(32);
    let (rx, mut summaries) = tokio::sync::mpsc::channel(32);
    service
        .workers(
            &roster.devices,
            ConsumerOptions {
                controlled_batch: controls.batch,
                after_event: Duration::from_millis(controls.after_event_ms),
                before_ack: Duration::from_millis(controls.before_ack_ms),
                progress: Some(tx),
            },
            DeliveryOptions {
                controlled_effect: controls.effect.as_deref().map(effect_hex).transpose()?,
                after_publication: Duration::from_millis(controls.after_summary_ms),
                drop_reply_once: controls.drop_reply,
                progress: Some(rx),
                ..Default::default()
            },
        )
        .await?;
    emit(&serde_json::json!({"event":"ready","devices":roster.devices}))?;
    let end = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let clients = roster
        .devices
        .iter()
        .map(|key| DeviceClient::new(service.handle.clone(), key.clone()))
        .collect::<cellule_runtime::Result<Vec<_>>>()?;
    let mut seen = std::collections::BTreeMap::<DeviceKey, Vec<u8>>::new();
    let mut observations = tokio::time::interval(Duration::from_secs(1));
    observations.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if !service.source.is_ready() || !service.summary.is_ready() {
            return Err(
                "telemetry worker failed readiness; preserve request and native evidence".into(),
            );
        }
        tokio::select! {
               _=tokio::time::sleep_until(end)=>return Ok(()),
               Some(value)=events.recv()=>emit(&serde_json::json!({"event":"consumer","progress":value}))?,
               Some(value)=summaries.recv()=>emit(&serde_json::json!({"event":"summary","progress":value}))?,
               _=observations.tick()=>{
          for (key,client) in roster.devices.iter().zip(&clients) {
            let value=match client.progress(None).await {Ok(value)=>value,Err(ProgressError::Changed)=>continue,Err(source)=>return Err(source.into())};
            let bytes=serde_json::to_vec(&value.output)?;
            if seen.get(key)!=Some(&bytes) {
              emit(&serde_json::json!({"event":"projection_progress","device":key,"progress":value.output,"receipt":receipt(value.receipt)}))?;
              seen.insert(key.clone(),bytes);
            }
          }
        },
        _=tokio::time::sleep(Duration::from_millis(100))=>{}
              }
    }
}
async fn operation(service: &Service, state: &Path, operation: Operation) -> Result<(), BoxError> {
    match operation {
        Operation::Apply(record) => apply(service, &record).await,
        Operation::Resolve(record) => resolve(service, record).await,
        Operation::Device(key) => {
            let value = service.device(key).await?.get(None).await?;
            emit(&serde_json::json!({"device":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Lookup(key) => {
            let value = service.lookup(&key).await?.get(key, None).await?;
            emit(&serde_json::json!({"summary":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Progress(key) => {
            let value = service.device(key).await?.progress(None).await?;
            emit(&serde_json::json!({"progress":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Effect(key, id) => {
            let value = service.device(key).await?.effect_status(id, None).await?;
            let status=value.output.map(|v|serde_json::json!({"effect_id":blake3::Hash::from_bytes(id).to_hex().to_string(),"state":format!("{:?}",v.state),"attempt":v.attempt,"token_present":v.token_present,"lease_until_ms":v.lease_until_ms,"result":v.result}));
            emit(&serde_json::json!({"effect":status,"receipt":receipt(value.receipt)}))
        }
        Operation::Batch(id) => {
            let value = service.audit().await?.get(id, None).await?;
            emit(&serde_json::json!({"batch":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Summaries(shard, page) => {
            let value = service.summaries(shard).await?.list(page, None).await?;
            emit(
                &serde_json::json!({"summary_shard":shard,"page":value.output,"receipt":receipt(value.receipt)}),
            )
        }
        Operation::Info => {
            service.producer().await?;
            let value = service.handle.queue::<Ingress>()?.info(0, None).await?;
            emit(
                &serde_json::json!({"info":{"ready":value.output.ready,"leased":value.output.leased,"acked":value.output.acked,"dead":value.output.dead,"paused":value.output.paused},"receipt":receipt(value.receipt)}),
            )
        }
        Operation::Serve(roster, seconds, controls) => {
            serve(service, roster, seconds, controls).await
        }
        Operation::Demo => demo::run(service, state).await,
    }
}
async fn run() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() || matches!(args.as_slice(),[op] if op=="help" || op=="--help") {
        println!("{HELP}");
        return Ok(());
    }
    if let [op, input, path] = args.as_slice()
        && op == "prepare"
    {
        let input: InputFile = files::load(Path::new(input))?;
        assembly::tenant(&input.tenant)?;
        input.operation.validate()?;
        let record = RequestFile {
            version: 1,
            tenant: input.tenant,
            identity: new_identity()?.into(),
            available_at_ms: now_ms()?,
            operation: input.operation,
        };
        record.validate()?;
        files::save(Path::new(path), &serde_json::to_vec(&record)?)?;
        return emit(
            &serde_json::json!({"prepared":path,"expires_at_ms":record.identity.expires_at_ms}),
        );
    }
    let invocation = parse(&args)?;
    if let Operation::Resolve(record) = &invocation.operation
        && record.identity.expires_at_ms <= now_ms()?
    {
        return resolved(Resolution::Expired, &record.operation);
    }
    let serving = matches!(&invocation.operation, Operation::Serve(..));
    let service = Service::start(invocation.state.clone(), &invocation.tenant).await?;
    let result = tokio::select! {biased;signal=shutdown_signal()=>match signal{Ok(()) if serving=>Ok(()),Ok(())=>Err("interrupted; retain the original telemetry request or active demo plan and resolve its outcome".into()),Err(source)=>Err(source.into())},result=operation(&service,&invocation.state,invocation.operation)=>result};
    let cleanup = service.shutdown().await;
    if cleanup.is_ok()
        && let Err(output) = emit(&serde_json::json!({"event":"drained"}))
    {
        if result.is_ok() {
            return Err(output);
        }
        tracing::error!(%output,"telemetry drain observation failed");
    }
    match (result, cleanup) {
        (Err(source), Err(cleanup)) => {
            tracing::error!(%cleanup,"telemetry drain also failed");
            Err(source)
        }
        (Err(source), _) => Err(source),
        (_, Err(source)) => Err(source),
        _ => Ok(()),
    }
}
#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            tracing::error!(%source,"telemetry command failed");
            let mut cause = source.source();
            while let Some(value) = cause {
                tracing::error!(cause=%value,"telemetry source");
                cause = value.source();
            }
            ExitCode::FAILURE
        }
    }
}
