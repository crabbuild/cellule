//! Shell ingress owns tenant authority, providers, retained evidence, and process lifetime.
mod assembly;
mod demo;
mod files;
mod http;
mod ingress;
mod receiver;

use assembly::Service;
use cellule_cookbook_support::{new_identity, now_ms, shutdown_signal};
use cellule_cookbook_support_desk::*;
use cellule_runtime::{
    Committed, InvocationError, Receipt, Resolution,
    codec::{BoundedDecoder, WireValue},
    primitives::blob::BlobMutationOutcome,
};
use files::{InputFile, PlanFile, RequestFile, Roster};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

const HELP: &str = "Cellule support desk\n\n  demo STATE\n  prepare INPUT_JSON REQUEST_FILE\n  prepare-attachment TENANT TICKET ATTACHMENT NAME SOURCE REVISION PLAN_FILE\n  apply STATE REQUEST_FILE\n  resolve STATE REQUEST_FILE\n  get STATE TENANT TICKET\n  messages STATE TENANT TICKET [AFTER LIMIT]\n  publish-attachment STATE PLAN_FILE [AFTER_PUBLICATION_MS]\n  reconcile-attachment STATE PLAN_FILE\n  resolve-attachment STATE PLAN_FILE [begin|part|complete|link]\n  download-attachment STATE PLAN_FILE OUTPUT_FILE\n  progress STATE TENANT TICKET\n  effect STATE TENANT TICKET EFFECT_HEX\n  callback-effect STATE TENANT EFFECT_HEX\n  serve STATE ROSTER_JSON SECONDS [CONTROLS_JSON]\n  receiver RECEIVER_STATE PORT [CONTROLS_JSON]\n  receiver-record RECEIVER_STATE NOTIFICATION_KEY\n\nInput JSON: {tenant,change:{ticket,action:{type:open|message|assign|resolve|reopen,...}}}.\nRoster JSON: {tenant,tickets:[1..2 canonical keys],port,requester,agent,notification_endpoint}.\nShell access authorizes one tenant; HTTP customer and agent capabilities bind configured actors.\nThe receiver uses CELLULE_SUPPORT_DESK_TOKEN.\nSource, Blob, native coordination, and external acknowledgments are independent.\nRetain original request files and attachment plans unchanged; expired evidence proves no absence.";

pub(crate) fn emit(value: &impl Serialize) -> Result<(), BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > files::LIMIT {
        return Err("support-desk output exceeds 384 KiB".into());
    }
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
pub(crate) fn receipt(value: Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn domain(value: Result<Committed<Outcome>, InvocationError<Outcome>>) -> Result<(), BoxError> {
    match value {
        Ok(value) => {
            emit(&serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)}))
        }
        Err(source) => {
            match &source {
                InvocationError::Rejected(value) => emit(
                    &serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)}),
                )?,
                InvocationError::Pending(_) => emit(
                    &serde_json::json!({"resolution":"unknown","retain_original_request":true}),
                )?,
                InvocationError::InvalidPublishedResult { receipt: value, .. } => emit(
                    &serde_json::json!({"resolution":"published_result_invalid","receipt":receipt(*value),"retain_original_request":true}),
                )?,
                InvocationError::NotStarted(_) => {}
            }
            Err(source.into())
        }
    }
}
fn resolved(value: Resolution, blob: bool) -> Result<(), BoxError> {
    match value {
        Resolution::Committed(value) => {
            let mut decoder = BoundedDecoder::new(value.result(), (128 << 10) + 1024)?;
            let output = if blob {
                match BlobMutationOutcome::decode(&mut decoder)? {
                    BlobMutationOutcome::Begun => serde_json::json!({"decision":"begun"}),
                    BlobMutationOutcome::PartStored { digest } => {
                        serde_json::json!({"decision":"part_stored","digest":digest})
                    }
                    BlobMutationOutcome::Committed { etag, size } => {
                        serde_json::json!({"decision":"committed","etag":etag,"size":size})
                    }
                    BlobMutationOutcome::Aborted => serde_json::json!({"decision":"aborted"}),
                    BlobMutationOutcome::Deleted => serde_json::json!({"decision":"deleted"}),
                    BlobMutationOutcome::NotFound => serde_json::json!({"decision":"not_found"}),
                    BlobMutationOutcome::Conflict => serde_json::json!({"decision":"conflict"}),
                }
            } else {
                serde_json::to_value(Outcome::decode(&mut decoder)?)?
            };
            decoder.finish()?;
            emit(
                &serde_json::json!({"resolution":"committed","outcome":output,"commit_sequence":value.commit_sequence()}),
            )
        }
        Resolution::Absent => emit(&serde_json::json!({"resolution":"absent"})),
        Resolution::Expired => {
            emit(&serde_json::json!({"resolution":"expired"}))?;
            Err("expired original evidence proves no absence; inspect permanent ticket or verified attachment state".into())
        }
        Resolution::Unknown => {
            emit(&serde_json::json!({"resolution":"unknown"}))?;
            Err("unknown support request outcome; retain the original file".into())
        }
    }
}
pub(crate) fn effect_hex(text: &str) -> Result<[u8; 32], BoxError> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err("support identity requires 64 lowercase hexadecimal digits".into());
    }
    let value = *blake3::Hash::from_hex(text)?.as_bytes();
    if value == [0; 32] {
        return Err("zero support identity".into());
    }
    Ok(value)
}
fn delay(text: &str) -> Result<Duration, BoxError> {
    let milliseconds = text.parse::<u64>()?;
    if milliseconds > 10000 {
        return Err("support fault delay must be at most ten seconds".into());
    }
    Ok(Duration::from_millis(milliseconds))
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Controls {
    deadline: Option<Deadline>,
    before_escalation_ms: u64,
    after_publication_ms: u64,
    drop_reply_once: bool,
}
impl Controls {
    fn validate(&self, roster: &Roster) -> Result<(), BoxError> {
        if self.before_escalation_ms > 10000 || self.after_publication_ms > 10000 {
            return Err("support callback delays must be at most ten seconds each".into());
        }
        if let Some(deadline) = &self.deadline {
            deadline.validate()?;
            if !roster.tickets.contains(&deadline.ticket) {
                return Err("controlled deadline is outside support roster".into());
            }
        }
        Ok(())
    }
}
enum Operation {
    Demo(String),
    Apply(RequestFile),
    Resolve(RequestFile),
    Get(TicketKey),
    Messages(TicketKey, PageRequest),
    Publish(PlanFile, Duration),
    Reconcile(PlanFile),
    ResolveAttachment(PlanFile, Option<AttachmentPhase>),
    Download(PlanFile, PathBuf),
    Progress(TicketKey),
    Effect(TicketKey, [u8; 32]),
    CallbackEffect([u8; 32]),
    Serve(Roster, u64, Controls),
}
struct Invocation {
    state: PathBuf,
    tenant: String,
    operation: Operation,
}
fn parse(args: &[String]) -> Result<Invocation, BoxError> {
    let (state, tenant, operation) = match args {
        [op, state] if op == "demo" => {
            let tenant = demo::tenant_for_state(Path::new(state))?;
            (state.clone(), tenant.clone(), Operation::Demo(tenant))
        }
        [op, state, path] if matches!(op.as_str(), "apply" | "resolve") => {
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
        [op, state, tenant, key] if matches!(op.as_str(), "get" | "progress") => {
            let key = TicketKey::new(key.clone())?;
            (
                state.clone(),
                tenant.clone(),
                if op == "get" {
                    Operation::Get(key)
                } else {
                    Operation::Progress(key)
                },
            )
        }
        [op, state, tenant, key, rest @ ..] if op == "messages" => {
            let (after, limit) = match rest {
                [] => (0, 16),
                [after, limit] => (after.parse()?, limit.parse()?),
                _ => return Err(HELP.into()),
            };
            if after > MAX_MESSAGES as u32 || !(1..=16).contains(&limit) {
                return Err("invalid bounded support conversation page".into());
            }
            (
                state.clone(),
                tenant.clone(),
                Operation::Messages(TicketKey::new(key.clone())?, PageRequest { after, limit }),
            )
        }
        [op, state, path, rest @ ..]
            if matches!(
                op.as_str(),
                "publish-attachment"
                    | "reconcile-attachment"
                    | "resolve-attachment"
                    | "download-attachment"
            ) =>
        {
            let record: PlanFile = files::load(Path::new(path))?;
            record.validate()?;
            let operation = match (op.as_str(), rest) {
                ("publish-attachment", []) => Operation::Publish(record.clone(), Duration::ZERO),
                ("publish-attachment", [wait]) => Operation::Publish(record.clone(), delay(wait)?),
                ("reconcile-attachment", []) => Operation::Reconcile(record.clone()),
                ("resolve-attachment", []) => Operation::ResolveAttachment(record.clone(), None),
                ("resolve-attachment", [phase]) => Operation::ResolveAttachment(
                    record.clone(),
                    match phase.as_str() {
                        "begin" => Some(AttachmentPhase::Begin),
                        "part" => Some(AttachmentPhase::Part),
                        "complete" => Some(AttachmentPhase::Complete),
                        "link" => None,
                        _ => return Err("unknown support attachment phase".into()),
                    },
                ),
                ("download-attachment", [output]) => {
                    Operation::Download(record.clone(), output.into())
                }
                _ => return Err(HELP.into()),
            };
            (state.clone(), record.tenant, operation)
        }
        [op, state, tenant, key, id] if op == "effect" => (
            state.clone(),
            tenant.clone(),
            Operation::Effect(TicketKey::new(key.clone())?, effect_hex(id)?),
        ),
        [op, state, tenant, id] if op == "callback-effect" => (
            state.clone(),
            tenant.clone(),
            Operation::CallbackEffect(effect_hex(id)?),
        ),
        [op, state, path, seconds, rest @ ..] if op == "serve" => {
            let roster: Roster = files::load(Path::new(path))?;
            roster.validate()?;
            let seconds = seconds.parse()?;
            if !(1..=3600).contains(&seconds) {
                return Err("support serve lifetime must be 1..3600 seconds".into());
            }
            let controls = match rest {
                [] => Controls::default(),
                [path] => files::load(Path::new(path))?,
                _ => return Err(HELP.into()),
            };
            controls.validate(&roster)?;
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
    let (sender, mut progress) = tokio::sync::mpsc::channel(32);
    service
        .workers(
            &roster.tickets,
            DeliveryOptions {
                controlled_deadline: controls.deadline,
                before_escalation: Duration::from_millis(controls.before_escalation_ms),
                after_publication: Duration::from_millis(controls.after_publication_ms),
                drop_reply_once: controls.drop_reply_once,
                progress: Some(sender),
            },
        )
        .await?;
    let address = ingress::install(service, roster.clone()).await?;
    emit(
        &serde_json::json!({"event":"ready","address":address.to_string(),"tickets":roster.tickets}),
    )?;
    let end = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        if !service.source.is_ready() || !service.coordinator.is_ready() {
            return Err("support coordination failed readiness; retain original native and external evidence".into());
        }
        tokio::select! {
            _=tokio::time::sleep_until(end)=>return Ok(()),
            Some(value)=progress.recv()=>emit(&serde_json::json!({"event":"coordination","progress":value}))?,
            _=tokio::time::sleep(Duration::from_millis(100))=>{},
        }
    }
}
fn effect_status(
    id: [u8; 32],
    value: cellule_runtime::primitives::effects::EffectStatus,
) -> serde_json::Value {
    serde_json::json!({"effect_id":blake3::Hash::from_bytes(id).to_hex().to_string(),"state":format!("{:?}",value.state),"attempt":value.attempt,"token_present":value.token_present,"lease_until_ms":value.lease_until_ms,"result":value.result})
}
async fn operation(service: &Service, state: &Path, operation: Operation) -> Result<(), BoxError> {
    match operation {
        Operation::Demo(tenant) => demo::run(service, state, &tenant).await,
        Operation::Apply(record) => domain(
            service
                .ticket(record.change.ticket)
                .await?
                .change(record.identity.native()?, record.change.action)
                .await,
        ),
        Operation::Resolve(record) => {
            let client = service.ticket(record.change.ticket).await?;
            let prepared = client
                .prepare(record.identity.native()?, record.change.action)
                .await?;
            resolved(client.resolve(prepared.evidence()).await?, false)
        }
        Operation::Get(key) => {
            let value = service.ticket(key).await?.get(None).await?;
            emit(&serde_json::json!({"ticket":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Messages(key, page) => {
            let value = service.ticket(key).await?.messages(page, None).await?;
            emit(&serde_json::json!({"page":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Publish(record, wait) => {
            let object = service.attachments().await?.publish(&record.plan).await?;
            emit(
                &serde_json::json!({"event":"attachment_published","publication":object.publication,"ticket_linked":false}),
            )?;
            tokio::time::sleep(wait).await;
            Ok(())
        }
        Operation::Reconcile(record) => {
            service
                .ticket(record.plan.descriptor.ticket.clone())
                .await?;
            let prepared = service
                .attachments()
                .await?
                .prepare_link(&record.plan)
                .await?;
            domain(prepared.execute().await)
        }
        Operation::ResolveAttachment(record, phase) => {
            let attachments = service.attachments().await?;
            if let Some(phase) = phase {
                let prepared = attachments.prepare(&record.plan, phase).await?;
                resolved(attachments.resolve_phase(prepared.evidence()).await?, true)
            } else {
                service
                    .ticket(record.plan.descriptor.ticket.clone())
                    .await?;
                resolved(attachments.resolve_link(&record.plan).await?, false)
            }
        }
        Operation::Download(record, path) => {
            let value = service
                .attachments()
                .await?
                .read(record.plan.descriptor.key(), None)
                .await?;
            let object = value.output.ok_or("support attachment absent")?;
            if object.publication.descriptor != record.plan.descriptor
                || object.bytes != record.plan.bytes
            {
                return Err("support download differs from original plan".into());
            }
            files::save(&path, &object.bytes)?;
            emit(
                &serde_json::json!({"downloaded":path,"publication":object.publication,"blob_receipt":receipt(value.receipt)}),
            )
        }
        Operation::Progress(key) => {
            let value = service.ticket(key.clone()).await?.get(None).await?;
            let coordinated = service.coordinated(key).await?;
            let deadline = match &value.output {
                Some(ticket) => coordinated.deadline(&ticket.deadline).await?,
                None => None,
            };
            let notification = match value
                .output
                .as_ref()
                .and_then(|ticket| ticket.notifications.last())
            {
                Some(record) => coordinated.notification(&record.notification).await?,
                None => None,
            };
            emit(
                &serde_json::json!({"ticket":value.output,"ticket_receipt":receipt(value.receipt),"deadline":deadline,"latest_notification":notification}),
            )
        }
        Operation::Effect(key, id) => {
            let client = service.ticket(key).await?;
            let value = service
                .handle
                .effects::<Tickets>(client.target().clone())?
                .status(id, None)
                .await?;
            emit(
                &serde_json::json!({"effect":value.output.map(|v|effect_status(id,v)),"receipt":receipt(value.receipt)}),
            )
        }
        Operation::CallbackEffect(id) => {
            let target = service
                .coordination
                .target_for_scope(DEADLINES, b"deadlines")?;
            service.coordinator.open_cell(&target, &Deadlines).await?;
            let value = service
                .coordination
                .effects::<Deadlines>(target)?
                .status(id, None)
                .await?;
            emit(
                &serde_json::json!({"effect":value.output.map(|v|effect_status(id,v)),"receipt":receipt(value.receipt)}),
            )
        }
        Operation::Serve(roster, seconds, controls) => {
            serve(service, roster, seconds, controls).await
        }
    }
}
async fn run() -> Result<(), BoxError> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {
            println!("{HELP}");
            return Ok(());
        }
        [op] if matches!(op.as_str(), "help" | "--help") => {
            println!("{HELP}");
            return Ok(());
        }
        [op, input, path] if op == "prepare" => {
            let input: InputFile = files::load(Path::new(input))?;
            let record = RequestFile {
                version: 1,
                tenant: input.tenant,
                identity: new_identity()?.into(),
                change: input.change,
            };
            record.validate()?;
            files::save(Path::new(path), &serde_json::to_vec(&record)?)?;
            return emit(
                &serde_json::json!({"prepared":path,"expires_at_ms":record.identity.expires_at_ms}),
            );
        }
        [op, tenant, ticket, id, name, source, revision, path] if op == "prepare-attachment" => {
            assembly::tenant(tenant)?;
            let file = std::fs::File::open(source)?;
            if !file.metadata()?.is_file() {
                return Err("support attachment source must be a regular file".into());
            }
            let mut bytes = Vec::new();
            file.take(MAX_ATTACHMENT_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            let descriptor = AttachmentDescriptor::new(
                TicketKey::new(ticket.clone())?,
                AttachmentId::new(id.clone())?,
                name.clone(),
                &bytes,
            )?;
            let record = PlanFile {
                tenant: tenant.clone(),
                plan: AttachmentPlan::new(descriptor, bytes, revision.parse()?)?,
            };
            record.validate()?;
            files::save(Path::new(path), &serde_json::to_vec(&record)?)?;
            return emit(&serde_json::json!({"prepared":path,"descriptor":record.plan.descriptor}));
        }
        [op, root, port, rest @ ..] if op == "receiver" => {
            let controls = match rest {
                [] => receiver::Controls::default(),
                [path] => files::load(Path::new(path))?,
                _ => return Err(HELP.into()),
            };
            return receiver::run(root.into(), port.parse()?, controls).await;
        }
        [op, root, key] if op == "receiver-record" => {
            return emit(
                &serde_json::json!({"external_record":receiver::read(Path::new(root),key)?}),
            );
        }
        _ => {}
    }
    let invocation = parse(&args)?;
    let expired = match &invocation.operation {
        Operation::Resolve(record) => Some((record.identity.expires_at_ms, false)),
        Operation::ResolveAttachment(record, phase) => Some((
            record.plan.identities[match phase {
                Some(AttachmentPhase::Begin) => 0,
                Some(AttachmentPhase::Part) => 1,
                Some(AttachmentPhase::Complete) => 2,
                None => 3,
            }]
            .expires_at_ms,
            phase.is_some(),
        )),
        _ => None,
    };
    if let Some((expires, blob)) = expired
        && expires <= now_ms()?
    {
        return resolved(Resolution::Expired, blob);
    }
    let serving = matches!(&invocation.operation, Operation::Serve(..));
    let demo_state =
        matches!(&invocation.operation, Operation::Demo(_)).then(|| invocation.state.clone());
    let service = Service::start(invocation.state.clone(), &invocation.tenant).await?;
    let result = tokio::select! {biased;
        signal=shutdown_signal()=>match signal {Ok(()) if serving=>Ok(()),Ok(())=>Err("interrupted; retain original support request or attachment plan and resolve its outcome".into()),Err(source)=>Err(source.into())},
        result=operation(&service,&invocation.state,invocation.operation)=>result,
    };
    let cleanup = service.shutdown().await;
    let finalization = if result.is_ok() && cleanup.is_ok() {
        match &demo_state {
            Some(state) => demo::finalize(state),
            None => Ok(()),
        }
    } else {
        Ok(())
    };
    if cleanup.is_ok()
        && let Err(output) = emit(&serde_json::json!({"event":"drained"}))
    {
        if result.is_ok() {
            return Err(output);
        }
        files::log_source(output.as_ref(), "support drain observation failed");
    }
    match (result, cleanup, finalization) {
        (Err(source), Err(cleanup), _) => {
            files::log_source(cleanup.as_ref(), "support drain also failed");
            Err(source)
        }
        (Err(source), _, _) => Err(source),
        (_, Err(source), _) => Err(source),
        (_, _, Err(source)) => Err(source),
        _ => Ok(()),
    }
}
#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            files::log_source(source.as_ref(), "support command failed");
            ExitCode::FAILURE
        }
    }
}
