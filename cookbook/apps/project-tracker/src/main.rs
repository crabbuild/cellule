//! Local shell ingress owns tenant selection, providers, retained files, and process lifetime.
mod assembly;
mod demo;
mod files;
use assembly::Service;
use cellule_cookbook_project_tracker::{
    AttachmentDescriptor, AttachmentId, AttachmentPlan, BoxError, ChangeOutcome,
    DashboardPageRequest, DeliveryOptions, IssueId, ProjectChange, ProjectKey, spawn_delivery,
};
use cellule_cookbook_support::{new_identity, shutdown_signal};
use cellule_runtime::{
    Committed, InvocationError, Receipt, Resolution,
    codec::{BoundedDecoder, WireValue},
};
use files::{ChangeFile, PlanFile, Roster};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
const HELP: &str = "Cellule project tracker\n\n  demo STATE\n  prepare INPUT_JSON REQUEST_FILE\n  prepare-attachment TENANT PROJECT ISSUE ATTACHMENT NAME SOURCE ISSUE_REVISION PLAN_FILE\n  apply STATE REQUEST_FILE\n  resolve STATE REQUEST_FILE\n  publish STATE PLAN_FILE [AFTER_PUBLICATION_MS]\n  reconcile STATE PLAN_FILE\n  resolve-link STATE PLAN_FILE\n  download STATE PLAN_FILE OUTPUT_FILE\n  get STATE TENANT PROJECT\n  issue STATE TENANT PROJECT ISSUE\n  lookup STATE TENANT PROJECT\n  progress STATE TENANT PROJECT\n  effect STATE TENANT PROJECT EFFECT_HEX\n  dashboard STATE TENANT [AFTER_PROJECT] [LIMIT]\n  serve STATE ROSTER_JSON SECONDS BEFORE_DELIVERY_MS AFTER_PUBLICATION_MS [drop-reply]\n\nInput JSON: {tenant, change:{project, mutation:{operation,...}}}.\nRoster JSON: {tenant, projects:[1..2 canonical project keys]}.\nShell access is the local authorization boundary; each invocation binds one tenant.\nPublication, source linking, and dashboard delivery have separate receipts.\nRetain files unchanged. Reconciliation never refreshes issue revisions or original windows.";
fn emit<T: Serialize>(value: &T) -> Result<(), BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 384 << 10 {
        return Err("tracker output exceeds 384 KiB".into());
    }
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
fn receipt(value: Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn outcome(
    value: Result<Committed<ChangeOutcome>, InvocationError<ChangeOutcome>>,
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
fn resolution(value: Resolution) -> Result<(), BoxError> {
    match value {
        Resolution::Committed(value) => {
            let mut decoder = BoundedDecoder::new(value.result(), 1024)?;
            let outcome = ChangeOutcome::decode(&mut decoder)?;
            decoder.finish()?;
            emit(
                &serde_json::json!({"resolution":"committed","outcome":outcome,"commit_sequence":value.commit_sequence()}),
            )
        }
        Resolution::Absent => emit(&serde_json::json!({"resolution":"absent"})),
        Resolution::Expired => {
            emit(&serde_json::json!({"resolution":"expired"}))?;
            Err("expired request evidence proves no absence; inspect permanent domain state".into())
        }
        Resolution::Unknown => {
            emit(&serde_json::json!({"resolution":"unknown"}))?;
            Err("unknown request outcome; retain the original file".into())
        }
    }
}
fn delay(value: &str) -> Result<Duration, BoxError> {
    let value = value.parse::<u64>()?;
    if value > 10000 {
        return Err("tracker delay must be at most ten seconds".into());
    }
    Ok(Duration::from_millis(value))
}
enum Operation {
    Apply(ChangeFile),
    Resolve(ChangeFile),
    Publish(PlanFile, Duration),
    Reconcile(PlanFile),
    ResolveLink(PlanFile),
    Download(PlanFile, PathBuf),
    Get(ProjectKey),
    Issue(ProjectKey, IssueId),
    Lookup(ProjectKey),
    Progress(ProjectKey),
    Effect(ProjectKey, [u8; 32]),
    Dashboard(DashboardPageRequest),
    Serve(Roster, u64, DeliveryOptions),
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
            let record: ChangeFile = files::load(Path::new(path))?;
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
        [op, state, path, rest @ ..]
            if matches!(op.as_str(), "publish" | "reconcile" | "resolve-link") =>
        {
            let record: PlanFile = files::load(Path::new(path))?;
            record.validate()?;
            let operation = match (op.as_str(), rest) {
                ("publish", []) => Operation::Publish(record.clone(), Duration::ZERO),
                ("publish", [value]) => Operation::Publish(record.clone(), delay(value)?),
                ("reconcile", []) => Operation::Reconcile(record.clone()),
                ("resolve-link", []) => Operation::ResolveLink(record.clone()),
                _ => return Err(HELP.into()),
            };
            (state.clone(), record.tenant, operation)
        }
        [op, state, path, output] if op == "download" => {
            let record: PlanFile = files::load(Path::new(path))?;
            record.validate()?;
            (
                state.clone(),
                record.tenant.clone(),
                Operation::Download(record, output.into()),
            )
        }
        [op, state, tenant, key] if matches!(op.as_str(), "get" | "lookup" | "progress") => {
            let key = ProjectKey::new(key.clone())?;
            let operation = match op.as_str() {
                "get" => Operation::Get(key),
                "lookup" => Operation::Lookup(key),
                _ => Operation::Progress(key),
            };
            (state.clone(), tenant.clone(), operation)
        }
        [op, state, tenant, key, id] if op == "issue" => (
            state.clone(),
            tenant.clone(),
            Operation::Issue(ProjectKey::new(key.clone())?, IssueId::new(id.clone())?),
        ),
        [op, state, tenant, key, id] if op == "effect" => {
            if id.len() != 64
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(
                    "tracker effect identity requires 64 lowercase hexadecimal digits".into(),
                );
            }
            (
                state.clone(),
                tenant.clone(),
                Operation::Effect(
                    ProjectKey::new(key.clone())?,
                    *blake3::Hash::from_hex(id)?.as_bytes(),
                ),
            )
        }
        [op, state, tenant, rest @ ..] if op == "dashboard" => {
            let (after, limit) = match rest {
                [] => (None, 16),
                [key] => (Some(ProjectKey::new(key.clone())?), 16),
                [key, limit] => (Some(ProjectKey::new(key.clone())?), limit.parse()?),
                _ => return Err(HELP.into()),
            };
            if !(1..=16).contains(&limit) {
                return Err("dashboard limit must be 1..16".into());
            }
            (
                state.clone(),
                tenant.clone(),
                Operation::Dashboard(DashboardPageRequest { after, limit }),
            )
        }
        [op, state, path, seconds, before, after, rest @ ..] if op == "serve" => {
            let roster: Roster = files::load(Path::new(path))?;
            roster.validate()?;
            let seconds = seconds.parse::<u64>()?;
            if !(1..=3600).contains(&seconds) {
                return Err("tracker serve lifetime must be 1..3600 seconds".into());
            }
            let drop_reply_once = match rest {
                [] => false,
                [flag] if flag == "drop-reply" => true,
                _ => return Err(HELP.into()),
            };
            (
                state.clone(),
                roster.tenant.clone(),
                Operation::Serve(
                    roster,
                    seconds,
                    DeliveryOptions {
                        before_delivery: delay(before)?,
                        after_publication: delay(after)?,
                        drop_reply_once,
                        ..Default::default()
                    },
                ),
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
    mut options: DeliveryOptions,
) -> Result<(), BoxError> {
    let (sender, mut observations) = tokio::sync::mpsc::channel(64);
    options.progress = Some(sender);
    spawn_delivery(
        &service.node,
        &service.node,
        service.handle.clone(),
        service.handle.clone(),
        &roster.projects,
        options,
    )
    .await?;
    emit(&serde_json::json!({"event":"ready","tenant":roster.tenant,"projects":roster.projects}))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        if !service.node.is_ready() {
            return Err("tracker serving readiness closed".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        tokio::select! {value=observations.recv()=>if let Some(progress)=value{emit(&serde_json::json!({"event":"projected","progress":progress}))?;},()=tokio::time::sleep(Duration::from_millis(50))=>{}}
    }
}
async fn operation(service: &Service, state: &Path, operation: Operation) -> Result<(), BoxError> {
    match operation {
        Operation::Apply(record) => {
            let project = service.project(record.change.project.clone()).await?;
            outcome(
                project
                    .change(record.identity.native()?, record.change)
                    .await,
            )
        }
        Operation::Resolve(record) => {
            let project = service.project(record.change.project.clone()).await?;
            let prepared = project
                .prepare(record.identity.native()?, record.change)
                .await?;
            resolution(project.resolve(prepared.evidence()).await?)
        }
        Operation::Publish(record, delay) => {
            let client = service.attachments().await?;
            let value = client.publish(&record.plan).await?;
            emit(
                &serde_json::json!({"event":"attachment_published","publication":value.publication,"key":blake3::Hash::from_bytes(record.plan.descriptor.key()).to_hex().as_str()}),
            )?;
            tokio::time::sleep(delay).await;
            Ok(())
        }
        Operation::Reconcile(record) => {
            service
                .project(record.plan.descriptor.project.clone())
                .await?;
            let prepared = service
                .attachments()
                .await?
                .prepare_link(&record.plan)
                .await?;
            outcome(prepared.execute().await)
        }
        Operation::ResolveLink(record) => {
            service
                .project(record.plan.descriptor.project.clone())
                .await?;
            resolution(
                service
                    .attachments()
                    .await?
                    .resolve_link(&record.plan)
                    .await?,
            )
        }
        Operation::Download(record, path) => {
            let object = service
                .attachments()
                .await?
                .read(record.plan.descriptor.key(), None)
                .await?
                .output
                .ok_or("tracker attachment is not published")?;
            if object.publication.descriptor != record.plan.descriptor
                || object.bytes != record.plan.bytes
            {
                return Err("tracker download differs from frozen plan".into());
            }
            files::save(&path, &object.bytes)?;
            emit(&serde_json::json!({"downloaded":path,"publication":object.publication}))
        }
        Operation::Get(key) => {
            let value = service.project(key).await?.get(None).await?;
            emit(&serde_json::json!({"project":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Issue(key, id) => {
            let value = service.project(key).await?.issue(&id, None).await?;
            emit(&serde_json::json!({"issue":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Lookup(key) => {
            let value = service.dashboard().await?.lookup(key, None).await?;
            emit(&serde_json::json!({"summary":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Progress(key) => {
            let value = service.project(key).await?.progress(None).await?;
            emit(&serde_json::json!({"progress":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Effect(key, effect_id) => {
            let value = service
                .project(key)
                .await?
                .effect_status(effect_id, None)
                .await?;
            let status = value.output.map(|status| -> Result<serde_json::Value, BoxError> {
                let state = match status.state {
                    cellule_runtime::primitives::effects::EffectState::Ready => "ready",
                    cellule_runtime::primitives::effects::EffectState::Leased => "leased",
                    cellule_runtime::primitives::effects::EffectState::Delivered => "delivered",
                    cellule_runtime::primitives::effects::EffectState::Failed => "failed",
                };
                let result = status.result.map(|bytes| -> Result<_, BoxError> {
                    let mut decoder = BoundedDecoder::new(&bytes, 16)?;
                    let result = cellule_cookbook_project_tracker::ProjectionOutcome::decode(&mut decoder)?;
                    decoder.finish()?; Ok(result)
                }).transpose()?;
                Ok(serde_json::json!({"state":state,"attempt":status.attempt,"lease_until_ms":status.lease_until_ms,"expires_at_ms":status.expires_at_ms,"result":result}))
            }).transpose()?;
            emit(&serde_json::json!({"effect":status,"receipt":receipt(value.receipt)}))
        }
        Operation::Dashboard(page) => {
            let value = service.dashboard().await?.list(page, None).await?;
            emit(&serde_json::json!({"dashboard":value.output,"receipt":receipt(value.receipt)}))
        }
        Operation::Serve(roster, seconds, options) => {
            serve(service, roster, seconds, options).await
        }
        Operation::Demo => demo::run(service, state).await,
    }
}
async fn run() -> Result<(), BoxError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => return Err(HELP.into()),
        [op] if matches!(op.as_str(), "help" | "--help") => {
            println!("{HELP}");
            return Ok(());
        }
        [op, input, path] if op == "prepare" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                tenant: String,
                change: ProjectChange,
            }
            let input: Input = files::load(Path::new(input))?;
            let record = ChangeFile {
                version: 1,
                tenant: input.tenant,
                identity: new_identity()?.into(),
                change: input.change,
            };
            record.validate()?;
            files::save(Path::new(path), &serde_json::to_vec(&record)?)?;
            return emit(&serde_json::json!({"prepared":path,"request":record}));
        }
        [op, tenant, project, issue, id, name, source, revision, path]
            if op == "prepare-attachment" =>
        {
            assembly::tenant(tenant)?;
            let mut bytes = Vec::new();
            let file = std::fs::File::open(source)?;
            if !file.metadata()?.is_file() {
                return Err("tracker attachment source must be a regular file".into());
            }
            file.take((64 << 10) + 1).read_to_end(&mut bytes)?;
            let descriptor = AttachmentDescriptor::new(
                ProjectKey::new(project.clone())?,
                IssueId::new(issue.clone())?,
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
        _ => {}
    }
    let invocation = parse(&args)?;
    let expired = match &invocation.operation {
        Operation::Resolve(value) => Some(value.identity.expires_at_ms),
        Operation::ResolveLink(value) => Some(value.plan.identities[3].expires_at_ms),
        _ => None,
    };
    if let Some(expires) = expired
        && expires <= cellule_cookbook_support::now_ms()?
    {
        return resolution(Resolution::Expired);
    }
    let serving = matches!(invocation.operation, Operation::Serve(..));
    let service = Service::start(invocation.state.clone(), &invocation.tenant).await?;
    let result = tokio::select! {biased;signal=shutdown_signal()=>match signal{Ok(()) if serving=>Ok(()),Ok(())=>Err("interrupted; retain the original tracker request or attachment plan and resolve its outcome".into()),Err(source)=>Err(source.into())},result=operation(&service,&invocation.state,invocation.operation)=>result};
    let cleanup = service.node.shutdown().await;
    if cleanup.is_ok()
        && let Err(output) = emit(&serde_json::json!({"event":"drained"}))
    {
        if result.is_ok() {
            return Err(output);
        }
        tracing::error!(%output,"tracker drained but output failed");
    }
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup,"additional tracker drain failure");
            }
            Err(source)
        }
        (Ok(()), Err(source)) => Err(source.into()),
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
            tracing::error!(error=%source,"tracker command failed");
            let mut cause = source.source();
            while let Some(value) = cause {
                tracing::error!(cause=%value,"tracker source");
                cause = value.source();
            }
            ExitCode::FAILURE
        }
    }
}
