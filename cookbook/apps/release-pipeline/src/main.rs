//! Local release ingress, provider assembly, version selection, and owned process lifetime.
mod assembly;
mod demo;
mod files;
mod http;
mod release_server;
#[cfg(test)]
mod server_tests;
mod target_server;
use assembly::{ReleaseService, TargetService};
use cellule_cookbook_release_pipeline::{
    Approval, BoxError, Reconcile, ReleaseId, ReleaseSpec, TargetName, spawn_release_workers,
    validate_origin,
};
use cellule_cookbook_support::{new_identity, now_ms};
use files::{Control, Retained};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
const HELP:&str="Cellule release pipeline
 demo STATE_DIRECTORY
 prepare INPUT_JSON REQUEST_FILE
 prepare-release SOURCE_FILE TARGET GENERATION TARGET_URL BROKER_URL DEADLINE_SECONDS REQUEST_FILE
 prepare-approval START_REQUEST approve|reject REQUEST_FILE
 prepare-rollback START_REQUEST REQUEST_FILE
 prepare-reconcile START_REQUEST REQUEST_FILE
 send LOOPBACK_URL REQUEST_FILE
 fetch LOOPBACK_URL workflow|record RELEASE_HEX
 fetch-target LOOPBACK_URL operation|target|artifact ID_OR_TARGET
 apply STATE_DIRECTORY 1|2 REQUEST_FILE
 resolve STATE_DIRECTORY 1|2 REQUEST_FILE
 workflow STATE_DIRECTORY 1|2 RELEASE_HEX
 record STATE_DIRECTORY 1|2 RELEASE_HEX
 prepare-target SOURCE_FILE RELEASE_HEX TARGET GENERATION deploy|rollback WORK_FILE
 download STATE_DIRECTORY 1|2 ARTIFACT_HEX OUTPUT_FILE
 serve STATE_DIRECTORY 1|2 PORT SECONDS
 recover-old STATE_DIRECTORY
 rollout STATE_DIRECTORY
 target-server STATE_DIRECTORY PORT SECONDS [FAULT_FILE]
 target-fault FAULT_FILE up|down|drop-deploy-reply|fail-rollback-once|delay-deploy
 target-key TARGET_WORK_JSON

Native request window: five minutes. Source: 1..4096 bytes. Approval: 1..86400 seconds.
Run recover-old before rollout after a version-one crash; ordinary version-two open refuses old code.
Serve lifetime: 1..3600 seconds. Ports bind only 127.0.0.1; credentials remain outside workflow state.
Defaults: worker/target cookbook-local-release; submitter cookbook-local-submitter;
approver cookbook-local-approver; operator cookbook-local-operator.
Credential variables: CELLULE_RELEASE_{ARTIFACT,TARGET,SUBMITTER,APPROVER,OPERATOR}_TOKEN.
Deployment crash checkpoint: CELLULE_RELEASE_AFTER_DEPLOY_MS=10000.";
fn emit(value: &impl serde::Serialize) -> Result<(), BoxError> {
    println!("{}", serde_json::to_string(value)?);
    std::io::stdout().flush()?;
    Ok(())
}
fn retained(path: &str) -> Result<Retained, BoxError> {
    let value: Retained = files::load(Path::new(path))?;
    value.validate()?;
    Ok(value)
}
fn original(path: &str) -> Result<ReleaseSpec, BoxError> {
    match retained(path)?.input {
        Control::Start { spec } => Ok(spec),
        _ => Err("control must derive from the original prepared start request".into()),
    }
}
fn prepare(input: Control, path: &str) -> Result<(), BoxError> {
    let value = Retained::new(input)?;
    files::save(Path::new(path), &serde_json::to_vec(&value)?)?;
    let release = match &value.input {
        Control::Start { spec } | Control::Rollback { spec } => spec.release,
        Control::Approve { vote } => vote.release,
        Control::Reconcile { input } => input.release,
    };
    emit(&serde_json::json!({"prepared":path,"release":release.to_string(),"request":value}))
}
fn seconds(value: &str) -> Result<u64, BoxError> {
    let seconds = value.parse()?;
    if !(1..=3600).contains(&seconds) {
        return Err("serve lifetime requires 1..3600 seconds".into());
    }
    Ok(seconds)
}
async fn lifetime(
    node: &cellule_cookbook_support::LocalNode,
    seconds: u64,
) -> Result<(), BoxError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        if !node.is_ready() {
            return Err("release node readiness closed".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn drain(
    node: &cellule_cookbook_support::LocalNode,
    result: Result<(), BoxError>,
) -> Result<(), BoxError> {
    let cleanup = node.shutdown().await;
    if cleanup.is_ok()
        && let Err(output) = emit(&serde_json::json!({"event":"drained"}))
    {
        if result.is_ok() {
            return Err(output);
        }
        tracing::error!(%output, "release drain completed but output failed");
    }
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup,"additional release drain failure");
            }
            Err(source)
        }
        (Ok(()), Err(source)) => Err(source.into()),
    }
}
async fn serve<const V: u8>(
    service: &ReleaseService<V>,
    port: u16,
    seconds: u64,
) -> Result<(), BoxError> {
    let address = release_server::install(
        service.node.clone(),
        service.client.clone(),
        service.artifacts.clone(),
        service.plans.clone(),
        port,
    )
    .await?;
    spawn_release_workers(&service.node, service.handle.clone()).await?;
    emit(
        &serde_json::json!({"event":"ready","endpoint":format!("http://{address}/"),"version":V}),
    )?;
    lifetime(&service.node, seconds).await
}
async fn operation<const V: u8>(
    service: &ReleaseService<V>,
    args: &[String],
) -> Result<(), BoxError> {
    match args {
        [op, _, _, path] if op == "apply" => {
            emit(&release_server::apply(&service.client, &retained(path)?).await?)
        }
        [op, _, _, path] if op == "resolve" => {
            emit(&release_server::resolve(&service.client, &retained(path)?).await?)
        }
        [op, _, _, id] if op == "workflow" => {
            let v = service.client.workflow(id.parse()?, None).await?;
            emit(&serde_json::json!({"workflow":v.output,"receipt":files::receipt(v.receipt)}))
        }
        [op, _, _, id] if op == "record" => {
            let v = service.client.record(id.parse()?, None).await?;
            emit(&serde_json::json!({"record":v.output,"receipt":files::receipt(v.receipt)}))
        }
        [op, _, _, key, path] if op == "download" => {
            let value = service
                .artifacts
                .read(files::key(key)?, None)
                .await?
                .output
                .ok_or("release artifact not published")?;
            files::save(Path::new(path), &value.bytes)?;
            emit(&serde_json::json!({"downloaded":path,"publication":value.publication}))
        }
        [op, _, _, port, duration] if op == "serve" => {
            serve(service, port.parse()?, seconds(duration)?).await
        }
        _ => Err(HELP.into()),
    }
}
async fn local<const V: u8>(args: Vec<String>) -> Result<(), BoxError> {
    let state = PathBuf::from(args.get(1).ok_or(HELP)?);
    let (store, parts) = assembly::stores()?;
    let service = ReleaseService::<V>::start(state, store, parts).await?;
    let result = tokio::select! {biased;signal=cellule_cookbook_support::shutdown_signal()=>match signal {Ok(()) if args.first().is_some_and(|v|v=="serve")=>Ok(()),Ok(())=>Err("interrupted; retain the original request and inspect or resolve its outcome".into()),Err(source)=>Err(source.into())},result=operation(&service,&args)=>result};
    drain(&service.node, result).await
}
async fn remote(
    endpoint: &str,
    route: &str,
    credential: &str,
    body: Option<&Retained>,
) -> Result<serde_json::Value, BoxError> {
    validate_origin(endpoint)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(10))
        .build()?;
    let url = format!("{endpoint}{route}");
    let request = match body {
        Some(v) => client.post(url).json(v),
        None => client.get(url),
    };
    let mut response = request
        .bearer_auth(credential)
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 65536 {
            return Err("release response exceeds 64 KiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
async fn run(args: Vec<String>) -> Result<(), BoxError> {
    match args.as_slice() {
        [] => {
            println!("{HELP}");
            return Ok(());
        }
        [op] if op == "help" || op == "--help" => {
            println!("{HELP}");
            return Ok(());
        }
        [op, input, path] if op == "prepare" => {
            return prepare(files::load(Path::new(input))?, path);
        }
        [
            op,
            source,
            target,
            generation,
            target_url,
            broker_url,
            duration,
            path,
        ] if op == "prepare-release" => {
            let seconds = duration.parse::<i64>()?;
            if !(1..=86400).contains(&seconds) {
                return Err("approval window requires 1..86400 seconds".into());
            }
            let mut bytes = Vec::new();
            std::fs::File::open(source)?
                .take(4097)
                .read_to_end(&mut bytes)?;
            let spec = ReleaseSpec {
                release: ReleaseId::from_bytes(*new_identity()?.request_id.as_bytes())?,
                target: TargetName::new(target.clone())?,
                source: bytes,
                expected_generation: generation.parse()?,
                target_endpoint: target_url.clone(),
                artifact_endpoint: broker_url.clone(),
                approval_deadline_ms: now_ms()?
                    .checked_add(seconds * 1000)
                    .ok_or("approval deadline overflow")?,
            };
            return prepare(Control::Start { spec }, path);
        }
        [op, start, vote, path] if op == "prepare-approval" => {
            let spec = original(start)?;
            let approve = match vote.as_str() {
                "approve" => true,
                "reject" => false,
                _ => return Err("approval requires approve or reject".into()),
            };
            return prepare(
                Control::Approve {
                    vote: Approval {
                        release: spec.release,
                        input_digest: spec.digest()?,
                        approve,
                    },
                },
                path,
            );
        }
        [op, start, path] if op == "prepare-rollback" => {
            return prepare(
                Control::Rollback {
                    spec: original(start)?,
                },
                path,
            );
        }
        [op, start, path] if op == "prepare-reconcile" => {
            let spec = original(start)?;
            return prepare(
                Control::Reconcile {
                    input: Reconcile {
                        release: spec.release,
                        input_digest: spec.digest()?,
                        token: ReleaseId::from_bytes(*new_identity()?.request_id.as_bytes())?,
                    },
                },
                path,
            );
        }
        [op, url, path] if op == "send" => {
            let value = retained(path)?;
            let (name, default) = match value.input.name() {
                "start" => (
                    "CELLULE_RELEASE_SUBMITTER_TOKEN",
                    "cookbook-local-submitter",
                ),
                "approve" => ("CELLULE_RELEASE_APPROVER_TOKEN", "cookbook-local-approver"),
                _ => ("CELLULE_RELEASE_OPERATOR_TOKEN", "cookbook-local-operator"),
            };
            return emit(
                &remote(
                    url,
                    value.input.name(),
                    &files::token(name, default)?,
                    Some(&value),
                )
                .await?,
            );
        }
        [op, url, kind, id] if op == "fetch" && matches!(kind.as_str(), "workflow" | "record") => {
            id.parse::<ReleaseId>()?;
            return emit(
                &remote(
                    url,
                    &format!("{kind}/{id}"),
                    &files::token("CELLULE_RELEASE_ARTIFACT_TOKEN", "cookbook-local-release")?,
                    None,
                )
                .await?,
            );
        }
        [op, url, kind, id]
            if op == "fetch-target"
                && matches!(kind.as_str(), "operation" | "artifact" | "target") =>
        {
            if kind == "target" {
                TargetName::new(id.clone())?;
            } else {
                id.parse::<ReleaseId>()?;
            }
            return emit(
                &remote(
                    url,
                    &format!("{kind}/{id}"),
                    &files::token("CELLULE_RELEASE_TARGET_TOKEN", "cookbook-local-release")?,
                    None,
                )
                .await?,
            );
        }
        [op, path, mode] if op == "target-fault" => {
            return target_server::set_fault(Path::new(path), mode);
        }
        [op, source, release, target, generation, action, path] if op == "prepare-target" => {
            let release = release.parse::<ReleaseId>()?;
            let mut bytes = Vec::new();
            std::fs::File::open(source)?
                .take(4097)
                .read_to_end(&mut bytes)?;
            let (artifact, bytes) =
                cellule_cookbook_release_pipeline::Artifact::build(release, &bytes)?;
            let action = match action.as_str() {
                "deploy" => cellule_cookbook_release_pipeline::TargetAction::Deploy(bytes),
                "rollback" => cellule_cookbook_release_pipeline::TargetAction::Rollback,
                _ => return Err("target action requires deploy or rollback".into()),
            };
            let work = cellule_cookbook_release_pipeline::TargetWork {
                deployment: cellule_cookbook_release_pipeline::Deployment {
                    release,
                    target: TargetName::new(target.clone())?,
                    artifact,
                    expected_generation: generation.parse()?,
                },
                action,
            };
            work.validate()?;
            files::save(Path::new(path), &serde_json::to_vec(&work)?)?;
            return emit(&serde_json::json!({"prepared": path}));
        }
        [op, path] if op == "target-key" => {
            let v: cellule_cookbook_release_pipeline::TargetWork = files::load(Path::new(path))?;
            v.validate()?;
            return emit(
                &serde_json::json!({"idempotency_key":blake3::Hash::from_bytes(v.deployment.operation_key(matches!(v.action,cellule_cookbook_release_pipeline::TargetAction::Rollback))).to_hex().as_str()}),
            );
        }
        [op, state] if op == "demo" => return demo::run(state.into()).await,
        [op, state] if op == "recover-old" => {
            let (store, parts) = assembly::stores()?;
            let service = ReleaseService::<1>::start(state.into(), store, parts).await?;
            return drain(
                &service.node,
                emit(&serde_json::json!({"event":"predecessor_recovered","version":1})),
            )
            .await;
        }
        [op, state] if op == "rollout" => {
            let (store, parts) = assembly::stores()?;
            // A crashed predecessor must recover and release its writer under
            // its own exact compiled release before the code upgrade acquires it.
            let predecessor =
                ReleaseService::<1>::start(state.into(), store.clone(), parts.clone()).await?;
            drain(&predecessor.node, Ok(())).await?;
            let successor = ReleaseService::<2>::rollout(state.into(), store, parts).await?;
            return drain(
                &successor.node,
                emit(&serde_json::json!({"event":"rolled_out","version":2})),
            )
            .await;
        }
        [op, state, port, duration, rest @ ..] if op == "target-server" && rest.len() <= 1 => {
            let port = port.parse()?;
            let seconds = seconds(duration)?;
            let (store, _) = assembly::stores()?;
            let service = TargetService::start(state.into(), store).await?;
            let result = tokio::select! {biased;signal=cellule_cookbook_support::shutdown_signal()=>signal.map_err(|e|Box::new(e) as BoxError),result=async {let address=target_server::install(service.node.clone(),service.client.clone(),port,rest.first().map(PathBuf::from)).await?;emit(&serde_json::json!({"event":"target_ready","endpoint":format!("http://{address}/")}))?;lifetime(&service.node,seconds).await}=>result};
            return drain(&service.node, result).await;
        }
        _ => {}
    }
    if args.first().is_some_and(|v| v == "resolve")
        && let Some(path) = args.get(3)
    {
        let value = retained(path)?;
        if now_ms()? >= value.identity.expires_at_ms {
            return emit(&serde_json::json!({"resolution":"expired","absence_proven":false}));
        }
    }
    if !args.first().is_some_and(|v| {
        matches!(
            v.as_str(),
            "apply" | "resolve" | "workflow" | "record" | "download" | "serve"
        )
    }) {
        return Err(HELP.into());
    }
    match args.get(2).map(String::as_str) {
        Some("1") => local::<1>(args).await,
        Some("2") => local::<2>(args).await,
        _ => Err("release version must be one or two".into()),
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
        Err(source) => {
            eprintln!("release-pipeline: {source}");
            let mut cause = source.source();
            while let Some(e) = cause {
                eprintln!("  caused by: {e}");
                cause = e.source();
            }
            ExitCode::FAILURE
        }
    }
}
