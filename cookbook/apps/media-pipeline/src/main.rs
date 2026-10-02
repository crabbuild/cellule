//! Local media pipeline ingress, private provider assembly, and owned process lifetime.
mod adapter;
use cellule_cookbook_media_pipeline::{
    Artifact, Artifacts, BoxError, Identity, Kind, MediaApplication, MediaClient, Request, Upload,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, new_identity, now_ms};
use cellule_runtime::{ApplicationId, BlobArtifactStore, InvocationError, Resolution, TenantId};
use cellule_store::Store;
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x35; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x45; 16]);
const HELP: &str = "Cellule media pipeline\n\
 demo STATE_DIRECTORY\n\
 sample PNG_FILE\n\
 prepare PNG_FILE UPLOAD_PLAN\n\
 upload STATE_DIRECTORY UPLOAD_PLAN\n\
 prepare-run SOURCE_ARTIFACT_JSON SIDE LOOPBACK_URL REQUEST_FILE\n\
 start STATE_DIRECTORY REQUEST_FILE\n\
 resolve STATE_DIRECTORY REQUEST_FILE\n\
 get STATE_DIRECTORY WORKFLOW_UUID\n\
 download STATE_DIRECTORY source|result HEX_KEY OUTPUT_FILE\n\
 serve STATE_DIRECTORY PORT [SECONDS]\n\nPNG input: 1..256 KiB, at most 1024x1024. Thumbnail side: 1..128.\n\
Prepared command validity: five minutes. Activity deadline: one hour.\n\
Adapter credential: CELLULE_MEDIA_TOKEN (synthetic local default).\n\
Crash checkpoint: CELLULE_MEDIA_AFTER_PUBLICATION_MS=10000.";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    version: u8,
    identity: Identity,
    request: Request,
}
struct Service {
    node: LocalNode,
    artifacts: Artifacts,
    client: MediaClient,
    handle: cellule_app::ApplicationHandle<MediaApplication>,
}
fn save(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
fn load<T: serde::de::DeserializeOwned>(path: &Path, limit: usize) -> Result<T, BoxError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err("retained file exceeds bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn retained(path: &str) -> Result<Retained, BoxError> {
    let value: Retained = load(Path::new(path), 8192)?;
    if value.version != 1 {
        return Err("unsupported retained media request".into());
    }
    value.request.validate()?;
    if value.identity.request == [0; 16] {
        return Err("zero retained request identity".into());
    }
    Ok(value)
}
fn key(value: &str) -> Result<[u8; 32], BoxError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("key must be 64 lowercase hex characters".into());
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(key)
}
async fn start(state: PathBuf) -> Result<Service, BoxError> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    let store = local_s3_store(&endpoint, "cellule-cookbook")?;
    let artifacts = BlobArtifactStore::new(Store::new(std::sync::Arc::new(
        object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/media-pipeline/artifacts",
        ),
    )));
    let node = LocalNode::start(
        cellule_cookbook_media_pipeline::compile()?,
        store,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/media-pipeline/cells"),
            application_id: APPLICATION,
        },
    )
    .await?;
    let opened = async {
        let handle = node
            .application_handle::<MediaApplication>(TENANT)?
            .with_blob_artifact_store(artifacts);
        cellule_cookbook_media_pipeline::open(&node, &handle).await?;
        Ok::<_, BoxError>(handle)
    }
    .await;
    let handle = match opened {
        Ok(handle) => handle,
        Err(source) => {
            if let Err(cleanup) = node.shutdown().await {
                tracing::error!(%cleanup,"media startup drain failed");
            }
            return Err(source);
        }
    };
    Ok(Service {
        artifacts: Artifacts::new(handle.clone()),
        client: MediaClient::new(handle.clone()),
        handle,
        node,
    })
}
async fn install(service: &Service, port: u16) -> Result<String, BoxError> {
    let address = adapter::install(&service.node, service.artifacts.clone(), port).await?;
    cellule_cookbook_media_pipeline::spawn_processing(&service.node, service.handle.clone())?;
    Ok(format!("http://{address}/"))
}
fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
async fn apply(service: &Service, record: Retained) -> Result<(), BoxError> {
    let prepared = service
        .client
        .prepare(record.identity.native(), record.request)
        .await?;
    match prepared.execute().await {
        Ok(value) => {
            println!(
                "{}",
                serde_json::json!({"outcome":format!("{:?}",value.output),"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(value)) => {
            println!(
                "{}",
                serde_json::json!({"outcome":format!("{:?}",value.output),"receipt":receipt(value.receipt)})
            );
            Err(InvocationError::Rejected(value).into())
        }
        Err(source) => Err(source.into()),
    }
}
async fn demo(service: &Service) -> Result<(), BoxError> {
    let endpoint = install(service, 0).await?;
    let bytes = cellule_cookbook_media_pipeline::sample_png()?;
    let source = service
        .artifacts
        .publish(&Upload::new(
            Kind::Source,
            *blake3::hash(&bytes).as_bytes(),
            bytes,
        )?)
        .await?;
    let request = Request {
        id: *uuid::Uuid::now_v7().as_bytes(),
        source: source.clone(),
        side: 128,
        deadline_ms: now_ms()?
            .checked_add(300_000)
            .ok_or("demo deadline overflow")?,
        endpoint,
    };
    let id = request.id;
    let output_key = request.output_key();
    let prepared = service
        .client
        .prepare(new_identity()?, request.clone())
        .await?;
    let original = prepared.clone().execute().await?;
    let replay = prepared.clone().execute().await?;
    if original.receipt != replay.receipt {
        return Err("start retry changed original receipt".into());
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let artifact = loop {
        if !service.node.is_ready() {
            return Err("media worker readiness closed".into());
        }
        let view = service
            .client
            .get(id, None)
            .await?
            .output
            .ok_or("demo Workflow disappeared")?;
        if let Some(failure) = view.state.failure {
            return Err(format!("thumbnail failed: {failure}").into());
        }
        if let Some(artifact) = view.state.result {
            break artifact;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("thumbnail did not complete within demo bound".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let (observed, output) = service
        .artifacts
        .read(Kind::Result, output_key, None)
        .await?
        .output
        .ok_or("linked thumbnail missing")?;
    if observed != artifact || cellule_cookbook_media_pipeline::dimensions(&output)? != (128, 64) {
        return Err("linked thumbnail verification failed".into());
    }
    let expected = cellule_cookbook_media_pipeline::thumbnail(
        &cellule_cookbook_media_pipeline::sample_png()?,
        128,
    )?;
    if output != expected {
        return Err("published PNG differs from deterministic thumbnail".into());
    }
    let reused = service
        .artifacts
        .publish(&Upload::new(Kind::Result, output_key, output)?)
        .await?;
    if reused != artifact {
        return Err("thumbnail reuse changed its manifest".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","workflow_id":uuid::Uuid::from_bytes(id),"source":source,"result":artifact,"checks":["native-Blob-source","actual-PNG-processing","pinned-input","native-Activity","Workflow-result-link","retained-start-replay","verified-deterministic-output","immutable-output-reuse"]})
    );
    Ok(())
}
async fn operation(service: &Service, args: &[String]) -> Result<(), BoxError> {
    match args {
        [op, _] if op == "demo" => demo(service).await,
        [op, _, path] if op == "upload" => {
            let plan: Upload = load(Path::new(path), 2 << 20)?;
            if plan.kind != Kind::Source {
                return Err("CLI only accepts source upload plans".into());
            }
            let artifact = service.artifacts.publish(&plan).await?;
            println!("{}", serde_json::to_string(&artifact)?);
            Ok(())
        }
        [op, _, path] if op == "start" => apply(service, retained(path)?).await,
        [op, _, path] if op == "resolve" => {
            let record = retained(path)?;
            let prepared = service
                .client
                .prepare(record.identity.native(), record.request)
                .await?;
            let result = service.client.resolve(prepared.evidence()).await?;
            let value = match result {
                Resolution::Absent => {
                    serde_json::json!({"resolution":"absent","absence_proven":true})
                }
                Resolution::Unknown => {
                    serde_json::json!({"resolution":"unknown","absence_proven":false})
                }
                Resolution::Expired => {
                    serde_json::json!({"resolution":"expired","absence_proven":false})
                }
                Resolution::Committed(stored) => {
                    serde_json::json!({"resolution":"committed","commit_sequence":stored.commit_sequence()})
                }
            };
            println!("{value}");
            Ok(())
        }
        [op, _, id] if op == "get" => {
            let view = service
                .client
                .get(*uuid::Uuid::parse_str(id)?.as_bytes(), None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"view":view.output,"receipt":receipt(view.receipt)})
            );
            Ok(())
        }
        [op, _, kind, hex, path] if op == "download" => {
            let kind = match kind.as_str() {
                "source" => Kind::Source,
                "result" => Kind::Result,
                _ => return Err("kind must be source or result".into()),
            };
            let (artifact, bytes) = service
                .artifacts
                .read(kind, key(hex)?, None)
                .await?
                .output
                .ok_or("artifact missing")?;
            save(Path::new(path), &bytes)?;
            println!(
                "{}",
                serde_json::json!({"downloaded":path,"artifact":artifact})
            );
            Ok(())
        }
        [op, _, port, rest @ ..] if op == "serve" && rest.len() <= 1 => {
            let seconds = rest
                .first()
                .map(|s| s.parse::<u64>())
                .transpose()?
                .unwrap_or(3600);
            if !(1..=3600).contains(&seconds) {
                return Err("serve lifetime must be 1..3600 seconds".into());
            }
            let endpoint = install(service, port.parse()?).await?;
            println!(
                "{}",
                serde_json::json!({"event":"ready","endpoint":endpoint})
            );
            let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
            while tokio::time::Instant::now() < deadline {
                if !service.node.is_ready() {
                    return Err("media worker readiness closed".into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        }
        _ => Err(HELP.into()),
    }
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
        [op, path] if op == "sample" => {
            save(
                Path::new(path),
                &cellule_cookbook_media_pipeline::sample_png()?,
            )?;
            return Ok(());
        }
        [op, source, path] if op == "prepare" => {
            let mut bytes = Vec::new();
            std::fs::File::open(source)?
                .take(cellule_cookbook_media_pipeline::MAX_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            let plan = Upload::new(Kind::Source, *blake3::hash(&bytes).as_bytes(), bytes)?;
            save(Path::new(path), &serde_json::to_vec(&plan)?)?;
            println!("{}", serde_json::json!({"prepared":path}));
            return Ok(());
        }
        [op, source, side, endpoint, path] if op == "prepare-run" => {
            let artifact: Artifact = load(Path::new(source), 8192)?;
            let request = Request {
                id: *uuid::Uuid::now_v7().as_bytes(),
                source: artifact,
                side: side.parse()?,
                deadline_ms: now_ms()?
                    .checked_add(3_600_000)
                    .ok_or("Activity deadline overflow")?,
                endpoint: endpoint.clone(),
            };
            request.validate()?;
            let id = request.id;
            let record = Retained {
                version: 1,
                identity: new_identity()?.into(),
                request,
            };
            save(Path::new(path), &serde_json::to_vec(&record)?)?;
            println!(
                "{}",
                serde_json::json!({"prepared":path,"workflow_id":uuid::Uuid::from_bytes(id)})
            );
            return Ok(());
        }
        _ => {}
    }
    if let [op, _, path] = args.as_slice()
        && op == "resolve"
    {
        let record = retained(path)?;
        if now_ms()? >= record.identity.expires_at_ms {
            println!(
                "{}",
                serde_json::json!({"resolution":"expired","absence_proven":false})
            );
            return Ok(());
        }
    }
    let service = start(PathBuf::from(args.get(1).ok_or(HELP)?)).await?;
    let result = tokio::select! {biased;signal=cellule_cookbook_support::shutdown_signal()=>match signal{Ok(()) if args.first().is_some_and(|op|op=="serve")=>Ok(()),Ok(())=>Err("interrupted; retain media upload plans and run requests for inspection and resolution".into()),Err(source)=>Err(source.into())},result=operation(&service,&args)=>result};
    let shutdown = service.node.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup,"media drain failed");
            }
            Err(source)
        }
        (Ok(()), Err(source)) => Err(source.into()),
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
            eprintln!("media-pipeline: {source}");
            let mut cause = source.source();
            while let Some(source) = cause {
                eprintln!("  caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
