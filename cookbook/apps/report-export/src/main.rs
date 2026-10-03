//! Local export ingress, authenticated adapter, private provider assembly, and process ownership.
mod adapter;
use cellule_cookbook_report_export::{
    Artifacts, BoxError, Change, DataClient, DataOutcome, ExportApplication, ExportClient,
    ExportEngine, Identity, PageRequest, Request, Row, Snapshot, Version, Work,
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
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x46; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x56; 16]);
const HELP: &str = "Cellule report export\n\
 demo STATE_DIRECTORY\n\
 sample COUNT EXPECTED_REVISION CHANGE_FILE\n\
 prepare-data CHANGE_JSON REQUEST_FILE\n\
 prepare-seal EXPECTED_REVISION VERSION_UUID REQUEST_FILE\n\
 data STATE_DIRECTORY REQUEST_FILE\n\
 resolve-data STATE_DIRECTORY REQUEST_FILE\n\
 info STATE_DIRECTORY\n\
 snapshot STATE_DIRECTORY VERSION_UUID\n\
 page STATE_DIRECTORY SNAPSHOT_JSON AFTER\n\
 prepare-run SNAPSHOT_JSON LOOPBACK_URL REQUEST_FILE\n\
 start STATE_DIRECTORY REQUEST_FILE\n\
 resolve STATE_DIRECTORY REQUEST_FILE\n\
 get STATE_DIRECTORY WORKFLOW_UUID\n\
 download STATE_DIRECTORY HEX_KEY OUTPUT_FILE\n\
 serve STATE_DIRECTORY PORT [SECONDS]\n\nSealed versions are immutable. Draft edits do not change active exports.\n\
Bounds: 512 rows, 16 versions, 32 rows/page, 256 KiB CSV, 1024 run bindings.\n\
Original mutation evidence lasts five minutes; Activities last at most one hour.\n\
CELLULE_EXPORT_TOKEN configures the local adapter credential.\n\
CELLULE_EXPORT_AFTER_PAGE_MS=10000 opens the first-page crash checkpoint.";
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedData {
    version: u8,
    identity: Identity,
    change: Change,
}
impl RetainedData {
    fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1 {
            return Err("unsupported retained dataset request".into());
        }
        self.identity.validate()?;
        self.change.validate()?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    version: u8,
    identity: Identity,
    request: Request,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotDocument {
    snapshot: Snapshot,
    source_cell: [u8; 32],
}
struct Service {
    node: LocalNode,
    data: DataClient,
    files: Artifacts,
    client: ExportClient,
    handle: cellule_app::ApplicationHandle<ExportApplication>,
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
        return Err("retained export file exceeds bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn retained(path: &str) -> Result<Retained, BoxError> {
    let value: Retained = load(Path::new(path), 8192)?;
    if value.version != 1 {
        return Err("unsupported retained export run request".into());
    }
    value.identity.validate()?;
    value.request.validate()?;
    Ok(value)
}
fn retained_data(path: &str) -> Result<RetainedData, BoxError> {
    let value: RetainedData = load(Path::new(path), 1 << 20)?;
    value.validate()?;
    Ok(value)
}
fn key(value: &str) -> Result<[u8; 32], BoxError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("key must contain 64 lowercase hex characters".into());
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(key)
}
pub(crate) fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({"cell":format!("{:?}",value.cell),"incarnation":format!("{:?}",value.incarnation),"commit_sequence":value.commit_sequence})
}
fn sample(count: u32) -> Result<Vec<Row>, BoxError> {
    if count > cellule_cookbook_report_export::MAX_ROWS {
        return Err("sample row count exceeds 512".into());
    }
    Ok((1..=count)
        .filter(|id| *id != 17)
        .map(|id| Row {
            id,
            label: if id == 3 {
                "Café, \"west\"\nqueue".into()
            } else {
                format!("Region {id}")
            },
            units: u64::from(id) * 10,
        })
        .collect())
}
async fn start(state: PathBuf) -> Result<Service, BoxError> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    let store = local_s3_store(&endpoint, "cellule-cookbook")?;
    let files = BlobArtifactStore::new(Store::new(std::sync::Arc::new(
        object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/report-export/artifacts",
        ),
    )));
    let node = LocalNode::start(
        cellule_cookbook_report_export::compile()?,
        store,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/report-export/cells"),
            application_id: APPLICATION,
        },
    )
    .await?;
    let opened = async {
        let handle = node
            .application_handle::<ExportApplication>(TENANT)?
            .with_blob_artifact_store(files);
        cellule_cookbook_report_export::open(&node, &handle).await?;
        Ok::<_, BoxError>(handle)
    }
    .await;
    let handle = match opened {
        Ok(handle) => handle,
        Err(source) => {
            if let Err(cleanup) = node.shutdown().await {
                tracing::error!(%cleanup,"export startup drain failed");
            }
            return Err(source);
        }
    };
    Ok(Service {
        files: Artifacts::new(handle.clone()),
        data: DataClient::new(handle.clone())?,
        client: ExportClient::new(handle.clone()),
        handle,
        node,
    })
}
async fn install(service: &Service, port: u16) -> Result<String, BoxError> {
    let address = adapter::install(
        &service.node,
        adapter::AdapterClient {
            engine: ExportEngine::new(service.data.clone(), service.files.clone()),
            data: service.data.clone(),
        },
        port,
    )
    .await?;
    cellule_cookbook_report_export::spawn_exports(&service.node, service.handle.clone())?;
    Ok(format!("http://{address}/"))
}
async fn data(service: &Service, record: RetainedData) -> Result<(), BoxError> {
    record.validate()?;
    let prepared = service
        .data
        .prepare(record.identity.native(), record.change)
        .await?;
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
            Err(InvocationError::Rejected(value).into())
        }
        Err(source) => Err(source.into()),
    }
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
                serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)})
            );
            Ok(())
        }
        Err(InvocationError::Rejected(value)) => {
            println!(
                "{}",
                serde_json::json!({"outcome":value.output,"receipt":receipt(value.receipt)})
            );
            Err(InvocationError::Rejected(value).into())
        }
        Err(source) => Err(source.into()),
    }
}
fn resolution(value: Resolution) -> serde_json::Value {
    match value {
        Resolution::Absent => serde_json::json!({"resolution":"absent","absence_proven":true}),
        Resolution::Unknown => serde_json::json!({"resolution":"unknown","absence_proven":false}),
        Resolution::Expired => serde_json::json!({"resolution":"expired","absence_proven":false}),
        Resolution::Committed(stored) => {
            serde_json::json!({"resolution":"committed","commit_sequence":stored.commit_sequence()})
        }
    }
}
async fn demo(service: &Service) -> Result<(), BoxError> {
    let endpoint = install(service, 0).await?;
    let rows = sample(70)?;
    let revision = service.data.info(None).await?.output.revision;
    let changed = service
        .data
        .prepare(
            new_identity()?,
            Change::Replace {
                expected_revision: revision,
                rows: rows.clone(),
            },
        )
        .await?
        .execute()
        .await?;
    let DataOutcome::Applied(revision) = changed.output else {
        return Err("demo draft replacement failed".into());
    };
    let version = Version(*uuid::Uuid::now_v7().as_bytes());
    let sealed = service
        .data
        .prepare(
            new_identity()?,
            Change::Seal {
                expected_revision: revision,
                version,
            },
        )
        .await?
        .execute()
        .await?;
    let DataOutcome::Sealed(snapshot) = sealed.output else {
        return Err("demo version sealing failed".into());
    };
    let request = Request {
        id: *uuid::Uuid::now_v7().as_bytes(),
        source_cell: *service.data.target().cell_id().as_bytes(),
        snapshot: snapshot.clone(),
        deadline_ms: now_ms()?
            .checked_add(300_000)
            .ok_or("demo deadline overflow")?,
        endpoint,
    };
    let prepared = service
        .client
        .prepare(new_identity()?, request.clone())
        .await?;
    let original = prepared.clone().execute().await?;
    if prepared.clone().execute().await?.receipt != original.receipt {
        return Err("retained export start changed its receipt".into());
    }
    // The next draft edit must not change the sealed rows read by the in-flight export.
    service
        .data
        .prepare(
            new_identity()?,
            Change::Put {
                expected_revision: revision,
                row: Row {
                    id: 1,
                    label: "Later draft".into(),
                    units: 999,
                },
            },
        )
        .await?
        .execute()
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let view = loop {
        if !service.node.is_ready() {
            return Err("export readiness closed".into());
        }
        let view = service
            .client
            .get(request.id, None)
            .await?
            .output
            .ok_or("export run missing")?;
        if let Some(failure) = &view.state.failure {
            return Err(format!("export failed: {failure}").into());
        }
        if view.state.report.is_some() {
            break view;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("export did not finish within demo bound".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let report = view.state.report.ok_or("report link missing")?;
    let (artifact, bytes) = service
        .files
        .read(report.artifact.key, None)
        .await?
        .output
        .ok_or("linked report missing")?;
    if artifact != report.artifact
        || bytes != cellule_cookbook_report_export::encode_report(&rows)?
        || view.state.chunks.len() != 3
    {
        return Err("sealed export differs, repeats rows, or skips a page".into());
    }
    let repeated = ExportEngine::new(service.data.clone(), service.files.clone())
        .execute(Work::Finalize {
            request: request.clone(),
            chunks: view.state.chunks,
        })
        .await?;
    if !matches!(repeated,cellule_cookbook_report_export::Completion::Report(value)if value==report)
    {
        return Err("report reuse changed the immutable manifest".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario":"passed","workflow_id":uuid::Uuid::from_bytes(request.id),"snapshot":snapshot,"report":report,"checks":["atomic-sealed-version","draft-edit-isolation","three-bounded-pages","native-Activities","ordered-durable-progress","canonical-CSV","sealed-row-digest","immutable-report-reuse","retained-start-replay"]})
    );
    Ok(())
}
async fn operation(service: &Service, args: &[String]) -> Result<(), BoxError> {
    match args {
        [op, _] if op == "demo" => demo(service).await,
        [op, _, path] if op == "data" => data(service, retained_data(path)?).await,
        [op, _, path] if op == "start" => apply(service, retained(path)?).await,
        [op, _, path] if op == "resolve-data" => {
            let record = retained_data(path)?;
            let prepared = service
                .data
                .prepare(record.identity.native(), record.change)
                .await?;
            println!(
                "{}",
                resolution(service.data.resolve(prepared.evidence()).await?)
            );
            Ok(())
        }
        [op, _, path] if op == "resolve" => {
            let record = retained(path)?;
            let prepared = service
                .client
                .prepare(record.identity.native(), record.request)
                .await?;
            println!(
                "{}",
                resolution(service.client.resolve(prepared.evidence()).await?)
            );
            Ok(())
        }
        [op, _] if op == "info" => {
            let observed = service.data.info(None).await?;
            println!(
                "{}",
                serde_json::json!({"dataset":observed.output,"source_cell":service.data.target().cell_id().as_bytes(),"receipt":receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, version] if op == "snapshot" => {
            let snapshot = service
                .data
                .snapshot(Version(*uuid::Uuid::parse_str(version)?.as_bytes()), None)
                .await?
                .output
                .ok_or("sealed dataset missing")?;
            println!(
                "{}",
                serde_json::to_string(&SnapshotDocument {
                    snapshot,
                    source_cell: *service.data.target().cell_id().as_bytes()
                })?
            );
            Ok(())
        }
        [op, _, path, after] if op == "page" => {
            let source: SnapshotDocument = load(Path::new(path), 8192)?;
            if source.source_cell != *service.data.target().cell_id().as_bytes() {
                return Err("foreign dataset Cell".into());
            }
            let page = service
                .data
                .page(
                    PageRequest {
                        snapshot: source.snapshot,
                        after: after.parse()?,
                    },
                    None,
                )
                .await?;
            println!(
                "{}",
                serde_json::json!({"page":page.output,"receipt":receipt(page.receipt)})
            );
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
        [op, _, hex, path] if op == "download" => {
            let (artifact, bytes) = service
                .files
                .read(key(hex)?, None)
                .await?
                .output
                .ok_or("CSV artifact missing")?;
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
                .map(|value| value.parse::<u64>())
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
                    return Err("export worker readiness closed".into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}
fn prepare_data(change: Change, path: &str) -> Result<(), BoxError> {
    change.validate()?;
    let record = RetainedData {
        version: 1,
        identity: new_identity()?.into(),
        change,
    };
    save(Path::new(path), &serde_json::to_vec(&record)?)?;
    println!("{}", serde_json::json!({"prepared":path}));
    Ok(())
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
        [op, count, revision, path] if op == "sample" => {
            let rows = sample(count.parse()?)?;
            save(
                Path::new(path),
                &serde_json::to_vec(&Change::Replace {
                    expected_revision: revision.parse()?,
                    rows,
                })?,
            )?;
            return Ok(());
        }
        [op, source, path] if op == "prepare-data" => {
            return prepare_data(load(Path::new(source), 1 << 20)?, path);
        }
        [op, revision, version, path] if op == "prepare-seal" => {
            return prepare_data(
                Change::Seal {
                    expected_revision: revision.parse()?,
                    version: Version(*uuid::Uuid::parse_str(version)?.as_bytes()),
                },
                path,
            );
        }
        [op, source, endpoint, path] if op == "prepare-run" => {
            let source: SnapshotDocument = load(Path::new(source), 8192)?;
            let request = Request {
                id: *uuid::Uuid::now_v7().as_bytes(),
                source_cell: source.source_cell,
                snapshot: source.snapshot,
                deadline_ms: now_ms()?
                    .checked_add(3_600_000)
                    .ok_or("export deadline overflow")?,
                endpoint: endpoint.clone(),
            };
            request.validate()?;
            let id = request.id;
            save(
                Path::new(path),
                &serde_json::to_vec(&Retained {
                    version: 1,
                    identity: new_identity()?.into(),
                    request,
                })?,
            )?;
            println!(
                "{}",
                serde_json::json!({"prepared":path,"workflow_id":uuid::Uuid::from_bytes(id)})
            );
            return Ok(());
        }
        _ => {}
    }
    if let [op, _, path] = args.as_slice()
        && matches!(op.as_str(), "resolve" | "resolve-data")
    {
        let identity = if op == "resolve" {
            retained(path)?.identity
        } else {
            retained_data(path)?.identity
        };
        if now_ms()? >= identity.expires_at_ms {
            println!("{}", resolution(Resolution::Expired));
            return Ok(());
        }
    }
    let service = start(PathBuf::from(args.get(1).ok_or(HELP)?)).await?;
    let result = tokio::select! {biased;signal=cellule_cookbook_support::shutdown_signal()=>match signal{Ok(())if args.first().is_some_and(|op|op=="serve")=>Ok(()),Ok(())=>Err("interrupted; retain original dataset and export request files".into()),Err(source)=>Err(source.into())},result=operation(&service,&args)=>result};
    let shutdown = service.node.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup,"export drain failed");
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
            eprintln!("report-export: {source}");
            let mut cause = source.source();
            while let Some(source) = cause {
                eprintln!("  caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}
