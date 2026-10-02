//! Persistent multipart file vault. Local source plans are retained before dispatch.

use cellule_cookbook_file_vault::{
    Etag, FileClient, FileKey, FileVault, Files, Metadata, PART_BYTES, Publication, RetainedUpload,
    Write,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, new_identity};
use cellule_runtime::{
    ApplicationId, BlobArtifactStore, Committed, InvocationError, MutationIdentity, Receipt,
    Resolution, TenantId,
    cell::executor::StoredOutcome,
    codec::{BoundedDecoder, WireValue},
    identity::RequestId,
    primitives::blob::BlobMutationOutcome,
};
use cellule_store::Store;
use serde::{Deserialize, Serialize};
use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::ExitCode,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x33; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x43; 16]);
const HELP: &str = "Cellule file vault\n\n\
  cellule-cookbook-file-vault demo STATE_DIRECTORY\n\
  cellule-cookbook-file-vault prepare FILE_KEY INPUT_FILE PLAN_DIRECTORY [PREVIOUS_ETAG]\n\
  cellule-cookbook-file-vault stage STATE_DIRECTORY PLAN_DIRECTORY [PARTS]\n\
  cellule-cookbook-file-vault resume STATE_DIRECTORY PLAN_DIRECTORY\n\
  cellule-cookbook-file-vault abort STATE_DIRECTORY PLAN_DIRECTORY\n\
  cellule-cookbook-file-vault head STATE_DIRECTORY FILE_KEY\n\
  cellule-cookbook-file-vault download STATE_DIRECTORY FILE_KEY OUTPUT_FILE [EXPECTED_ETAG]\n\
  cellule-cookbook-file-vault prepare-delete FILE_KEY ETAG REQUEST_FILE\n\
  cellule-cookbook-file-vault delete STATE_DIRECTORY REQUEST_FILE\n\n\
Plans retain frozen bytes and original identities. Request validity is five minutes.\n\
Upload lifetime is one hour. Staged files become visible only after resume completes.\n\
Files are nonempty and at most 8 MiB; reads and parts are bounded at 256 KiB.";

struct Service {
    node: LocalNode,
    artifacts: BlobArtifactStore,
}
async fn start(state: PathBuf) -> Result<Service> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    let store = local_s3_store(&endpoint, "cellule-cookbook")?;
    // The artifact view is private to this application. Metadata and immutable
    // bodies use the same provider but distinct physical prefixes.
    let artifacts = BlobArtifactStore::new(Store::new(std::sync::Arc::new(
        object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/file-vault/artifacts",
        ),
    )));
    let node = LocalNode::start(
        cellule_cookbook_file_vault::compile()?,
        store,
        NodeConfig {
            state_directory: state,
            storage_prefix: object_store::path::Path::from("cookbook/file-vault/cells"),
            application_id: APPLICATION,
        },
    )
    .await?;
    Ok(Service { node, artifacts })
}
async fn client(service: &Service, key: FileKey) -> Result<FileClient> {
    // The OS principal owns the fixed local tenant. A network embedding must
    // authenticate before selecting its tenant and private artifact scope.
    let client = FileClient::new(
        service
            .node
            .application_handle::<FileVault>(TENANT)?
            .with_blob_artifact_store(service.artifacts.clone()),
        key,
    )?;
    service.node.open_cell(client.target(), &Files).await?;
    Ok(client)
}
fn parse_etag(value: &str) -> Result<Etag> {
    Ok(serde_json::from_value(serde_json::Value::String(
        value.into(),
    ))?)
}
fn receipt(value: Receipt) -> serde_json::Value {
    serde_json::json!({"cell": format!("{:?}", value.cell), "incarnation": format!("{:?}", value.incarnation), "commit_sequence": value.commit_sequence})
}
fn outcome(value: BlobMutationOutcome) -> serde_json::Value {
    match value {
        BlobMutationOutcome::Committed { etag, size } => {
            serde_json::json!({"status": "committed", "size": size,
            "etag": etag.iter().map(|b| format!("{b:02x}")).collect::<String>()})
        }
        BlobMutationOutcome::PartStored { .. } => serde_json::json!({"status": "part_stored"}),
        BlobMutationOutcome::Begun => serde_json::json!({"status": "begun"}),
        BlobMutationOutcome::Aborted => serde_json::json!({"status": "aborted"}),
        BlobMutationOutcome::Deleted => serde_json::json!({"status": "deleted"}),
        BlobMutationOutcome::NotFound => serde_json::json!({"status": "not_found"}),
        BlobMutationOutcome::Conflict => serde_json::json!({"status": "conflict"}),
    }
}
async fn phase(
    client: &FileClient,
    request: (MutationIdentity, Write),
) -> Result<Committed<BlobMutationOutcome>> {
    let prepared = client.prepare(request.0, request.1).await?;
    // Resolve first: a previous process may have published this exact phase
    // before losing its response. Dispatch only when its evidence is absent.
    match client.resolve(prepared.evidence()).await? {
        Resolution::Absent => match prepared.execute().await {
            Ok(committed) => Ok(committed),
            Err(InvocationError::Rejected(rejected)) => {
                println!(
                    "{}",
                    serde_json::json!({"outcome": outcome(rejected.output), "receipt": receipt(rejected.receipt)})
                );
                Err(InvocationError::Rejected(rejected).into())
            }
            Err(error) => Err(error.into()),
        },
        Resolution::Committed(stored) => {
            let mut decoder = BoundedDecoder::new(stored.result(), 128)?;
            let output = BlobMutationOutcome::decode(&mut decoder)?;
            decoder.finish()?;
            let committed = Committed {
                output,
                receipt: Receipt {
                    cell: prepared.evidence().target().cell_id(),
                    incarnation: prepared.evidence().incarnation(),
                    commit_sequence: stored.commit_sequence(),
                },
            };
            if matches!(stored, StoredOutcome::Rejected { .. }) {
                println!(
                    "{}",
                    serde_json::json!({"outcome": outcome(committed.output), "receipt": receipt(committed.receipt)})
                );
                return Err(InvocationError::Rejected(Box::new(committed)).into());
            }
            Ok(committed)
        }
        Resolution::Unknown => Err("phase outcome unknown; retain the upload plan".into()),
        Resolution::Expired => {
            Err("phase evidence expired; inspect the file before choosing new work".into())
        }
    }
}
async fn upload(
    service: &Service,
    plan: &RetainedUpload,
    count: u32,
    complete: bool,
) -> Result<()> {
    if count == 0 || count > plan.part_count() {
        return Err("stage count is outside the upload plan".into());
    }
    let client = client(service, plan.key()?).await?;
    phase(&client, plan.begin()?).await?;
    for number in 1..=count {
        phase(&client, plan.part(number)?).await?;
    }
    if complete {
        let committed = phase(&client, plan.complete()?).await?;
        let BlobMutationOutcome::Committed { etag, size } = committed.output else {
            return Err("upload completion did not publish a file".into());
        };
        if size != plan.size() {
            return Err("published size differs from frozen source".into());
        }
        // Report the recorded publication, even if a later command replaced
        // or deleted it. A read at an old receipt may observe newer state.
        let metadata = Metadata {
            key: std::str::from_utf8(plan.key()?.as_bytes())?.into(),
            etag: etag.into(),
            size,
            parts: plan.part_count(),
        };
        println!(
            "{}",
            serde_json::json!({"outcome": outcome(committed.output), "file": metadata, "receipt": receipt(committed.receipt)})
        );
    } else {
        println!(
            "{}",
            serde_json::json!({"staged_parts": count, "visible_file": client.head(None).await?.output})
        );
    }
    Ok(())
}
async fn download(
    service: &Service,
    key: FileKey,
    destination: &Path,
    expected: Option<Etag>,
) -> Result<()> {
    let client = client(service, key).await?;
    let head = client.head(None).await?;
    let metadata = head.output.ok_or("published file does not exist")?;
    if expected.is_some_and(|etag| etag != metadata.etag) {
        return Err("file differs from expected ETag".into());
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut output = tempfile::NamedTempFile::new_in(parent)?;
    let mut offset = 0;
    let mut minimum = head.receipt;
    let mut digest = blake3::Hasher::new();
    while offset < metadata.size {
        let observed = client
            .read(offset, PART_BYTES as u32, Some(minimum))
            .await?;
        let range = observed.output.ok_or("file disappeared during download")?;
        if range.metadata.etag != metadata.etag || range.metadata.size != metadata.size {
            return Err("file changed during download; retry with the required ETag".into());
        }
        if range.offset != offset
            || range.bytes.is_empty()
            || range.bytes.len() as u64 > metadata.size - offset
        {
            return Err("file range did not advance within the expected size".into());
        }
        output.write_all(&range.bytes)?;
        digest.update(&range.bytes);
        offset += range.bytes.len() as u64;
        minimum = observed.receipt;
    }
    output.as_file().sync_all()?;
    output.persist_noclobber(destination)?;
    std::fs::File::open(parent)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"downloaded": destination, "file": metadata, "digest": digest.finalize().to_hex().to_string(), "receipt": receipt(minimum)})
    );
    Ok(())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteFile {
    version: u8,
    key: String,
    etag: Etag,
    request: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
}
fn prepare_delete(key: &str, etag: &str, destination: &Path) -> Result<()> {
    FileKey::new(key)?;
    let identity = new_identity()?;
    let request = DeleteFile {
        version: 1,
        key: key.into(),
        etag: parse_etag(etag)?,
        request: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    file.write_all(&serde_json::to_vec_pretty(&request)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"prepared": destination, "request_id": request.request})
    );
    Ok(())
}
async fn delete(service: &Service, path: &Path) -> Result<()> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err("delete request exceeds 4 KiB".into());
    }
    let record: DeleteFile = serde_json::from_slice(&bytes)?;
    if record.version != 1 {
        return Err("unsupported delete request".into());
    }
    let request = uuid::Uuid::parse_str(&record.request)?;
    if request.is_nil() {
        return Err("zero delete request identity".into());
    }
    let client = client(service, FileKey::new(record.key)?).await?;
    let committed = phase(
        &client,
        (
            MutationIdentity {
                request_id: RequestId::from_bytes(*request.as_bytes()),
                issued_at_ms: record.issued_at_ms,
                expires_at_ms: record.expires_at_ms,
            },
            Write::Delete { etag: record.etag },
        ),
    )
    .await?;
    println!(
        "{}",
        serde_json::json!({"outcome": outcome(committed.output), "receipt": receipt(committed.receipt)})
    );
    Ok(())
}
async fn demo(service: &Service) -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("source.bin");
    let expected: Vec<u8> = (0..PART_BYTES + 37)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source, &expected)?;
    let key = FileKey::new(format!(
        "demo/{}/artifact.bin",
        uuid::Uuid::now_v7().simple()
    ))?;
    let plan = RetainedUpload::prepare(
        &source,
        key,
        Publication::Missing,
        &temporary.path().join("plan"),
    )?;
    upload(service, &plan, 1, false).await?;
    if client(service, plan.key()?)
        .await?
        .head(None)
        .await?
        .output
        .is_some()
    {
        return Err("staged bytes must remain invisible".into());
    }
    upload(service, &plan, plan.part_count(), true).await?;
    let output = temporary.path().join("download.bin");
    download(service, plan.key()?, &output, None).await?;
    if std::fs::read(output)? != expected {
        return Err("download differs from frozen source".into());
    }
    println!(
        "{}",
        serde_json::json!({"scenario": "passed", "checks": ["multipart", "staged-invisibility", "retained-phase-resolution", "verified-download"], "bytes": plan.size()})
    );
    Ok(())
}
async fn operation(service: &Service, args: &[String]) -> Result<()> {
    match args {
        [op, _] if op == "demo" => demo(service).await,
        [op, _, directory, rest @ ..]
            if (op == "stage" && rest.len() <= 1) || (op == "resume" && rest.is_empty()) =>
        {
            let plan = RetainedUpload::load(Path::new(directory))?;
            let count = if op == "resume" {
                plan.part_count()
            } else {
                rest.first()
                    .map(|value| value.parse())
                    .transpose()?
                    .unwrap_or(1)
            };
            upload(service, &plan, count, op == "resume").await
        }
        [op, _, directory] if op == "abort" => {
            let plan = RetainedUpload::load(Path::new(directory))?;
            let committed = phase(&client(service, plan.key()?).await?, plan.abort()?).await?;
            println!(
                "{}",
                serde_json::json!({"outcome": outcome(committed.output), "receipt": receipt(committed.receipt)})
            );
            Ok(())
        }
        [op, _, key] if op == "head" => {
            let observed = client(service, FileKey::new(key)?)
                .await?
                .head(None)
                .await?;
            println!(
                "{}",
                serde_json::json!({"file": observed.output, "receipt": receipt(observed.receipt)})
            );
            Ok(())
        }
        [op, _, key, destination, rest @ ..] if op == "download" && rest.len() <= 1 => {
            download(
                service,
                FileKey::new(key)?,
                Path::new(destination),
                rest.first().map(|value| parse_etag(value)).transpose()?,
            )
            .await
        }
        [op, _, path] if op == "delete" => delete(service, Path::new(path)).await,
        _ => Err(HELP.into()),
    }
}
async fn run(args: Vec<String>) -> Result<()> {
    match args.as_slice() {
        [] => {
            println!("{HELP}");
            return Ok(());
        }
        [help] if help == "help" || help == "--help" => {
            println!("{HELP}");
            return Ok(());
        }
        [op, key, source, destination, rest @ ..] if op == "prepare" && rest.len() <= 1 => {
            let publication = rest
                .first()
                .map(|value| parse_etag(value).map(Publication::Match))
                .transpose()?
                .unwrap_or(Publication::Missing);
            let plan = RetainedUpload::prepare(
                Path::new(source),
                FileKey::new(key)?,
                publication,
                Path::new(destination),
            )?;
            println!(
                "{}",
                serde_json::json!({"prepared": destination, "parts": plan.part_count(), "bytes": plan.size()})
            );
            return Ok(());
        }
        [op, key, etag, destination] if op == "prepare-delete" => {
            return prepare_delete(key, etag, Path::new(destination));
        }
        _ => {}
    }
    let service = start(PathBuf::from(args.get(1).ok_or(HELP)?)).await?;
    let result = tokio::select! {
        biased;
        interrupted = cellule_cookbook_support::shutdown_signal() => match interrupted {
            Ok(()) => Err("interrupted; retain upload plans and delete requests for outcome resolution".into()),
            Err(error) => Err(error.into()),
        },
        result = operation(&service, &args) => result,
    };
    let shutdown = service.node.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), cleanup) => {
            if let Err(cleanup) = cleanup {
                tracing::error!(%cleanup, "file vault drain failed");
            }
            Err(error)
        }
        (Ok(()), Err(error)) => Err(error.into()),
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
        Err(error) => {
            eprintln!("file-vault: {error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
