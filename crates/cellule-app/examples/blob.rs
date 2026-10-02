//! Upload one multipart Blob, then read it at the completion receipt.
//!
//! Run: `cargo run -p cellule-app --example blob --locked`
//!
//!   Blob Cell: Begin upload -> PutPart -> Complete
//!   Object store: staged immutable part -> referenced published Blob
//!   Application: completion receipt -> receipt-bound Read -> verify bytes
//!
//! Each mutation has its own stable request ID. Staging a part does not make
//! the Blob visible: Complete publishes the reference in the Cell's durable
//! state. The in-memory store and one small text attachment are local fixtures.

use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::identity::{IncarnationId, RequestId};
use cellule_runtime::ltx::CellStorageLayout;
use cellule_runtime::primitives::blob::{
    BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    install_blob_schema, register_blob,
};
use cellule_runtime::primitives::maintenance::MaintenanceModule;
use cellule_runtime::registry::OperationDescriptor;
use cellule_runtime::{
    ApplicationId, BlobArtifactStore, BlobModule, BlobNamespace, BuildDescriptor, CatalogRole,
    CellClient, CellModule, CellRuntime, CellTarget, Digest, Error, MigrationDescriptor,
    ModuleDescriptor, MutationIdentity, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    SessionId, SqlWorkerPool, TenantId, partition_for_shard,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const ATTACHMENTS: NamespaceId = NamespaceId::from_bytes([61; 16]);
// The bootstrap callback below installs Blob tables; this pins the example's
// module version in its descriptor.
const MIGRATION: &str = "-- attachments example schema v1";
const ATTACHMENT_KEY: &[u8] = b"orders/42/receipt.txt";
const ATTACHMENT_BODY: &[u8] = b"receipt for order 42";
const CONTENT_TYPE: &str = "text/plain";
const COMMANDS: [OperationDescriptor; 2] = [operation(1), operation(3)];
const QUERIES: [OperationDescriptor; 1] = [operation(2)];
type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

struct Attachments;

impl MaintenanceModule for Attachments {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}

impl BlobModule for Attachments {
    const NAMESPACE: NamespaceId = ATTACHMENTS;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}

impl CellModule for Attachments {
    const NAME: &'static str = "attachments";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(*blake3::hash(include_bytes!("blob.rs")).as_bytes()),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: MIGRATION,
                    digest: Digest::from_bytes(*blake3::hash(MIGRATION.as_bytes()).as_bytes()),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ATTACHMENTS,
                name: Self::NAME,
                role: CatalogRole::Blob,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_blob::<Self>(registry)
    }
}

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: 1 << 20,
        output_limit: 1 << 20,
    }
}

struct AttachmentsApp;

impl CellApplication for AttachmentsApp {
    const NAME: &'static str = "attachments-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Attachments)?;
        builder.cell_type(CellType::new(
            Attachments::NAME,
            "attachments",
            ATTACHMENTS,
            CatalogRole::Blob,
            1,
        )?)?;
        Ok(())
    }
}

fn identity(request_id: u8, now_ms: i64) -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes([request_id; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

async fn upload_and_read_attachment(blob: &BlobNamespace<Attachments>) -> ExampleResult<String> {
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let key = ATTACHMENT_KEY.to_vec();
    let payload = ATTACHMENT_BODY.to_vec();
    let upload_id = [66; 16];

    let begun = blob
        .mutate(
            identity(67, now_ms),
            BlobMutation::Begin {
                key: key.clone(),
                upload_id,
                condition: BlobCondition::Missing,
                content_type: Some(CONTENT_TYPE.into()),
                metadata: Vec::new(),
                expires_at_ms: now_ms + 60_000,
            },
        )
        .await?;
    if begun.output != BlobMutationOutcome::Begun {
        return Err(Error::Control("attachment upload did not begin").into());
    }

    // Staging and preparation retain exact evidence before the Cell command
    // is dispatched. Part bytes alone do not publish the attachment.
    let prepared_part = blob
        .prepare_mutation(
            identity(68, now_ms),
            BlobMutation::PutPart {
                key: key.clone(),
                upload_id,
                part_number: 1,
                payload: payload.clone(),
            },
        )
        .await?;
    let _part_evidence = prepared_part.evidence().clone();
    let part = prepared_part.execute().await?;
    if !matches!(part.output, BlobMutationOutcome::PartStored { .. }) {
        return Err(Error::Control("attachment part was not stored").into());
    }

    let committed = blob
        .mutate(
            identity(69, now_ms),
            BlobMutation::Complete {
                key: key.clone(),
                upload_id,
                part_count: 1,
            },
        )
        .await?;
    if !matches!(
        committed.output,
        BlobMutationOutcome::Committed { size, .. } if size == payload.len() as u64
    ) {
        return Err(Error::Control("attachment was not published").into());
    }

    // A read at the completion receipt observes the published attachment.
    let observed = blob
        .query(
            BlobQuery::Read {
                key,
                offset: 0,
                limit: 128,
            },
            Some(committed.receipt),
        )
        .await?;
    let BlobQueryResult::Read(Some(read)) = observed.output else {
        return Err(Error::Control("attachment read is missing").into());
    };
    if read.bytes != payload || read.metadata.content_type.as_deref() != Some(CONTENT_TYPE) {
        return Err(Error::Control("attachment read differs").into());
    }
    Ok(String::from_utf8(read.bytes)?)
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let application = Arc::new(AttachmentsApp::compile(BuildDescriptor {
        source_revision: "local-attachments-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([62; 16]);
    let application_id = ApplicationId::from_bytes([63; 16]);
    let target = CellTarget::new(tenant, application_id, ATTACHMENTS, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store.clone(),
        Path::from("attachments-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Attachments::NAME)
        .ok_or(Error::Registry("attachments module is missing"))?;
    // Publish the catalog entry and fenced owner before bootstrapping the Cell.
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Blob, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([64; 16]);
    let session = SessionId::from_bytes([65; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://attachments.local".into(),
            },
        )
        .await?;
    let files = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: ExampleResult<String> = async {
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )?,
                authority,
                observed,
                files.path().join("attachments.sqlite"),
                install_blob_schema,
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed =
            ApplicationHandle::<AttachmentsApp>::new(client, application, tenant, application_id)?
                .with_blob_artifact_store(BlobArtifactStore::new(store));
        let blob = typed.blob::<Attachments>()?;
        upload_and_read_attachment(&blob).await
    }
    .await;
    // Drain the runtime on both the success and error paths.
    let shutdown = runtime.shutdown().await;
    let attachment = result?;
    shutdown?;
    println!("attachment stored: {attachment}");
    Ok(())
}
