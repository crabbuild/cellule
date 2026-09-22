//! One local KV Cell that saves and reads a customer's cart.

use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellStorageLayout, CellTarget, Digest, Error,
    IncarnationId, KV_SCHEMA_SQL, KvAtomicOutcome, KvAtomicRequest, KvCheck, KvCondition, KvModule,
    KvMutation, MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor,
    NamespaceId, OperationDescriptor, Owner, RegistryBuilder, RequestId, SessionId, SqlWorkerPool,
    TenantId, install_kv_schema, partition_for_shard, register_kv,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const CARTS: NamespaceId = NamespaceId::from_bytes([11; 16]);
const COMMANDS: [OperationDescriptor; 1] = [operation(1)];
const QUERIES: [OperationDescriptor; 2] = [operation(2), operation(3)];

struct Carts;

impl KvModule for Carts {
    const MODULE: &'static str = Self::NAME;
    const ATOMIC_COMMAND_ID: u32 = 1;
    const GET_QUERY_ID: u32 = 2;
    const LIST_QUERY_ID: u32 = 3;
}

impl CellModule for Carts {
    const NAME: &'static str = "carts";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(*blake3::hash(include_bytes!("carts.rs")).as_bytes()),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: KV_SCHEMA_SQL,
                    digest: Digest::from_bytes(*blake3::hash(KV_SCHEMA_SQL.as_bytes()).as_bytes()),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: CARTS,
                name: Self::NAME,
                role: CatalogRole::Kv,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_kv::<Self>(registry)
    }
}

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: 4 * 1024,
        output_limit: 4 * 1024,
    }
}

struct CartsApp;

impl CellApplication for CartsApp {
    const NAME: &'static str = "carts-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Carts)?;
        builder.cell_type(CellType::new(
            Carts::NAME,
            "carts",
            CARTS,
            CatalogRole::Kv,
            1,
        )?)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CartsApp::compile(BuildDescriptor {
        source_revision: "local-carts-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([12; 16]);
    let application_id = ApplicationId::from_bytes([13; 16]);
    let target = CellTarget::new(tenant, application_id, CARTS, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("carts-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Carts::NAME)
        .ok_or(Error::Registry("carts module is missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Kv, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([14; 16]);
    let session = SessionId::from_bytes([15; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://carts.local".into(),
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
    let result: Result<(), Box<dyn std::error::Error>> = async {
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
                files.path().join("carts.sqlite"),
                install_kv_schema,
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed = ApplicationHandle::<CartsApp>::new(client, application, tenant, application_id);
        let kv = typed.kv::<Carts>(CARTS)?;
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let scope = b"customer/42".to_vec();
        let key = b"cart".to_vec();
        let value = b"book x2".to_vec();
        let committed = kv
            .atomic(
                MutationIdentity {
                    request_id: RequestId::from_bytes([16; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                KvAtomicRequest {
                    scope: scope.clone(),
                    checks: vec![KvCheck {
                        key: key.clone(),
                        condition: KvCondition::Absent,
                    }],
                    mutations: vec![KvMutation::Put {
                        key: key.clone(),
                        value: value.clone(),
                        expires_at_ms: None,
                    }],
                },
            )
            .await?;
        if !matches!(committed.output, KvAtomicOutcome::Applied(_)) {
            return Err(Error::Control("cart was not saved").into());
        }
        let observed = kv.get(scope, key, Some(committed.receipt)).await?;
        if observed.output.as_ref().map(|entry| &entry.value) != Some(&value) {
            return Err(Error::Control("published cart differs").into());
        }
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    result?;
    shutdown?;
    println!("cart for customer 42: book x2");
    Ok(())
}
