//! One local Workflow Cell that packs an order through a native Activity.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ActivityContext, ActivityExecution, ActivityHandler, ActivityRunOutcome, ApplicationId,
    BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog, CellClient, CellModule,
    CellRuntime, CellStorageLayout, CellTarget, Digest, Error, IncarnationId, MaintenanceModule,
    MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor, NamespaceId,
    OperationDescriptor, Owner, RegistryBuilder, RequestId, SessionId, SqlWorkerPool, TenantId,
    WORKFLOW_SCHEMA_SQL, WorkflowAction, WorkflowActivityEvent, WorkflowActivityModule,
    WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowModule, WorkflowOutcome,
    WorkflowStatus, decode_workflow_activity_event, install_workflow_schema, partition_for_shard,
    register_activity, register_maintenance, register_workflow, register_workflow_activities,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const FULFILLMENT: NamespaceId = NamespaceId::from_bytes([31; 16]);
const COMMANDS: [OperationDescriptor; 8] = [
    operation(1),
    operation(2),
    operation(3),
    operation(4),
    operation(6),
    operation(7),
    operation(8),
    operation(9),
];
const QUERIES: [OperationDescriptor; 2] = [operation(5), operation(10)];
const ACTIVITY_TYPES: &[&str] = &["pack-order"];

struct PackingDefinition;

impl WorkflowDefinition for PackingDefinition {
    fn digest(&self) -> Digest {
        Digest::from_bytes(*blake3::hash(include_bytes!("fulfillment.rs")).as_bytes())
    }

    fn transition(
        &self,
        _state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if let Some(order) = event.strip_prefix(b"pack ") {
            if order.is_empty() {
                return Err(Error::Command("order is empty"));
            }
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: b"packing".to_vec(),
                result: None,
                actions: vec![WorkflowAction::Activity {
                    activity_type: "pack-order".into(),
                    input: order.to_vec(),
                    due_at_ms: context.now_ms(),
                    expires_at_ms: context.now_ms() + 60_000,
                }],
            });
        }

        match decode_workflow_activity_event(event)? {
            Some(WorkflowActivityEvent::Completed { result, .. }) => Ok(WorkflowDecision {
                status: WorkflowStatus::Completed,
                state: result.to_vec(),
                result: Some(result.to_vec()),
                actions: Vec::new(),
            }),
            Some(WorkflowActivityEvent::Failed { details, .. }) => Ok(WorkflowDecision {
                status: WorkflowStatus::Failed,
                state: details.to_vec(),
                result: None,
                actions: Vec::new(),
            }),
            None => Err(Error::Command("unexpected workflow event")),
        }
    }
}

static PACKING: PackingDefinition = PackingDefinition;
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&PACKING];

struct PackOrder;

impl ActivityHandler for PackOrder {
    const TYPE: &'static str = "pack-order";

    fn execute(
        _context: ActivityContext,
        input: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ActivityExecution> + Send + 'static>> {
        Box::pin(async move {
            let mut result = b"packed ".to_vec();
            result.extend_from_slice(&input);
            ActivityExecution::Completed(result)
        })
    }
}

struct Fulfillment;

impl MaintenanceModule for Fulfillment {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}

impl WorkflowModule for Fulfillment {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = FULFILLMENT;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &PACKING;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}

impl WorkflowActivityModule for Fulfillment {
    const ACTIVITY_TYPES: &'static [&'static str] = ACTIVITY_TYPES;
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}

impl CellModule for Fulfillment {
    const NAME: &'static str = "fulfillment";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DEFINITIONS: OnceLock<[Digest; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("fulfillment.rs")).as_bytes(),
            ),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: WORKFLOW_SCHEMA_SQL,
                    digest: Digest::from_bytes(
                        *blake3::hash(WORKFLOW_SCHEMA_SQL.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: DEFINITIONS.get_or_init(|| [PACKING.digest()]),
            activity_types: ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: FULFILLMENT,
                name: Self::NAME,
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, PackOrder>(registry)?;
        register_maintenance::<Self>(registry)
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

struct FulfillmentApp;

impl CellApplication for FulfillmentApp {
    const NAME: &'static str = "fulfillment-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Fulfillment)?;
        builder.cell_type(CellType::new(
            Fulfillment::NAME,
            "fulfillment",
            FULFILLMENT,
            CatalogRole::Workflow,
            1,
        )?)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(FulfillmentApp::compile(BuildDescriptor {
        source_revision: "local-fulfillment-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([32; 16]);
    let application_id = ApplicationId::from_bytes([33; 16]);
    let target = CellTarget::new(tenant, application_id, FULFILLMENT, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("fulfillment-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Fulfillment::NAME)
        .ok_or(Error::Registry("fulfillment module is missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Workflow, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([34; 16]);
    let session = SessionId::from_bytes([35; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://fulfillment.local".into(),
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
    let result: Result<String, Box<dyn std::error::Error>> = async {
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
                files.path().join("fulfillment.sqlite"),
                install_workflow_schema,
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed =
            ApplicationHandle::<FulfillmentApp>::new(client, application, tenant, application_id);
        let workflow = typed.workflow::<Fulfillment>()?;
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let workflow_id = b"order/42".to_vec();
        let started = workflow
            .start(
                MutationIdentity {
                    request_id: RequestId::from_bytes([36; 16]),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + 60_000,
                },
                workflow_id.clone(),
                b"pack order 42".to_vec(),
            )
            .await?;
        if !matches!(
            started.output,
            WorkflowOutcome::Applied {
                status: WorkflowStatus::Running,
                ..
            }
        ) {
            return Err(Error::Control("fulfillment did not start").into());
        }
        let activity =
            cellule_runtime::ActivitySupervisor::new(typed.activities::<Fulfillment>()?, 5_000)?;
        let receipt = match activity.run_once(0, None).await? {
            ActivityRunOutcome::Completed { receipt, .. } => receipt,
            _ => return Err(Error::Control("packing activity did not complete").into()),
        };
        let completed = workflow
            .state(workflow_id, Some(receipt))
            .await?
            .output
            .ok_or(Error::Control("fulfillment state is missing"))?;
        let result = completed
            .result
            .ok_or(Error::Control("fulfillment result is missing"))?;
        if completed.status != WorkflowStatus::Completed || result != b"packed order 42" {
            return Err(Error::Control("fulfillment result differs").into());
        }
        Ok(String::from_utf8(result)?)
    }
    .await;
    let shutdown = runtime.shutdown().await;
    let result = result?;
    shutdown?;
    println!("fulfillment completed: {result}");
    Ok(())
}
