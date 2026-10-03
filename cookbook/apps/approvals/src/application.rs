use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowActivityModule, WorkflowDefinition, WorkflowModule, register_activity,
            register_workflow, register_workflow_activities,
        },
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// Two fixed Workflow shards selected by the canonical purchase UUID bytes.
pub const APPROVALS: NamespaceId = NamespaceId::from_bytes([0x91; 16]);
/// Native Workflow and Activity module with one pinned approval definition.
pub struct Approvals;
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl MaintenanceModule for Approvals {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl WorkflowModule for Approvals {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = APPROVALS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Approvals {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::mailbox::REMINDER_TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
const fn operation(id: u32, input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}
impl CellModule for Approvals {
    const NAME: &'static str = "approvals.requests";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = "-- native Workflow schema v1; application business state codec v1";
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 16 << 10, 64),
            operation(2, 1024, 64),
            operation(3, 1024, 64),
            operation(4, 16 << 10, 64),
            operation(6, 16, 1 << 20),
            operation(7, 1 << 20, 1 << 20),
            operation(8, 1 << 20, 64),
            operation(9, 8, 8),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(5, 64, 16 << 10),
            operation(10, 1 << 20, 8),
            operation(11, 256, 8192),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DEFINITIONS: OnceLock<[Digest; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DEFINITIONS.get_or_init(|| [crate::definition::digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: APPROVALS,
                name: "purchase-approvals",
                role: CatalogRole::Workflow,
                shards: 2,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::mailbox::SendReminder>(registry)?;
        registry.bind_query::<crate::History>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("application.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("mailbox.rs"),
        include_bytes!("lib.rs"),
        include_bytes!("service.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Reusable human approval application; authentication and ingress belong to its embedding.
pub struct ApprovalApplication;
impl CellApplication for ApprovalApplication {
    const NAME: &'static str = "cellule-cookbook-approvals";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Approvals)?;
        builder.cell_type(
            CellType::new(
                Approvals::NAME,
                "approvals",
                APPROVALS,
                CatalogRole::Workflow,
                2,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles pinned business source, workflow definition, namespace, and operation contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(ApprovalApplication::compile(BuildDescriptor {
        source_revision: format!("approvals:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
