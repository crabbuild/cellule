use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowActivityModule, WorkflowDefinition, WorkflowModule, register_activity,
            register_workflow, register_workflow_activities,
        },
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// Separate directory transaction domain.
pub const DIRECTORY: NamespaceId = NamespaceId::from_bytes([0x81; 16]);
/// Typed directory module and pinned storage contracts.
#[derive(Clone)]
pub struct Directory;
/// Separate flows transaction domain.
pub const FLOWS: NamespaceId = NamespaceId::from_bytes([0x82; 16]);
/// Typed flows module and pinned storage contracts.
#[derive(Clone)]
pub struct Flows;
/// Separate provider transaction domain.
pub const PROVIDER: NamespaceId = NamespaceId::from_bytes([0x83; 16]);
/// Typed provider module and pinned storage contracts.
#[derive(Clone)]
pub struct Provider;
const fn op(id: u32, input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}
fn migration(sql: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql,
        digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
    }]
}
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl WorkflowModule for Flows {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = FLOWS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Flows {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::activity::TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
impl MaintenanceModule for Flows {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl MaintenanceModule for Directory {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Provider {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Directory {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl EffectModule for Flows {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 14;
    const LEASE_COMMAND_ID: u32 = 15;
    const VALIDATE_QUERY_ID: u32 = 16;
    const STATUS_QUERY_ID: u32 = 17;
}
impl CellModule for Directory {
    const NAME: &'static str = "provisioning.directory";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 2048, 256),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 4096, 128),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(2, 64, 4096),
            op(6, 1 << 20, 8),
            op(7, 64, 1 << 20),
            op(9, 256, 262144),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("directory.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: DIRECTORY,
                name: "provisioning-directory",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[FLOWS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeResource>()?;
        registry.bind_command::<crate::ProjectResource>()?;
        registry.bind_query::<crate::directory::GetResource>()?;
        registry.bind_query::<crate::directory::ListResources>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Flows {
    const NAME: &'static str = "provisioning.flows";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 64),
            op(2, 8192, 64),
            op(3, 1024, 64),
            op(4, 4096, 64),
            op(6, 16, 1 << 20),
            op(7, 1 << 20, 1 << 20),
            op(8, 1 << 20, 64),
            op(9, 8, 8),
            op(11, 2048, 128),
            op(12, 2048, 128),
            op(13, 4096, 128),
            op(14, 8, 1 << 20),
            op(15, 1 << 20, 1 << 20),
            op(18, 512, 128),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(5, 64, 16384),
            op(10, 1 << 20, 8),
            op(16, 1 << 20, 8),
            op(17, 64, 1 << 20),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static DIGESTS: OnceLock<[Digest; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("flows.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGESTS.get_or_init(|| [crate::definition::digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: FLOWS,
                name: "provisioning-flows",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[DIRECTORY],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::activity::HttpProvider>(registry)?;
        register_effect_delivery::<Self>(registry)?;
        registry.bind_command::<crate::StartResource>()?;
        registry.bind_command::<crate::DeleteResource>()?;
        registry.bind_command::<crate::ReplyResource>()?;
        registry.bind_command::<crate::ReconcileResource>()?;
        register_maintenance::<Self>(registry)
    }
}
impl CellModule for Provider {
    const NAME: &'static str = "provisioning.provider";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 4096, 128), op(3, 8, 8), op(8, 128, 128)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 64, 4096), op(9, 8, 64)];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("provider.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: PROVIDER,
                name: "provisioning-provider",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ApplyProvider>()?;
        registry.bind_command::<crate::AdvanceProvider>()?;
        registry.bind_query::<crate::provider::GetProviderResource>()?;
        registry.bind_query::<crate::provider::NextProviderDue>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("application.rs"),
        include_bytes!("directory.rs"),
        include_bytes!("flows.rs"),
        include_bytes!("provider.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("service.rs"),
        include_bytes!("directory.sql"),
        include_bytes!("flows.sql"),
        include_bytes!("provider.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Resource directory, native lifecycle coordination, and independently owned provider simulator.
pub struct ProvisioningApplication;
impl CellApplication for ProvisioningApplication {
    const NAME: &'static str = "cellule-cookbook-provisioning";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Directory)?;
        builder.register(Flows)?;
        builder.register(Provider)?;
        for (module, name, namespace, role) in [
            (Directory::NAME, "directory", DIRECTORY, CatalogRole::Sql),
            (Flows::NAME, "flows", FLOWS, CatalogRole::Workflow),
            (Provider::NAME, "provider", PROVIDER, CatalogRole::Sql),
        ] {
            builder.cell_type(
                CellType::new(module, name, namespace, role, 1)?.with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles immutable source, dependency, schema, and wire contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(ProvisioningApplication::compile(
        BuildDescriptor {
            source_revision: format!("provisioning:{:?}", source_digest()),
            cargo_lock_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
            ),
        },
    )?))
}
