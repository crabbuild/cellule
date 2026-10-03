use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BlobModule, BuildDescriptor, CatalogRole, CellModule, Digest, Error, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        blob::register_blob,
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowActivityModule, WorkflowDefinition, WorkflowModule, register_activity,
            register_workflow, register_workflow_activities,
        },
    },
    registry::{OperationDescriptor, RetainedCodeDescriptor},
};
use std::sync::{Arc, OnceLock};
/// Private immutable release artifact transaction domain.
pub const ARTIFACTS: NamespaceId = NamespaceId::from_bytes([0x92; 16]);
/// Native release coordination transaction domain.
pub const FLOWS: NamespaceId = NamespaceId::from_bytes([0x93; 16]);
/// Receiver-local release progress transaction domain.
pub const RECORDS: NamespaceId = NamespaceId::from_bytes([0x94; 16]);
/// Native immutable artifact manifests and staged parts.
#[derive(Clone)]
pub struct Artifacts;
/// Signed monotonic SQL release-record receiver and callback source.
#[derive(Clone)]
pub struct Records;
/// Compiled versioned Workflow module; only the fallible constructor can select its descriptor.
#[derive(Clone)]
pub struct Flows<const V: u8> {
    descriptor: &'static ModuleDescriptor,
}
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
fn source_digest() -> Digest {
    let mut h = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("pipeline.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("flow.rs"),
        include_bytes!("records.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("artifacts.rs"),
        include_bytes!("client.rs"),
        include_bytes!("service.rs"),
        include_bytes!("release_application.rs"),
        include_bytes!("flows.sql"),
        include_bytes!("records.sql"),
    ] {
        h.update(&(source.len() as u64).to_be_bytes());
        h.update(source);
    }
    Digest::from_bytes(*h.finalize().as_bytes())
}
impl BlobModule for Artifacts {
    const NAMESPACE: NamespaceId = ARTIFACTS;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl MaintenanceModule for Artifacts {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl CellModule for Artifacts {
    const NAME: &'static str = "release.artifacts";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 1024, 128), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 1024, 8192)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION
                .get_or_init(|| migration("-- release private native Blob schema v1")),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ARTIFACTS,
                name: "release-artifacts",
                role: CatalogRole::Blob,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_blob::<Self>(r)
    }
}
impl MaintenanceModule for Records {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Records {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl CellModule for Records {
    const NAME: &'static str = "release.records";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 8192, 8),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[op(2, 64, 8192), op(6, 1 << 20, 8), op(7, 64, 1 << 20)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("records.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: RECORDS,
                name: "release-records",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[FLOWS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        r.bind_command::<crate::ProjectRelease>()?;
        r.bind_query::<crate::GetReleaseRecord>()?;
        register_effect_delivery::<Self>(r)?;
        register_maintenance::<Self>(r)
    }
}
static OLD_DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::V1];
static ALL_DEFINITIONS: &[&dyn WorkflowDefinition] =
    &[&crate::definition::V1, &crate::definition::V2];
impl<const V: u8> WorkflowModule for Flows<V> {
    const MODULE: &'static str = "release.flows";
    const NAMESPACE: NamespaceId = FLOWS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = if V == 1 {
        &crate::definition::V1
    } else {
        &crate::definition::V2
    };
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = if V == 1 {
        OLD_DEFINITIONS
    } else {
        ALL_DEFINITIONS
    };
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl<const V: u8> WorkflowActivityModule for Flows<V> {
    const ACTIVITY_TYPES: &'static [&'static str] = if V == 1 {
        &[crate::activity::BUILD, crate::activity::TARGET]
    } else {
        &[
            crate::activity::BUILD,
            crate::activity::TARGET,
            crate::activity::REBUILD,
        ]
    };
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
impl<const V: u8> MaintenanceModule for Flows<V> {
    const MODULE: &'static str = "release.flows";
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] =
        <Self as WorkflowModule>::DEFINITIONS;
}
impl<const V: u8> EffectModule for Flows<V> {
    const MODULE: &'static str = "release.flows";
    const CLAIM_COMMAND_ID: u32 = 15;
    const LEASE_COMMAND_ID: u32 = 16;
    const VALIDATE_QUERY_ID: u32 = 17;
    const STATUS_QUERY_ID: u32 = 19;
}
impl<const V: u8> Flows<V> {
    /// Compiles the exact predecessor before retaining its code. No descriptor is cached partially.
    pub fn new() -> cellule_runtime::Result<Self> {
        if !matches!(V, 1 | 2) {
            return Err(Error::Registry(
                "release definition version must be one or two",
            ));
        }
        static RETAINED: OnceLock<[RetainedCodeDescriptor; 1]> = OnceLock::new();
        let retained = if V == 2 {
            let predecessor = compile_release::<1>()?;
            let code = predecessor
                .registry()
                .module_code(Self::NAME)
                .ok_or(Error::Registry("release predecessor module code missing"))?;
            RETAINED
                .get_or_init(|| {
                    [RetainedCodeDescriptor {
                        code,
                        schema_min: 1,
                        schema_max: 1,
                    }]
                })
                .as_slice()
        } else {
            &[]
        };
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 32768, 64),
            op(2, 65536, 64),
            op(3, 1024, 64),
            op(4, 32768, 64),
            op(6, 16, 1 << 20),
            op(7, 1 << 20, 1 << 20),
            op(8, 1 << 20, 64),
            op(9, 8, 8),
            op(11, 8192, 8),
            op(12, 128, 8),
            op(13, 8192, 8),
            op(14, 8192, 8),
            op(15, 8, 1 << 20),
            op(16, 1 << 20, 1 << 20),
            op(18, 128, 8),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(5, 64, 65536),
            op(10, 1 << 20, 8),
            op(17, 1 << 20, 8),
            op(19, 64, 1 << 20),
        ];
        static ONE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static TWO: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static OLD: OnceLock<[Digest; 1]> = OnceLock::new();
        static ALL: OnceLock<[Digest; 2]> = OnceLock::new();
        let slot = if V == 1 { &ONE } else { &TWO };
        let descriptor = slot.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: retained,
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("flows.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: if V == 1 {
                OLD.get_or_init(|| [crate::definition::digest(1)])
            } else {
                ALL.get_or_init(|| [crate::definition::digest(1), crate::definition::digest(2)])
            },
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: FLOWS,
                name: "release-flows",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[RECORDS],
                dead_letter: None,
            }],
        });
        Ok(Self { descriptor })
    }
}
impl<const V: u8> CellModule for Flows<V> {
    const NAME: &'static str = "release.flows";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        self.descriptor
    }
    fn register(self, r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(r)?;
        register_workflow_activities::<Self>(r)?;
        register_activity::<Self, crate::activity::Adapter<0>>(r)?;
        register_activity::<Self, crate::activity::Adapter<1>>(r)?;
        if V == 2 {
            register_activity::<Self, crate::activity::Adapter<2>>(r)?;
        }
        r.bind_command::<crate::StartRelease<V>>()?;
        r.bind_command::<crate::ApproveRelease<V>>()?;
        r.bind_command::<crate::RollbackRelease<V>>()?;
        r.bind_command::<crate::ReplyRelease<V>>()?;
        r.bind_command::<crate::ReconcileRelease<V>>()?;
        register_effect_delivery::<Self>(r)?;
        register_maintenance::<Self>(r)
    }
}
/// Private artifacts, native workflows, and signed progress receiver, independent of TargetApplication.
pub struct ReleaseApplication<const V: u8>;
impl<const V: u8> CellApplication for ReleaseApplication<V> {
    const NAME: &'static str = "cellule-cookbook-release-pipeline";
    fn register(b: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        b.register(Artifacts)?;
        b.register(Records)?;
        b.register(Flows::<V>::new()?)?;
        for (module, name, namespace, role) in [
            (Artifacts::NAME, "artifacts", ARTIFACTS, CatalogRole::Blob),
            (Records::NAME, "records", RECORDS, CatalogRole::Sql),
            (Flows::<V>::NAME, "flows", FLOWS, CatalogRole::Workflow),
        ] {
            b.cell_type(
                CellType::new(module, name, namespace, role, 1)?.with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles a real versioned inventory; version two retains the exact executable predecessor.
pub fn compile_release<const V: u8>() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    let app = ReleaseApplication::<V>::compile(BuildDescriptor {
        source_revision: format!("release-pipeline-{V}:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?;
    if V == 2 {
        app.registry()
            .verify_rolling_from(compile_release::<1>()?.registry().release_bytes())?;
    }
    Ok(Arc::new(app))
}
