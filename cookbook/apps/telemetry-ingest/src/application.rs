use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
        queue::{QueueModule, register_queue},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// Single native Queue shard for immutable ingress batches.
pub const INGRESS: NamespaceId = NamespaceId::from_bytes([0x81; 16]);
/// Canonical entity-partitioned SQL device histories and transactional summary intents.
pub const DEVICES: NamespaceId = NamespaceId::from_bytes([0x82; 16]);
/// Two independently committed SQL summary shards selected by canonical device keys.
pub const SUMMARIES: NamespaceId = NamespaceId::from_bytes([0x83; 16]);
/// One permanent physical-message processing audit per tenant.
pub const AUDITS: NamespaceId = NamespaceId::from_bytes([0x84; 16]);
/// Native producer deduplication, message claims, and lease acknowledgments.
pub struct Ingress;
/// Permanent device sequence bindings and native summary delivery.
pub struct Devices;
/// Bounded monotonic device contributions and grouped minute queries.
pub struct Summaries;
/// Complete batch outcomes committed before independent Queue acknowledgment.
pub struct Audit;
impl QueueModule for Ingress {
    const NAMESPACE: NamespaceId = INGRESS;
    const SEND_COMMAND_ID: u32 = 1;
    const CLAIM_COMMAND_ID: u32 = 2;
    const LEASE_COMMAND_ID: u32 = 3;
    const VALIDATE_QUERY_ID: u32 = 4;
    const CONTROL_COMMAND_ID: u32 = 5;
    const INFO_QUERY_ID: u32 = 6;
}
impl MaintenanceModule for Ingress {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 7;
}
impl EffectModule for Devices {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
macro_rules! maintenance {
    ($ty:ty) => {
        impl MaintenanceModule for $ty {
            const MODULE: &'static str = Self::NAME;
            const TICK_COMMAND_ID: u32 = 3;
        }
    };
}
maintenance!(Devices);
maintenance!(Summaries);
maintenance!(Audit);
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
macro_rules! descriptor {
    ($ty:ty,$namespace:ident,$role:ident,$shards:literal,$schema:expr,$commands:expr,$queries:expr,$effects:expr) => {
        impl CellModule for $ty {
            const NAME: &'static str = concat!("telemetry-ingest.", stringify!($namespace));
            fn descriptor(&self) -> &'static ModuleDescriptor {
                const COMMANDS: &[OperationDescriptor] = $commands;
                const QUERIES: &[OperationDescriptor] = $queries;
                static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
                static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
                VALUE.get_or_init(|| ModuleDescriptor {
                    name: Self::NAME,
                    source_digest: source_digest(),
                    retained_codes: &[],
                    schema_min: 1,
                    schema_max: 1,
                    migrations: MIGRATION.get_or_init(|| {
                        [MigrationDescriptor {
                            version: 1,
                            sql: $schema,
                            digest: Digest::from_bytes(
                                *blake3::hash($schema.as_bytes()).as_bytes(),
                            ),
                        }]
                    }),
                    commands: COMMANDS,
                    queries: QUERIES,
                    workflow_definitions: &[],
                    activity_types: &[],
                    namespaces: &[NamespaceDescriptor {
                        id: $namespace,
                        name: Self::NAME,
                        role: CatalogRole::$role,
                        shards: $shards,
                        effect_targets: $effects,
                        dead_letter: None,
                    }],
                })
            }
            fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
                register::<Self>(registry)
            }
        }
    };
}
trait Bind: CellModule {
    fn bind(registry: &mut RegistryBuilder) -> cellule_runtime::Result<()>;
}
fn register<T: Bind>(registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
    T::bind(registry)
}
impl Bind for Ingress {
    fn bind(r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_queue::<Self>(r)
    }
}
impl Bind for Devices {
    fn bind(r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        r.bind_command::<crate::RegisterDevice>()?;
        r.bind_command::<crate::RecordEvent>()?;
        r.bind_query::<crate::GetDevice>()?;
        register_maintenance::<Self>(r)?;
        register_effect_delivery::<Self>(r)
    }
}
impl Bind for Summaries {
    fn bind(r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        r.bind_command::<crate::PublishSummary>()?;
        r.bind_query::<crate::GetSummary>()?;
        r.bind_query::<crate::ListBuckets>()?;
        register_maintenance::<Self>(r)
    }
}
impl Bind for Audit {
    fn bind(r: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        r.bind_command::<crate::CompleteBatch>()?;
        r.bind_query::<crate::GetBatch>()?;
        register_maintenance::<Self>(r)
    }
}
descriptor!(
    Ingress,
    INGRESS,
    Queue,
    1,
    "-- native telemetry ingress Queue schema v1",
    &[
        op(1, 1 << 20, 1 << 20),
        op(2, 1 << 20, 1 << 20),
        op(3, 1 << 20, 1 << 20),
        op(5, 1 << 20, 1 << 20),
        op(7, 8, 8)
    ],
    &[op(4, 1 << 20, 8), op(6, 8, 1 << 20)],
    &[]
);
descriptor!(
    Devices,
    DEVICES,
    Sql,
    1,
    include_str!("device.sql"),
    &[
        op(1, 128, 4096),
        op(3, 8, 8),
        op(4, 8, 1 << 20),
        op(5, 1 << 20, 1 << 20),
        op(8, 256, 4096)
    ],
    &[op(2, 64, 64 << 10), op(6, 1 << 20, 8), op(7, 64, 1 << 20)],
    &[SUMMARIES]
);
descriptor!(
    Summaries,
    SUMMARIES,
    Sql,
    2,
    include_str!("summary.sql"),
    &[op(1, 4096, 16), op(3, 8, 8)],
    &[op(2, 64, 4096), op(8, 64, 4096)],
    &[]
);
descriptor!(
    Audit,
    AUDITS,
    Sql,
    1,
    include_str!("audit.sql"),
    &[op(1, 64 << 10, 64 << 10), op(3, 8, 8)],
    &[op(2, 24, 64 << 10)],
    &[]
);
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("device.rs"),
        include_bytes!("audit.rs"),
        include_bytes!("summary.rs"),
        include_bytes!("client.rs"),
        include_bytes!("service.rs"),
        include_bytes!("consumer.rs"),
        include_bytes!("device.sql"),
        include_bytes!("audit.sql"),
        include_bytes!("summary.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Queue ingress, independent device aggregates, permanent audits, and two eventual summary shards.
pub struct TelemetryIngest;
impl CellApplication for TelemetryIngest {
    const NAME: &'static str = "cellule-cookbook-telemetry-ingest";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Ingress)?;
        builder.register(Devices)?;
        builder.register(Summaries)?;
        builder.register(Audit)?;
        builder.cell_type(
            CellType::new(Devices::NAME, "devices", DEVICES, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        for (name, namespace, role, shards) in [
            (Ingress::NAME, INGRESS, CatalogRole::Queue, 1),
            (Summaries::NAME, SUMMARIES, CatalogRole::Sql, 2),
            (Audit::NAME, AUDITS, CatalogRole::Sql, 1),
        ] {
            builder.cell_type(
                CellType::new(name, name, namespace, role, shards)?
                    .with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles stable namespace, schema, codec, and dependency contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(TelemetryIngest::compile(BuildDescriptor {
        source_revision: format!("telemetry-ingest:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
