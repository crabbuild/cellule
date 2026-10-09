use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::primitives::{
    effects::{EffectModule, register_effect_delivery},
    maintenance::{MaintenanceModule, register_maintenance},
    queue::{QueueDeadLetterTarget, QueueModule, register_queue},
};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder, registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

/// Fixed two-shard job namespace.
pub const JOBS: NamespaceId = NamespaceId::from_bytes([0x71; 16]);
/// Fixed one-shard native dead-letter queue.
pub const DEAD: NamespaceId = NamespaceId::from_bytes([0x72; 16]);
/// One SQL receiver, outside the queue's transaction.
pub const RECEIVER: NamespaceId = NamespaceId::from_bytes([0x73; 16]);
/// Native job queue and durable dead-letter effect source.
pub struct Jobs;
/// Native dead-letter queue consumed into the inspection table.
pub struct DeadLetters;
/// Durable application receiver with permanent business-key deduplication.
pub struct Receiver;

macro_rules! queue {
    ($module:ty, $namespace:ident, $dead:expr) => {
        impl MaintenanceModule for $module {
            const MODULE: &'static str = Self::NAME;
            const TICK_COMMAND_ID: u32 = 7;
            const QUEUE_DEAD_LETTER: Option<QueueDeadLetterTarget> = $dead;
        }
        impl QueueModule for $module {
            const NAMESPACE: NamespaceId = $namespace;
            const SEND_COMMAND_ID: u32 = 1;
            const CLAIM_COMMAND_ID: u32 = 2;
            const LEASE_COMMAND_ID: u32 = 3;
            const VALIDATE_QUERY_ID: u32 = 4;
            const CONTROL_COMMAND_ID: u32 = 5;
            const INFO_QUERY_ID: u32 = 6;
        }
    };
}
queue!(
    Jobs,
    JOBS,
    Some(QueueDeadLetterTarget::new("work-queue.dead", DEAD, 1, 1, 1))
);
queue!(DeadLetters, DEAD, None);
impl EffectModule for Jobs {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 8;
    const LEASE_COMMAND_ID: u32 = 9;
    const VALIDATE_QUERY_ID: u32 = 10;
    const STATUS_QUERY_ID: u32 = 11;
}
impl MaintenanceModule for Receiver {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 4;
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
macro_rules! descriptor {
    ($name:expr,$namespace:expr,$role:expr,$shards:expr,$schema:expr,$commands:expr,$queries:expr,$effects:expr,$dead:expr) => {{
        const COMMANDS: &[OperationDescriptor] = $commands;
        const QUERIES: &[OperationDescriptor] = $queries;
        const NAMESPACES: &[NamespaceDescriptor] = &[NamespaceDescriptor {
            id: $namespace,
            name: $name,
            role: $role,
            shards: $shards,
            effect_targets: $effects,
            dead_letter: $dead,
        }];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        ModuleDescriptor {
            name: $name,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: $schema,
                    digest: Digest::from_bytes(*blake3::hash($schema.as_bytes()).as_bytes()),
                }]
            }),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: NAMESPACES,
        }
    }};
}
impl CellModule for Jobs {
    const NAME: &'static str = "work-queue.jobs";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| {
            descriptor!(
                Jobs::NAME,
                JOBS,
                CatalogRole::Queue,
                2,
                "-- native queue v1",
                &[
                    operation(1),
                    operation(2),
                    operation(3),
                    operation(5),
                    operation(7),
                    operation(8),
                    operation(9)
                ],
                &[operation(4), operation(6), operation(10), operation(11)],
                &[DEAD],
                Some(DEAD)
            )
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_queue::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for DeadLetters {
    const NAME: &'static str = "work-queue.dead";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| {
            descriptor!(
                DeadLetters::NAME,
                DEAD,
                CatalogRole::Queue,
                1,
                "-- native queue v1",
                &[
                    operation(1),
                    operation(2),
                    operation(3),
                    operation(5),
                    operation(7)
                ],
                &[operation(4), operation(6)],
                &[],
                None
            )
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_queue::<Self>(registry)
    }
}
impl CellModule for Receiver {
    const NAME: &'static str = "work-queue.receiver";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| {
            descriptor!(
                Receiver::NAME,
                RECEIVER,
                CatalogRole::Sql,
                1,
                include_str!("schema.sql"),
                &[operation(1), operation(2), operation(4)],
                &[OperationDescriptor {
                    codec_version: 2,
                    input_limit: 64,
                    output_limit: 64 << 10,
                    ..operation(3)
                }],
                &[],
                None
            )
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::receiver::Record>()?;
        registry.bind_command::<crate::receiver::SetEnabled>()?;
        registry.bind_query::<crate::receiver::Inspect>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("receiver.rs"),
        include_bytes!("worker.rs"),
        include_bytes!("peer.rs"),
        include_bytes!("schema.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// A job queue, native dead-letter destination, and idempotent SQL receiver.
pub struct WorkQueue;
impl CellApplication for WorkQueue {
    const NAME: &'static str = "cellule-cookbook-work-queue";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Jobs)?;
        builder.register(DeadLetters)?;
        builder.register(Receiver)?;
        for (module, namespace, role, shards) in [
            (Jobs::NAME, JOBS, CatalogRole::Queue, 2),
            (DeadLetters::NAME, DEAD, CatalogRole::Queue, 1),
            (Receiver::NAME, RECEIVER, CatalogRole::Sql, 1),
        ] {
            builder.cell_type(
                CellType::new(module, module, namespace, role, shards)?
                    .with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles immutable schema, source, fixed-shard, and delivery contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(WorkQueue::compile(BuildDescriptor {
        source_revision: format!("work-queue:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
