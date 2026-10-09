//! Public composition contracts: persisted topology and tenant-scoped factories.

use super::*;
use cellule_app::{ApplicationBinding, CellBinding, CompiledApplication};
use std::sync::atomic::{AtomicUsize, Ordering};

const PRIMARY: NamespaceId = NamespaceId::from_bytes([81; 16]);
const SECONDARY: NamespaceId = NamespaceId::from_bytes([82; 16]);

struct Module {
    descriptor: &'static ModuleDescriptor,
    registrations: Arc<AtomicUsize>,
}

impl CellModule for Module {
    const NAME: &'static str = "binding-module";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        self.descriptor
    }
    fn register(self, _: &mut RegistryBuilder) -> Result<()> {
        self.registrations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn descriptor(role: CatalogRole, count: usize) -> &'static ModuleDescriptor {
    let namespaces = (0..count)
        .map(|index| NamespaceDescriptor {
            id: NamespaceId::from_bytes([81 + index as u8; 16]),
            name: Box::leak(format!("namespace-{index}").into_boxed_str()),
            role,
            shards: if index == 1 { 4 } else { 1 },
            effect_targets: &[],
            dead_letter: None,
        })
        .collect::<Vec<_>>();
    let migrations = [
        "-- binding schema one",
        "-- binding schema two",
        "-- binding schema three",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, sql)| cellule_runtime::MigrationDescriptor {
        version: index as u32 + 1,
        sql,
        digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
    })
    .collect::<Vec<_>>();
    Box::leak(Box::new(ModuleDescriptor {
        name: Module::NAME,
        source_digest: Digest::from_bytes([83; 32]),
        retained_codes: &[],
        schema_min: 1,
        schema_max: 3,
        migrations: Box::leak(migrations.into_boxed_slice()),
        commands: &[],
        queries: &[],
        workflow_definitions: &[],
        activity_types: &[],
        namespaces: Box::leak(namespaces.into_boxed_slice()),
    }))
}

fn build(revision: &str) -> BuildDescriptor {
    BuildDescriptor {
        source_revision: revision.into(),
        cargo_lock_digest: Digest::from_bytes([84; 32]),
    }
}

fn module(descriptor: &'static ModuleDescriptor) -> Module {
    Module {
        descriptor,
        registrations: Arc::new(AtomicUsize::new(0)),
    }
}

struct App;
impl CellApplication for App {
    const NAME: &'static str = "binding-app";
    fn register(builder: &mut ApplicationBuilder) -> Result<()> {
        builder.module(
            module(descriptor(CatalogRole::Sql, 2)),
            [
                CellBinding::sharded(PRIMARY, "orders"),
                CellBinding::sharded(SECONDARY, "inventory"),
            ],
        )
    }
}

struct OtherApp;
impl CellApplication for OtherApp {
    const NAME: &'static str = "other-app";
    fn register(_: &mut ApplicationBuilder) -> Result<()> {
        Ok(())
    }
}

struct QueryNeverDispatched;
impl cellule_runtime::Query for QueryNeverDispatched {
    const MODULE: &'static str = Module::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = ();
    fn execute(_: &mut cellule_runtime::registry::QueryContext<'_>, (): ()) -> Result<()> {
        panic!("scope rejection must happen before dispatch")
    }
}

#[test]
fn module_bindings_preserve_all_partition_descriptor_versions_and_schema_ranges() {
    let descriptor = descriptor(CatalogRole::Sql, 2);
    for mode in 1..=3 {
        let primary = match mode {
            1 => CellBinding::sharded(PRIMARY, "orders"),
            2 => CellBinding::entity(PRIMARY, "orders"),
            _ => CellBinding::entity_uuid(PRIMARY, "orders"),
        }
        .with_limits(8 << 20, 2 << 20);
        let mut combined = ApplicationBuilder::new(App::NAME, build("same-release")).unwrap();
        combined
            .module(
                module(descriptor),
                [primary, CellBinding::sharded(SECONDARY, "inventory")],
            )
            .unwrap();
        let combined = combined.finish().unwrap();

        let mut explicit = ApplicationBuilder::new(App::NAME, build("same-release")).unwrap();
        explicit.register(module(descriptor)).unwrap();
        let primary = match mode {
            1 => CellType::new(Module::NAME, "orders", PRIMARY, CatalogRole::Sql, 1).unwrap(),
            2 => CellType::new(Module::NAME, "orders", PRIMARY, CatalogRole::Sql, 1)
                .unwrap()
                .with_entity_partitions()
                .unwrap(),
            _ => CellType::entity_uuid(Module::NAME, "orders", PRIMARY).unwrap(),
        }
        .with_schema_range(1, 3)
        .unwrap()
        .with_limits(8 << 20, 2 << 20)
        .unwrap();
        explicit.cell_type(primary).unwrap();
        explicit
            .cell_type(
                CellType::new(Module::NAME, "inventory", SECONDARY, CatalogRole::Sql, 4)
                    .unwrap()
                    .with_schema_range(1, 3)
                    .unwrap(),
            )
            .unwrap();
        let explicit = explicit.finish().unwrap();
        assert_eq!(combined.cell_types(), explicit.cell_types());
        assert_eq!(combined.descriptor_bytes(), explicit.descriptor_bytes());
        assert_eq!(
            combined.registry().release_bytes(),
            explicit.registry().release_bytes()
        );
    }
}

#[test]
fn module_topology_errors_are_rejected_before_registration() {
    let descriptor = descriptor(CatalogRole::Sql, 2);
    let first = CellBinding::sharded(PRIMARY, "orders");
    let second = CellBinding::sharded(SECONDARY, "inventory");
    for bindings in [
        vec![first],
        vec![first, first],
        vec![first, CellBinding::sharded(SECONDARY, "orders")],
        vec![
            first,
            CellBinding::sharded(NamespaceId::from_bytes([99; 16]), "unknown"),
        ],
        vec![first.with_limits(0, 128), second],
        vec![first, CellBinding::entity(SECONDARY, "inventory")],
        vec![first, CellBinding::entity_uuid(SECONDARY, "inventory")],
    ] {
        let candidate = module(descriptor);
        let registrations = candidate.registrations.clone();
        let mut builder = ApplicationBuilder::new(App::NAME, build("invalid")).unwrap();
        assert!(builder.module(candidate, bindings).is_err());
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
        // Invalid topology did not consume identities or partially register a module.
        builder.module(module(descriptor), [first, second]).unwrap();
        assert!(builder.finish().is_ok());
    }
}

#[test]
fn module_bindings_reject_conflicts_with_existing_topology_before_registration() {
    let descriptor = descriptor(CatalogRole::Sql, 2);
    for (namespace, name, shards) in [(PRIMARY, "reserved", 1), (SECONDARY, "orders", 4)] {
        let mut builder = ApplicationBuilder::new(App::NAME, build("mixed-api")).unwrap();
        builder
            .cell_type(
                CellType::new(Module::NAME, name, namespace, CatalogRole::Sql, shards).unwrap(),
            )
            .unwrap();
        let candidate = module(descriptor);
        let registrations = candidate.registrations.clone();
        assert!(matches!(
            builder.module(
                candidate,
                [
                    CellBinding::sharded(PRIMARY, "orders"),
                    CellBinding::sharded(SECONDARY, "inventory"),
                ],
            ),
            Err(Error::Registry("duplicate Cell type identity"))
        ));
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn module_bindings_refuse_uuid_on_non_sql_and_excessive_cell_counts() {
    for (descriptor, bindings) in [
        (
            descriptor(CatalogRole::Kv, 1),
            vec![CellBinding::entity_uuid(PRIMARY, "orders")],
        ),
        (
            descriptor(CatalogRole::Sql, 129),
            (0..129)
                .map(|index| {
                    CellBinding::sharded(
                        NamespaceId::from_bytes([81 + index as u8; 16]),
                        Box::leak(format!("cell-{index}").into_boxed_str()),
                    )
                })
                .collect(),
        ),
    ] {
        let module = module(descriptor);
        let registrations = module.registrations.clone();
        let mut builder = ApplicationBuilder::new(App::NAME, build("invalid")).unwrap();
        assert!(builder.module(module, bindings).is_err());
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn application_binding_validates_once_and_preserves_tenant_and_application_scope() {
    let compiled = Arc::new(App::compile(build("factory")).unwrap());
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 4).unwrap(),
        16 << 20,
        cellule_runtime::SessionId::from_bytes([85; 16]),
    )
    .unwrap();
    let application = ApplicationId::from_bytes([86; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        object_store::path::Path::from("bindings"),
        *application.as_bytes(),
    );
    let client = CellClient::local_runtime(compiled.registry(), runtime.clone(), layout);
    assert!(matches!(
        ApplicationBinding::<OtherApp>::new(client.clone(), compiled.clone(), application),
        Err(Error::Registry(_))
    ));
    let other_release: CompiledApplication = App::compile(build("different-release")).unwrap();
    assert!(matches!(
        ApplicationBinding::<App>::new(client.clone(), Arc::new(other_release), application),
        Err(Error::Registry(_))
    ));
    let binding = ApplicationBinding::<App>::new(client, compiled.clone(), application).unwrap();
    assert_eq!(binding.application_id(), application);
    let alpha = binding.scope(TenantId::from_bytes([87; 16]));
    let beta = binding.clone().scope(TenantId::from_bytes([88; 16]));
    assert!(std::ptr::eq(alpha.compiled(), binding.compiled()));
    let a = alpha.target_for_scope(PRIMARY, b"orders").unwrap();
    let b = beta.target_for_scope(PRIMARY, b"orders").unwrap();
    assert_eq!(a.application(), application);
    assert_ne!(a.cell_id(), b.cell_id());
    assert!(matches!(
        alpha.query::<QueryNeverDispatched>(&b, None, ()).await,
        Err(InvocationError::NotStarted(Error::Identity(_)))
    ));
    let foreign = CellTarget::new(
        a.tenant(),
        ApplicationId::from_bytes([89; 16]),
        PRIMARY,
        a.partition(),
    )
    .unwrap();
    assert!(matches!(
        alpha
            .query::<QueryNeverDispatched>(&foreign, None, ())
            .await,
        Err(InvocationError::NotStarted(Error::Identity(_)))
    ));
    runtime.shutdown().await.unwrap();
}
