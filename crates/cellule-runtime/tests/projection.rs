use std::{future::Future, pin::Pin, sync::Arc, time::UNIX_EPOCH};

use cellule_ltx::CellStorageLayout;
use cellule_ltx::{CellReplica, Limits};
use cellule_runtime::{
    ApplicationId, BoundedDecoder, BoundedEncoder, BuildDescriptor, CatalogEntry, CatalogRole,
    CellAuthority, CellCatalog, CellClient, CellHandle, CellModule, CellRuntime, CellTarget,
    CodecError, Command, CommandContext, CommandResult, Digest, EffectModule, EffectPeerClient,
    EffectRunOutcome, Error, IncarnationId, MigrationDescriptor, ModuleDescriptor,
    MutationIdentity, NamespaceDescriptor, NamespaceId, OperationDescriptor, Owner,
    PROJECTION_SCHEMA_SQL, PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal,
    PeerRoundTrip, PeerSigner, PeerVerifier, ProjectionModule, ProjectionRecord,
    ProjectionStatusQuery, ProjectionStatusRequest, ProjectionTarget, RegistryBuilder, RequestId,
    Result, SessionId, SqlBatch, SqlBatchQuery, SqlModule, SqlStatement, SqlValue, SqlWorkerPool,
    TenantId, VerifiedPeerRequest, WireValue, emit_projection, partition_for_shard,
    register_effect_delivery, register_projection, register_projection_targets, register_sql,
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

const CATALOG_MODULE: &str = "change-catalog";
const CATALOG_NAMESPACE: NamespaceId = NamespaceId::from_bytes([101; 16]);
const READ_MODEL_MODULE: &str = "change-read-model";
const READ_MODEL_NAMESPACE: NamespaceId = NamespaceId::from_bytes([102; 16]);
const CATALOG_SCHEMA: &str = "CREATE TABLE changes(id INTEGER PRIMARY KEY, value BLOB NOT NULL)";
const READ_MODEL_TABLE: &str =
    "CREATE TABLE read_model(id INTEGER PRIMARY KEY, value BLOB NOT NULL)";
const CATALOG_COMMANDS: &[OperationDescriptor] = &[
    operation(1, 300 * 1024, 16),
    operation(2, 8, 530 * 1024),
    operation(3, 530 * 1024, 16),
];
const CATALOG_QUERIES: &[OperationDescriptor] =
    &[operation(4, 530 * 1024, 1), operation(5, 64, 64)];
const READ_MODEL_COMMANDS: &[OperationDescriptor] = &[
    operation(1, 300 * 1024, 16),
    operation(2, 300 * 1024, 300 * 1024),
];
const READ_MODEL_QUERIES: &[OperationDescriptor] =
    &[operation(3, 64, 64), operation(4, 300 * 1024, 300 * 1024)];
const PROJECTION_TARGETS: &[ProjectionTarget] = &[ProjectionTarget::new(
    READ_MODEL_MODULE,
    READ_MODEL_NAMESPACE,
    1,
    1,
    300 * 1024,
)];

#[derive(Clone, Debug, PartialEq, Eq)]
struct PublishInput {
    id: u64,
    payload: Vec<u8>,
}

impl WireValue for PublishInput {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_u64(self.id)?;
        encoder.write_bytes(&self.payload)
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        Ok(Self {
            id: decoder.read_u64()?,
            payload: decoder.read_bytes()?.to_vec(),
        })
    }
}

struct PublishChange;

impl Command for PublishChange {
    const MODULE: &'static str = CATALOG_MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = PublishInput;
    type Output = ();

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        context.sql(&cellule_runtime::SqlBatch {
            statements: vec![cellule_runtime::SqlStatement {
                sql: "INSERT INTO changes(id, value) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET value = excluded.value".into(),
                parameters: vec![
                    cellule_runtime::SqlValue::Integer(
                        i64::try_from(input.id)
                            .map_err(|_| Error::Command("change id exceeds i64"))?,
                    ),
                    cellule_runtime::SqlValue::Blob(input.payload.clone()),
                ],
            }],
        })?;
        emit_projection(
            context,
            PROJECTION_TARGETS[0],
            &partition_for_shard(0),
            input.payload,
        )?;
        Ok(CommandResult::Success(()))
    }
}

struct Catalog;

impl EffectModule for Catalog {
    const MODULE: &'static str = CATALOG_MODULE;
    const CLAIM_COMMAND_ID: u32 = 2;
    const LEASE_COMMAND_ID: u32 = 3;
    const VALIDATE_QUERY_ID: u32 = 4;
    const STATUS_QUERY_ID: u32 = 5;
}

impl CellModule for Catalog {
    const NAME: &'static str = CATALOG_MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        Box::leak(Box::new(ModuleDescriptor {
            name: CATALOG_MODULE,
            source_digest: Digest::from_bytes([103; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: CATALOG_SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(CATALOG_SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: CATALOG_COMMANDS,
            queries: CATALOG_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: Box::leak(Box::new([NamespaceDescriptor {
                id: CATALOG_NAMESPACE,
                name: "change-catalog",
                role: CatalogRole::Application,
                shards: 1,
                effect_targets: &[READ_MODEL_NAMESPACE],
                dead_letter: None,
            }])),
        }))
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        registry.bind_command::<PublishChange>()?;
        register_effect_delivery::<Self>(registry)?;
        register_projection_targets(registry, CATALOG_MODULE, PROJECTION_TARGETS)
    }
}

struct ReadModel;

impl SqlModule for ReadModel {
    const MODULE: &'static str = READ_MODEL_MODULE;
    const BATCH_COMMAND_ID: u32 = 2;
    const BATCH_QUERY_ID: u32 = 4;
}

impl ProjectionModule for ReadModel {
    const MODULE: &'static str = READ_MODEL_MODULE;
    const NAMESPACE: NamespaceId = READ_MODEL_NAMESPACE;
    const APPLY_COMMAND_ID: u32 = 1;
    const STATUS_QUERY_ID: u32 = 3;

    fn apply(context: &mut CommandContext<'_, '_>, record: &ProjectionRecord) -> Result<()> {
        context.sql(&cellule_runtime::SqlBatch {
            statements: vec![cellule_runtime::SqlStatement {
                sql: "INSERT INTO read_model(id, value) VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET value = excluded.value".into(),
                parameters: vec![cellule_runtime::SqlValue::Blob(record.payload.clone())],
            }],
        })?;
        Ok(())
    }
}

impl CellModule for ReadModel {
    const NAME: &'static str = READ_MODEL_MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        let schema =
            Box::leak(format!("{PROJECTION_SCHEMA_SQL}\n{READ_MODEL_TABLE}").into_boxed_str());
        Box::leak(Box::new(ModuleDescriptor {
            name: READ_MODEL_MODULE,
            source_digest: Digest::from_bytes([104; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: schema,
                digest: Digest::from_bytes(*blake3::hash(schema.as_bytes()).as_bytes()),
            }])),
            commands: READ_MODEL_COMMANDS,
            queries: READ_MODEL_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: Box::leak(Box::new([NamespaceDescriptor {
                id: READ_MODEL_NAMESPACE,
                name: "change-read-model",
                role: CatalogRole::Application,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }])),
        }))
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        register_sql::<Self>(registry)?;
        register_projection::<Self>(registry)
    }
}

struct ReadModelResolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for ReadModelResolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellHandle>> + Send + 'static>> {
        let allowed = target == self.target;
        let handle = self.handle.clone();
        Box::pin(async move {
            if allowed {
                Ok(handle)
            } else {
                Err(Error::CellNotActive)
            }
        })
    }
}

struct ReadModelAuthorizer;

impl PeerAuthorizer for ReadModelAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> Result<()> {
        if request.permits("projection.deliver") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization(
                "projection delivery is not authorized",
            ))
        }
    }
}

struct Loopback {
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
}

impl PeerRoundTrip for Loopback {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>> {
        let verifier = Arc::clone(&self.verifier);
        let dispatcher = Arc::clone(&self.dispatcher);
        Box::pin(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(u64::from(remaining_ms)),
                async {
                    let verified = verifier.verify(&request, now_ms())?;
                    if verified.target() != &target {
                        return Err(Error::Peer("projection target changed in transit"));
                    }
                    dispatcher.dispatch_bytes(&verified, now_ms()).await
                },
            )
            .await
            .map_err(|source| Error::PeerTransportUnknown {
                context: "projection peer deadline",
                source: Box::new(source),
            })?
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_records_advance_a_read_model_watermark_once() {
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "projection-test".into(),
        cargo_lock_digest: Digest::from_bytes([105; 32]),
    });
    builder.register(Catalog).unwrap();
    builder.register(ReadModel).unwrap();
    let registry = Arc::new(builder.finish().unwrap());

    let tenant = TenantId::from_bytes([106; 16]);
    let application = ApplicationId::from_bytes([107; 16]);
    let catalog_target =
        CellTarget::new(tenant, application, CATALOG_NAMESPACE, &0_u32.to_be_bytes()).unwrap();
    let read_model_target = CellTarget::new(
        tenant,
        application,
        READ_MODEL_NAMESPACE,
        &0_u32.to_be_bytes(),
    )
    .unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("projection"),
        *application.as_bytes(),
    );
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let authority = CellAuthority::new(layout.clone());
    let directory = tempfile::TempDir::new().unwrap();
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 10).unwrap(),
        16 * 1024 * 1024,
        SessionId::from_bytes([108; 16]),
    )
    .unwrap();
    let catalog_handle = bootstrap(
        &runtime,
        SessionId::from_bytes([108; 16]),
        &registry,
        &catalog,
        &authority,
        &layout,
        &directory,
        &catalog_target,
        CatalogRole::Application,
        CATALOG_MODULE,
        IncarnationId::from_bytes([109; 16]),
        |transaction| {
            transaction.execute_batch(CATALOG_SCHEMA)?;
            Ok(())
        },
    )
    .await;
    let read_model_schema = format!("{PROJECTION_SCHEMA_SQL}\n{READ_MODEL_TABLE}");
    let read_model_handle = bootstrap(
        &runtime,
        SessionId::from_bytes([108; 16]),
        &registry,
        &catalog,
        &authority,
        &layout,
        &directory,
        &read_model_target,
        CatalogRole::Application,
        READ_MODEL_MODULE,
        IncarnationId::from_bytes([110; 16]),
        move |transaction| {
            transaction.execute_batch(&read_model_schema)?;
            Ok(())
        },
    )
    .await;
    let client = CellClient::local_many(
        Arc::clone(&registry),
        vec![catalog_handle, read_model_handle.clone()],
    )
    .unwrap();

    let peer_session = SessionId::from_bytes([111; 16]);
    let signer = Arc::new(PeerSigner::new(
        peer_session,
        registry.release_digest(),
        SigningKey::from_bytes(&[112; 32]),
    ));
    let verifier = Arc::new(PeerVerifier::new(
        peer_session,
        registry.release_digest(),
        signer.verifying_key(),
    ));
    let dispatcher = Arc::new(PeerDispatcher::new(
        Arc::clone(&registry),
        Arc::new(ReadModelResolver {
            target: read_model_target.clone(),
            handle: read_model_handle,
        }),
        Arc::new(ReadModelAuthorizer),
    ));
    let peer = EffectPeerClient::new(
        signer,
        PeerPrincipal {
            issuer: "projection-test".into(),
            subject: "projection-runner".into(),
            actions: vec!["projection.deliver".into()],
        },
        Arc::new(Loopback {
            verifier,
            dispatcher,
        }),
    );

    let published = client
        .command::<PublishChange>(
            &catalog_target,
            identity(113),
            PublishInput {
                id: 1,
                payload: b"first".to_vec(),
            },
        )
        .await
        .unwrap();
    let delivered = registry
        .run_effect_once(client.clone(), catalog_target.clone(), peer.clone(), 5_000)
        .await
        .unwrap();
    assert!(
        matches!(delivered, EffectRunOutcome::Delivered { .. }),
        "projection effect was not delivered: {delivered:?}"
    );

    let status = client
        .query::<ProjectionStatusQuery<ReadModel>>(
            &read_model_target,
            None,
            ProjectionStatusRequest {
                source: catalog_target.cell_id(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        status.output.applied_through,
        Some(published.receipt.commit_sequence)
    );
    assert_eq!(
        read_model_value(&client, &read_model_target).await,
        b"first".to_vec()
    );

    // A second change advances both the read model and its watermark.
    let second = client
        .command::<PublishChange>(
            &catalog_target,
            identity(114),
            PublishInput {
                id: 1,
                payload: b"second".to_vec(),
            },
        )
        .await
        .unwrap();
    let second_delivery = registry
        .run_effect_once(client.clone(), catalog_target.clone(), peer, 5_000)
        .await
        .unwrap();
    assert!(
        matches!(second_delivery, EffectRunOutcome::Delivered { .. }),
        "second projection effect was not delivered: {second_delivery:?}"
    );
    assert_eq!(
        read_model_value(&client, &read_model_target).await,
        b"second".to_vec()
    );
    assert_eq!(
        client
            .query::<ProjectionStatusQuery<ReadModel>>(
                &read_model_target,
                None,
                ProjectionStatusRequest {
                    source: catalog_target.cell_id(),
                },
            )
            .await
            .unwrap()
            .output
            .applied_through,
        Some(second.receipt.commit_sequence)
    );

    runtime.shutdown().await.unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn bootstrap<F>(
    runtime: &CellRuntime,
    session: SessionId,
    registry: &Arc<cellule_runtime::Registry>,
    catalog: &CellCatalog,
    authority: &CellAuthority,
    layout: &CellStorageLayout,
    directory: &tempfile::TempDir,
    target: &CellTarget,
    role: CatalogRole,
    module: &'static str,
    incarnation: IncarnationId,
    install: F,
) -> CellHandle
where
    F: FnOnce(&cellule_ltx::rusqlite::Transaction<'_>) -> Result<()> + Send + 'static,
{
    let proof = catalog
        .provision(
            CatalogEntry::new(target, role, registry.module_code(module).unwrap(), 1).unwrap(),
        )
        .await
        .unwrap();
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://projection.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    runtime
        .bootstrap(
            proof,
            CellReplica::new(
                layout.clone(),
                *target.cell_id().as_bytes(),
                *incarnation.as_bytes(),
                Limits::default(),
            )
            .unwrap(),
            authority.clone(),
            observed,
            directory.path().join(format!("{module}.sqlite")),
            install,
        )
        .await
        .unwrap()
}

async fn read_model_value(client: &CellClient, target: &CellTarget) -> Vec<u8> {
    let observed = client
        .query::<SqlBatchQuery<ReadModel>>(
            target,
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT value FROM read_model WHERE id = 1".into(),
                    parameters: Vec::new(),
                }],
            },
        )
        .await
        .unwrap();
    match observed
        .output
        .first()
        .and_then(|set| set.rows.first())
        .and_then(|row| row.first())
    {
        Some(SqlValue::Blob(value)) => value.clone(),
        other => panic!("read model value is missing: {other:?}"),
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn identity(byte: u8) -> MutationIdentity {
    let now_ms = now_ms();
    MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
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
