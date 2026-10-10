use std::sync::OnceLock;

use super::*;

const CODE_ONLY_MODULE: &str = "code-only-test";
const CODE_ONLY_NAMESPACE: crate::NamespaceId = crate::NamespaceId::from_bytes([45; 16]);
const PREDECESSOR_CODE: Digest = Digest::from_bytes([43; 32]);

struct CodeOnlyModule;

impl crate::CellModule for CodeOnlyModule {
    const NAME: &'static str = CODE_ONLY_MODULE;

    fn descriptor(&self) -> &'static crate::ModuleDescriptor {
        static DESCRIPTOR: OnceLock<crate::ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| {
            let sql = "SELECT 1";
            crate::ModuleDescriptor {
                name: CODE_ONLY_MODULE,
                source_digest: Digest::from_bytes([46; 32]),
                retained_codes: &[crate::registry::RetainedCodeDescriptor {
                    code: PREDECESSOR_CODE,
                    schema_min: 1,
                    schema_max: 1,
                }],
                schema_min: 1,
                schema_max: 1,
                migrations: Box::leak(Box::new([crate::registry::MigrationDescriptor {
                    version: 1,
                    sql,
                    digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
                }])),
                commands: &[],
                queries: &[],
                workflow_definitions: &[],
                activity_types: &[],
                namespaces: &[crate::NamespaceDescriptor {
                    id: CODE_ONLY_NAMESPACE,
                    name: CODE_ONLY_MODULE,
                    role: crate::CatalogRole::Sql,
                    shards: 1,
                    effect_targets: &[],
                    dead_letter: None,
                }],
            }
        })
    }

    fn register(self, _registry: &mut crate::RegistryBuilder) -> Result<()> {
        Ok(())
    }
}

fn code_only_registry() -> crate::Registry {
    let mut registry = crate::RegistryBuilder::new(crate::BuildDescriptor {
        source_revision: CODE_ONLY_MODULE.into(),
        cargo_lock_digest: Digest::from_bytes([47; 32]),
    });
    registry.register(CodeOnlyModule).unwrap();
    registry.finish().unwrap()
}

#[tokio::test]
async fn externally_durable_activation_uses_normal_before_sql_and_after_restore() {
    let directory = tempfile::TempDir::new().unwrap();
    let cell = CellId::from_bytes([211; 32]);
    let incarnation = IncarnationId::from_bytes([212; 16]);
    let replica = cellule_ltx::CellReplica::new(
        cellule_ltx::CellStorageLayout::new(
            cellule_store::Store::new(std::sync::Arc::new(object_store::memory::InMemory::new())),
            object_store::path::Path::from("external-durability"),
            [213; 16],
        ),
        *cell.as_bytes(),
        *incarnation.as_bytes(),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    let db = replica
        .open_new(&directory.path().join("initial.sqlite"))
        .unwrap();
    let (mut executor, cuts, _) = CellExecutor::bootstrap(db, cell, incarnation, 1, |tx| {
        let synchronous: i64 = tx.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        assert_eq!(synchronous, 1);
        tx.execute_batch("CREATE TABLE witness(value INTEGER); INSERT INTO witness VALUES(7)")?;
        Ok(())
    })
    .unwrap();
    let root = replica.prepare(None, &cuts, 0, 1).await.unwrap().root();
    executor.confirm_bootstrap_published(&cuts).unwrap();
    executor.close().unwrap();
    let restored = directory.path().join("restored.sqlite");
    let db = replica
        .open_root(&root)
        .await
        .unwrap()
        .paged()
        .prepare_writable(&restored)
        .await
        .unwrap()
        .open_writable(&restored)
        .unwrap();
    let mut executor = CellExecutor::from_restored(db, cell, incarnation, 1, root).unwrap();
    executor
        .db
        .query_with(|connection| {
            let synchronous: i64 =
                connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
            let value: i64 =
                connection.query_row("SELECT value FROM witness", [], |row| row.get(0))?;
            assert_eq!(synchronous, 1);
            assert_eq!(value, 7);
            Ok::<_, cellule_ltx::rusqlite::Error>(())
        })
        .unwrap();
    executor.close().unwrap();
}

#[test]
fn full_pending_publication_budget_refuses_new_commands() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cell.sqlite");
    let cell = CellId::from_bytes([61; 32]);
    let incarnation = IncarnationId::from_bytes([62; 16]);
    let mut connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
    crate::cell::schema::install_runtime_schema(&mut connection, cell, incarnation, 1).unwrap();
    drop(connection);
    let db = Db::open(&path, cellule_ltx::Limits::default()).unwrap();
    let mut executor = CellExecutor::new(db, cell, incarnation, 1);

    // Each command commits locally, becomes pending, and is released by its
    // fleet proof, so object publication may lag behind the actor. The budget
    // that stops that lag from growing without bound is the admission under
    // test.
    for sequence in 1_u64..=MAX_PENDING_PUBLICATIONS as u64 {
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes([sequence as u8; 16]),
            issued_at_ms: 10,
            expires_at_ms: 10_000,
        };
        let execution = executor
            .execute(
                identity,
                Digest::from_bytes([sequence as u8; 32]),
                20,
                1 << 20,
                |transaction| {
                    transaction.execute(
                        "UPDATE sys_meta SET logical_time_ms = logical_time_ms + 1",
                        [],
                    )?;
                    let mut result = Vec::with_capacity(4_096);
                    result.push(sequence as u8);
                    Ok(HandlerOutcome::Success(result))
                },
            )
            .unwrap();
        assert!(
            matches!(execution, CommandExecution::Pending),
            "sequence {sequence}"
        );
        match &executor.latest_pending().unwrap().outcome {
            StoredOutcome::Success { result, .. } => {
                assert_eq!(result.as_slice(), &[sequence as u8]);
                assert_eq!(result.capacity(), result.len());
            }
            StoredOutcome::Rejected { .. } => panic!("expected success"),
        }
        executor.confirm_durable(sequence).unwrap();
    }

    // The budget is full: the next command is refused instead of queueing more
    // unpublished work behind a provider that is already behind.
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([201; 16]),
        issued_at_ms: 10,
        expires_at_ms: 10_000,
    };
    assert!(matches!(
        executor.execute(identity, Digest::from_bytes([9; 32]), 20, 1 << 20, |_| {
            Ok(HandlerOutcome::Success(Vec::new()))
        }),
        Err(Error::PendingPublication)
    ));
}

/// Coalescing one root over several retained commits must append every
/// retained cut, and the range confirmation must release exactly those commits.
#[tokio::test]
async fn one_prepared_root_covers_every_retained_commit() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cell.sqlite");
    let cell = CellId::from_bytes([71; 32]);
    let incarnation = IncarnationId::from_bytes([72; 16]);
    let mut connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
    crate::cell::schema::install_runtime_schema(&mut connection, cell, incarnation, 1).unwrap();
    drop(connection);
    let db = Db::open(&path, cellule_ltx::Limits::default()).unwrap();
    let mut executor = CellExecutor::new(db, cell, incarnation, 1);

    // Two commands commit locally and become durable behind their fleet proofs,
    // which is what lets a second commit exist while the first is unpublished.
    for sequence in 1_u64..=2 {
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes([sequence as u8; 16]),
            issued_at_ms: 10,
            expires_at_ms: 10_000,
        };
        assert!(matches!(
            executor
                .execute(
                    identity,
                    Digest::from_bytes([sequence as u8; 32]),
                    20,
                    1 << 20,
                    |transaction| {
                        transaction.execute(
                            "UPDATE sys_meta SET logical_time_ms = logical_time_ms + 1",
                            [],
                        )?;
                        Ok(HandlerOutcome::Success(vec![sequence as u8]))
                    },
                )
                .unwrap(),
            CommandExecution::Pending
        ));
        executor.confirm_durable(sequence).unwrap();
    }

    let merged = merge_captures(executor.pending.iter().map(|pending| pending.cuts()))
        .expect("retained cuts");
    // Each commit retained one incremental cut; the merge appends both and ends
    // at the newest commit's position.
    assert_eq!(merged.segments.len(), 2, "both commits must be appended");
    assert_eq!(
        merged.position,
        executor.latest_pending().unwrap().cuts().position
    );

    let layout = cellule_ltx::CellStorageLayout::new(
        cellule_store::Store::new(std::sync::Arc::new(object_store::memory::InMemory::new())),
        object_store::path::Path::from("coalesced"),
        [73; 16],
    );
    let replica =
        cellule_ltx::CellReplica::new(layout, [71; 32], [72; 16], cellule_ltx::Limits::default())
            .unwrap();
    let prepared = replica.prepare(None, &merged, 2, 1).await.unwrap();
    assert_eq!(prepared.root().commit_sequence, 2);
    assert_eq!(prepared.root().position, merged.position);

    assert_eq!(executor.bind_prepared_all(&prepared, &merged).unwrap(), 2);
    assert!(
        executor.confirm_published(&prepared.root()).is_err(),
        "the single-commit confirmation must refuse a coalesced root"
    );
    let outcomes = executor.confirm_published_range(&prepared.root()).unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].commit_sequence(), 1);
    assert_eq!(outcomes[1].commit_sequence(), 2);
    assert!(executor.pending().is_none());
}

#[test]
fn restored_executor_rejects_root_sequence_ahead_of_sqlite_metadata() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cell.sqlite");
    let cell = CellId::from_bytes([31; 32]);
    let incarnation = IncarnationId::from_bytes([32; 16]);
    let mut connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
    crate::cell::schema::install_runtime_schema(&mut connection, cell, incarnation, 1).unwrap();
    drop(connection);
    let mut db = Db::open(&path, cellule_ltx::Limits::default()).unwrap();
    db.transaction(|transaction| {
        transaction.execute("UPDATE sys_meta SET logical_time_ms = 1", [])?;
        Ok(())
    })
    .unwrap();
    db.capture().unwrap();
    let root = cellule_ltx::RootRef {
        cell: *cell.as_bytes(),
        incarnation: *incarnation.as_bytes(),
        digest: [33; 32],
        position: db.position(),
        commit_sequence: 1,
    };
    assert!(matches!(
        CellExecutor::from_restored(db, cell, incarnation, 1, root),
        Err(Error::Control(
            "restored SQLite metadata does not match authoritative root"
        ))
    ));
}

#[test]
fn code_only_migration_commits_a_captured_system_cut() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cell.sqlite");
    let cell = CellId::from_bytes([41; 32]);
    let incarnation = IncarnationId::from_bytes([42; 16]);
    let mut connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
    crate::cell::schema::install_runtime_schema(&mut connection, cell, incarnation, 1).unwrap();
    drop(connection);
    let db = Db::open(&path, cellule_ltx::Limits::default()).unwrap();
    let mut executor = CellExecutor::new(db, cell, incarnation, 1);
    let registry = code_only_registry();
    let target_code = registry.module_code(CODE_ONLY_MODULE).unwrap();
    let plan = registry
        .next_migration(CODE_ONLY_NAMESPACE, PREDECESSOR_CODE, 1)
        .unwrap()
        .unwrap();
    executor.migrate(plan, 10).unwrap();
    let pending = executor.pending_migration().unwrap();
    assert_eq!(pending.code(), target_code);
    assert_eq!(pending.from_schema(), 1);
    assert_eq!(pending.to_schema(), 1);
    assert_eq!(pending.digest(), None);
    assert_eq!(pending.commit_sequence(), 1);
    assert!(!pending.cuts().segments.is_empty());
}

#[test]
fn declared_admission_limits_become_capacity_errors() {
    let disk = admission_error(cellule_ltx::LtxError::Limit(
        cellule_ltx::LimitKind::LocalDiskBytes,
    ));
    assert!(matches!(disk, Error::Capacity("local disk bytes")));

    let database = cellule_ltx::LtxError::Limit(cellule_ltx::LimitKind::DatabaseBytes);
    assert!(matches!(
        admission_error(database),
        Error::Capacity("database bytes")
    ));

    // A fenced session is not a capacity refusal: the caller must restore
    // authoritative state instead of retrying the same request.
    let fenced = admission_error(cellule_ltx::LtxError::Fenced);
    assert!(matches!(fenced, Error::Ltx(cellule_ltx::LtxError::Fenced)));
}

fn schema_cache_executor() -> (tempfile::TempDir, CellExecutor) {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("cell.sqlite");
    let cell = CellId::from_bytes([81; 32]);
    let incarnation = IncarnationId::from_bytes([82; 16]);
    let mut connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
    crate::cell::schema::install_runtime_schema(&mut connection, cell, incarnation, 1).unwrap();
    drop(connection);
    let db = Db::open(&path, cellule_ltx::Limits::default()).unwrap();
    (directory, CellExecutor::new(db, cell, incarnation, 1))
}

fn schema_cache_identity(number: u8) -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes([number; 16]),
        issued_at_ms: 10,
        expires_at_ms: 10_000,
    }
}

#[test]
fn failed_schema_validation_cannot_poison_a_later_same_cookie_command() {
    let (_directory, mut executor) = schema_cache_executor();
    executor
        .execute(
            schema_cache_identity(1),
            Digest::from_bytes([1; 32]),
            20,
            32,
            |transaction| {
                transaction.execute_batch(crate::primitives::capacity::SCHEMA)?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .unwrap();
    executor.confirm_durable(1).unwrap();

    let failure = executor.execute(
        schema_cache_identity(2),
        Digest::from_bytes([2; 32]),
        20,
        32,
        |transaction| {
            transaction.execute_batch("DROP TABLE capacity_total")?;
            Ok(HandlerOutcome::Success(Vec::new()))
        },
    );
    assert!(matches!(
        failure,
        Err(Error::Command("database reservation schema is incomplete"))
    ));

    // Both the rejected DROP and this CREATE increment the cookie once.
    // Retaining the rejected observation would refuse this valid command.
    executor
        .execute(
            schema_cache_identity(3),
            Digest::from_bytes([3; 32]),
            20,
            32,
            |transaction| {
                transaction.execute_batch("CREATE TABLE application_state (value BLOB)")?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .unwrap();
    assert_eq!(
        executor
            .latest_pending()
            .unwrap()
            .outcome()
            .commit_sequence(),
        2
    );
    executor.confirm_durable(2).unwrap();
    executor
        .query(32, |connection| {
            let requests: i64 =
                connection.query_row("SELECT COUNT(*) FROM sys_requests", [], |row| row.get(0))?;
            assert_eq!(requests, 2);
            crate::primitives::capacity::validate(connection)?;
            Ok(Vec::new())
        })
        .unwrap();
}

#[test]
fn newly_installed_primitive_deadlines_are_visible_after_a_warm_command() {
    let (_directory, mut executor) = schema_cache_executor();
    executor
        .execute(
            schema_cache_identity(1),
            Digest::from_bytes([1; 32]),
            20,
            32,
            |_| Ok(HandlerOutcome::Success(Vec::new())),
        )
        .unwrap();
    assert!(executor.latest_pending().unwrap().next_due_ms().unwrap() > 50);
    executor.confirm_durable(1).unwrap();
    executor
        .execute(
            schema_cache_identity(2),
            Digest::from_bytes([2; 32]),
            20,
            32,
            |transaction| {
                crate::primitives::kv::install_kv_schema(transaction)?;
                transaction.execute(
                    "INSERT INTO kv_entries VALUES (X'01', X'02', zeroblob(28), X'03', 50)",
                    [],
                )?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .unwrap();
    assert_eq!(executor.latest_pending().unwrap().next_due_ms(), Some(50));
}

#[test]
fn mixed_owner_reads_preserve_warm_kv_write_statements() {
    use crate::primitives::kv::{KvAtomicRequest, KvMutation, kv_atomic};
    use cellule_ltx::rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let (_directory, mut executor) = schema_cache_executor();
    let preparations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&preparations);
    executor
        .db
        .transaction(|transaction| {
            crate::primitives::kv::install_kv_schema(transaction).map_err(|error| {
                cellule_ltx::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
            })?;
            transaction.authorizer(Some(move |context: AuthContext<'_>| {
                if matches!(
                    context.action,
                    AuthAction::Read { .. } | AuthAction::Insert { .. } | AuthAction::Update { .. }
                ) {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
                Authorization::Allow
            }));
            Ok(())
        })
        .unwrap();
    let request = KvAtomicRequest {
        scope: vec![1],
        checks: Vec::new(),
        mutations: vec![KvMutation::Put {
            key: vec![2],
            value: vec![3; 96],
            expires_at_ms: None,
        }],
    };
    for sequence in 1..=2 {
        let before = preparations.load(Ordering::Relaxed);
        executor
            .execute(
                schema_cache_identity(sequence),
                Digest::from_bytes([sequence; 32]),
                20,
                32,
                |transaction| {
                    kv_atomic(transaction, 20, &request)?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            )
            .unwrap();
        executor.confirm_durable(u64::from(sequence)).unwrap();
        let newly_prepared = preparations.load(Ordering::Relaxed) - before;
        if sequence == 1 {
            assert!(newly_prepared > 0);
        } else {
            assert_eq!(newly_prepared, 0, "warm fixed SQL must not compile again");
        }
    }
    executor
        .query(128, |connection| {
            Ok(crate::primitives::kv::kv_get(connection, &[1], &[2], 20)?
                .unwrap()
                .value)
        })
        .unwrap();
    let before = preparations.load(Ordering::Relaxed);
    executor
        .execute(
            schema_cache_identity(3),
            Digest::from_bytes([3; 32]),
            20,
            32,
            |transaction| {
                kv_atomic(transaction, 20, &request)?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .unwrap();
    assert_eq!(
        preparations.load(Ordering::Relaxed),
        before,
        "owner read must preserve the writer cache"
    );
}

#[test]
fn grouped_schema_changes_cache_only_the_committed_member_schemas() {
    let (_directory, mut executor) = schema_cache_executor();
    let handlers: Vec<crate::cell::worker::Handler> = vec![
        Box::new(|transaction| {
            crate::primitives::kv::install_kv_schema(transaction)?;
            Ok(HandlerOutcome::Success(Vec::new()))
        }),
        Box::new(|transaction| {
            crate::primitives::workflow::install_workflow_schema(transaction)?;
            Err(Error::Command("discard this member's DDL"))
        }),
        Box::new(|transaction| {
            crate::primitives::queue::install_queue_schema(transaction)?;
            Ok(HandlerOutcome::Success(Vec::new()))
        }),
    ];
    let commands = handlers
        .into_iter()
        .enumerate()
        .map(|(index, handler)| NativeCommand {
            identity: schema_cache_identity(index as u8 + 1),
            operation_digest: Digest::from_bytes([index as u8 + 1; 32]),
            now_ms: 20,
            max_result_bytes: 32,
            handler,
        })
        .collect();
    let group = executor
        .execute_group(
            commands,
            std::time::Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(group.outcomes[0].as_ref().unwrap().commit_sequence(), 1);
    assert!(matches!(
        group.outcomes[1],
        Err(Error::Command("discard this member's DDL"))
    ));
    assert_eq!(group.outcomes[2].as_ref().unwrap().commit_sequence(), 2);
    let observation = executor
        .db
        .query_with(|connection| executor.schema_cache.observe(connection))
        .unwrap();
    assert!(
        observation
            .capabilities
            .contains(crate::primitives::kv::KV_TABLE)
    );
    assert!(
        observation
            .capabilities
            .contains(crate::primitives::queue::QUEUE_TABLE)
    );
    assert!(
        !observation
            .capabilities
            .contains(crate::primitives::workflow::WORKFLOW_TABLE)
    );
    executor.confirm_durable(2).unwrap();
}
