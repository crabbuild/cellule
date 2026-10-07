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
