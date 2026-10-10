//! Matched workload SQL: bounded keys, incompressible values, and complete audit.

use super::*;

pub(super) const KEY_COUNT: u64 = 100_000;
pub(super) const CELLS: usize = 64;

pub(super) fn compiled() -> Arc<cellule_app::CompiledApplication> {
    let mut builder = ApplicationBuilder::new(
        EntityReferenceApplication::NAME,
        BuildDescriptor {
            source_revision: "write-performance-v1".into(),
            cargo_lock_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("../../../../Cargo.lock")).as_bytes(),
            ),
        },
    )
    .unwrap();
    builder.register(ReferenceSql).unwrap();
    builder
        .cell_type(
            CellType::new(SQL_MODULE, "orders", SQL_NAMESPACE, CatalogRole::Sql, 1)
                .unwrap()
                .with_limits(1 << 30, 16 << 20)
                .unwrap()
                .with_entity_partitions()
                .unwrap(),
        )
        .unwrap();
    Arc::new(builder.finish().unwrap())
}

// Explicit process-only profiles; ordinary entity tests retain their
// original application, schema, Cell count, and qualification thresholds.
pub(super) fn enabled() -> bool {
    match std::env::var("CELLULE_WRITE_PROFILE") {
        Ok(profile) => {
            assert!(matches!(
                profile.as_str(),
                "v1" | "population-v1"
                    | "owner-reads-v1"
                    | "small-kv-fleet-v1"
                    | "small-kv-attribution-v1"
                    | "small-kv-attribution-v2"
            ));
            true
        }
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("invalid write profile: {error}"),
    }
}

pub(super) fn population_enabled() -> bool {
    matches!(
        std::env::var("CELLULE_WRITE_PROFILE").as_deref(),
        Ok("population-v1" | "owner-reads-v1")
    )
}

pub(super) fn owner_reads_enabled() -> bool {
    std::env::var("CELLULE_WRITE_PROFILE").as_deref() == Ok("owner-reads-v1")
}

pub(super) fn small_kv_enabled() -> bool {
    matches!(
        std::env::var("CELLULE_WRITE_PROFILE").as_deref(),
        Ok("small-kv-fleet-v1" | "small-kv-attribution-v1" | "small-kv-attribution-v2")
    )
}

pub(super) fn attribution_enabled() -> bool {
    matches!(
        std::env::var("CELLULE_WRITE_PROFILE").as_deref(),
        Ok("small-kv-attribution-v1" | "small-kv-attribution-v2")
    )
}

pub(super) fn write_payload_bytes() -> usize {
    if small_kv_enabled() { 96 } else { 1024 }
}

pub(super) fn cell_count(nodes: usize) -> usize {
    if enabled() {
        assert_eq!(nodes, 3);
        if small_kv_enabled() { 1000 } else { CELLS }
    } else {
        nodes * ENTITIES_PER_NODE
    }
}

pub(super) fn owner(entity: usize) -> usize {
    if enabled() {
        assert!(entity < cell_count(3));
        entity % 3
    } else {
        entity / ENTITIES_PER_NODE
    }
}

pub(super) fn schema(tx: &cellule_ltx::rusqlite::Transaction<'_>) -> Result<()> {
    super::super::performance_fixture::install_sql_tables(tx)?;
    tx.execute_batch("CREATE TABLE write_values(key INTEGER PRIMARY KEY, payload BLOB NOT NULL, digest BLOB NOT NULL CHECK(length(digest) = 32))")?;
    Ok(())
}

/// The ordinal is globally unique within one fresh qualification namespace.
/// The fixed generator and SQL must also be used by the celld application.
pub(super) fn payload(ordinal: u64, bytes: usize) -> Vec<u8> {
    assert!(matches!(bytes, 96 | 1024 | 4096 | 16384));
    let mut generator = blake3::Hasher::new();
    generator.update(b"cellule.write-performance.v1\0seed=7\0");
    generator.update(&ordinal.to_be_bytes());
    let mut value = vec![0; bytes];
    generator.finalize_xof().fill(&mut value);
    value
}

pub(super) fn key(ordinal: u64) -> u64 {
    let hash = blake3::hash(&ordinal.to_be_bytes());
    u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()) % KEY_COUNT
}

pub(super) fn batch(ordinal: u64, value: Vec<u8>) -> SqlBatch {
    let request = qualification_identity(ordinal, 0).request_id;
    let digest = blake3::hash(&value);
    let mut audit = digest.as_bytes().to_vec();
    audit.extend_from_slice(&key(ordinal).to_be_bytes());
    SqlBatch {
        statements: vec![
            SqlStatement {
                sql: "INSERT INTO write_values(key, payload, digest) VALUES (?1, ?2, ?3) ON CONFLICT(key) DO UPDATE SET payload = excluded.payload, digest = excluded.digest".into(),
                parameters: vec![SqlValue::Integer(key(ordinal) as i64), SqlValue::Blob(value), SqlValue::Blob(digest.as_bytes().to_vec())],
            },
            SqlStatement {
                sql: "INSERT INTO invoice_receipts(schedule_id, occurrence, payload) VALUES (?1, ?2, ?3)".into(),
                parameters: vec![SqlValue::Blob(request.as_bytes().to_vec()), SqlValue::Integer(i64::try_from(ordinal).unwrap()), SqlValue::Blob(audit)],
            },
        ],
    }
}

#[test]
fn primary_descriptor_keeps_entity_identity_and_explicit_database_limits() {
    let legacy = compiled_entities();
    let primary = compiled();
    for entity in 0..CELLS {
        assert_eq!(
            entity_target(&legacy, entity),
            entity_target(&primary, entity)
        );
    }
    assert_eq!(primary.cell_types()[0].database_limit_bytes(), 1 << 30);
    assert_ne!(legacy.descriptor_digest(), primary.descriptor_digest());
    // This normal test runs without process-profile environment wiring.
    assert!(!enabled());
    assert_eq!(cell_count(3), 12);
    assert_eq!(owner(11), 2);
    let cells = (0..2_000)
        .map(|entity| entity_target(&primary, entity).cell_id())
        .collect::<std::collections::HashSet<_>>();
    let incarnations = (0..2_000)
        .map(entity_incarnation)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(cells.len(), 2_000);
    assert_eq!(incarnations.len(), 2_000);
}

#[test]
fn small_kv_values_keep_the_same_deterministic_generator_and_distinct_requests() {
    let small = payload(42, 96);
    assert_eq!(small.len(), 96);
    assert_eq!(small, payload(42, 1024)[..96]);
    assert_ne!(small, payload(43, 96));
}

#[test]
fn matched_write_audits_every_command_and_rolls_back_a_duplicate() {
    let root = tempfile::TempDir::new().unwrap();
    let mut db =
        cellule_ltx::Db::open(&root.path().join("source.sqlite"), Limits::default()).unwrap();
    db.transaction_with(schema).unwrap();
    let value = payload(42, 1024);
    db.transaction_with(|tx| {
        cellule_runtime::primitives::sql::sql_batch(tx, &batch(42, value.clone()))?;
        Ok::<_, Error>(())
    })
    .unwrap();
    // A duplicate audit ID fails the batch. Its preceding upsert must not
    // leave a changed entity behind, even with a different supplied value.
    assert!(
        db.transaction_with(|tx| {
            cellule_runtime::primitives::sql::sql_batch(tx, &batch(42, payload(43, 1024)))?;
            Ok::<_, Error>(())
        })
        .is_err()
    );
    db.transaction_with(|tx| {
        let stored: Vec<u8> = tx.query_row(
            "SELECT payload FROM write_values WHERE key = ?1",
            [key(42) as i64],
            |row| row.get(0),
        )?;
        assert_eq!(stored, value);
        let (id, digest, count): (Vec<u8>, Vec<u8>, u64) = tx.query_row(
            "SELECT schedule_id, payload, COUNT(*) FROM invoice_receipts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(id, qualification_identity(42, 0).request_id.as_bytes());
        assert_eq!(&digest[..32], blake3::hash(&stored).as_bytes());
        assert_eq!(&digest[32..], &key(42).to_be_bytes());
        assert_eq!(count, 1);
        Ok::<_, Error>(())
    })
    .unwrap();
    db.close().unwrap();
}

#[tokio::test]
async fn primary_author_path_replays_one_durable_outcome_and_one_audit() {
    let application = compiled();
    let directory = tempfile::TempDir::new().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        "write-profile-test".into(),
        *ApplicationId::from_bytes([82; 16]).as_bytes(),
    );
    let (host, _, _) = super::super::process_node::start(
        0,
        application.clone(),
        &layout,
        directory.path(),
        "https://write-profile-test.internal".into(),
    )
    .await;
    let cell = provision_entity_with_schema(
        &host,
        &layout,
        directory.path(),
        0,
        0,
        "https://write-profile-test.internal".into(),
        schema,
    )
    .await;
    let typed = ApplicationHandle::<EntityReferenceApplication>::new(
        CellClient::local(application.registry(), cell),
        application.clone(),
        TenantId::from_bytes([81; 16]),
        ApplicationId::from_bytes([82; 16]),
    )
    .unwrap();
    let target = entity_target(&application, 0);
    let sql = typed.sql::<ReferenceSql>(target.clone()).unwrap();
    let identity = qualification_identity(42, super::super::performance_fixture::now_ms());
    let first = sql
        .batch(identity, batch(42, payload(42, 1024)))
        .await
        .unwrap();
    let replay = sql
        .batch(identity, batch(42, payload(42, 1024)))
        .await
        .unwrap();
    assert_eq!(first, replay);
    let observed = sql
        .query(
            Some(first.receipt),
            SqlBatch {
                statements: vec![
                    SqlStatement {
                        sql: "SELECT COUNT(*) FROM invoice_receipts".into(),
                        parameters: vec![],
                    },
                    SqlStatement {
                        sql: "SELECT payload, digest FROM write_values WHERE key = ?1".into(),
                        parameters: vec![SqlValue::Integer(key(42) as i64)],
                    },
                ],
            },
        )
        .await
        .unwrap();
    assert_eq!(observed.output[0].rows, vec![vec![SqlValue::Integer(1)]]);
    assert_eq!(
        observed.output[1].rows,
        vec![vec![
            SqlValue::Blob(payload(42, 1024)),
            SqlValue::Blob(blake3::hash(&payload(42, 1024)).as_bytes().to_vec())
        ]]
    );
    let control = CellAuthority::new(layout)
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        control.value().root.as_ref().unwrap().commit_sequence >= first.receipt.commit_sequence
    );
    host.shutdown().await.unwrap();
}
