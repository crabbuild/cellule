use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use super::*;

#[test]
fn unchanged_schema_reuses_capabilities_and_committed_ddl_refreshes_them() {
    let mut connection = Connection::open_in_memory().unwrap();
    let scans = Arc::new(AtomicUsize::new(0));
    let observed_scans = Arc::clone(&scans);
    connection.authorizer(Some(move |context: AuthContext<'_>| {
        if let AuthAction::Read {
            table_name: "sqlite_master",
            column_name: "name",
        } = context.action
        {
            observed_scans.fetch_add(1, Ordering::Relaxed);
        }
        Authorization::Allow
    }));
    let mut cache = SchemaCache::default();
    for _ in 0..10 {
        let transaction = connection.transaction().unwrap();
        let observation = cache.observe(&transaction).unwrap();
        assert!(!observation.capabilities.contains(kv::KV_TABLE));
        transaction.commit().unwrap();
        cache.record_commit(observation);
    }
    assert_eq!(scans.load(Ordering::Relaxed), 1);

    let transaction = connection.transaction().unwrap();
    crate::primitives::kv::install_kv_schema(&transaction).unwrap();
    let observation = cache.observe(&transaction).unwrap();
    assert!(observation.capabilities.contains(kv::KV_TABLE));
    transaction.commit().unwrap();
    cache.record_commit(observation);
    let after_ddl = scans.load(Ordering::Relaxed);
    assert!(after_ddl > 1);
    let transaction = connection.transaction().unwrap();
    assert!(
        cache
            .observe(&transaction)
            .unwrap()
            .capabilities
            .contains(kv::KV_TABLE)
    );
    assert_eq!(scans.load(Ordering::Relaxed), after_ddl);
}

#[test]
fn rolled_back_schema_cookie_is_not_reused_for_different_ddl() {
    let mut connection = Connection::open_in_memory().unwrap();
    let mut cache = SchemaCache::default();
    let transaction = connection.transaction().unwrap();
    let original = cache.observe(&transaction).unwrap();
    transaction.commit().unwrap();
    cache.record_commit(original);

    let transaction = connection.transaction().unwrap();
    transaction
        .execute_batch("CREATE TABLE kv_entries (key BLOB)")
        .unwrap();
    let discarded = cache.observe(&transaction).unwrap();
    assert!(discarded.capabilities.contains(kv::KV_TABLE));
    transaction.rollback().unwrap();

    let transaction = connection.transaction().unwrap();
    transaction
        .execute_batch("CREATE TABLE queue_messages (key BLOB)")
        .unwrap();
    let replacement = cache.observe(&transaction).unwrap();
    assert_eq!(replacement.version, discarded.version);
    assert!(replacement.capabilities.contains(queue::QUEUE_TABLE));
    assert!(!replacement.capabilities.contains(kv::KV_TABLE));
    transaction.commit().unwrap();
    cache.record_commit(replacement);
}

#[test]
fn capacity_presence_tracks_partial_installation_and_removal() {
    let mut connection = Connection::open_in_memory().unwrap();
    let mut cache = SchemaCache::default();
    connection
        .execute_batch(crate::primitives::capacity::SCHEMA)
        .unwrap();
    let transaction = connection.transaction().unwrap();
    let observation = cache.observe(&transaction).unwrap();
    assert_eq!(observation.capabilities.capacity_tables(), 2);
    crate::primitives::capacity::validate_installed(&transaction, 2).unwrap();
    transaction.commit().unwrap();
    cache.record_commit(observation);

    let transaction = connection.transaction().unwrap();
    transaction
        .execute_batch("DROP TABLE capacity_total")
        .unwrap();
    let partial = cache.observe(&transaction).unwrap();
    assert_eq!(partial.capabilities.capacity_tables(), 1);
    assert!(matches!(
        crate::primitives::capacity::validate_installed(
            &transaction,
            partial.capabilities.capacity_tables()
        ),
        Err(crate::Error::Command(
            "database reservation schema is incomplete"
        ))
    ));
    transaction.rollback().unwrap();
    let transaction = connection.transaction().unwrap();
    assert_eq!(
        cache
            .observe(&transaction)
            .unwrap()
            .capabilities
            .capacity_tables(),
        2
    );
}
