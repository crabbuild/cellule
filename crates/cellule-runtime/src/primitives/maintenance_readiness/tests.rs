use super::*;
use crate::cell::schema::install_runtime_schema;
use crate::identity::{CellId, IncarnationId};
use crate::primitives::maintenance::inspect_transfer_work;
use crate::primitives::{
    cron::install_cron_schema, queue::install_queue_schema, workflow::install_workflow_schema,
};

fn database() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    install_runtime_schema(
        &mut connection,
        CellId::from_bytes([1; 32]),
        IncarnationId::from_bytes([2; 16]),
        1,
    )
    .unwrap();
    let transaction = connection.transaction().unwrap();
    install_queue_schema(&transaction).unwrap();
    install_workflow_schema(&transaction).unwrap();
    install_cron_schema(&transaction).unwrap();
    transaction.commit().unwrap();
    connection
}

fn pending_effect(connection: &Connection) {
    connection.execute("INSERT INTO sys_effects(effect_id, destination, operation, state, attempt, due_at_ms, expires_at_ms, token, lease_until_ms, created_sequence, result) VALUES (?1, ?2, X'01', 0, 0, 0, 1000, NULL, NULL, 1, NULL)", ([3_u8; 32].as_slice(), [4_u8; 32].as_slice())).unwrap();
}

fn pending_queue(connection: &Connection) {
    connection.execute("INSERT INTO queue_messages(message_id, payload, state, attempt, due_at_ms, expires_at_ms, token, lease_until_ms, result_code) VALUES (?1, X'01', 0, 0, 0, 1000, NULL, NULL, NULL)", [[5_u8; 16].as_slice()]).unwrap();
}

fn pending_workflow(connection: &Connection) {
    connection
        .execute(
            "INSERT INTO workflow_runs VALUES (X'01', ?1, ?2, 0, X'', 0, NULL, NULL)",
            ([6_u8; 16].as_slice(), [7_u8; 32].as_slice()),
        )
        .unwrap();
    connection.execute("INSERT INTO workflow_activities(run_id, activity_id, activity_type, input, state, attempt, due_at_ms, expires_at_ms, token, lease_until_ms, completion_token, completion_digest, result) VALUES (?1, ?2, 'test', X'', 0, 0, 0, 1000, NULL, NULL, NULL, NULL, NULL)", ([6_u8;16].as_slice(),[8_u8;16].as_slice())).unwrap();
    connection
        .execute(
            "INSERT INTO workflow_timers(run_id, timer_id, due_at_ms, state) VALUES (?1, ?2, 0, 0)",
            ([6_u8; 16].as_slice(), [9_u8; 16].as_slice()),
        )
        .unwrap();
}

#[test]
fn pending_durable_work_can_move_at_a_maintenance_barrier_but_idle_transfer_stays_closed() {
    let connection = database();
    pending_effect(&connection);
    pending_queue(&connection);
    pending_workflow(&connection);
    connection
        .execute(
            "INSERT INTO cron_schedules VALUES (?1, 0, X'', X'', 1000, 0, 0, 1, 1, 0)",
            [[10_u8; 16].as_slice()],
        )
        .unwrap();
    for role in [
        CatalogRole::Sql,
        CatalogRole::Queue,
        CatalogRole::Workflow,
        CatalogRole::Cron,
    ] {
        assert!(inspect(&connection, role, 10).unwrap().is_transferable());
        assert!(
            !inspect_transfer_work(&connection, role, 10)
                .unwrap()
                .is_settled()
        );
    }
}

#[test]
fn every_live_lease_remains_visible_and_expiry_inspection_does_not_rewrite_it() {
    let connection = database();
    pending_effect(&connection);
    pending_queue(&connection);
    pending_workflow(&connection);
    let token = [11_u8; 16];
    for table in ["sys_effects", "queue_messages", "workflow_activities"] {
        connection
            .execute(
                &format!("UPDATE {table} SET state = 1, token = ?1, lease_until_ms = 100"),
                [token.as_slice()],
            )
            .unwrap();
    }
    assert!(
        inspect(&connection, CatalogRole::Sql, 99)
            .unwrap()
            .has_blocker(MaintenanceWorkBlocker::EffectLease)
    );
    let queue = inspect(&connection, CatalogRole::Queue, 99).unwrap();
    assert!(queue.has_blocker(MaintenanceWorkBlocker::EffectLease));
    assert!(queue.has_blocker(MaintenanceWorkBlocker::QueueLease));
    let workflow = inspect(&connection, CatalogRole::Workflow, 99).unwrap();
    assert!(workflow.has_blocker(MaintenanceWorkBlocker::EffectLease));
    assert!(workflow.has_blocker(MaintenanceWorkBlocker::ActivityLease));
    for role in [CatalogRole::Sql, CatalogRole::Queue, CatalogRole::Workflow] {
        assert!(inspect(&connection, role, 100).unwrap().is_transferable());
    }
    for table in ["sys_effects", "queue_messages", "workflow_activities"] {
        let retained: (i64, Vec<u8>, i64) = connection
            .query_row(
                &format!("SELECT state, token, lease_until_ms FROM {table}"),
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(retained, (1, token.to_vec(), 100));
    }
}

#[test]
fn malformed_time_schema_and_unproven_blob_inventory_never_report_ready() {
    let connection = database();
    assert!(inspect(&connection, CatalogRole::Sql, -1).is_err());
    pending_queue(&connection);
    connection
        .execute(
            "UPDATE queue_messages SET state = 1, token = ?1, lease_until_ms = -1",
            [[12_u8; 16].as_slice()],
        )
        .unwrap();
    assert!(
        inspect(&connection, CatalogRole::Queue, 100)
            .unwrap()
            .has_blocker(MaintenanceWorkBlocker::QueueLease)
    );
    assert!(
        inspect(&connection, CatalogRole::Blob, 100)
            .unwrap()
            .has_blocker(MaintenanceWorkBlocker::BlobInventory)
    );
    connection
        .execute_batch("DROP TABLE workflow_activities")
        .unwrap();
    assert!(inspect(&connection, CatalogRole::Workflow, 100).is_err());
}
