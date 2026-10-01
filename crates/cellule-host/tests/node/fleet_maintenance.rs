//! Exact boot-bound cordon through the public node-owned action boundary.

use super::fleet_actions::{Fixture, clock, fixture};
use super::*;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::NodeId;
use cellule_runtime::node::NodeMode;

fn cordon(fixture: &Fixture) -> FleetAction {
    fixture.journal.reset_maintenance(
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([201; 16]),
    )
}

async fn check_existing_owner(fixture: &Fixture) {
    let value = fixture
        .handle
        .query(64, 64, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(value, 42_i64.to_be_bytes());
    assert_eq!(fixture.node.stats().active_cells(), 1);
}

async fn close(fixture: Fixture) {
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.stats().active_cells(), 0);
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
    assert_eq!(fixture.node.state(), NodeState::Stopped);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_cordon_closes_shared_role_gate_keeps_owner_and_is_idempotent() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    let gate = fixture.node.runtime().node_admission();
    assert_eq!(gate.mode().unwrap(), NodeMode::Active);
    let (a, b) = tokio::join!(
        fixture.node.apply_fleet_action(action.clone(), clock()),
        fixture.node.apply_fleet_action(action.clone(), clock()),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a.committed && b.committed);
    assert_eq!(a.outcome, b.outcome);
    assert_eq!(a.outcome.outcome, FleetOutcome::Cordoned);
    assert_eq!(fixture.journal.accepted_count(), 1);
    assert_eq!(gate.mode().unwrap(), NodeMode::Draining);
    assert!(matches!(gate.check_new_role(), Err(Error::CellDraining)));
    // Even another lifecycle cordon cannot clear terminal maintenance intent.
    gate.cordon().unwrap();
    assert_eq!(gate.mode().unwrap(), NodeMode::Draining);
    assert!(fixture.node.is_ready());
    check_existing_owner(&fixture).await;
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_publication_reply_replays_original_cordon_and_keeps_gate_closed() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    fixture.journal.lose_next_result_reply();
    let original = fixture
        .node
        .apply_fleet_action(action.clone(), clock())
        .await
        .unwrap();
    assert!(!original.committed);
    assert!(original.journal_error.is_some());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    let replay = fixture
        .node
        .apply_fleet_action(action, clock())
        .await
        .unwrap();
    assert!(replay.committed);
    assert_eq!(replay.accepted, original.accepted);
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(fixture.journal.accepted_count(), 1);
    check_existing_owner(&fixture).await;
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_cordon_waiter_does_not_cancel_node_owned_publication() {
    let fixture = fixture().await;
    let action = cordon(&fixture);
    fixture.journal.hold_next_result();
    let node = fixture.node.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(action, clock()).await });
    fixture.journal.wait_for_result_publication().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    fixture.journal.resume_result_publication();
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_boot_stale_intent_and_unsupported_roles_do_not_accept_effects() {
    let fixture = fixture().await;
    let wrong = fixture.journal.reset_maintenance(
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([202; 16]),
    );
    assert!(
        fixture
            .node
            .apply_fleet_action(wrong, clock())
            .await
            .is_err()
    );
    assert_eq!(fixture.journal.accepted_count(), 0);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    let stale = cordon(&fixture);
    fixture.journal.transition(JournalTransition::Maintenance(
        MaintenanceEvent::ExtendDeadline(clock() + 120_000),
    ));
    assert!(
        fixture
            .node
            .apply_fleet_action(stale, clock())
            .await
            .is_err()
    );
    fixture
        .journal
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned));
    fixture.journal.transition(JournalTransition::Maintenance(
        MaintenanceEvent::BeginEvacuation,
    ));
    let roles = fixture
        .journal
        .maintenance_action(MaintenanceAction::SettleRoles);
    assert!(
        fixture
            .node
            .apply_fleet_action(roles, clock())
            .await
            .is_err()
    );
    let inspection = FleetInspectionRequest::new(
        fixture
            .journal
            .maintenance_action(MaintenanceAction::Inspect),
        fixture.journal.registry(),
        Digest::from_bytes([211; 32]),
        NodeId::from_bytes([201; 16]),
        SessionId::from_bytes([201; 16]),
        clock() + 30_000,
    )
    .unwrap();
    assert!(fixture.node.inspect_fleet_action(inspection).await.is_err());
    assert_eq!(fixture.journal.accepted_count(), 0);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    check_existing_owner(&fixture).await;
    close(fixture).await;
}
