//! Live-owner replacement through four managed boots and the original supervisor.
use super::*;
use cellule_host::{FollowerEvacuation, NodeLogRotationPhase};
use cellule_runtime::fleet::operations::{
    JournalTransition, MaintenanceEvent, MaintenanceOperation, OperationId,
};

async fn maintenance(fixture: &ManagedFixture) -> (EnrollmentRecord, MaintenanceOperation) {
    let original = fixture
        .native
        .rows()
        .await
        .into_iter()
        .find(|row| {
            row.spec().target.node == node_id(1)
                && matches!(
                    row.spec().role,
                    cellule_runtime::fleet::operations::EnrollmentRole::Follower { log_epoch: 1 }
                )
        })
        .unwrap();
    assert_eq!(original.status(), EnrollmentStatus::Established);
    let now = clock().unwrap();
    let mut snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    for transition in [
        JournalTransition::BeginMaintenance(
            MaintenanceOperation::new(
                OperationId::from_bytes([91; 16]).unwrap(),
                Digest::from_bytes([92; 32]),
                node_id(1),
                session(1),
                2,
                now,
                now + 60_000,
            )
            .unwrap(),
        ),
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    ] {
        snapshot = fixture
            .native
            .journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                clock().unwrap(),
                &transition,
            )
            .await
            .unwrap();
    }
    fixture.boots[1]
        .refresh_capacity(
            1,
            fixture.native.journal.as_ref(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    let donor = fixture
        .native
        .directory
        .load(session(1), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    let store = fixture.nodes[1]
        .try_owned_component::<FollowerStore>(cellule_host::FOLLOWER_STORE_COMPONENT)
        .unwrap()
        .unwrap();
    assert!(store.retained_bytes() > 0);
    assert_eq!(
        donor.advertisement().capacity().follower_retained_bytes,
        store.retained_bytes()
    );
    (original, snapshot.head().maintenance().unwrap().clone())
}

async fn spare(fixture: &ManagedFixture) {
    fixture.boots[3]
        .refresh_capacity(
            3,
            fixture.native.journal.as_ref(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .native
            .directory
            .select_log_members(session(0), 1, clock().unwrap(), 4)
            .await
            .unwrap(),
        [node_id(2), node_id(3)]
    );
}

async fn rotated(fixture: &ManagedFixture) -> cellule_host::NodeLogRotationRequest {
    // The actual actor publication barrier covers the acknowledged old tail.
    fixture.handle.drain().await.unwrap();
    let request = fixture.native.node.request_node_log_rotation(1).unwrap();
    until(|| request.observe().unwrap().phase() == NodeLogRotationPhase::Completed).await;
    request
}

async fn evidence(
    fixture: &ManagedFixture,
    original: &EnrollmentRecord,
    operation: &MaintenanceOperation,
) -> cellule_runtime::Result<FollowerEvacuation> {
    fixture
        .native
        .node
        .follower_evacuation(
            original,
            operation,
            2,
            Instant::now() + Duration::from_secs(3),
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_proves_full_replacement_and_exact_replay() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    assert_eq!(
        fixture
            .handle
            .query(64, 64, |tx| {
                let value: i64 = tx.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap(),
        29i64.to_be_bytes()
    );
    let request = rotated(&fixture).await;
    let completion = request.observe().unwrap().completion().unwrap().clone();
    assert!(completion.retirement().barrier().covered_through() > 0);
    let first = evidence(&fixture, &original, &operation).await.unwrap();
    assert_eq!(first.replacement().prepared().followers().len(), 2);
    assert!(Arc::ptr_eq(first.rotation(), &completion));
    assert_eq!(first.original(), &original);
    assert_eq!(first.retired().status(), EnrollmentStatus::Retired);
    assert_eq!(first.minimum_members(), 2);
    assert_eq!(first.authority().log().unwrap().epoch(), 2);
    assert_eq!(
        first.authority().log().unwrap().members(),
        [node_id(2), node_id(3)]
    );
    assert_eq!(
        first
            .replacements()
            .iter()
            .map(|row| row.spec().target.node)
            .collect::<Vec<_>>(),
        [node_id(2), node_id(3)]
    );
    let second = evidence(&fixture, &original, &operation).await.unwrap();
    assert_eq!(first.retired(), second.retired());
    assert_eq!(first.replacements(), second.replacements());
    assert_eq!(first.snapshot(), second.snapshot());
    assert!(second.interval().0 >= first.interval().1);
    assert!(Arc::ptr_eq(first.rotation(), second.rotation()));
    let references = fixture
        .native
        .directory
        .follower_logs_page(node_id(1), None, 1, clock().unwrap())
        .await
        .unwrap();
    assert!(references.entries().is_empty());
    // Retired lane fences remain present; role evacuation never deletes files.
    let store = fixture.nodes[1]
        .try_owned_component::<FollowerStore>(cellule_host::FOLLOWER_STORE_COMPONENT)
        .unwrap()
        .unwrap();
    let lane = store
        .fleet_lanes_page(None, 1, clock().unwrap())
        .await
        .unwrap();
    assert_eq!(
        lane.entries()[0].state,
        cellule_runtime::follower::FollowerLaneState::Retired
    );
    drop((first, second, lane, store));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_cannot_settle_without_spare_or_completion() {
    let fixture = ManagedFixture::with_members(3, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    assert!(
        fixture
            .native
            .directory
            .select_log_members(session(0), 1, clock().unwrap(), 3)
            .await
            .unwrap()
            .is_empty()
    );
    fixture.handle.drain().await.unwrap();
    let request = fixture.native.node.request_node_log_rotation(1).unwrap();
    until(|| request.observe().unwrap().phase() == NodeLogRotationPhase::Recruiting).await;
    assert!(request.observe().unwrap().retirement().is_some());
    assert!(request.observe().unwrap().completion().is_none());
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_preserves_lost_member_reply_across_retry() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    fixture.handle.drain().await.unwrap();
    fixture
        .native
        .transport
        .lose_retire
        .store(true, Ordering::Release);
    let (entered, resume) = fixture.native.transport.pause_retirement_retry();
    let request = fixture.native.node.request_node_log_rotation(1).unwrap();
    captured(entered).await;
    let first = request.observe().unwrap().first_failure().unwrap().clone();
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    drop(request);
    fixture
        .native
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    resume.send(()).unwrap();
    let retained = fixture
        .native
        .node
        .node_log_rotation_request(1)
        .unwrap()
        .unwrap();
    until(|| retained.observe().unwrap().phase() == NodeLogRotationPhase::Completed).await;
    assert!(Arc::ptr_eq(
        retained.observe().unwrap().first_failure().unwrap(),
        &first
    ));
    let settled = evidence(&fixture, &original, &operation).await.unwrap();
    assert_eq!(
        settled.retired().accepted_at_ms(),
        original.accepted_at_ms()
    );
    assert_eq!(
        settled.retired().established_evidence(),
        original.established_evidence()
    );
    drop(settled);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_rejects_expired_waiter_and_invalid_original_or_policy() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    rotated(&fixture).await;
    for minimum in [0, 3] {
        assert!(
            fixture
                .native
                .node
                .follower_evacuation(
                    &original,
                    &operation,
                    minimum,
                    Instant::now() + Duration::from_secs(3)
                )
                .await
                .is_err()
        );
    }
    assert!(matches!(
        fixture
            .native
            .node
            .follower_evacuation(&original, &operation, 2, Instant::now())
            .await,
        Err(Error::Deadline)
    ));
    let foreign = fixture
        .native
        .rows()
        .await
        .into_iter()
        .find(|row| {
            row.spec().target.node == node_id(2)
                && matches!(
                    row.spec().role,
                    cellule_runtime::fleet::operations::EnrollmentRole::Follower { log_epoch: 2 }
                )
        })
        .unwrap();
    assert!(evidence(&fixture, &foreign, &operation).await.is_err());
    let settled = evidence(&fixture, &original, &operation).await.unwrap();
    drop(settled);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_rejects_withdrawn_replacement_after_local_completion() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    rotated(&fixture).await;
    let settled = evidence(&fixture, &original, &operation).await.unwrap();
    fixture.nodes[3].shutdown().await.unwrap();
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    assert_eq!(settled.replacements().len(), 2);
    drop(settled);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_rejects_fenced_live_owner_after_local_completion() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    rotated(&fixture).await;
    let settled = evidence(&fixture, &original, &operation).await.unwrap();
    fixture.boots[0].guard.as_ref().unwrap().fence();
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    assert_eq!(settled.replacements().len(), 2);
    drop(settled);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_requires_current_operation_after_deadline_extension() {
    let fixture = ManagedFixture::with_members(4, 2).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    rotated(&fixture).await;
    let settled = evidence(&fixture, &original, &operation).await.unwrap();
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let after = fixture
        .native
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::ExtendDeadline(
                operation.deadline_ms() + 10_000,
            )),
        )
        .await
        .unwrap();
    assert!(evidence(&fixture, &original, &operation).await.is_err());
    let updated = evidence(&fixture, &original, after.head().maintenance().unwrap())
        .await
        .unwrap();
    assert_eq!(updated.original(), settled.original());
    assert_eq!(updated.retired(), settled.retired());
    assert_eq!(updated.replacements(), settled.replacements());
    assert!(Arc::ptr_eq(updated.rotation(), settled.rotation()));
    assert_ne!(updated.snapshot(), settled.snapshot());
    drop((updated, settled));
    fixture.finish().await;
}
