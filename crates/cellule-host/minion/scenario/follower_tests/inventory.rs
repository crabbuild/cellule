//! Public capture remains advisory through the original supervisor's paused I/O.
use super::*;
use cellule_host::{FollowerEnrollmentInventoryCursor, FollowerEnrollmentInventoryPage};

pub(super) fn capture(fixture: &Fixture) -> FollowerEnrollmentInventoryPage {
    fixture
        .node
        .fleet_follower_enrollments_page(None, 32, clock().unwrap())
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_without_a_request_is_visible_and_inventory_does_not_wait() {
    let fixture = Fixture::new().await;
    assert!(
        fixture
            .node
            .fleet_follower_enrollments_page(None, 1, clock().unwrap())
            .unwrap()
            .is_none()
    );
    let (entered, resume) = fixture.provider.pause_preparation();
    fixture.install();
    captured(entered).await;
    let before = fixture.node.stats().retained_bytes();
    let now = clock().unwrap();
    let page = fixture
        .node
        .fleet_follower_enrollments_page(None, 1, now)
        .unwrap()
        .unwrap();
    assert_eq!(page.scope(), scope());
    assert_eq!(page.node(), node_id(0));
    assert_eq!(page.session(), session(0));
    assert_eq!(page.observed_at_ms(), now);
    assert!(page.protocol_busy());
    assert!(!page.draining());
    assert_eq!(page.pending_epoch(), None);
    assert_eq!(page.total_epochs(), 0);
    assert!(page.entries().is_empty());
    assert!(page.next().is_none());
    assert!(fixture.rows().await.is_empty());
    assert!(fixture.node.runtime().node_durability().is_none());
    assert!(fixture.transport.requests.lock().unwrap().is_empty());
    assert_eq!(fixture.node.stats().retained_bytes(), before + (1 << 20));
    drop(page);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    resume.send(()).unwrap();
    fixture.installed().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_member_capture_preserves_inputs_budget_and_changed_continuations() {
    let fixture = Fixture::new().await;
    let (first, resume_first) = fixture.journal.pause_next_enrollment_reply(false, false);
    fixture.install();
    captured(first).await;
    let (second, resume_second) = fixture.journal.pause_before_enrollment_acceptance();
    resume_first.send(()).unwrap();
    captured(second).await;
    let before = fixture.node.stats().retained_bytes();
    let original = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    let page = capture(&fixture);
    assert!(page.protocol_busy());
    assert_eq!(page.pending_epoch(), Some(1));
    assert_eq!(page.total_epochs(), 1);
    assert!(page.next().is_none());
    let entry = &page.entries()[0];
    assert_eq!(entry.epoch, 1);
    assert_eq!(entry.attempt, original.attempt.evidence_digest().unwrap());
    assert_eq!(entry.members.len(), 2);
    for (observed, original) in entry.members.iter().zip(&original.members) {
        assert_eq!(observed.spec, original.spec);
        assert_eq!(observed.accepted, original.accepted);
        assert_eq!(observed.event, original.event);
        assert_eq!(observed.published, original.published);
    }
    assert!(entry.members[0].accepted.is_some());
    assert!(entry.members[1].accepted.is_none());
    assert!(!entry.native_started);
    assert!(!entry.delivered);
    assert!(!entry.no_effect);
    assert!(!entry.native_closed);
    assert!(entry.enrollment.is_none());
    assert!(entry.refusal.is_none());
    assert!(entry.retirement.is_none());
    assert_eq!(fixture.node.stats().retained_bytes(), before + (1 << 20));
    let mut bytes = [0; 40];
    bytes[..32].copy_from_slice(page.topology().as_bytes());
    bytes[32..].copy_from_slice(&1u64.to_le_bytes());
    let cursor = FollowerEnrollmentInventoryCursor::from_bytes(&bytes).unwrap();
    drop(page);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    let continuation = fixture
        .node
        .fleet_follower_enrollments_page(Some(cursor), 1, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(continuation.total_epochs(), 1);
    assert!(continuation.entries().is_empty());
    drop(continuation);
    bytes[32..].copy_from_slice(&2u64.to_le_bytes());
    let absent = FollowerEnrollmentInventoryCursor::from_bytes(&bytes).unwrap();
    assert!(
        fixture
            .node
            .fleet_follower_enrollments_page(Some(absent), 1, clock().unwrap())
            .is_err()
    );
    for (limit, now) in [(0, 0), (33, 0), (1, -1)] {
        assert!(
            fixture
                .node
                .fleet_follower_enrollments_page(None, limit, now)
                .is_err()
        );
    }
    let runtime = fixture.node.runtime();
    let available = fixture.node.stats().retained_capacity_bytes() - before;
    let exhausted = runtime.try_reserve_node_bytes(available).unwrap();
    assert!(matches!(
        fixture
            .node
            .fleet_follower_enrollments_page(None, 1, clock().unwrap()),
        Err(Error::Capacity(_))
    ));
    drop(exhausted);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    assert_eq!(fixture.rows().await.len(), 1);
    assert!(fixture.node.runtime().node_durability().is_none());
    resume_second.send(()).unwrap();
    fixture.installed().await;
    assert!(
        fixture
            .node
            .fleet_follower_enrollments_page(Some(cursor), 1, clock().unwrap())
            .is_err()
    );
    let page = capture(&fixture);
    let entry = &page.entries()[0];
    assert!(entry.native_started);
    assert!(entry.delivered);
    assert!(entry.enrollment.is_some());
    assert!(entry.members.iter().all(|member| member.published));
    drop(page);
    let node = fixture.node.clone();
    fixture.finish().await;
    assert!(
        node.fleet_follower_enrollments_page(None, 1, clock().unwrap())
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_rotation_inventory_preserves_original_failed_member_and_error() {
    let fixture = Fixture::new().await;
    fixture.install();
    fixture.installed().await;
    fixture.transport.lose_retire.store(true, Ordering::Release);
    let (retry, resume) = fixture.transport.pause_retirement_retry();
    let request = fixture.node.request_node_log_rotation(1).unwrap();
    captured(retry).await;
    let completion = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let completion = fixture
                .node
                .follower_enrollment_completion(1)
                .unwrap()
                .unwrap();
            if completion.execution_error.is_some() && completion.retirement.is_some() {
                return completion;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = fixture.node.stats().retained_bytes();
    let page = capture(&fixture);
    let entry = &page.entries()[0];
    assert_eq!(entry.epoch, 1);
    assert!(entry.delivered);
    assert!(!entry.native_closed);
    assert!(Arc::ptr_eq(
        entry.execution_error.as_ref().unwrap(),
        completion.execution_error.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        entry.retirement.as_ref().unwrap(),
        completion.retirement.as_ref().unwrap()
    ));
    assert!(entry.retirement.as_ref().unwrap().confirmed().is_err());
    assert_eq!(entry.retirement.as_ref().unwrap().members().len(), 2);
    assert_eq!(fixture.authority.attempts.load(Ordering::Acquire), 0);
    assert!(
        fixture
            .rows()
            .await
            .iter()
            .all(|row| row.status() == EnrollmentStatus::Established)
    );
    assert_eq!(fixture.node.stats().retained_bytes(), before + (1 << 20));
    drop(page);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    drop(request);
    fixture
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    resume.send(()).unwrap();
    fixture.finish().await;
}
