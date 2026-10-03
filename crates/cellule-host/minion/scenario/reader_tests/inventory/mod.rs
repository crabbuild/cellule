//! Inventory is read-only through the original producer's paused native/journal work.
use super::*;
use cellule_host::read_replicas::{ReaderEnrollmentInventoryCursor, ReaderEnrollmentInventoryPage};
use cellule_runtime::{fleet::operations::EnrollmentEvent, node::NodeMode};

mod variable_rows;

fn capture(fixture: &ReaderFixture, limit: usize) -> ReaderEnrollmentInventoryPage {
    fixture
        .manager
        .fleet_reader_enrollments_page(None, limit, clock().unwrap())
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_acceptance_exposes_original_request_without_waiting_or_settling() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let pending = fixture.rows().await;
    let before = fixture.node.stats().retained_bytes();
    let now = clock().unwrap();
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, now)
        .unwrap()
        .unwrap();
    assert_eq!(page.session(), session(1));
    assert_eq!(page.observed_at_ms(), now);
    assert_eq!(page.total_enrollments(), 1);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(page.entries()[0].spec, *pending[0].spec());
    assert!(page.entries()[0].accepted.is_none());
    assert!(!page.entries()[0].opening_started);
    assert!(!page.entries()[0].opening_joined);
    assert!(page.entries()[0].event.is_none());
    assert_eq!(page.jobs().retained(), 1);
    assert_eq!(page.jobs().running(), 1);
    assert_eq!(page.jobs().unobserved(), 0);
    assert_eq!(page.jobs().joining(), 0);
    assert!(page.next().is_none());
    assert_eq!(fixture.node.stats().retained_bytes(), before + (1 << 20));
    let completion = tokio::time::timeout(
        Duration::from_secs(3),
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(completion.spec, page.entries()[0].spec);
    assert_eq!(fixture.rows().await, pending);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    drop(page);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    resume.send(()).unwrap();
    assert!(opening.await.unwrap().is_err());
    let page = capture(&fixture, 1);
    let completion = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(
        page.entries()[0].journal_error.as_ref().unwrap(),
        completion.journal_error.as_ref().unwrap()
    ));
    assert_eq!(page.jobs().running(), 0);
    assert_eq!(page.jobs().unobserved(), 0);
    assert!(page.jobs().protocol_failure().is_some());
    drop(page);
    fixture
        .manager
        .remove(fixture.target.cell_id())
        .await
        .unwrap();
    fixture.finish_status(EnrollmentStatus::Refused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_establishment_exposes_joined_native_open_and_original_pending_reply() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let original = fixture.rows().await;
    let page = capture(&fixture, 1);
    let progress = &page.entries()[0];
    assert_eq!(progress.spec, *original[0].spec());
    assert_eq!(
        progress.accepted.as_ref().unwrap().status(),
        EnrollmentStatus::Pending
    );
    assert!(progress.opening_started && progress.opening_joined);
    assert!(!progress.published);
    assert!(
        matches!(progress.event, Some(EnrollmentEvent::Established(e)) if Some(e) == original[0].established_evidence())
    );
    assert_eq!(page.jobs().running(), 1);
    assert_eq!(fixture.rows().await, original);
    drop(page);
    resume.send(()).unwrap();
    opening.await.unwrap().unwrap();
    fixture.read().await;
    let page = capture(&fixture, 1);
    assert!(page.entries()[0].published);
    assert_eq!(page.jobs().running(), 0);
    drop(page);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pages_traverse_all_original_readers_and_reject_progress_and_mode_changes() {
    let fixture = ReaderFixture::new().await;
    let (second, second_handle) = fixture.additional_cell(2).await;
    let (third, third_handle) = fixture.additional_cell(3).await;
    for target in [&fixture.target, &second] {
        fixture
            .manager
            .activate(target.clone(), session(0))
            .await
            .unwrap();
    }
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let target = third.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let rows = fixture.rows().await;
    let before = fixture.node.stats().retained_bytes();
    let first = capture(&fixture, 1);
    assert_eq!(first.total_enrollments(), 3);
    let cursor =
        ReaderEnrollmentInventoryCursor::from_bytes(&first.next().unwrap().to_bytes()).unwrap();
    let rest = fixture
        .manager
        .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(rest.topology(), first.topology());
    assert_eq!(rest.entries().len(), 2);
    assert!(rest.next().is_none());
    let mut observed = first
        .entries()
        .iter()
        .chain(rest.entries())
        .map(|r| r.spec.clone())
        .collect::<Vec<_>>();
    let cells = first
        .entries()
        .iter()
        .chain(rest.entries())
        .map(|r| r.source.description().cell)
        .collect::<Vec<_>>();
    assert!(
        cells
            .windows(2)
            .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
    );
    observed.sort_by_key(|r| *r.key().unwrap().as_bytes());
    let mut expected = rows.iter().map(|r| r.spec().clone()).collect::<Vec<_>>();
    expected.sort_by_key(|r| *r.key().unwrap().as_bytes());
    assert_eq!(observed, expected);
    assert_eq!(fixture.node.stats().retained_bytes(), before + (2 << 20));
    drop(rest);
    drop(first);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    resume.send(()).unwrap();
    opening.await.unwrap().unwrap();
    assert!(
        fixture
            .manager
            .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
            .is_err()
    );
    fixture.manager.remove(second.cell_id()).await.unwrap();
    assert!(
        fixture
            .manager
            .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
            .is_err()
    );
    let first = capture(&fixture, 1);
    let cursor = first.next().unwrap();
    drop(first);
    fixture.node.runtime().node_admission().cordon().unwrap();
    assert!(
        fixture
            .manager
            .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
            .is_err()
    );
    let cordoned = capture(&fixture, 128);
    assert_eq!(cordoned.mode(), NodeMode::Cordoned);
    assert_eq!(cordoned.total_enrollments(), 2);
    drop(cordoned);
    fixture.manager.shutdown().await.unwrap();
    let closed = capture(&fixture, 1);
    assert_eq!(closed.total_enrollments(), 0);
    assert!(closed.jobs().draining());
    assert_eq!(closed.jobs().retained(), 0);
    assert_eq!(closed.jobs().unobserved(), 0);
    drop(closed);
    fixture.node.shutdown().await.unwrap();
    assert!(matches!(
        fixture
            .manager
            .fleet_reader_enrollments_page(None, 1, clock().unwrap()),
        Err(Error::RuntimeClosed)
    ));
    second_handle.drain().await.unwrap();
    third_handle.drain().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_bounds_stale_keys_and_memory_refusal_preserve_obligations() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let original = fixture.rows().await;
    let before = fixture.node.stats().retained_bytes();
    for (limit, now) in [(0, 0), (129, 0), (1, -1), (usize::MAX, 0)] {
        assert!(
            fixture
                .manager
                .fleet_reader_enrollments_page(None, limit, now)
                .is_err()
        );
    }
    let page = capture(&fixture, 1);
    let mut bytes = [9; 64];
    bytes[..32].copy_from_slice(page.topology().as_bytes());
    drop(page);
    let cursor = ReaderEnrollmentInventoryCursor::from_bytes(&bytes).unwrap();
    assert!(
        fixture
            .manager
            .fleet_reader_enrollments_page(Some(cursor), 128, clock().unwrap())
            .is_err()
    );
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    let mut held = Vec::new();
    while let Ok(reservation) = fixture.node.runtime().try_reserve_node_bytes(1 << 20) {
        held.push(reservation);
    }
    let occupied = fixture.node.stats().retained_bytes();
    assert!(matches!(
        fixture
            .manager
            .fleet_reader_enrollments_page(None, 128, clock().unwrap()),
        Err(Error::Capacity(_))
    ));
    assert_eq!(fixture.node.stats().retained_bytes(), occupied);
    assert_eq!(fixture.rows().await, original);
    drop(held);
    assert_eq!(fixture.node.stats().retained_bytes(), before);
    drop(capture(&fixture, 1));
    resume.send(()).unwrap();
    opening.await.unwrap().unwrap();
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_preparation_without_a_request_is_visible_as_running_work() {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    let armed = Arc::new(AtomicBool::new(false));
    let arm = armed.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, receive) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some(receive));
    let fixture = ReaderFixture::with_store(
        Store::new(Arc::new(InMemory::new())).with_read_request_observer(Arc::new(move |_| {
            if arm.swap(false, Ordering::AcqRel) {
                let receiver = gate.lock().unwrap().take().unwrap();
                signal.notify_one();
                let _ = receiver.recv();
            }
        })),
    )
    .await;
    armed.store(true, Ordering::Release);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    let reached = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    if reached.is_err() {
        let _ = release.send(());
        panic!("original reader preparation read was not captured");
    }
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 1, clock().unwrap());
    let completion = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await;
    let rows = fixture.rows().await;
    release.send(()).unwrap();
    let page = page.unwrap().unwrap();
    assert_eq!(page.total_enrollments(), 0);
    assert!(page.entries().is_empty());
    assert_eq!(page.jobs().retained(), 1);
    assert_eq!(page.jobs().running(), 1);
    assert_eq!(page.jobs().unobserved(), 0);
    assert!(completion.unwrap().is_none());
    assert!(rows.is_empty());
    drop(page);
    opening.await.unwrap().unwrap();
    fixture.read().await;
    fixture.finish().await;
}
