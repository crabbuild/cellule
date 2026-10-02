//! The installed manager repairs original obligations without another hint or drain.
use super::*;
use cellule_host::fleet::FleetEnrollmentAcceptance;

async fn wait_for_repair(fixture: &ReaderFixture, cell: CellId, removed: bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let completion = fixture.manager.enrollment_completion(cell).await.unwrap();
            if (removed && completion.is_none())
                || (!removed && completion.is_some_and(|c| c.published))
            {
                let page = fixture
                    .manager
                    .fleet_reader_enrollments_page(None, 128, clock().unwrap())
                    .unwrap()
                    .unwrap();
                if page.jobs().retained() == 0 {
                    return;
                }
            }
            fixture.leases.renew().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconciliation_fences_lost_acceptance_without_reopening_or_shutdown() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    assert_eq!(original.status(), EnrollmentStatus::Pending);
    resume.send(()).unwrap();
    assert!(opening.await.unwrap().is_err());
    wait_for_repair(&fixture, fixture.target.cell_id(), true).await;
    let exclusion = fixture.rows().await[0].clone();
    assert_eq!(exclusion.spec(), original.spec());
    assert_eq!(exclusion.accepted_at_ms(), original.accepted_at_ms());
    assert_eq!(exclusion.status(), EnrollmentStatus::Refused);
    assert!(exclusion.established_evidence().is_none());
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    let delayed = fixture
        .journal
        .accept_enrollment(original.spec(), clock().unwrap())
        .await
        .unwrap();
    assert!(matches!(delayed, FleetEnrollmentAcceptance::Existing(row) if row == exclusion));
    fixture.finish_status(EnrollmentStatus::Refused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconciliation_republishes_original_opening_and_preserves_its_error() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    resume.send(()).unwrap();
    assert!(opening.await.unwrap().is_err());
    let retained = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(!retained.published);
    assert!(retained.opening_started && retained.opening_joined);
    wait_for_repair(&fixture, fixture.target.cell_id(), false).await;
    let repaired = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired.spec, retained.spec);
    assert_eq!(repaired.accepted, retained.accepted);
    assert_eq!(repaired.event, retained.event);
    assert!(Arc::ptr_eq(
        repaired.journal_error.as_ref().unwrap(),
        retained.journal_error.as_ref().unwrap(),
    ));
    assert_eq!(fixture.rows().await, vec![original]);
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconciliation_resumes_cancelled_removal_without_restamping_retirement() {
    let fixture = ReaderFixture::new().await;
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let peer = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let cell = fixture.target.cell_id();
    let removal = tokio::spawn(async move { manager.remove(cell).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    assert_eq!(original.status(), EnrollmentStatus::Retired);
    removal.abort();
    assert!(removal.await.unwrap_err().is_cancelled());
    let _ = resume.send(());
    wait_for_repair(&fixture, cell, true).await;
    let page = fixture
        .manager
        .fleet_readers_page(None, 128, clock().unwrap())
        .await
        .unwrap();
    assert_eq!(page.total_views(), 0);
    drop(page);
    assert_eq!(fixture.rows().await, vec![original]);
    assert!(peer.lifecycle_observation().await.locally_joined());
    assert!(matches!(
        peer.query::<application::ReadValue>(None, 0).await,
        Err(Error::Fenced)
    ));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconciliation_cannot_refuse_a_still_owned_pending_opening() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    opening.abort();
    assert!(opening.await.unwrap_err().is_cancelled());
    // Let the real five-second reconciliation tick encounter this original
    // record. Its lane is held by the retained acceptance owner, not the waiter.
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    assert_eq!(fixture.rows().await, vec![original.clone()]);
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(page.jobs().running(), 1);
    assert_eq!(page.entries()[0].spec, *original.spec());
    assert!(!page.entries()[0].opening_started);
    assert!(page.entries()[0].event.is_none());
    drop(page);
    resume.send(()).unwrap();
    wait_for_repair(&fixture, fixture.target.cell_id(), false).await;
    let established = fixture.rows().await[0].clone();
    assert_eq!(established.spec(), original.spec());
    assert_eq!(established.accepted_at_ms(), original.accepted_at_ms());
    assert_eq!(established.status(), EnrollmentStatus::Established);
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconciliation_retains_requests_when_inventory_credit_is_refused() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    resume.send(()).unwrap();
    assert!(opening.await.unwrap().is_err());
    // Activation joins its exact finite task before returning. Charge all
    // remaining credit; its epilogue cannot later reopen scan admission.
    let stats = fixture.node.stats();
    let held = fixture
        .node
        .runtime()
        .try_reserve_node_bytes(stats.retained_capacity_bytes() - stats.retained_bytes() - 128)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    assert_eq!(fixture.rows().await, vec![original.clone()]);
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_some()
    );
    drop(held);
    wait_for_repair(&fixture, fixture.target.cell_id(), true).await;
    let exclusion = fixture.rows().await[0].clone();
    assert_eq!(exclusion.spec(), original.spec());
    assert_eq!(exclusion.accepted_at_ms(), original.accepted_at_ms());
    assert_eq!(exclusion.status(), EnrollmentStatus::Refused);
    fixture.finish_status(EnrollmentStatus::Refused).await;
}
