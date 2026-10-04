//! Exact request closure on a real managed reader; newer generations stay owned.
use super::*;
use cellule_runtime::{fleet::operations::EnrollmentRole, node::NodeMode};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
async fn installed(fixture: &ReaderFixture) -> EnrollmentRecord {
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let row = fixture
        .rows()
        .await
        .into_iter()
        .find(|row| row.status() == EnrollmentStatus::Established)
        .unwrap();
    assert_eq!(row.spec().source.unwrap().node, node_id(0));
    assert_eq!(row.spec().target.node, node_id(1));
    row
}
async fn assert_joined(fixture: &ReaderFixture, reader: &cellule_runtime::client::CellReadReplica) {
    assert!(reader.lifecycle_observation().await.locally_joined());
    assert!(
        reader
            .query::<application::ReadValue>(None, 0)
            .await
            .is_err()
    );
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_joins_original_peer_clones_and_retains_confirmed_prefix() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let reader = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let clone = reader.clone();
    let before = reader.receipt().await;
    let root = reader.lifecycle_observation().await.root();
    fixture.read().await;
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    assert_eq!(capture.original(), &original);
    assert_eq!(capture.source().owner().session, session(0));
    assert_eq!(capture.receipt(), before);
    assert_eq!(capture.root(), root);
    assert_eq!(capture.retired(), &fixture.rows().await[0]);
    assert_eq!(
        capture.retired().accepted_at_ms(),
        original.accepted_at_ms()
    );
    assert_eq!(
        capture.retired().established_evidence(),
        original.established_evidence()
    );
    assert_eq!(capture.retired().status(), EnrollmentStatus::Retired);
    assert!(capture.interval().0 <= capture.interval().1);
    assert!(capture.interval().1 - capture.interval().0 <= 30_000);
    assert_joined(&fixture, &clone).await;
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    // A terminal row alone cannot recapture native joining after the local owner
    // was discarded. The application must retain the original successful capsule.
    assert!(matches!(
        fixture.manager.remove_enrolled(&original, deadline()).await,
        Err(Error::Fenced)
    ));
    drop((capture, clone, reader));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_cannot_close_a_new_request_for_the_same_cell() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let current = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(current.spec, *original.spec());
    let before = fixture.rows().await;
    assert!(matches!(
        fixture.manager.remove_enrolled(&original, deadline()).await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.rows().await, before);
    fixture.read().await;
    assert!(
        !fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .unwrap()
            .lifecycle_observation()
            .await
            .admission_closed()
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_rejects_substituted_opening_proof_without_closing() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let pending = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .accepted
        .unwrap();
    let changed = pending
        .establish(Digest::from_bytes([217; 32]), original.updated_at_ms())
        .unwrap();
    assert!(matches!(
        fixture.manager.remove_enrolled(&changed, deadline()).await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.rows().await, vec![original]);
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_retains_publication_error_and_retries_the_same_join() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let reader = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let manager = fixture.manager.clone();
    let input = original.clone();
    let removal = tokio::spawn(async move { manager.remove_enrolled(&input, deadline()).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    assert!(reader.lifecycle_observation().await.locally_joined());
    let committed = fixture.rows().await[0].clone();
    assert_eq!(committed.status(), EnrollmentStatus::Retired);
    resume.send(()).unwrap();
    let error = match removal.await.unwrap() {
        Err(error) => error,
        Ok(_) => panic!("lost reply certified closure"),
    };
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(io) = source.downcast_ref::<std::io::Error>() {
            assert!(io.to_string().contains("injected lost enrollment reply"));
            break;
        }
        source = source
            .source()
            .expect("original publication source was discarded");
    }
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .journal_error
            .is_some()
    );
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    assert_eq!(capture.retired(), &committed);
    assert_eq!(capture.receipt(), reader.receipt().await);
    assert_joined(&fixture, &reader).await;
    drop((capture, reader));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_cancelled_publication_waiter_keeps_the_original_view_and_event() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let reader = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let input = original.clone();
    let removal = tokio::spawn(async move { manager.remove_enrolled(&input, deadline()).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let committed = fixture.rows().await[0].clone();
    removal.abort();
    assert!(matches!(removal.await, Err(error) if error.is_cancelled()));
    assert!(resume.send(()).is_err());
    assert!(reader.lifecycle_observation().await.locally_joined());
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    assert_eq!(capture.retired(), &committed);
    assert_joined(&fixture, &reader).await;
    drop((capture, reader));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_deadline_keeps_the_original_owner_for_same_request_retry() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    assert!(matches!(
        fixture
            .manager
            .remove_enrolled(&original, Instant::now())
            .await,
        Err(Error::Deadline)
    ));
    fixture.read().await;
    let reader = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let input = original.clone();
    let removal = tokio::spawn(async move { manager.remove_enrolled(&input, deadline()).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let committed = fixture.rows().await[0].clone();
    assert!(matches!(
        removal.await.unwrap(),
        Err(Error::Facility {
            name: "reader-exact-removal-deadline",
            ..
        })
    ));
    assert!(resume.send(()).is_err());
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    assert_eq!(capture.retired(), &committed);
    assert_joined(&fixture, &reader).await;
    drop((capture, reader));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_refuses_unknown_original_acceptance_before_waiting_for_activation() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let pending = fixture.rows().await[0].clone();
    assert_eq!(pending.status(), EnrollmentStatus::Pending);
    assert!(matches!(
        fixture.manager.remove_enrolled(&pending, deadline()).await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.rows().await, vec![pending]);
    resume.send(()).unwrap();
    opening.await.unwrap().unwrap();
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_rejects_foreign_boot_before_closing_the_original() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let EnrollmentRole::Reader { .. } = &original.spec().role else {
        panic!("not reader")
    };
    let mut spec = original.spec().clone();
    spec.target.session = session(2);
    let target = NodeIntent::initial(scope(), node_id(1), session(2)).unwrap();
    let source = NodeIntent::initial(scope(), node_id(0), session(0)).unwrap();
    let changed = EnrollmentRecord::pending(
        spec,
        Some(&source),
        None,
        &target,
        original.accepted_at_ms(),
    )
    .unwrap()
    .establish(
        original.established_evidence().unwrap(),
        original.updated_at_ms(),
    )
    .unwrap();
    assert!(matches!(
        fixture.manager.remove_enrolled(&changed, deadline()).await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.rows().await, vec![original]);
    fixture.read().await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_reader_removal_joins_the_original_source_role_after_real_writer_handoff() {
    let fixture = ReaderFixture::new().await;
    let original = installed(&fixture).await;
    let reader = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let opened_root = reader.lifecycle_observation().await.root();
    let now = clock().unwrap();
    fixture
        .handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([220; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 30_000,
            },
            Digest::from_bytes([221; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value = 17", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    reader
        .refresh(&fixture.root.path().join("handoff-reader-refresh.sqlite"))
        .await
        .unwrap();
    let final_root = reader.lifecycle_observation().await.root();
    assert_ne!(final_root, opened_root);
    assert!(final_root.commit_sequence > opened_root.commit_sequence);
    // This source has just accepted a command, so the ordinary idle-transfer
    // grace correctly excludes it. Join and release this exact busy handle
    // through canonical drain; do not shorten or bypass idle eligibility.
    fixture.handle.drain().await.unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        idle.value().state,
        cellule_runtime::control::ControlState::Idle
    );
    let catalog = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let replica = CellReplica::new(
        fixture.layout.clone(),
        *fixture.target.cell_id().as_bytes(),
        *idle.value().incarnation.as_bytes(),
        fixture.limits,
    )
    .unwrap();
    let successor = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 8).unwrap(),
        // The newly exercised origin verifier reserves its ordinary bounded
        // metadata envelope through this runtime, in addition to the writer.
        64 << 20,
        session(2),
        Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
    )
    .unwrap();
    let next = successor
        .acquire_idle_restored(
            catalog.clone(),
            replica.clone(),
            authority.clone(),
            idle,
            fixture.root.path().join("successor.sqlite"),
            owner(2),
        )
        .await
        .unwrap();
    let current = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().owner.as_ref().unwrap().session, session(2));
    let EnrollmentRole::Reader { position, .. } = &original.spec().role else {
        panic!("not reader")
    };
    assert!(current.value().epoch > position.epoch);
    assert!(
        current.value().root.as_ref().unwrap().commit_sequence >= position.root.commit_sequence
    );
    assert_eq!(read_counter(&next).await, 17_i64.to_be_bytes());
    let capture = fixture
        .manager
        .remove_enrolled(&original, deadline())
        .await
        .unwrap();
    assert_eq!(
        capture.original().spec().source.unwrap().session,
        session(0)
    );
    assert_eq!(capture.source().owner().session, session(0));
    assert_eq!(capture.retired().status(), EnrollmentStatus::Retired);
    assert_eq!(capture.root(), reader.lifecycle_observation().await.root());
    assert_eq!(capture.root(), final_root);
    assert_eq!(
        capture.root().commit_sequence,
        capture.receipt().commit_sequence
    );
    // Verify the retained exact native root through the successor's canonical
    // origin path, rather than treating equal/higher counters as derivation.
    let descendant = current.value().ltx_root().unwrap();
    successor
        .verify_root_prefix(
            &catalog,
            &authority,
            replica.clone(),
            capture.root(),
            descendant,
            10_000,
        )
        .await
        .unwrap();
    let substituted = cellule_runtime::ltx::RootRef {
        digest: [219; 32],
        ..capture.root()
    };
    assert!(
        successor
            .verify_root_prefix(
                &catalog,
                &authority,
                replica,
                substituted,
                descendant,
                10_000,
            )
            .await
            .is_err()
    );
    assert_joined(&fixture, &reader).await;
    // The local closure cannot erase the separately serving writer or report
    // any reader redundancy/physical maintenance completion.
    assert_eq!(read_counter(&next).await, 17_i64.to_be_bytes());
    drop((capture, reader));
    next.drain().await.unwrap();
    successor.shutdown().await.unwrap();
    fixture.node.shutdown().await.unwrap();
    fixture.source.shutdown().await.unwrap();
    fixture.leases.fence();
    for stats in [
        successor.stats(),
        fixture.node.stats(),
        fixture.source.stats(),
    ] {
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
    }
    fixture.journal.close().await.unwrap();
}

async fn read_counter(handle: &CellHandle) -> Vec<u8> {
    handle
        .query(0, 8, |connection| {
            let value = connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap()
}
