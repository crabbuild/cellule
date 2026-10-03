use super::*;
use cellule_runtime::fleet::operations::EnrollmentEvent;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_withdrawal_during_initial_probe_preserves_original_reader() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.transport.pause_probe(1);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    fixture.nodes[2].shutdown().await.unwrap();
    assert!(fixture.directory.is_withdrawn(session(2)).await.unwrap());
    resume.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(fixture.original_row().await, fixture.original);
    assert!(
        !fixture
            .reader
            .lifecycle_observation()
            .await
            .admission_closed()
    );
    fixture.read_original(17).await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_withdrawal_during_final_probe_refuses_completion_after_retirement() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.transport.pause_probe(2);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let retired = fixture.original_row().await;
    assert_eq!(retired.status(), EnrollmentStatus::Retired);
    assert!(
        fixture
            .reader
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    fixture.nodes[2].shutdown().await.unwrap();
    resume.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert!(fixture.evacuate().await.is_err());
    assert_eq!(fixture.original_row().await, retired);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_reader_without_spare_remains_open_and_durably_established() {
    let fixture = Fixture::new().await;
    assert!(matches!(fixture.evacuate().await, Err(Error::Capacity(_))));
    assert_eq!(fixture.transport.probes.load(Ordering::SeqCst), 0);
    // Exercise the installed periodic repair loop beyond its real five-second
    // tick; a changed placement must not silently prune this managed reader.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(fixture.original_row().await, fixture.original);
    assert!(
        !fixture
            .reader
            .lifecycle_observation()
            .await
            .admission_closed()
    );
    fixture.read_original(17).await;
    assert!(fixture.nodes[1].is_management_ready());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_native_replacement_precedes_joined_reader_retirement() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let proof = fixture.evacuate().await.unwrap();
    assert_eq!(proof.original(), &fixture.original);
    assert_eq!(proof.retired(), &fixture.original_row().await);
    assert_eq!(proof.retired().status(), EnrollmentStatus::Retired);
    assert_eq!(proof.policy().unwrap().desired_readers(), 1);
    assert_eq!(proof.replacements().len(), 1);
    assert_eq!(proof.replacements()[0].node, node_id(2));
    assert_eq!(proof.replacements()[0].session, session(2));
    assert!(proof.replacements()[0].receipt.commit_sequence >= proof.minimum().commit_sequence);
    assert!(proof.interval().0 <= proof.interval().1);
    assert_eq!(fixture.transport.probes.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .reader
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    assert!(matches!(
        fixture
            .reader
            .query::<application::ReadValue>(None, 0)
            .await,
        Err(Error::Fenced)
    ));
    let spare = fixture.managers[2]
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    assert_eq!(
        spare
            .query::<application::ReadValue>(Some(proof.minimum()), 0)
            .await
            .unwrap()
            .output,
        17
    );
    assert!(fixture.nodes[1].is_management_ready());
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_retirement_reply_keeps_original_view_and_evidence_for_replay() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let retired = fixture.original_row().await;
    assert_eq!(retired.status(), EnrollmentStatus::Retired);
    assert!(
        fixture
            .reader
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    resume.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    let pending = fixture.managers[1]
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(!pending.published);
    assert!(pending.journal_error.is_some());
    let proof = fixture.evacuate().await.unwrap();
    assert_eq!(proof.retired(), &retired);
    assert!(
        fixture.managers[1]
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_retirement_waiter_resumes_original_committed_closure() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let retired = fixture.original_row().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(
        fixture
            .reader
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    // The journal transaction committed before this reply-only pause. Cancelling
    // its waiter drops that reply receiver, while the producer retains its event.
    assert!(resume.send(()).is_err());
    let completion = fixture.managers[1]
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(!completion.published);
    assert!(matches!(
        completion.event,
        Some(EnrollmentEvent::Retired(_))
    ));
    let proof = fixture.evacuate().await.unwrap();
    assert_eq!(proof.retired(), &retired);
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_preserves_committed_retirement_and_original_error_source() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), captured)
        .await
        .unwrap()
        .unwrap();
    let retired = fixture.original_row().await;
    let error = match task.await.unwrap() {
        Ok(_) => panic!("paused publication returned proof"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, Error::Facility { name: "reader-evacuation-deadline", source } if source.downcast_ref::<tokio::time::error::Elapsed>().is_some())
    );
    assert!(resume.send(()).is_err());
    let completion = fixture.managers[1]
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(!completion.published);
    assert!(matches!(
        completion.event,
        Some(EnrollmentEvent::Retired(_))
    ));
    let proof = fixture.evacuate().await.unwrap();
    assert_eq!(proof.retired(), &retired);
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_change_during_signed_status_refuses_close_then_zero_policy_can_settle() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.transport.pause_probe(1);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    fixture.managers[0]
        .set_target(&fixture.target, 1, 0)
        .await
        .unwrap()
        .unwrap();
    resume.send(()).unwrap();
    assert!(matches!(task.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(fixture.original_row().await, fixture.original);
    fixture.read_original(17).await;
    let proof = fixture.evacuate().await.unwrap();
    assert_eq!(proof.policy().unwrap().desired_readers(), 0);
    assert!(proof.replacements().is_empty());
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_metadata_refuses_before_closure_and_releases_all_temporary_credit() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let stats = fixture.nodes[1].stats();
    let credit = fixture.nodes[1]
        .runtime()
        .try_reserve_node_metadata_bytes(stats.retained_capacity_bytes() - stats.retained_bytes())
        .unwrap();
    assert!(matches!(fixture.evacuate().await, Err(Error::Capacity(_))));
    assert_eq!(fixture.transport.probes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.original_row().await, fixture.original);
    assert!(
        !fixture
            .reader
            .lifecycle_observation()
            .await
            .admission_closed()
    );
    drop(credit);
    fixture.read_original(17).await;
    let proof = fixture.evacuate().await.unwrap();
    drop(proof);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_refresh_after_initial_probe_requires_replacement_to_cover_closed_prefix() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let (captured, resume) = fixture.transport.pause_probe(1);
    let manager = fixture.managers[1].clone();
    let original = fixture.original.clone();
    let operation = fixture.operation.clone();
    let peer = fixture.peer.clone();
    let task = tokio::spawn(async move {
        manager
            .evacuate(
                &original,
                &operation,
                &peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let now = clock().unwrap();
    fixture
        .handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([90; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 30_000,
            },
            Digest::from_bytes([91; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value = 18", [])?;
                Ok(HandlerOutcome::Success(vec![]))
            },
        )
        .await
        .unwrap();
    let closed_prefix = fixture
        .reader
        .refresh(&fixture.root.path().join("peer-refresh.sqlite"))
        .await
        .unwrap();
    let cellule_runtime::fleet::operations::EnrollmentRole::Reader { position, .. } =
        &fixture.original.spec().role
    else {
        panic!("original is not a reader");
    };
    assert!(closed_prefix.commit_sequence > position.root.commit_sequence);
    resume.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::ReplicaUnavailable)
    ));
    assert!(
        fixture
            .reader
            .lifecycle_observation()
            .await
            .locally_joined()
    );
    assert_eq!(
        fixture.original_row().await.status(),
        EnrollmentStatus::Retired
    );
    fixture.managers[2]
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let proof = fixture.evacuate().await.unwrap();
    assert!(proof.minimum().commit_sequence >= closed_prefix.commit_sequence);
    assert!(proof.replacements()[0].receipt.commit_sequence >= closed_prefix.commit_sequence);
    let reader = fixture.managers[2]
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    assert_eq!(
        reader
            .query::<application::ReadValue>(Some(proof.minimum()), 0)
            .await
            .unwrap()
            .output,
        18
    );
    drop(proof);
    fixture.finish().await;
}
