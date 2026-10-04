use super::*;

async fn refused(fixture: &Fixture) -> Vec<EnrollmentRecord> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let rows = fixture.rows().await;
            if rows.len() == 2
                && rows
                    .iter()
                    .all(|row| row.status() == EnrollmentStatus::Refused)
            {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_selected_members_are_pending_before_the_one_canonical_enrollment() {
    let fixture = Fixture::new().await;
    let (first, resume_first) = fixture.journal.pause_next_enrollment_reply(false, false);
    fixture.install();
    captured(first).await;
    let rows = fixture.rows().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status(), EnrollmentStatus::Pending);
    let (second, resume_second) = fixture.journal.pause_before_enrollment_acceptance();
    resume_first.send(()).unwrap();
    captured(second).await;
    let completion = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    assert_eq!(completion.members.len(), 2);
    assert!(!completion.native_started);
    assert!(completion.members[0].accepted.is_some());
    assert!(completion.members[1].accepted.is_none());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    assert!(fixture.node.runtime().node_durability().is_none());
    resume_second.send(()).unwrap();
    fixture.installed().await;
    let completion = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    assert!(completion.native_started);
    assert!(completion.enrollment.is_some());
    assert!(completion.members.iter().all(|member| member.published
        && member.accepted.as_ref().unwrap().status() == EnrollmentStatus::Pending
        && matches!(member.event, Some(EnrollmentEvent::Established(_)))));
    assert_eq!(
        completion.attempt.prepared().log().members(),
        &[node_id(1), node_id(2)]
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_acceptance_fences_every_original_member_without_native_dispatch() {
    let fixture = Fixture::new().await;
    let (accepted, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    fixture.install();
    captured(accepted).await;
    let original = fixture.rows().await.remove(0);
    resume.send(()).unwrap();
    let rows = refused(&fixture).await;
    let first = rows
        .iter()
        .find(|row| row.spec() == original.spec())
        .unwrap();
    assert_eq!(first.accepted_at_ms(), original.accepted_at_ms());
    for row in &rows {
        assert!(
            matches!(fixture.journal.accept_enrollment(row.spec(), clock().unwrap()).await.unwrap(),
            cellule_host::fleet::FleetEnrollmentAcceptance::Existing(ref replay) if replay == row)
        );
    }
    assert!(fixture.node.runtime().node_durability().is_none());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    assert!(fixture.transport.requests.lock().unwrap().is_empty());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_acceptance_and_receiver_cordon_refuse_the_complete_fixed_ensemble() {
    use cellule_runtime::fleet::operations::{
        JournalTransition, MaintenanceOperation, OperationId,
    };
    let fixture = Fixture::new().await;
    let (accepted, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    fixture.install();
    captured(accepted).await;
    let completion = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    let receiver = completion.members[1].spec.target;
    let now = clock().unwrap();
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let claimed = fixture
        .journal
        .claim_controller(scope(), snapshot.head().revision(), session(9), now)
        .await
        .unwrap();
    fixture
        .journal
        .compare_exchange(
            &claimed,
            claimed.head().controller().unwrap().epoch,
            now,
            &JournalTransition::BeginMaintenance(
                MaintenanceOperation::new(
                    OperationId::from_bytes([203; 16]).unwrap(),
                    Digest::from_bytes([204; 32]),
                    receiver.node,
                    receiver.session,
                    receiver.intent_revision + 1,
                    now,
                    now + 60_000,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    let rows = refused(&fixture).await;
    assert!(rows.iter().all(|row| {
        completion
            .members
            .iter()
            .any(|member| &member.spec == row.spec())
    }));
    assert!(fixture.node.runtime().node_durability().is_none());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_establishment_replays_original_events_without_a_new_native_attempt() {
    let fixture = Fixture::new().await;
    let (published, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    fixture.install();
    captured(published).await;
    let original = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    let generation = fixture
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap()
        .advertisement()
        .generation();
    assert!(original.native_started);
    assert!(original.enrollment.is_some());
    assert!(fixture.node.runtime().node_durability().is_none());
    resume.send(()).unwrap();
    fixture.installed().await;
    let replay = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    assert!(replay.execution_error.is_none());
    assert!(replay.journal_error.is_some());
    for (a, b) in original.members.iter().zip(&replay.members) {
        assert_eq!(a.spec, b.spec);
        assert_eq!(a.accepted, b.accepted);
        assert_eq!(a.event, b.event);
    }
    assert_eq!(fixture.provider.prepared.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .generation(),
        generation
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_close_and_retirement_publication_keep_original_fences_and_owned_drain() {
    let fixture = Fixture::new().await;
    fixture.install();
    fixture.installed().await;
    fixture.authority.lose_reply.store(true, Ordering::Release);
    let (published, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let node = fixture.node.clone();
    let waiter = tokio::spawn(async move { node.shutdown().await });
    captured(published).await;
    let completion = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap();
    assert!(completion.native_closed);
    assert!(completion.execution_error.is_some());
    let original = completion.retirement.unwrap();
    original.confirmed().unwrap();
    assert_eq!(original.members().len(), 2);
    assert_eq!(fixture.authority.attempts.load(Ordering::Acquire), 2);
    assert_eq!(fixture.transport.requests.lock().unwrap().len(), 2);
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    assert_eq!(fixture.node.state(), NodeState::Draining);
    assert!(fixture.node.stats().retained_bytes() > 0);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.authority.attempts.load(Ordering::Acquire), 2);
    assert_eq!(fixture.transport.requests.lock().unwrap().len(), 2);
    let rows = fixture.rows().await;
    assert!(
        rows.iter()
            .all(|row| row.status() == EnrollmentStatus::Retired)
    );
    assert!(original.confirmed().is_ok());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_member_keeps_established_rows_until_a_later_native_fence() {
    let fixture = Fixture::new().await;
    fixture.install();
    fixture.installed().await;
    fixture.transport.lose_retire.store(true, Ordering::Release);
    assert!(
        fixture
            .node
            .shutdown_until(std::time::Instant::now() + Duration::from_millis(80))
            .await
            .is_err()
    );
    assert_eq!(fixture.node.state(), NodeState::Draining);
    // The deadline cancels the drain waiter, not the retained native owner.
    // A busy runner may still be awaiting the original member reply at 80 ms.
    // Await that actual failure before asserting its retained evidence.
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
    assert!(!completion.native_closed);
    assert!(completion.execution_error.is_some());
    assert!(matches!(
        fixture
            .node
            .fleet_follower_enrollments_page(None, 1, clock().unwrap()),
        Err(Error::RuntimeClosed)
    ));
    assert!(completion.retirement.unwrap().confirmed().is_err());
    assert_eq!(fixture.authority.attempts.load(Ordering::Acquire), 0);
    assert!(
        fixture
            .rows()
            .await
            .iter()
            .all(|row| row.status() == EnrollmentStatus::Established)
    );
    assert!(fixture.node.stats().retained_bytes() > 0);
    fixture
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_during_acceptance_preserves_supervisor_join_then_refuses_without_cas() {
    let fixture = Fixture::new().await;
    let (accepted, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    fixture.install();
    captured(accepted).await;
    assert!(
        fixture
            .node
            .shutdown_until(std::time::Instant::now() + Duration::from_millis(30))
            .await
            .is_err()
    );
    assert_eq!(fixture.node.state(), NodeState::Draining);
    assert!(
        !fixture
            .node
            .follower_enrollment_completion(1)
            .unwrap()
            .unwrap()
            .native_started
    );
    assert!(fixture.node.stats().retained_bytes() > 0);
    resume.send(()).unwrap();
    fixture.node.shutdown().await.unwrap();
    refused(&fixture).await;
    assert!(fixture.transport.requests.lock().unwrap().is_empty());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_shipper_limits_fail_before_pending_or_authority_mutation() {
    let fixture = Fixture::new().await;
    fixture.install_limits(Limits {
        max_capture_bytes: u64::MAX,
        ..Limits::default()
    });
    until(|| {
        fixture
            .provider
            .events
            .lock()
            .unwrap()
            .contains(&NodeDurabilityRotation::Failed)
    })
    .await;
    assert!(fixture.rows().await.is_empty());
    assert!(
        fixture
            .node
            .follower_enrollment_completion(1)
            .unwrap()
            .is_none()
    );
    assert!(fixture.node.runtime().node_durability().is_none());
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_enrollment_preserves_acknowledged_command_and_exact_root_through_drain() {
    let fixture = Fixture::new().await;
    fixture.install();
    fixture.installed().await;
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope().application,
        application::NAMESPACE,
        b"managed-follower-command",
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([241; 16]);
    let catalog = CellCatalog::new(fixture.layout.clone(), target.tenant());
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Sql,
                fixture.node.application().registry().module_digests()[0],
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let initial = authority
        .create_initial(&proof, incarnation, owner(0))
        .await
        .unwrap();
    let replica = CellReplica::new(
        fixture.layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        limits(),
    )
    .unwrap();
    let handle = fixture
        .node
        .runtime()
        .bootstrap(
            proof,
            replica.clone(),
            authority.clone(),
            initial,
            fixture.root.path().join("command.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (16)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let now = clock().unwrap();
    let request = RequestId::from_bytes([245; 16]);
    let response = handle
        .execute(
            MutationIdentity {
                request_id: request,
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([245; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(vec![17]))
            },
        )
        .await
        .unwrap();
    assert!(matches!(response, StoredOutcome::Success { ref result, .. } if result==&[17]));
    let (published, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let node = fixture.node.clone();
    let drain = tokio::spawn(async move { node.shutdown().await });
    captured(published).await;
    let observation = fixture
        .node
        .follower_enrollment_completion(1)
        .unwrap()
        .unwrap()
        .retirement
        .unwrap();
    assert!(observation.barrier().covered_through() > 0);
    observation.confirmed().unwrap();
    for member in observation.members() {
        assert_eq!(
            member.result().unwrap().durable_through,
            observation.barrier().covered_through()
        );
    }
    resume.send(()).unwrap();
    drain.await.unwrap().unwrap();
    let control = authority.load(target.cell_id()).await.unwrap().unwrap();
    let root = control.value().ltx_root().unwrap();
    let restored = fixture.root.path().join("restored.sqlite");
    assert_eq!(
        replica
            .open_root(&root)
            .await
            .unwrap()
            .restore(&restored)
            .await
            .unwrap(),
        root.position
    );
    let database = rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        database
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        17
    );
    assert_eq!(
        database
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id=?1",
                [request.as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
        vec![17]
    );
    drop(database);
    fixture.finish().await;
}
