use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_can_refresh_redundancy_during_closing_without_restamping_retirement()
 {
    use cellule_runtime::fleet::operations::{
        DrainEvidence, JournalTransition, MaintenanceEvent, MaintenancePhase,
    };
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap();
    let retired = fixture.original_row().await;
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    crate::scenario::commit_test_role_settlement(&fixture.journal, clock().unwrap())
        .await
        .unwrap();
    let snapshot = fixture
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(DrainEvidence {
                node: node_id(1),
                session: session(1),
                remaining_cells: 0,
                unresolved_attempts: 0,
                relocated: true,
                readers_settled: true,
                followers_settled: true,
                facilities_closed: false,
                stopped: false,
                withdrawn: false,
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Closing
    );
    let verifier = fixture.verifier();
    verifier
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    fixture.managers[0]
        .set_target(&fixture.target, 1, 0)
        .await
        .unwrap()
        .unwrap();
    let candidate = verifier
        .refresh(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(
        candidate.record().operation().phase(),
        MaintenancePhase::Closing
    );
    let updated = FleetReaderEvacuationPublication::publish_refreshed(
        &candidate,
        fixture.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    updated.confirmed().unwrap();
    assert_eq!(updated.record().unwrap().retired(), &retired);
    assert_eq!(fixture.original_row().await, retired);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_commits_complete_pages_and_rechecks_after_independent_reconstruction()
 {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let (record, pages) = capture.durable_record().unwrap();
    let before = capture.snapshot().registry().revision();
    let publication = fixture.publish(&capture).await;
    let check = publication.confirmed().unwrap();
    assert_eq!(publication.record().unwrap(), &record);
    assert_eq!(check.record_digest(), record.digest().unwrap());
    assert_eq!(check.snapshot().registry().revision(), before + 1);
    assert_eq!(check.replacements()[0].node, node_id(2));
    assert_eq!(record.retired(), &fixture.original_row().await);
    for page in &pages {
        assert_eq!(
            fixture
                .journal
                .load_reader_evacuation_page(scope(), page.digest().unwrap())
                .await
                .unwrap()
                .as_ref(),
            Some(page)
        );
    }
    let client = fixture.client().await;
    let loaded = client
        .load_reader_evacuation(scope(), record.digest().unwrap())
        .await
        .unwrap()
        .unwrap();
    let verifier = fixture.verifier();
    verifier
        .recheck(&client, &loaded, deadline(), clock)
        .await
        .unwrap();
    let repeated = fixture.publish(&capture).await;
    assert_eq!(repeated.record().unwrap(), &record);
    assert_eq!(repeated.confirmed().unwrap().snapshot(), check.snapshot());
    assert_eq!(loaded.interval(), capture.interval());
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_adopts_lost_commit_reply_without_restamping_native_history() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let (record, _) = capture.durable_record().unwrap();
    let verifier = fixture.verifier();
    let (entered, resume) = fixture.journal.pause_next_reader_evacuation_reply(true);
    let mut work = Box::pin(FleetReaderEvacuationPublication::publish(
        &capture,
        fixture.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    assert_eq!(fixture.stored(record.digest().unwrap()).await, record);
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    let error = result.record().unwrap_err();
    assert!(Arc::ptr_eq(&error, &result.confirmed().err().unwrap()));
    let Error::Facility { source, .. } = error.as_ref() else {
        panic!("original source absent")
    };
    assert!(source.downcast_ref::<std::io::Error>().is_some());
    let client = fixture.client().await;
    let original = client
        .load_reader_evacuation(scope(), record.digest().unwrap())
        .await
        .unwrap()
        .unwrap();
    verifier
        .recheck(&client, &original, deadline(), clock)
        .await
        .unwrap();
    let replay = fixture.publish(&capture).await;
    assert_eq!(replay.record().unwrap(), &record);
    assert!(Arc::ptr_eq(&error, &result.record().unwrap_err()));
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_refreshes_changed_policy_without_repeating_native_retirement() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let verifier = fixture.verifier();
    let (entered, resume) = fixture.journal.pause_next_reader_evacuation_reply(false);
    let mut work = Box::pin(FleetReaderEvacuationPublication::publish(
        &capture,
        fixture.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    fixture.managers[0]
        .set_target(&fixture.target, 1, 0)
        .await
        .unwrap()
        .unwrap();
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    let original = result.record().unwrap();
    assert!(result.confirmed().is_err());
    let retired = fixture.original_row().await;
    let candidate = verifier
        .refresh(fixture.journal.as_ref(), original, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(candidate.record().desired_readers(), 0);
    assert!(candidate.pages().is_empty());
    assert_eq!(candidate.record().retired(), &retired);
    let updated = FleetReaderEvacuationPublication::publish_refreshed(
        &candidate,
        fixture.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let updated = updated.record().unwrap();
    verifier
        .recheck(fixture.journal.as_ref(), updated, deadline(), clock)
        .await
        .unwrap();
    assert_ne!(original.digest().unwrap(), updated.digest().unwrap());
    assert_eq!(fixture.latest().await, *updated);
    assert_eq!(fixture.stored(original.digest().unwrap()).await, *original);
    // Old exact replay cannot restore the superseded latest pointer or revision.
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let (_, pages) = capture.durable_record().unwrap();
    assert_eq!(
        fixture
            .journal
            .persist_reader_evacuation(capture.snapshot(), original, &pages, clock().unwrap())
            .await
            .unwrap(),
        *original
    );
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    assert_eq!(fixture.latest().await, *updated);
    assert!(
        verifier
            .recheck(fixture.journal.as_ref(), original, deadline(), clock)
            .await
            .is_err()
    );
    assert_eq!(fixture.original_row().await, retired);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_missing_ready_replacement_preserves_committed_history_and_blocks_refresh()
 {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let result = fixture.publish(&capture).await;
    let record = result.record().unwrap();
    let verifier = fixture.verifier();
    fixture.nodes[2].shutdown().await.unwrap();
    assert!(
        verifier
            .recheck(fixture.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert!(
        verifier
            .refresh(fixture.journal.as_ref(), record, deadline(), clock)
            .await
            .is_err()
    );
    assert_eq!(fixture.stored(record.digest().unwrap()).await, *record);
    assert_eq!(fixture.original_row().await, *record.retired());
    drop(capture);
    fixture.finish().await;
}
