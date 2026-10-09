use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_cancelled_waiter_leaves_complete_original_commit_owned() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let (record, _) = capture.durable_record().unwrap();
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
    drop(work);
    let _ = resume.send(());
    let client = fixture.client().await;
    assert_eq!(
        client
            .load_reader_evacuation(scope(), record.digest().unwrap())
            .await
            .unwrap()
            .unwrap(),
        record
    );
    verifier
        .recheck(&client, &record, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(fixture.latest().await, record);
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_stale_registry_and_corrupt_pages_cannot_publish_a_candidate() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let (record, pages) = capture.durable_record().unwrap();
    let mut bytes = record.to_bytes().unwrap();
    let digest = record.pages()[0];
    let position = bytes
        .windows(32)
        .rposition(|bytes| bytes == digest.as_bytes())
        .unwrap();
    bytes[position] ^= 1;
    let changed = ReaderEvacuationRecord::from_bytes(&bytes).unwrap();
    assert!(
        fixture
            .journal
            .persist_reader_evacuation(capture.snapshot(), &changed, &pages, clock().unwrap())
            .await
            .is_err()
    );
    assert!(
        fixture
            .journal
            .load_reader_evacuation(scope(), changed.digest().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .set_scheduling(
            snapshot.registry(),
            !snapshot.registry().scheduling_enabled(),
        )
        .await
        .unwrap();
    assert!(
        FleetReaderEvacuationPublication::publish(
            &capture,
            fixture.journal.as_ref(),
            &fixture.verifier(),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    assert!(
        fixture
            .journal
            .load_reader_evacuation(scope(), record.digest().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.original_row().await, *record.retired());
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_policy_publication_rechecks_authority_after_suspended_native_status() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let result = fixture.publish(&capture).await;
    let record = result.record().unwrap();
    let verifier = fixture.verifier();
    let (entered, resume) = fixture.transport.pause_probe(1);
    let mut work = Box::pin(verifier.recheck(fixture.journal.as_ref(), record, deadline(), clock));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    fixture.handle.drain().await.unwrap();
    resume.send(()).unwrap();
    assert!(work.await.is_err());
    assert_eq!(fixture.stored(record.digest().unwrap()).await, *record);
    assert_eq!(fixture.original_row().await, *record.retired());
    drop(capture);
    fixture.finish().await;
}
