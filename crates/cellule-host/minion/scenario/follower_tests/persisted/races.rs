use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_fenced_native_owner_cannot_hide_behind_live_directory() {
    let (fixture, capture, policy) = setup().await;
    let result = publish(&fixture, &capture, policy).await;
    let record = result.record().unwrap();
    result.confirmed().unwrap();
    let transport = Arc::new(Snapshots::new(&fixture));
    let verifier =
        FleetFollowerEvacuationVerifier::new(fixture.native.directory.clone(), transport.clone());
    let (entered, resume) = transport.pause_next();
    let mut work =
        Box::pin(verifier.recheck(fixture.native.journal.as_ref(), record, deadline(), clock));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    fixture.boots[0].guard.as_ref().unwrap().fence();
    assert!(
        fixture
            .native
            .directory
            .load_if_live(session(0), clock().unwrap())
            .await
            .unwrap()
            .is_some()
    );
    resume.send(()).unwrap();
    assert!(work.await.is_err());
    assert_eq!(stored(&fixture, record.digest().unwrap()).await, *record);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_adopts_lost_reply_and_preserves_original_error() {
    let (fixture, capture, policy) = setup().await;
    let (record, verifier) = (capture.durable_record(policy).unwrap(), verifier(&fixture));
    let (entered, resume) = fixture
        .native
        .journal
        .pause_next_follower_evacuation_reply(true);
    let mut work = Box::pin(FleetFollowerEvacuationPublication::publish(
        &capture,
        policy,
        fixture.native.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    let error = result.record().err().unwrap();
    assert!(Arc::ptr_eq(&error, &result.record().err().unwrap()));
    let Error::Facility { source, .. } = &*error else {
        panic!("source error flattened")
    };
    assert!(
        source
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .to_string()
            .contains("after durable commit")
    );
    assert_eq!(stored(&fixture, record.digest().unwrap()).await, record);
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let adopted = publish(&fixture, &capture, policy).await;
    adopted.confirmed().unwrap();
    assert_eq!(adopted.record().unwrap(), &record);
    assert_eq!(
        fixture.native.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_cancelled_waiter_keeps_complete_durable_history() {
    let (fixture, capture, policy) = setup().await;
    let record = capture.durable_record(policy).unwrap();
    let verifier = verifier(&fixture);
    let (entered, resume) = fixture
        .native
        .journal
        .pause_next_follower_evacuation_reply(false);
    let mut work = Box::pin(FleetFollowerEvacuationPublication::publish(
        &capture,
        policy,
        fixture.native.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    drop(work);
    let _ = resume.send(());
    let client = client(&fixture).await;
    assert_eq!(
        client
            .load_follower_evacuation(scope(), record.digest().unwrap())
            .await
            .unwrap()
            .unwrap(),
        record
    );
    verifier
        .recheck(&client, &record, deadline(), clock)
        .await
        .unwrap();
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_policy_change_after_commit_prevents_final_confirmation() {
    let (fixture, capture, policy) = setup().await;
    let verifier = verifier(&fixture);
    let (entered, resume) = fixture
        .native
        .journal
        .pause_next_follower_evacuation_reply(false);
    let mut work = Box::pin(FleetFollowerEvacuationPublication::publish(
        &capture,
        policy,
        fixture.native.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    ));
    tokio::select! {result=&mut work=>panic!("unexpected completion {}",result.is_ok()),result=entered=>result.unwrap()}
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .set_follower_replacement_policy(
            &snapshot,
            FollowerReplacementPolicy::new(scope(), 2, 1).unwrap(),
            clock().unwrap(),
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    let result = work.await.unwrap();
    let record = result.record().unwrap();
    assert!(result.confirmed().is_err());
    assert_eq!(stored(&fixture, record.digest().unwrap()).await, *record);
    let fresh = verifier
        .refresh(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let updated = FleetFollowerEvacuationPublication::publish_refreshed(
        &fresh,
        fixture.native.journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    updated.confirmed().unwrap();
    assert_eq!(updated.record().unwrap().retired(), record.retired());
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_policy_publication_policy_cas_race_has_one_registry_mutation() {
    let (fixture, capture, policy) = setup().await;
    let client = client(&fixture).await;
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let next = FollowerReplacementPolicy::new(scope(), policy.revision() + 1, 1).unwrap();
    let (one, two) = tokio::join!(
        fixture
            .native
            .journal
            .set_follower_replacement_policy(&snapshot, next, clock().unwrap()),
        client.set_follower_replacement_policy(&snapshot, next, clock().unwrap())
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let after = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        after.registry().revision(),
        snapshot.registry().revision() + 1
    );
    assert_eq!(
        client.follower_replacement_policy(&after).await.unwrap(),
        Some(next)
    );
    assert!(
        client
            .set_follower_replacement_policy(&after, next, clock().unwrap())
            .await
            .is_err()
    );
    assert_eq!(client.load_snapshot(scope()).await.unwrap(), after);
    client.close().await.unwrap();
    drop(capture);
    fixture.finish().await;
}
