use super::*;

async fn authority() -> Arc<Authority> {
    let root = PathBuf::from(std::env::var_os("CELLULE_TEST_FLEET_TLS").unwrap());
    let tls = Arc::new(
        LoadedPeerTls::load(
            &root.join("node-0.crt"),
            &root.join("node-0.key"),
            &root.join("ca.crt"),
            "localhost",
        )
        .unwrap(),
    );
    let code = Digest::from_bytes([7; 32]);
    let directory = NodeDirectory::new(
        cellule_ltx::CellStorageLayout::new(
            cellule_store::Store::new(Arc::new(object_store::memory::InMemory::new())),
            object_store::path::Path::from("renewal-test"),
            [9; 16],
        ),
        tls.fleet(),
        code,
        code,
    );
    let enrollment = Enrollment::start(
        directory,
        tls,
        0,
        SessionId::from_bytes([11; 16]),
        "https://localhost:8081".into(),
        code,
        None,
    )
    .await
    .unwrap();
    enrollment.stop.send(true).unwrap();
    enrollment.heartbeat.await.unwrap().unwrap();
    // A shorter local deadline is conservative relative to the signed
    // advertisement. It reproduces a nearly exhausted renewal window.
    let mut authority = Arc::try_unwrap(enrollment.authority).ok().unwrap();
    let now = clock().unwrap();
    authority.lease = NodeLeaseGuard::new(now, now + 1_000).unwrap();
    Arc::new(authority)
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn queued_authority_work_renews_before_the_original_local_deadline() {
    let authority = authority().await;
    let held = authority.state.lock().await;
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let authority = Arc::clone(&authority);
        tasks.push(Box::pin(async move {
            let _state = authority.live_state().await?;
            tokio::time::sleep(Duration::from_millis(75)).await;
            authority.lease.check()
        }));
    }
    for task in &mut tasks {
        assert!(futures_util::poll!(task.as_mut()).is_pending());
    }
    drop(held);
    for result in futures_util::future::join_all(tasks).await {
        result.unwrap();
    }
    let state = authority.state.lock().await;
    assert_eq!(state.observed.advertisement().progress(), 2);
    assert!(authority.lease.remaining() > Duration::from_secs(20));
    authority.lease.fence();
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn heartbeat_fenced_while_queued_does_not_publish_a_new_advertisement() {
    let authority = authority().await;
    let state = authority.state.lock().await;
    let session = state.observed.advertisement().session();
    let heartbeat = authority.refresh();
    tokio::pin!(heartbeat);
    assert!(futures_util::poll!(heartbeat.as_mut()).is_pending());
    authority.lease.fence();
    drop(state);
    assert!(matches!(heartbeat.await, Err(Error::Fenced)));
    let observed = authority
        .directory
        .load_if_live(session, clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.advertisement().progress(), 1);
}
