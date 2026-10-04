//! Native Effect completion and exact inbox recovery during maintenance.

use super::*;
use cellule_runtime::Error;
use cellule_runtime::cell::actor::{CellInventoryEntry, MaintenanceCellRelease};
use cellule_runtime::control::ControlState;
use cellule_runtime::primitives::effects::EffectState;

fn peer(fixture: &Fixture, handle: cellule_runtime::cell::actor::CellHandle) -> EffectPeerClient {
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([42; 16]),
        fixture.registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[43; 32]),
    ));
    EffectPeerClient::new(
        signer.clone(),
        PeerPrincipal {
            issuer: "crab-runtime:test".into(),
            subject: "source-session".into(),
            actions: vec!["repository.issue.create".into()],
        },
        Arc::new(LoopbackRoundTrip {
            verifier: Arc::new(PeerVerifier::new(
                SessionId::from_bytes([42; 16]),
                fixture.registry.release_digest(),
                signer.verifying_key(),
            )),
            dispatcher: Arc::new(PeerDispatcher::new(
                fixture.registry.clone(),
                Arc::new(LocalResolver {
                    target: fixture.target.clone(),
                    handle,
                }),
                Arc::new(RepositoryAuthorizer),
            )),
        }),
    )
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

async fn maintenance_effect_movement(drop_waiter: bool, expire: bool) {
    let fixture = fixture().await;
    let runtime = fixture.runtime.as_ref().unwrap();
    let client = CellClient::local(fixture.registry.clone(), fixture.handle().clone());
    let mut encoder = BoundedEncoder::new(64).unwrap();
    b"leased-effect".to_vec().encode(&mut encoder).unwrap();
    let prepared = client
        .prepare_command::<EmitEffectComment>(
            &fixture.target,
            mutation_identity(70),
            encoder.finish(),
        )
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let emitted = prepared.execute().await.unwrap();
    let source = EffectSource::<RepositoryModule>::new(client.clone(), fixture.target.clone());
    let claimed = source
        .claim(
            mutation_identity(71),
            EffectClaimRequest {
                limit: 1,
                lease_ms: if expire { 5_000 } else { 30_000 },
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.output.len(), 1);
    let lease = claimed.output[0].clone();
    // A destination publication can precede a lost source acknowledgement.
    let delivered = peer(&fixture, fixture.handle().clone())
        .deliver(&lease, now_ms())
        .await
        .unwrap();
    let mut encoder = BoundedEncoder::new(64).unwrap();
    b"unclaimed-effect".to_vec().encode(&mut encoder).unwrap();
    client
        .command::<EmitEffectComment>(&fixture.target, mutation_identity(72), encoder.finish())
        .await
        .unwrap();
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("missing owner")
    };
    let generation = owner.generation;
    let epoch = owner.position.as_ref().unwrap().epoch;
    drop(page);
    let mut moving = {
        let runtime = runtime.clone();
        let cell = fixture.target.cell_id();
        let session = fixture.session;
        let incarnation = fixture.incarnation;
        tokio::spawn(async move {
            runtime
                .release_maintenance_cell_at(
                    cell,
                    session,
                    generation,
                    incarnation,
                    epoch,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let page = runtime.fleet_cells_page(None, 128).await.unwrap();
            if let CellInventoryEntry::Owned(owner) = &page.entries()[0]
                && owner.quiescing
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut moving)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .state,
        ControlState::Serving
    );
    assert!(
        source
            .validate(vec![lease.clone()], claimed.receipt)
            .await
            .unwrap()
            .output
    );
    let mut wrong = lease.clone();
    wrong.token = [99; 16];
    assert!(
        !source
            .validate(vec![wrong], claimed.receipt)
            .await
            .unwrap()
            .output
    );
    assert!(matches!(
        source
            .claim(
                mutation_identity(73),
                EffectClaimRequest {
                    limit: 1,
                    lease_ms: 5_000
                }
            )
            .await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        source.status(lease.effect_id, None).await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    if drop_waiter {
        moving.abort();
    }
    if !expire {
        assert_eq!(
            source
                .ack(
                    mutation_identity(74),
                    lease.clone(),
                    b"published destination".to_vec()
                )
                .await
                .unwrap()
                .output,
            EffectLeaseOutcome::Delivered
        );
    }
    let release = if drop_waiter {
        assert!(moving.await.unwrap_err().is_cancelled());
        None
    } else {
        let MaintenanceCellRelease::Released(position) = moving.await.unwrap().unwrap() else {
            panic!("effect release refused")
        };
        Some(position)
    };
    let idle = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let observed = fixture
                .authority
                .load(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if observed.value().state == ControlState::Idle
                && runtime.unreleased_cell_count().await.unwrap() == 0
            {
                return observed;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if let Some(position) = release {
        assert_eq!(idle.value().root.as_ref(), Some(&position.root));
        assert_eq!(position.epoch, epoch);
    }
    let session = SessionId::from_bytes([80; 16]);
    let destination =
        CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20, session).unwrap();
    let restored = destination
        .acquire_idle_restored(
            fixture.proof.clone(),
            fixture.replica.clone(),
            fixture.authority.clone(),
            idle,
            fixture
                ._directory
                .path()
                .join("maintenance-receiver.sqlite"),
            Owner {
                session,
                endpoint: "https://receiver.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let client = CellClient::local(fixture.registry.clone(), restored.clone());
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(), Resolution::Committed(outcome)
        if outcome.commit_sequence() == emitted.receipt.commit_sequence)
    );
    let source = EffectSource::<RepositoryModule>::new(client.clone(), fixture.target.clone());
    assert!(
        !source
            .validate(vec![lease.clone()], claimed.receipt)
            .await
            .unwrap()
            .output
    );
    assert!(
        matches!(source.ack(mutation_identity(75), lease.clone(), b"late result".to_vec()).await,
        Err(InvocationError::Rejected(outcome)) if outcome.output == EffectLeaseOutcome::LeaseLost)
    );
    let peer = peer(&fixture, restored.clone());
    // The migrated inbox answers the identical destination receipt, even when
    // an expired source lease will be reclaimed and delivered again.
    assert_eq!(
        peer.resolve(&lease, now_ms()).await.unwrap(),
        Resolution::Committed(delivered.clone())
    );
    if expire {
        let mut reclaimed = source
            .claim(
                mutation_identity(76),
                EffectClaimRequest {
                    limit: 2,
                    lease_ms: 30_000,
                },
            )
            .await
            .unwrap();
        // Expiry uses the existing retry backoff. The already-ready survivor
        // is claimable first; the old lease is retained as Ready, not erased.
        assert_eq!(reclaimed.output.len(), 1);
        assert_ne!(reclaimed.output[0].effect_id, lease.effect_id);
        assert_eq!(
            source
                .status(lease.effect_id, Some(reclaimed.receipt))
                .await
                .unwrap()
                .output
                .unwrap()
                .state,
            EffectState::Ready
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        let retried = source
            .claim(
                mutation_identity(79),
                EffectClaimRequest {
                    limit: 1,
                    lease_ms: 30_000,
                },
            )
            .await
            .unwrap();
        assert_eq!(retried.output.len(), 1);
        reclaimed.receipt = retried.receipt;
        reclaimed.output.extend(retried.output);
        assert_eq!(reclaimed.output.len(), 2);
        let retry = reclaimed
            .output
            .iter()
            .find(|claim| claim.effect_id == lease.effect_id)
            .unwrap();
        assert_eq!(retry.attempt, 2);
        assert_ne!(retry.token, lease.token);
        assert_eq!(retry.operation_digest, lease.operation_digest);
        assert!(
            source
                .validate(reclaimed.output.clone(), reclaimed.receipt)
                .await
                .unwrap()
                .output
        );
        for claim in reclaimed.output {
            let outcome = peer.deliver(&claim, now_ms()).await.unwrap();
            if claim.effect_id == lease.effect_id {
                assert_eq!(outcome, delivered);
            }
            source
                .ack(
                    mutation_identity(if claim.effect_id == lease.effect_id {
                        77
                    } else {
                        78
                    }),
                    claim,
                    b"delivered".to_vec(),
                )
                .await
                .unwrap();
        }
    } else {
        let outcome = fixture
            .registry
            .run_effect_once(client.clone(), fixture.target.clone(), peer, 5_000)
            .await
            .unwrap();
        assert!(matches!(outcome, EffectRunOutcome::Delivered { .. }));
    }
    assert_eq!(
        source
            .status(lease.effect_id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state,
        EffectState::Delivered
    );
    assert_eq!(
        client
            .query::<CountComments>(&fixture.target, None, ())
            .await
            .unwrap()
            .output,
        2
    );
    restored.drain().await.unwrap();
    destination.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(destination.stats().retained_bytes(), 0);
    assert_eq!(runtime.stats().retained_bytes(), 0);
}

#[tokio::test]
async fn maintenance_waits_for_effect_ack_and_resumes_native_delivery() {
    maintenance_effect_movement(false, false).await;
}

#[tokio::test]
async fn dropped_maintenance_waiter_preserves_effect_completion_and_inbox() {
    maintenance_effect_movement(true, false).await;
}

#[tokio::test]
async fn maintenance_preserves_expired_effect_for_normal_reclaim_and_inbox_dedup() {
    maintenance_effect_movement(false, true).await;
}
