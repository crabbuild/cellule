use super::*;

#[tokio::test]
async fn session_closure_requires_exact_permanent_physical_fence_and_live_claimant() {
    let directory = directory();
    let leader = SessionId::from_bytes([11; 16]);
    let physical = NodeId::from_bytes([71; 16]);
    let claimant = SessionId::from_bytes([12; 16]);
    let key = SigningKey::from_bytes(&[9; 32]);
    directory
        .create(
            advertisement_for_node_capacity(
                physical,
                leader,
                &key,
                1,
                NOW_MS,
                NodeCapacity::default(),
            ),
            NOW_MS,
        )
        .await
        .unwrap();
    directory
        .create(
            advertisement_for(claimant, &key, 1, NOW_MS + 9_000),
            NOW_MS + 9_000,
        )
        .await
        .unwrap();
    for now in [NOW_MS + 9_001, NOW_MS + 10_001] {
        assert!(
            directory
                .closed_session(physical, leader, claimant, now)
                .await
                .is_err()
        );
    }
    directory
        .claim_expired(leader, claimant, NOW_MS + 10_001)
        .await
        .unwrap();
    let closure = directory
        .closed_session(physical, leader, claimant, NOW_MS + 10_002)
        .await
        .unwrap();
    assert_eq!(closure.node(), physical);
    assert_eq!(closure.session(), leader);
    assert_eq!(closure.expires_at_ms(), NOW_MS + 10_000);
    assert_eq!(closure.retired_at_ms(), NOW_MS + 10_001);
    assert!(closure.log().is_none());
    assert_eq!(
        directory
            .fenced_session(physical, leader, claimant, NOW_MS + 10_002)
            .await
            .unwrap(),
        closure.fence()
    );
    assert!(
        directory
            .closed_session(node(leader), leader, claimant, NOW_MS + 10_002)
            .await
            .is_err()
    );
    assert!(
        directory
            .closed_session(
                physical,
                SessionId::from_bytes([90; 16]),
                claimant,
                NOW_MS + 10_002
            )
            .await
            .is_err()
    );
    assert!(
        directory
            .closed_session(physical, leader, leader, NOW_MS + 10_002)
            .await
            .is_err()
    );
    assert!(
        directory
            .closed_session(physical, leader, claimant, NOW_MS + 19_001)
            .await
            .is_err()
    );
    // Claim renewal changes no original immutable closure field.
    let fenced = directory
        .claim_expired(leader, claimant, NOW_MS + 10_003)
        .await
        .unwrap();
    directory
        .refresh_recovery_claim(&fenced, NOW_MS + 10_004)
        .await
        .unwrap();
    assert_eq!(
        directory
            .closed_session(physical, leader, claimant, NOW_MS + 10_005)
            .await
            .unwrap(),
        closure
    );
}

#[tokio::test]
async fn permanent_session_fence_precedes_active_recovery_and_survives_claim_adoption() {
    let directory = directory();
    let key = SigningKey::from_bytes(&[7; 32]);
    let leader = SessionId::from_bytes([1; 16]);
    let claimant = SessionId::from_bytes([2; 16]);
    let original = directory
        .create(advertisement_for(leader, &key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    directory
        .create(
            advertisement_for(claimant, &key, 1, NOW_MS + 9_000),
            NOW_MS + 9_000,
        )
        .await
        .unwrap();
    let enrolled = directory
        .recruit_log(&original, 4, 1, 2, NOW_MS + 9_001)
        .await
        .unwrap();
    directory
        .activate_log(&enrolled, NOW_MS + 9_002)
        .await
        .unwrap();
    for now in [NOW_MS + 9_003, NOW_MS + 10_001] {
        assert!(matches!(
            directory
                .fenced_session(node(leader), leader, claimant, now)
                .await,
            Err(Error::Control("node session has no permanent fence"))
        ));
    }
    let fenced = directory
        .claim_expired(leader, claimant, NOW_MS + 10_001)
        .await
        .unwrap();
    assert_eq!(fenced.log().unwrap().phase(), NodeLogPhase::Recovering);
    assert!(fenced.log().unwrap().active());
    assert!(matches!(
        fenced.direct_takeover(),
        Err(Error::PendingPublication)
    ));
    let fence = directory
        .fenced_session(node(leader), leader, claimant, NOW_MS + 10_002)
        .await
        .unwrap();
    assert_eq!(fence.node(), node(leader));
    assert_eq!(fence.session(), leader);
    assert_eq!(fence.expires_at_ms(), NOW_MS + 10_000);
    assert_eq!(fence.retired_at_ms(), NOW_MS + 10_001);
    assert!(matches!(
        directory
            .closed_session(node(leader), leader, claimant, NOW_MS + 10_002)
            .await,
        Err(Error::Control("node session log retirement is incomplete"))
    ));
    for (physical, session, sender, now) in [
        (node(claimant), leader, claimant, NOW_MS + 10_002),
        (
            node(leader),
            SessionId::from_bytes([90; 16]),
            claimant,
            NOW_MS + 10_002,
        ),
        (node(leader), leader, leader, NOW_MS + 10_002),
        (node(leader), leader, claimant, NOW_MS + 19_001),
        (node(leader), leader, claimant, -1),
    ] {
        assert!(
            directory
                .fenced_session(physical, session, sender, now)
                .await
                .is_err()
        );
    }
    let replacement = SessionId::from_bytes([3; 16]);
    directory
        .create(
            advertisement_for(replacement, &key, 1, NOW_MS + 40_001),
            NOW_MS + 40_001,
        )
        .await
        .unwrap();
    let adopted = directory
        .claim_expired(leader, replacement, NOW_MS + 40_002)
        .await
        .unwrap();
    assert!(adopted.claim_generation() > fenced.claim_generation());
    assert_eq!(
        directory
            .fenced_session(node(leader), leader, replacement, NOW_MS + 40_003)
            .await
            .unwrap(),
        fence
    );
    // Authority-only stage fixture: real overlay/bundle pinning is qualified in
    // the public lifecycle suite. The permanent fence contains no manifest.
    directory
        .seal_recovery(
            &adopted,
            Some(Digest::from_bytes([81; 32])),
            NOW_MS + 40_003,
        )
        .await
        .unwrap();
    assert_eq!(
        directory
            .fenced_session(node(leader), leader, replacement, NOW_MS + 40_004)
            .await
            .unwrap(),
        fence
    );
    assert!(matches!(
        directory
            .closed_session(node(leader), leader, replacement, NOW_MS + 40_004)
            .await,
        Err(Error::Control("node session log retirement is incomplete"))
    ));
}
