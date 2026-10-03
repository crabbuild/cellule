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
