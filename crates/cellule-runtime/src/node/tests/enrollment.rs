//! Prepared enrollment uses the canonical selector and immutable CAS inputs.
use super::*;

async fn ensemble() -> (NodeDirectory, VersionedNodeAdvertisement, SigningKey) {
    let directory = directory();
    let key = SigningKey::from_bytes(&[7; 32]);
    let source = directory
        .create(advertisement(&key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    for member in [
        SessionId::from_bytes([2; 16]),
        SessionId::from_bytes([3; 16]),
    ] {
        directory
            .create(advertisement_for(member, &key, 1, NOW_MS), NOW_MS)
            .await
            .unwrap();
    }
    (directory, source, key)
}

#[tokio::test]
async fn preparation_is_read_only_and_rebases_heartbeats_without_reselecting_members() {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.source(), source.advertisement());
    assert_eq!(
        prepared.log().members(),
        directory
            .select_log_members(source.advertisement().session(), 1, NOW_MS + 1, 3)
            .await
            .unwrap()
    );
    assert_eq!(prepared.followers().len(), 2);
    for (member, signed) in prepared.log().members().iter().zip(prepared.followers()) {
        assert_eq!(*member, signed.node());
        signed.verify_signature().unwrap();
    }
    assert!(
        directory
            .load(source.advertisement().session(), NOW_MS + 1)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
    directory
        .refresh(&source, advertisement(&key, 2, NOW_MS + 100), NOW_MS + 100)
        .await
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 101)
        .await
        .unwrap();
    assert_eq!(attempt.observed().advertisement().progress(), 2);
    assert_eq!(attempt.observed().advertisement().generation(), 2);
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 101)
            .await
            .unwrap()
            .is_none()
    );
    let proof = directory
        .clone()
        .commit_log_enrollment(&attempt, NOW_MS + 102)
        .await
        .unwrap();
    assert_eq!(proof.enrollment().advertisement().generation(), 3);
    assert_eq!(
        proof.enrollment().advertisement().log(),
        Some(prepared.log())
    );
    assert_eq!(proof.prepared().followers(), prepared.followers());
}

#[tokio::test]
async fn an_attempt_keeps_its_original_cas_version_when_heartbeat_races_dispatch() {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
        .await
        .unwrap();
    directory
        .refresh(&source, advertisement(&key, 2, NOW_MS + 100), NOW_MS + 100)
        .await
        .unwrap();
    assert!(
        directory
            .commit_log_enrollment(&attempt, NOW_MS + 101)
            .await
            .is_err()
    );
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 101)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        directory
            .load(source.advertisement().session(), NOW_MS + 101)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
}

#[tokio::test]
async fn enrollment_inspection_reconciles_activation_coverage_and_heartbeat_then_keeps_absence_unknown()
 {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
        .await
        .unwrap();
    let enrolled = directory
        .commit_log_enrollment(&attempt, NOW_MS + 3)
        .await
        .unwrap();
    let active = directory
        .activate_log(enrolled.enrollment(), NOW_MS + 4)
        .await
        .unwrap();
    let covered = directory
        .advance_log_coverage(&active, 1, NOW_MS + 5)
        .await
        .unwrap();
    let refreshed = directory
        .refresh(&covered, advertisement(&key, 2, NOW_MS + 100), NOW_MS + 100)
        .await
        .unwrap();
    let proof = directory
        .inspect_log_enrollment(&attempt, NOW_MS + 101)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        proof.enrollment().advertisement(),
        refreshed.advertisement()
    );
    let gate = crate::node::log::DurabilityGate::new(
        source.advertisement().session(),
        source.advertisement().node(),
        7,
        prepared.log().members().iter().copied(),
    )
    .unwrap();
    let ticket = gate.issue(1).unwrap();
    gate.prove_object(ticket).unwrap();
    let closed = directory
        .close_log(&refreshed, &gate.begin_rotation().unwrap(), NOW_MS + 102)
        .await
        .unwrap();
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 103)
            .await
            .unwrap()
            .is_none()
    );
    directory.withdraw(&closed, NOW_MS + 104).await.unwrap();
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 105)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn prepared_enrollment_cannot_cross_directory_instances_with_identical_fleet_scope() {
    let (directory, source, _) = ensemble().await;
    let foreign = NodeDirectory::new(
        directory.layout.clone(),
        directory.fleet,
        directory.image,
        directory.release,
    );
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
        .await
        .unwrap();
    assert!(
        foreign
            .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
            .await
            .is_err()
    );
    assert!(
        foreign
            .commit_log_enrollment(&attempt, NOW_MS + 2)
            .await
            .is_err()
    );
    assert!(
        foreign
            .inspect_log_enrollment(&attempt, NOW_MS + 2)
            .await
            .is_err()
    );
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 2)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn prepared_enrollment_rejects_expired_followers_even_when_new_boots_share_physical_ids() {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let later = NOW_MS + 10_001;
    directory
        .refresh(&source, advertisement(&key, 2, later), later)
        .await
        .unwrap();
    for (index, original) in prepared.followers().iter().enumerate() {
        directory
            .create(
                advertisement_for_node_capacity(
                    original.node(),
                    SessionId::from_bytes([20 + index as u8; 16]),
                    &key,
                    1,
                    later,
                    original.capacity(),
                ),
                later,
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        directory
            .prepare_log_enrollment_attempt(&prepared, later)
            .await,
        Err(Error::Node("prepared node-log follower is not live"))
    ));
    assert!(
        directory
            .load(source.advertisement().session(), later)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .log()
            .is_none()
    );
}

#[tokio::test]
async fn prepared_enrollment_revalidates_selected_capacity_without_substituting_other_members() {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let original = &prepared.followers()[0];
    let observed = directory
        .load(original.session(), NOW_MS + 2)
        .await
        .unwrap()
        .unwrap();
    let mut capacity = original.capacity();
    capacity.follower_free_bytes = 0;
    directory
        .refresh(
            &observed,
            advertisement_for_node_capacity(
                original.node(),
                original.session(),
                &key,
                2,
                NOW_MS + 100,
                capacity,
            ),
            NOW_MS + 100,
        )
        .await
        .unwrap();
    assert!(matches!(
        directory
            .prepare_log_enrollment_attempt(&prepared, NOW_MS + 101)
            .await,
        Err(Error::Node(
            "prepared node-log follower cannot receive enrollment"
        ))
    ));
    assert_eq!(prepared.followers()[0], *original);
}

#[tokio::test]
async fn selected_receivers_revalidate_cordon_and_pressure_while_source_can_evacuate() {
    for (mode, pressure) in [
        (NodeMode::Cordoned, NodePressure::Normal),
        (NodeMode::Draining, NodePressure::Normal),
        (NodeMode::Active, NodePressure::Critical),
    ] {
        let (directory, source, key) = ensemble().await;
        let prepared = directory
            .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        let signed = |node, session, capacity, mode, pressure| {
            advertisement_for_node_capacity(node, session, &key, 2, NOW_MS + 100, capacity)
                .with_operational_placement(
                    NodePlacementCapacity {
                        memory_capacity_bytes: 10_000,
                        disk_capacity_bytes: 20_000,
                        max_active_cells: 10,
                        job_capacity: 10,
                        ..NodePlacementCapacity::default()
                    },
                    NodeOperationalSample {
                        mode,
                        pressure,
                        sequence: 2,
                        observed_at_ms: NOW_MS + 100,
                    },
                    &key,
                )
                .unwrap()
        };
        directory
            .refresh(
                &source,
                signed(
                    source.advertisement().node(),
                    source.advertisement().session(),
                    source.advertisement().capacity(),
                    NodeMode::Cordoned,
                    NodePressure::Normal,
                ),
                NOW_MS + 100,
            )
            .await
            .unwrap();
        // Outbound replacement of existing obligations must remain available.
        let attempt = directory
            .prepare_log_enrollment_attempt(&prepared, NOW_MS + 101)
            .await
            .unwrap();
        let receiver = &prepared.followers()[0];
        let observed = directory
            .load(receiver.session(), NOW_MS + 101)
            .await
            .unwrap()
            .unwrap();
        directory
            .refresh(
                &observed,
                signed(
                    receiver.node(),
                    receiver.session(),
                    receiver.capacity(),
                    mode,
                    pressure,
                ),
                NOW_MS + 101,
            )
            .await
            .unwrap();
        assert!(matches!(
            directory
                .prepare_log_enrollment_attempt(&prepared, NOW_MS + 102)
                .await,
            Err(Error::Node(
                "prepared node-log follower cannot receive enrollment"
            ))
        ));
        assert!(
            directory
                .inspect_log_enrollment(&attempt, NOW_MS + 102)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn exact_boot_revalidation_rejects_replaced_signers_even_with_equal_node_and_session_ids() {
    for replace_source in [false, true] {
        let (directory, source, _) = ensemble().await;
        let prepared = directory
            .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        let original = if replace_source {
            prepared.source()
        } else {
            &prepared.followers()[0]
        };
        let observed = directory
            .load(original.session(), NOW_MS + 2)
            .await
            .unwrap()
            .unwrap();
        let replacement = advertisement_for_node_capacity(
            original.node(),
            original.session(),
            &SigningKey::from_bytes(&[88; 32]),
            2,
            NOW_MS + 100,
            original.capacity(),
        );
        // Deliberately bypass the canonical refresh contract to simulate a
        // backing-store replacement. The replacement is validly signed itself.
        directory
            .layout
            .store()
            .update(
                &directory.layout.node_path(original.session().as_bytes()),
                Bytes::from(replacement.encode().unwrap()),
                observed.token,
            )
            .await
            .unwrap();
        let result = directory
            .prepare_log_enrollment_attempt(&prepared, NOW_MS + 101)
            .await;
        let expected = if replace_source {
            "node-log leader boot differs from preparation"
        } else {
            "node-log follower boot differs from preparation"
        };
        assert!(matches!(result, Err(Error::Node(message)) if message == expected));
    }
}

#[tokio::test]
async fn refusal_fence_and_enrollment_compete_on_the_same_exact_source_version() {
    for enrollment_wins in [false, true] {
        let (directory, source, _) = ensemble().await;
        let prepared = directory
            .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        let attempt = directory
            .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
            .await
            .unwrap();
        if enrollment_wins {
            directory
                .commit_log_enrollment(&attempt, NOW_MS + 3)
                .await
                .unwrap();
            assert!(
                directory
                    .fence_log_enrollment(&attempt, NOW_MS + 4)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .inspect_log_enrollment(&attempt, NOW_MS + 4)
                    .await
                    .unwrap()
                    .is_some()
            );
        } else {
            let refusal = directory
                .fence_log_enrollment(&attempt, NOW_MS + 3)
                .await
                .unwrap();
            assert_eq!(refusal.prepared().log(), prepared.log());
            assert_eq!(refusal.refusal().advertisement().generation(), 2);
            assert!(refusal.refusal().advertisement().log().is_none());
            // Delayed dispatch and duplicated fence requests keep the same token.
            assert!(
                directory
                    .commit_log_enrollment(&attempt, NOW_MS + 4)
                    .await
                    .is_err()
            );
            let duplicate = directory
                .fence_log_enrollment(&attempt, NOW_MS + 4)
                .await
                .unwrap();
            assert_eq!(
                duplicate.refusal().advertisement(),
                refusal.refusal().advertisement()
            );
            assert!(
                directory
                    .inspect_log_enrollment(&attempt, NOW_MS + 4)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test]
async fn newer_empty_source_does_not_authorize_a_rebased_refusal_fence() {
    let (directory, source, key) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
        .await
        .unwrap();
    let refreshed = directory
        .refresh(&source, advertisement(&key, 2, NOW_MS + 100), NOW_MS + 100)
        .await
        .unwrap();
    assert!(
        directory
            .fence_log_enrollment(&attempt, NOW_MS + 101)
            .await
            .is_err()
    );
    let current = directory
        .load(source.advertisement().session(), NOW_MS + 101)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.advertisement(), refreshed.advertisement());
    assert!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 101)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn racing_refusal_and_enrollment_have_exactly_one_conditional_winner() {
    let (directory, source, _) = ensemble().await;
    let prepared = directory
        .prepare_log_enrollment(&source, 7, 1, 3, NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    let attempt = directory
        .prepare_log_enrollment_attempt(&prepared, NOW_MS + 2)
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let enroll = {
        let directory = directory.clone();
        let attempt = attempt.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            directory.commit_log_enrollment(&attempt, NOW_MS + 3).await
        })
    };
    let refuse = {
        let directory = directory.clone();
        let attempt = attempt.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            directory.fence_log_enrollment(&attempt, NOW_MS + 3).await
        })
    };
    barrier.wait().await;
    let enrolled = enroll.await.unwrap();
    let refused = refuse.await.unwrap();
    assert_ne!(enrolled.is_ok(), refused.is_ok());
    assert_eq!(
        directory
            .inspect_log_enrollment(&attempt, NOW_MS + 4)
            .await
            .unwrap()
            .is_some(),
        enrolled.is_ok()
    );
    let current = directory
        .load(source.advertisement().session(), NOW_MS + 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.advertisement().generation(), 2);
}
