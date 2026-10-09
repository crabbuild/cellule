//! Advertisement records: create, load, refresh, and validation.

use super::*;

#[test]
fn signed_advertisement_accepts_thirty_second_lifetime_and_rejects_longer() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let sign = |lifetime_ms| {
        NodeAdvertisement::sign(
            NodeId::from_bytes([1; 16]),
            SessionId::from_bytes([1; 16]),
            "https://node-1.internal:8789".into(),
            Digest::from_bytes([2; 32]),
            Digest::from_bytes([3; 32]),
            Digest::from_bytes([4; 32]),
            Digest::from_bytes([5; 32]),
            &key,
            1,
            NOW_MS,
            NOW_MS + lifetime_ms,
            vec![Digest::from_bytes([6; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity::default(),
        )
    };
    assert!(sign(30_000).is_ok());
    assert!(sign(30_001).is_err());
}

#[tokio::test]
async fn create_load_and_refresh_preserve_signed_boot_identity() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let directory = directory();
    let created = directory
        .create(advertisement(&key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    let adopted = directory
        .create(advertisement(&key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    assert_eq!(adopted.advertisement(), created.advertisement());
    let loaded = directory
        .load(SessionId::from_bytes([1; 16]), NOW_MS + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.advertisement(), created.advertisement());
    assert!(
        directory
            .is_live(SessionId::from_bytes([1; 16]), NOW_MS + 1)
            .await
            .unwrap()
    );
    assert!(
        !directory
            .is_live(SessionId::from_bytes([9; 16]), NOW_MS + 1)
            .await
            .unwrap()
    );

    let refreshed = directory
        .refresh(
            &created,
            advertisement(&key, 1, NOW_MS + 1_000),
            NOW_MS + 1_000,
        )
        .await
        .unwrap();
    assert_eq!(refreshed.advertisement().progress(), 1);
    assert_eq!(
        refreshed.advertisement().signature,
        created.advertisement().signature
    );
    let refreshed = directory
        .refresh(
            &refreshed,
            advertisement(&key, 2, NOW_MS + 2_000),
            NOW_MS + 2_000,
        )
        .await
        .unwrap();
    assert_eq!(refreshed.advertisement().progress(), 2);
}

#[tokio::test]
async fn invalid_signature_expiry_and_identity_change_fail_closed() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let directory = directory();
    assert!(
        NodeAdvertisement::sign(
            NodeId::from_bytes([1; 16]),
            SessionId::from_bytes([1; 16]),
            "https:///not-an-authority".into(),
            Digest::from_bytes([2; 32]),
            Digest::from_bytes([3; 32]),
            Digest::from_bytes([4; 32]),
            Digest::from_bytes([5; 32]),
            &key,
            1,
            NOW_MS,
            NOW_MS + 10_000,
            vec![Digest::from_bytes([6; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 1,
                free_disk_bytes: 1,
                job_credits: 1,
                ..NodeCapacity::default()
            },
        )
        .is_err()
    );
    let original = advertisement(&key, 1, NOW_MS);
    let mut tampered_capacity = original.clone();
    tampered_capacity.capacity.free_memory_bytes = tampered_capacity
        .capacity
        .free_memory_bytes
        .saturating_add(1);
    // Capacity is part of the signed heartbeat; changing it without a new
    // signature must fail closed. The optional placement block is authenticated
    // independently because it drives ownership placement.
    assert!(tampered_capacity.verify_signature().is_err());
    let signed = original
        .clone()
        .with_placement_capacity(
            NodePlacementCapacity {
                memory_capacity_bytes: 8_192,
                disk_capacity_bytes: 16_384,
                active_cells: 1,
                max_active_cells: 8,
                running_jobs: 1,
                job_capacity: 4,
                publication_backlog: 0,
                hydration_backlog: 0,
                primitive_backlog: 0,
            }
            .validated()
            .unwrap(),
            &key,
        )
        .unwrap();
    let mut tampered_placement = signed;
    tampered_placement
        .placement
        .as_mut()
        .expect("signed placement is present")
        .memory_capacity_bytes = 8_193;
    assert!(tampered_placement.verify_signature().is_err());
    let mut tampered = original.encode().unwrap();
    let endpoint_byte = tampered
        .windows(b"node-1".len())
        .position(|window| window == b"node-1")
        .unwrap()
        + b"node-".len();
    tampered[endpoint_byte] = b'2';
    assert!(NodeAdvertisement::decode_canonical(&tampered).is_err());

    let created = directory.create(original, NOW_MS).await.unwrap();
    assert!(
        directory
            .load(SessionId::from_bytes([1; 16]), NOW_MS + 10_000)
            .await
            .is_err()
    );
    assert!(
        !directory
            .is_live(SessionId::from_bytes([1; 16]), NOW_MS + 10_000)
            .await
            .unwrap()
    );

    let other_key = SigningKey::from_bytes(&[8; 32]);
    assert!(
        directory
            .refresh(
                &created,
                advertisement(&other_key, 2, NOW_MS + 1_000),
                NOW_MS + 1_000,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn refresh_accepts_new_signed_capacity_for_same_boot_session() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let directory = directory();
    let session = SessionId::from_bytes([1; 16]);
    let created = directory
        .create(advertisement_for(session, &key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    let next_capacity = NodeCapacity {
        free_memory_bytes: 900,
        free_disk_bytes: 1_800,
        follower_free_bytes: 1_700,
        follower_retained_bytes: 700,
        job_credits: 2,
        log_protocol: NODE_LOG_PROTOCOL_VERSION,
    };
    let next = advertisement_for_capacity(session, &key, 2, NOW_MS + 1_000, next_capacity);
    assert_ne!(next.signature, created.advertisement().signature);
    let refreshed = directory
        .refresh(&created, next, NOW_MS + 1_000)
        .await
        .unwrap();
    assert_eq!(refreshed.advertisement().capacity(), next_capacity);
    assert!(refreshed.advertisement().verify_signature().is_ok());
}

#[test]
fn canonical_advertisement_decode_verifies_each_immutable_signature_set_once() {
    for original in canonical_advertisements() {
        let bytes = original.encode().unwrap();
        let before = signature_passes();
        let decoded = NodeAdvertisement::decode_canonical(&bytes).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(signature_passes() - before, 1);
        assert_eq!(decoded.encode().unwrap(), bytes);
    }
}

pub(super) fn canonical_advertisements() -> [NodeAdvertisement; 3] {
    let key = SigningKey::from_bytes(&[7; 32]);
    let legacy = advertisement(&key, 1, NOW_MS);
    let placement = NodePlacementCapacity {
        memory_capacity_bytes: 2_000,
        disk_capacity_bytes: 4_000,
        active_cells: 1,
        max_active_cells: 8,
        running_jobs: 0,
        job_capacity: 3,
        publication_backlog: 0,
        hydration_backlog: 0,
        primitive_backlog: 0,
    };
    let bridge = legacy
        .clone()
        .with_placement_capacity(placement, &key)
        .unwrap();
    let operational = legacy
        .clone()
        .with_operational_placement(
            placement,
            NodeOperationalSample {
                mode: NodeMode::Draining,
                pressure: NodePressure::Critical,
                sequence: 1,
                observed_at_ms: NOW_MS,
            },
            &key,
        )
        .unwrap();
    [legacy, bridge, operational]
}

#[test]
fn canonical_advertisement_decode_keeps_both_signature_gates_and_original_errors() {
    for original in canonical_advertisements() {
        let bytes = original.encode().unwrap();
        let mut base: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        base["identity"]["signature"] = serde_json::Value::String("00".repeat(64));
        let before = signature_passes();
        assert!(matches!(
            NodeAdvertisement::decode_canonical(&serde_json::to_vec(&base).unwrap()),
            Err(Error::PeerSignature(_)),
        ));
        assert_eq!(signature_passes() - before, 1);
        if original.has_signed_placement() {
            let mut placement: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let signature = placement["placement_signature"].as_str().unwrap();
            let mut corrupt = signature.as_bytes().to_vec();
            corrupt[0] = if corrupt[0] == b'0' { b'1' } else { b'0' };
            placement["placement_signature"] =
                serde_json::Value::String(String::from_utf8(corrupt).unwrap());
            let before = signature_passes();
            assert!(matches!(
                NodeAdvertisement::decode_canonical(&serde_json::to_vec(&placement).unwrap()),
                Err(Error::PeerSignature(_)),
            ));
            assert_eq!(signature_passes() - before, 1);
        }
    }
}

#[test]
fn canonical_advertisement_decode_keeps_exact_byte_and_size_gates() {
    for original in canonical_advertisements() {
        let mut bytes = original.encode().unwrap();
        bytes.push(b' ');
        let before = signature_passes();
        assert!(matches!(
            NodeAdvertisement::decode_canonical(&bytes),
            Err(Error::Node("advertisement JSON is not canonical")),
        ));
        assert_eq!(signature_passes() - before, 1);
    }
    let before = signature_passes();
    assert!(matches!(
        NodeAdvertisement::decode_canonical(&vec![b' '; MAX_NODE_BYTES as usize + 1]),
        Err(Error::Node("advertisement exceeds 64 KiB")),
    ));
    assert_eq!(signature_passes(), before);
}
