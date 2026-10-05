//! Fresh directory reads authenticate immutable bytes once, including scans.
use super::*;

#[tokio::test]
async fn shared_follower_windows_authenticate_each_fresh_record_once() {
    let requests = (9..=12)
        .map(|n| (NodeId::from_bytes([n; 16]), None))
        .collect::<Vec<_>>();
    for original in records::canonical_advertisements() {
        let directory = directory();
        directory.create(original.clone(), NOW_MS).await.unwrap();
        let before = signature_passes();
        let pages = directory
            .follower_logs_pages(&requests, 32, NOW_MS + 1)
            .await
            .unwrap();
        assert_eq!(signature_passes() - before, 1);
        assert_eq!(pages.len(), requests.len());
        for (page, (member, _)) in pages.iter().zip(&requests) {
            assert_eq!(page.member(), *member);
            assert_eq!(page.total_logs(), 0);
            assert!(page.entries().is_empty());
            assert!(page.next().is_none());
        }
        let before = signature_passes();
        directory
            .follower_logs_pages(&requests, 32, NOW_MS + 2)
            .await
            .unwrap();
        assert_eq!(signature_passes() - before, 1);
    }
}

#[tokio::test]
async fn canonical_producers_verify_once_before_creating_or_refreshing_bytes() {
    let key = SigningKey::from_bytes(&[7; 32]);
    for original in records::canonical_advertisements() {
        let directory = directory();
        let before = signature_passes();
        let current = directory.create(original.clone(), NOW_MS).await.unwrap();
        assert_eq!(signature_passes() - before, 1);
        let next = advertisement(&key, 2, NOW_MS + 1_000);
        let next = match original.placement_version {
            0 => next,
            PLACEMENT_SCHEMA_VERSION => next
                .with_placement_capacity(original.placement.unwrap(), &key)
                .unwrap(),
            OPERATIONAL_PLACEMENT_SCHEMA_VERSION => next
                .with_operational_placement(
                    original.placement.unwrap(),
                    NodeOperationalSample {
                        sequence: 2,
                        observed_at_ms: NOW_MS + 1_000,
                        mode: NodeMode::Draining,
                        pressure: NodePressure::Critical,
                    },
                    &key,
                )
                .unwrap(),
            _ => unreachable!(),
        };
        let before = signature_passes();
        let refreshed = directory
            .refresh(&current, next.clone(), NOW_MS + 1_000)
            .await
            .unwrap();
        assert_eq!(signature_passes() - before, 1);
        assert_eq!(
            refreshed.advertisement().placement_version,
            original.placement_version
        );
        let bytes = directory
            .layout
            .store()
            .get_with_etag(&directory.layout.node_path(original.session().as_bytes()))
            .await
            .unwrap()
            .0;
        let decoded = NodeAdvertisement::decode_canonical(&bytes).unwrap();
        assert_eq!(&decoded, refreshed.advertisement());
        assert_eq!(decoded.encode().unwrap(), bytes);
    }
}

#[tokio::test]
async fn exact_canonical_reads_verify_once_per_fresh_record() {
    for original in records::canonical_advertisements() {
        let directory = directory();
        directory.create(original.clone(), NOW_MS).await.unwrap();
        let before = signature_passes();
        let loaded = directory
            .load(original.session(), NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.advertisement(), &original);
        assert_eq!(signature_passes() - before, 1);
        let before = signature_passes();
        let loaded = directory
            .load_if_live(original.session(), NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.advertisement(), &original);
        assert_eq!(signature_passes() - before, 1);
        let before = signature_passes();
        assert_eq!(
            directory
                .inspect_advertisement(original.session(), NOW_MS + 1)
                .await
                .unwrap(),
            Some(original)
        );
        assert_eq!(signature_passes() - before, 1);
    }
}

#[tokio::test]
async fn directory_scans_verify_once_and_preserve_expired_obligations() {
    for original in records::canonical_advertisements() {
        let directory = directory();
        directory.create(original.clone(), NOW_MS).await.unwrap();
        let before = signature_passes();
        assert_eq!(
            directory.live(NOW_MS + 1, 128).await.unwrap(),
            vec![original.clone()]
        );
        assert_eq!(signature_passes() - before, 1);
        let before = signature_passes();
        assert_eq!(
            directory
                .advertised_sessions(NOW_MS + 20_000, 128)
                .await
                .unwrap(),
            vec![original.session()]
        );
        assert_eq!(signature_passes() - before, 1);
        let current = directory
            .load(original.session(), NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        let mut with_log = original.clone();
        with_log.log = Some(
            NodeLogStatus::open(original.node(), 7, vec![NodeId::from_bytes([9; 16])]).unwrap(),
        );
        directory
            .update_advertisement(&current, with_log, NOW_MS + 1)
            .await
            .unwrap();
        let before = signature_passes();
        let page = directory
            .follower_logs_page(NodeId::from_bytes([9; 16]), None, 128, NOW_MS + 20_000)
            .await
            .unwrap();
        assert_eq!(page.entries().len(), 1);
        assert_eq!(page.entries()[0].leader, original.session());
        assert_eq!(page.entries()[0].leader_state, LogLeaderState::Expired);
        assert_eq!(page.entries()[0].log.epoch(), 7);
        assert_eq!(signature_passes() - before, 1);
        let before = signature_passes();
        assert!(
            directory
                .log_epoch_referenced(original.session(), 7)
                .await
                .unwrap()
        );
        assert_eq!(signature_passes() - before, 1);
    }
}

#[tokio::test]
async fn every_fresh_scan_rejects_forged_identity_and_operational_signatures() {
    for original in records::canonical_advertisements() {
        let bytes = original.encode().unwrap();
        for placement in [false, true] {
            if placement && !original.has_signed_placement() {
                continue;
            }
            let directory = directory();
            let mut raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if placement {
                raw["placement_signature"] = serde_json::Value::String("00".repeat(64));
            } else {
                raw["identity"]["signature"] = serde_json::Value::String("00".repeat(64));
            }
            directory
                .layout
                .store()
                .create_strict(
                    &directory.layout.node_path(original.session().as_bytes()),
                    Bytes::from(serde_json::to_vec(&raw).unwrap()),
                )
                .await
                .unwrap();
            assert!(
                directory
                    .load(original.session(), NOW_MS + 1)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .log_epoch_referenced(original.session(), 7)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .load_if_live(original.session(), NOW_MS + 1)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .inspect_advertisement(original.session(), NOW_MS + 20_000)
                    .await
                    .is_err()
            );
            assert!(directory.live(NOW_MS + 1, 128).await.is_err());
            assert!(
                directory
                    .advertised_sessions(NOW_MS + 20_000, 128)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .follower_logs_page(NodeId::from_bytes([9; 16]), None, 128, NOW_MS + 20_000)
                    .await
                    .is_err()
            );
            assert!(
                directory
                    .follower_logs_pages(
                        &[
                            (NodeId::from_bytes([9; 16]), None),
                            (NodeId::from_bytes([10; 16]), None)
                        ],
                        64,
                        NOW_MS + 20_000
                    )
                    .await
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn later_canonical_read_observes_new_bytes_and_authenticates_them_again() {
    let [original, bridge, operational] = records::canonical_advertisements();
    for next in [bridge, operational] {
        let directory = directory();
        let current = directory.create(original.clone(), NOW_MS).await.unwrap();
        let before = signature_passes();
        assert_eq!(
            directory
                .load(original.session(), NOW_MS + 1)
                .await
                .unwrap()
                .unwrap()
                .advertisement(),
            &original
        );
        assert_eq!(signature_passes() - before, 1);
        // Replace the same path with different valid signed bytes. No process,
        // page or ETag cache may reuse an earlier signature decision.
        let body = Bytes::from(next.encode().unwrap());
        directory
            .layout
            .store()
            .update(
                &directory.layout.node_path(original.session().as_bytes()),
                body,
                current.token.clone(),
            )
            .await
            .unwrap();
        let before = signature_passes();
        let loaded = directory
            .load(original.session(), NOW_MS + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.advertisement(), &next);
        assert_eq!(signature_passes() - before, 1);
    }
}

#[tokio::test]
async fn authenticated_reads_keep_path_scope_and_time_policies() {
    for original in records::canonical_advertisements() {
        let directory = directory();
        directory.create(original.clone(), NOW_MS).await.unwrap();
        assert!(
            directory
                .load(original.session(), NOW_MS + 20_000)
                .await
                .is_err()
        );
        assert!(
            directory
                .load_if_live(original.session(), NOW_MS + 20_000)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            directory
                .live(NOW_MS + 20_000, 128)
                .await
                .unwrap()
                .is_empty()
        );
        let future = NOW_MS - MAX_CLOCK_SKEW_MS - 1;
        assert!(directory.load(original.session(), future).await.is_err());
        assert!(
            directory
                .load_if_live(original.session(), future)
                .await
                .is_err()
        );
        assert!(
            directory
                .inspect_advertisement(original.session(), future)
                .await
                .is_err()
        );
        assert!(directory.advertised_sessions(future, 128).await.is_err());
        assert!(
            directory
                .follower_logs_page(NodeId::from_bytes([9; 16]), None, 128, future)
                .await
                .is_err()
        );
        let foreign = NodeDirectory::new(
            directory.layout.clone(),
            Digest::from_bytes([88; 32]),
            directory.image,
            directory.release,
        );
        assert!(foreign.load(original.session(), NOW_MS + 1).await.is_err());
        assert!(
            foreign
                .load_if_live(original.session(), NOW_MS + 1)
                .await
                .is_err()
        );
        assert!(
            foreign
                .inspect_advertisement(original.session(), NOW_MS + 1)
                .await
                .is_err()
        );
        assert!(foreign.live(NOW_MS + 1, 128).await.is_err());
        assert!(
            foreign
                .advertised_sessions(NOW_MS + 20_000, 128)
                .await
                .is_err()
        );
        assert!(
            foreign
                .follower_logs_page(NodeId::from_bytes([9; 16]), None, 128, NOW_MS + 20_000)
                .await
                .is_err()
        );
        let body = Bytes::from(original.encode().unwrap());
        directory
            .layout
            .store()
            .create_strict(&directory.layout.node_path(&[77; 16]), body)
            .await
            .unwrap();
        assert!(
            directory
                .load(SessionId::from_bytes([77; 16]), NOW_MS + 1)
                .await
                .is_err()
        );
        assert!(directory.live(NOW_MS + 1, 128).await.is_err());
        assert!(
            directory
                .advertised_sessions(NOW_MS + 20_000, 128)
                .await
                .is_err()
        );
        assert!(
            directory
                .follower_logs_page(NodeId::from_bytes([9; 16]), None, 128, NOW_MS + 20_000)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn invalid_producer_signatures_never_reach_create_or_refresh_cas() {
    for original in records::canonical_advertisements() {
        for placement in [false, true] {
            if placement && !original.has_signed_placement() {
                continue;
            }
            let directory = directory();
            let mut invalid = original.clone();
            if placement {
                invalid.placement_signature[0] ^= 1;
            } else {
                invalid.signature[0] ^= 1;
            }
            assert!(matches!(
                directory.create(invalid.clone(), NOW_MS).await,
                Err(Error::PeerSignature(_))
            ));
            assert!(
                directory
                    .load(original.session(), NOW_MS)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = directory.create(original.clone(), NOW_MS).await.unwrap();
            invalid.issued_at_ms += 1_000;
            invalid.expires_at_ms += 1_000;
            assert!(matches!(
                directory.refresh(&current, invalid, NOW_MS + 1_000).await,
                Err(Error::PeerSignature(_))
            ));
            let loaded = directory
                .load(original.session(), NOW_MS + 1_000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.advertisement(), &original);
            assert_eq!(loaded.token, current.token);
        }
    }
}
