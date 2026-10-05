//! Fresh directory reads authenticate immutable bytes once, including scans.
use super::*;

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
