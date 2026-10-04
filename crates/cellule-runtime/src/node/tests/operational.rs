//! Operational placement and the schema 2 bridge signing contract.

use super::*;
use crate::fleet::placement::{PlacementEligibility, PlacementPressure};
use crate::node::advertisement::{
    RawCapacity, RawIdentity, RawLease, RawNodeLog, RawPlacementCapacity, validate_successor,
};

// The top-level schema 2 shape and field ordering from the baseline. Keeping
// this independent of RawAdvertisement catches fields accidentally emitted by
// bridge writers and demonstrates the strict old reader's rollout barrier.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Schema2Advertisement {
    version: u8,
    identity: RawIdentity,
    lease: RawLease,
    log: Option<RawNodeLog>,
    capacity: RawCapacity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement: Option<RawPlacementCapacity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement_signature: Option<String>,
}

fn placement() -> NodePlacementCapacity {
    NodePlacementCapacity {
        memory_capacity_bytes: 8_192,
        disk_capacity_bytes: 16_384,
        active_cells: 3,
        max_active_cells: 16,
        running_jobs: 2,
        job_capacity: 8,
        publication_backlog: 4,
        hydration_backlog: 5,
        primitive_backlog: 6,
    }
}

fn sample(mode: NodeMode, pressure: NodePressure) -> NodeOperationalSample {
    NodeOperationalSample {
        mode,
        pressure,
        sequence: 7,
        observed_at_ms: NOW_MS,
    }
}

fn operational(mode: NodeMode, pressure: NodePressure) -> NodeAdvertisement {
    let key = SigningKey::from_bytes(&[7; 32]);
    advertisement(&key, 1, NOW_MS)
        .with_operational_placement(placement(), sample(mode, pressure), &key)
        .unwrap()
}

#[test]
fn bridge_writer_preserves_the_schema2_serializer_and_signing_payload() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let current = advertisement(&key, 1, NOW_MS)
        .with_placement_capacity(placement(), &key)
        .unwrap();
    let bytes = current.encode().unwrap();
    let mut legacy: Schema2Advertisement = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&legacy).unwrap(), bytes);
    assert_eq!(legacy.placement_version, Some(2));
    assert_eq!(current.operational_sample(), None);
    legacy.placement_signature = None;
    legacy.log = None;
    legacy.lease.generation.clear();
    let mut expected = PLACEMENT_SIGNING_DOMAIN.to_vec();
    expected.extend(serde_json::to_vec(&legacy).unwrap());
    assert_eq!(current.placement_signing_bytes().unwrap(), expected);
    key.verifying_key()
        .verify(
            &expected,
            &Signature::from_bytes(&current.placement_signature),
        )
        .unwrap();
    let schema3 = operational(NodeMode::Active, NodePressure::Normal)
        .encode()
        .unwrap();
    assert!(serde_json::from_slice::<Schema2Advertisement>(&schema3).is_err());
    assert!(NodeAdvertisement::decode_canonical(&schema3).is_ok());
}

#[test]
fn signed_mode_and_every_stable_pressure_tier_drive_proactive_eligibility() {
    for mode in [NodeMode::Active, NodeMode::Cordoned, NodeMode::Draining] {
        for pressure in [
            NodePressure::Normal,
            NodePressure::Constrained,
            NodePressure::Shedding,
            NodePressure::Critical,
        ] {
            let advertisement = operational(mode, pressure);
            let decoded =
                NodeAdvertisement::decode_canonical(&advertisement.encode().unwrap()).unwrap();
            assert_eq!(decoded.operational_sample(), Some(sample(mode, pressure)));
            let observation =
                PlacementObservation::from_signed_advertisement(&decoded, NOW_MS + 1, false)
                    .unwrap();
            assert_eq!(observation.draining, mode != NodeMode::Active);
            assert_eq!(
                observation.free_memory_bytes,
                advertisement.capacity.free_memory_bytes
            );
            let score = PlacementPlanner::default()
                .rank(
                    crate::CellId::from_bytes([9; 32]),
                    NOW_MS + 1,
                    &[observation],
                )
                .unwrap()[0];
            let eligible = mode == NodeMode::Active && pressure == NodePressure::Normal;
            assert_eq!(
                score.eligibility == PlacementEligibility::Eligible,
                eligible
            );
            assert_eq!(decoded.accepts_new_roles(NOW_MS + 1), eligible);
            if pressure == NodePressure::Shedding {
                assert_eq!(observation.pressure, PlacementPressure::Shedding);
            }
        }
    }
    assert!(NodePressure::try_from(crate::fleet::pressure::PressureState::Recovering).is_err());
}

#[test]
fn operational_fields_are_signed_and_partial_or_unknown_tiers_are_rejected() {
    let original = operational(NodeMode::Active, NodePressure::Normal)
        .encode()
        .unwrap();
    for (field, value) in [
        ("mode", serde_json::json!("cordoned")),
        ("pressure", serde_json::json!("shedding")),
        ("sequence", serde_json::json!("8")),
        (
            "observed_at_ms",
            serde_json::json!((NOW_MS - 1).to_string()),
        ),
    ] {
        let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
        changed["operational_sample"][field] = value;
        assert!(
            NodeAdvertisement::decode_canonical(&serde_json::to_vec(&changed).unwrap()).is_err()
        );
    }
    for field in ["mode", "pressure", "sequence", "observed_at_ms"] {
        let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
        changed["operational_sample"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            NodeAdvertisement::decode_canonical(&serde_json::to_vec(&changed).unwrap()).is_err()
        );
    }
    let mut invalid = operational(NodeMode::Active, NodePressure::Normal);
    invalid.operational_sample = None;
    assert!(invalid.encode().is_err());
    invalid = operational(NodeMode::Active, NodePressure::Normal);
    invalid.placement_version = 2;
    assert!(invalid.encode().is_err());
    let wrong_key = SigningKey::from_bytes(&[99; 32]);
    assert!(
        operational(NodeMode::Active, NodePressure::Normal)
            .with_placement_capacity(placement(), &wrong_key)
            .is_err()
    );
    let mut forged = operational(NodeMode::Active, NodePressure::Normal);
    forged.operational_sample.as_mut().unwrap().pressure = NodePressure::Shedding;
    assert!(PlacementObservation::from_signed_advertisement(&forged, NOW_MS + 1, false).is_err());
}

#[test]
fn heartbeat_republication_does_not_advance_the_measurement_time() {
    let previous = operational(NodeMode::Active, NodePressure::Normal);
    let key = SigningKey::from_bytes(&[7; 32]);
    let next_at = NOW_MS + 1;
    let mut next = advertisement(&key, 2, next_at)
        .with_operational_placement(placement(), previous.operational_sample.unwrap(), &key)
        .unwrap();
    next.generation = previous.generation + 1;
    validate_successor(&previous, &next).unwrap();
    assert_eq!(
        PlacementObservation::from_signed_advertisement(&next, next_at, false)
            .unwrap()
            .observed_at_ms,
        NOW_MS
    );
    let mut changed = next.clone();
    changed.operational_sample.as_mut().unwrap().pressure = NodePressure::Shedding;
    assert!(validate_successor(&previous, &changed).is_err());
    changed.operational_sample.as_mut().unwrap().sequence += 1;
    validate_successor(&previous, &changed).unwrap();
    changed.operational_sample.as_mut().unwrap().observed_at_ms = NOW_MS - 1;
    assert!(validate_successor(&previous, &changed).is_err());
    let mut stale = next;
    stale.operational_sample.as_mut().unwrap().observed_at_ms = NOW_MS - 30_001;
    assert!(!stale.accepts_new_roles(next_at));
    let mut current = previous;
    current.operational_sample.as_mut().unwrap().mode = NodeMode::Cordoned;
    let legacy = advertisement(&key, 2, next_at)
        .with_placement_capacity(placement(), &key)
        .unwrap();
    assert!(validate_successor(&current, &legacy).is_err());
}

#[tokio::test]
async fn closed_operational_mode_excludes_new_reader_and_follower_selection() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let directory = directory();
    let sessions = [1, 2, 3, 4].map(|byte| SessionId::from_bytes([byte; 16]));
    for (session, mode) in sessions.into_iter().zip([
        NodeMode::Active,
        NodeMode::Cordoned,
        NodeMode::Active,
        NodeMode::Active,
    ]) {
        directory
            .create(
                advertisement_for_capacity(
                    session,
                    &key,
                    1,
                    NOW_MS,
                    NodeCapacity {
                        free_memory_bytes: 64 << 20,
                        free_disk_bytes: 64 << 20,
                        follower_free_bytes: 32 << 20,
                        follower_retained_bytes: 0,
                        job_credits: 3,
                        log_protocol: NODE_LOG_PROTOCOL_VERSION,
                    },
                )
                .with_operational_placement(placement(), sample(mode, NodePressure::Normal), &key)
                .unwrap(),
                NOW_MS,
            )
            .await
            .unwrap();
    }
    let followers = directory
        .select_log_members(sessions[0], 1_000, NOW_MS + 1, 4)
        .await
        .unwrap();
    assert_eq!(followers.len(), 2);
    assert!(!followers.contains(&node(sessions[1])));
    let readers = directory
        .select_readers(
            crate::CellId::from_bytes([9; 32]),
            sessions[0],
            Digest::from_bytes([6; 32]),
            3,
            NOW_MS + 1,
            4,
        )
        .await
        .unwrap();
    assert_eq!(readers.len(), 2);
    assert!(
        !readers
            .iter()
            .any(|reader| reader.node() == node(sessions[1]))
    );
    // Recovery executor admission uses the same explicit mode; an existing
    // follower tail remains accessible through its separately fenced path.
    assert!(!crate::node::directory::recovery_executor_eligible(
        &operational(NodeMode::Cordoned, NodePressure::Normal),
        NOW_MS + 1
    ));
}
