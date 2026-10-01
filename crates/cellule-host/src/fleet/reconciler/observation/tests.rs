use super::*;
use cellule_runtime::cell::catalog::CatalogRole;
use cellule_runtime::fleet::operations::{DrainBlocker, TransferCost};
use cellule_runtime::identity::{ApplicationId, CellTarget, IncarnationId, NamespaceId, TenantId};
use cellule_runtime::node::{
    NodeCapacity, NodeFailureDomain, NodeOperationalSample, NodePlacementCapacity,
};

fn observation() -> FleetObservation {
    let scope = FleetScope {
        fleet: Digest::from_bytes([1; 32]),
        application: ApplicationId::from_bytes([2; 16]),
    };
    let node = NodeId::from_bytes([3; 16]);
    let session = SessionId::from_bytes([4; 16]);
    let key = ed25519_dalek::SigningKey::from_bytes(&[5; 32]);
    let signed = NodeAdvertisement::sign(
        node,
        session,
        "https://node.internal:8789".into(),
        scope.fleet,
        Digest::from_bytes([6; 32]),
        Digest::from_bytes([7; 32]),
        Digest::from_bytes([8; 32]),
        &key,
        1,
        100,
        30_100,
        vec![Digest::from_bytes([12; 32])],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1000,
            free_disk_bytes: 1000,
            job_credits: 2,
            log_protocol: 1,
            ..NodeCapacity::default()
        },
    )
    .unwrap()
    .with_operational_placement(
        NodePlacementCapacity {
            memory_capacity_bytes: 1000,
            disk_capacity_bytes: 1000,
            active_cells: 1,
            max_active_cells: 2,
            job_capacity: 2,
            ..NodePlacementCapacity::default()
        },
        NodeOperationalSample {
            sequence: 1,
            observed_at_ms: 100,
            mode: cellule_runtime::node::NodeMode::Active,
            pressure: cellule_runtime::node::NodePressure::Normal,
        },
        &key,
    )
    .unwrap();
    FleetObservation::new(
        scope,
        RegistryVersion::new(scope).unwrap(),
        1,
        100,
        100,
        false,
        vec![signed],
        vec![FleetOwnedCell {
            node,
            session,
            observation: OwnedCellObservation {
                target: CellTarget::new(
                    TenantId::from_bytes([9; 16]),
                    scope.application,
                    NamespaceId::from_bytes([10; 16]),
                    b"digest",
                )
                .unwrap(),
                generation: 1,
                incarnation: IncarnationId::from_bytes([11; 16]),
                code: Digest::from_bytes([12; 32]),
                schema: 1,
                role: CatalogRole::Sql,
                resident_since_ms: 50,
                last_used_ms: 100,
                position: None,
                cost: None,
                maintenance_cost: Some(TransferCost {
                    memory_bytes: 100,
                    disk_bytes: 200,
                    file_descriptors: 8,
                    job_credits: 1,
                }),
                database_bytes: None,
                sampled_at_ms: None,
                stable_observations: 0,
                work_blocker: None,
                quiescing: false,
                maintenance_work: None,
                blockers: vec![DrainBlocker::BusyExecution, DrainBlocker::UnknownInventory],
            },
        }],
    )
    .unwrap()
}

#[test]
fn planner_digest_binds_peak_cost_presence_and_every_admission_dimension() {
    let baseline = observation().digest(100).unwrap();
    for field in 0..5 {
        let mut inputs = observation();
        let cost = &mut inputs.cells[0].observation.maintenance_cost;
        if field == 0 {
            *cost = None;
        } else {
            let cost = cost.as_mut().unwrap();
            match field {
                1 => cost.memory_bytes += 1,
                2 => cost.disk_bytes += 1,
                3 => cost.file_descriptors += 1,
                _ => cost.job_credits += 1,
            }
        }
        assert_ne!(inputs.digest(100).unwrap(), baseline);
    }
    assert_eq!(observation().digest(100).unwrap(), baseline);
}

#[test]
fn planner_digest_binds_role_blocker_and_executable_identity() {
    let baseline = observation().digest(100).unwrap();
    for field in 0..4 {
        let mut inputs = observation();
        let row = &mut inputs.cells[0].observation;
        match field {
            0 => row.role = CatalogRole::Blob,
            1 => row.blockers[1] = DrainBlocker::FollowerObligation,
            2 => row.code = Digest::from_bytes([99; 32]),
            _ => row.schema += 1,
        }
        assert_ne!(inputs.digest(100).unwrap(), baseline);
    }
}

#[test]
fn busy_envelope_cannot_refresh_an_expired_collection_barrier() {
    let observation = observation();
    assert!(observation.digest(30_101).is_err());
    assert!(observation.placements(30_101).is_err());
}
