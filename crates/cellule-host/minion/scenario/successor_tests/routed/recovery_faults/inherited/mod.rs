//! Actual sealed suffix inherited through an interrupted native receiver claim.
use super::*;
use crate::scenario::recovered_followers::Members;
use bytes::Bytes;
use cellule_host::fleet::{
    FleetEnrollmentAcceptance, FleetRecoveredFollowerRetirement, FleetRoster,
};
use cellule_runtime::fleet::operations::{EnrollmentEndpoint, EnrollmentRole, EnrollmentSpec};
use cellule_runtime::{
    Error,
    follower::FollowerStore,
    node::{
        NodeAdvertisement,
        log_recovery::{
            NodeLogRecovery, RecoveryCell, RecoveryCoordinator,
            retirement::retire_recovered_members,
        },
        log_transport::{
            AppendRequest, LocalFollowerTransport, LocalRecoveredFollowerTransport,
            NodeLogTransport,
        },
    },
    recovery::manifest::RecoveryManifestStore,
};
use fixture::FaultFixture;

mod setup;
mod tests;

struct Inherited {
    native: super::super::fixture::Fixture,
    path: ObjectPath,
    manifest: Bytes,
    original: Control,
    value: i64,
}
