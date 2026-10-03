//! Public host producer against native followers and the shared durable journal.
use super::*;
use bytes::Bytes;
use cellule_host::{
    FacilityResult, FleetNodeDurabilityProvider, FleetNodeLogRecruitment, NodeDurabilityRotation,
    NodeDurabilitySupervisorConfig,
};
use cellule_runtime::{
    Error,
    fleet::operations::{EnrollmentEvent, EnrollmentRecord, EnrollmentStatus},
    follower::{FollowerReceipt, FollowerStore},
    node::{
        NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain,
        durability::NodeLogAuthority,
        log::{NodeLogRetirementObservation, NodeLogRotationBarrier},
        log_transport::{
            AppendRequest, LocalFollowerTransport, NodeLogTransport, RetireRequest, SealRequest,
            TailRequest,
        },
    },
};
use ed25519_dalek::SigningKey;
use futures_util::future::BoxFuture;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

mod fixture;
use fixture::*;
mod aggregate;
mod coverage;
mod evacuation;
mod inventory;
mod observation;
mod tests;

async fn captured(reply: tokio::sync::oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(3), reply)
        .await
        .unwrap()
        .unwrap();
}
