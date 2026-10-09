//! Provider-neutral lifecycle boundary for a compiled Cell application.
//!
//! `CellNode` owns the embedded runtime and its shared admission ledger. A
//! product server supplies providers, authentication and network transports;
//! it must not construct another runtime alongside this host.

#![deny(missing_docs)]
// Production panics can abandon accepted work and persistence resources; tests
// retain assertions while runtime paths propagate typed errors.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]

pub use builder::CellNodeBuilder;
pub use durability::{
    FacilityResult, FleetNodeDurabilityProvider, FleetNodeLogRecruitment,
    FollowerEnrollmentCompletion, FollowerEnrollmentInventoryCursor,
    FollowerEnrollmentInventoryPage, FollowerEnrollmentMember, FollowerEnrollmentProgress,
    FollowerEvacuation, NodeDurabilityProvider, NodeDurabilityRotation,
    NodeDurabilitySupervisorConfig, NodeDurabilitySupervisorObservation,
    NodeDurabilitySupervisorState, NodeLogRotationCompletion, NodeLogRotationEntry,
    NodeLogRotationInventory, NodeLogRotationObservation, NodeLogRotationPhase,
    NodeLogRotationRequest,
};
pub use facility::CellNodeFacility;
pub use node::CellNode;
pub use status::{NodeDrainObservation, NodeDrainPhase, NodeState, NodeStatus, ScaleDownStatus};
use std::{
    any::Any,
    collections::HashSet,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
pub use tasks::CellNodeTaskGroup;
mod builder;
mod durability;
mod facility;
pub mod fleet;
mod node;
pub mod read_replicas;
mod status;
mod tasks;

use cellule_app::{ApplicationBinding, ApplicationHandle, CellApplication, CompiledApplication};
use cellule_runtime::Error;
use cellule_runtime::cell::actor::{CellRuntime, CellRuntimeStats};
use cellule_runtime::cell::worker::SqlWorkerPool;
use cellule_runtime::client::CellClient;
use cellule_runtime::follower::FollowerStore;
use cellule_runtime::identity::{ApplicationId, CellId, SessionId, TenantId};
use cellule_runtime::ltx::DiskBudget;
use cellule_runtime::ltx::{Host as ReplicaHost, Limits as ReplicaLimits};
use cellule_runtime::node::durability::NodeDurabilityConfig;
use cellule_runtime::qualification::{
    QualificationOperationExecutor, QualificationRunSummary, QualificationWorkload,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MAX_NODE_FACILITIES: usize = 64;
const MAX_NODE_TASKS: usize = 256;

/// Stable host-owned component name for the follower store.
pub const FOLLOWER_STORE_COMPONENT: &str = "follower-store";
/// Stable host-owned component name for the Blob artifact store.
pub const BLOB_ARTIFACT_STORE_COMPONENT: &str = "blob-artifact-store";
/// Stable host-owned component name for the node-log enrollment provider.
pub const NODE_DURABILITY_PROVIDER_COMPONENT: &str = "node-durability-provider";
pub(crate) const NODE_DURABILITY_SUPERVISOR_COMPONENT: &str = "node-durability-supervisor";
