//! Durability internals for the Cell node host.

use super::*;

/// Error returned by a provider-owned node facility during drain.
pub type FacilityResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Node-log rotation events emitted by the host supervisor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeDurabilityRotation {
    /// The supervisor is retiring the current node-log generation.
    Started,
    /// Shutdown is waiting for pending publications to settle.
    Pending,
    /// A rotation step failed; requested rotations retain failure and may retry.
    Failed,
    /// The replacement generation is installed and serving.
    Completed,
}

/// Provider-owned enrollment adapter used by the host durability supervisor.
///
/// The provider is responsible for authority and transport enrollment. The
/// host consumes the resulting provider-neutral configuration and is the only
/// owner that constructs and installs [`cellule_runtime::node::durability::NodeDurability`].
pub trait NodeDurabilityProvider: Send + Sync + 'static {
    /// Recruits one enrollment round for the replacement generation.
    ///
    /// `Ok(None)` means the provider is not ready and the supervisor should
    /// ask again after its recruit interval.
    /// Return the same physical leader and boot with a strictly newer epoch on
    /// replacement. The provider owns Pending-before-CAS enrollment, original
    /// reply reconciliation, maintenance-node exclusion and redundancy policy.
    fn recruit(
        self: Arc<Self>,
        limits: ReplicaLimits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>>;

    /// Reports one rotation event to the provider; the default ignores it.
    fn rotation_event(&self, _event: NodeDurabilityRotation) {}
}

/// Fixed host-owned bounds and identity for the node-log supervisor.
#[derive(Clone, Copy, Debug)]
pub struct NodeDurabilitySupervisorConfig {
    pub(super) application: ApplicationId,
    pub(super) limits: ReplicaLimits,
    pub(super) required_follower_bytes: u64,
    pub(super) live_node_limit: usize,
    pub(super) recruit_interval: std::time::Duration,
    pub(super) rotation_interval: std::time::Duration,
    pub(super) max_issued_frames: u64,
}

impl NodeDurabilitySupervisorConfig {
    /// Creates a bounded supervisor configuration.
    pub fn new(
        application: ApplicationId,
        limits: ReplicaLimits,
        required_follower_bytes: u64,
        live_node_limit: usize,
        recruit_interval: std::time::Duration,
        rotation_interval: std::time::Duration,
        max_issued_frames: u64,
    ) -> cellule_runtime::Result<Self> {
        if application.as_bytes().iter().all(|byte| *byte == 0)
            || required_follower_bytes == 0
            || live_node_limit == 0
            || recruit_interval.is_zero()
            || rotation_interval.is_zero()
            || max_issued_frames == 0
        {
            return Err(Error::Control(
                "invalid CellNode durability supervisor configuration",
            ));
        }
        Ok(Self {
            application,
            limits,
            required_follower_bytes,
            live_node_limit,
            recruit_interval,
            rotation_interval,
            max_issued_frames,
        })
    }
}

mod owner;
mod requests;
mod supervisor;
pub(crate) use owner::DurabilitySupervisor;
pub use requests::{
    NodeLogRotationCompletion, NodeLogRotationObservation, NodeLogRotationPhase,
    NodeLogRotationRequest,
};
use supervisor::run_node_durability_supervisor;
