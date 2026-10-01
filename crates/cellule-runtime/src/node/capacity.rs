//! Node capacity and failure-domain records.

use super::*;

/// Signed role-admission mode, independent of measured resource capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeMode {
    /// The node may admit new roles if its pressure and ledgers permit them.
    #[default]
    Active,
    /// Existing roles continue while new role acquisition is closed.
    Cordoned,
    /// Accepted work and role obligations are being settled for shutdown.
    Draining,
}

/// Stable pressure tier published by the node's existing classifier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePressure {
    /// Normal measured pressure.
    #[default]
    Normal,
    /// Optional receive is paused, including when the local sample is stale.
    Constrained,
    /// Sustained pressure on at least one resource dimension.
    Shedding,
    /// Sustained critical memory and disk pressure.
    Critical,
}

impl TryFrom<crate::fleet::pressure::PressureState> for NodePressure {
    type Error = Error;

    fn try_from(state: crate::fleet::pressure::PressureState) -> Result<Self> {
        use crate::fleet::pressure::PressureState;
        match state {
            PressureState::Normal => Ok(Self::Normal),
            PressureState::Constrained => Ok(Self::Constrained),
            PressureState::Shedding => Ok(Self::Shedding),
            PressureState::Critical => Ok(Self::Critical),
            PressureState::Recovering => Err(Error::Node("transient pressure is not a wire tier")),
        }
    }
}

/// Schema 3 operational sample; republication must preserve its sample identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeOperationalSample {
    /// Role-admission mode at the time of the sample.
    pub mode: NodeMode,
    /// Stable output of the local hysteretic pressure classifier.
    pub pressure: NodePressure,
    /// Strictly increasing sequence scoped to the boot session.
    pub sequence: u64,
    /// Logical time of measurement, not time of heartbeat republication.
    pub observed_at_ms: i64,
}

impl NodeOperationalSample {
    /// Validates sample identity and nonnegative observation time.
    pub const fn validated(self) -> Result<Self> {
        if self.sequence == 0 || self.observed_at_ms < 0 {
            return Err(Error::Node("operational sample identity is invalid"));
        }
        Ok(self)
    }

    /// Whether this sample admits proactive writer, reader, or follower receive.
    #[must_use]
    pub fn accepts_roles(self) -> bool {
        self.mode == NodeMode::Active && self.pressure == NodePressure::Normal
    }
}

/// Capacity hints published by one node boot session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCapacity {
    /// Free memory the node reports.
    pub free_memory_bytes: u64,
    /// Free scratch disk the node reports.
    pub free_disk_bytes: u64,
    /// Free space in the node's follower store.
    pub follower_free_bytes: u64,
    /// Bytes the node's follower store retains.
    pub follower_retained_bytes: u64,
    /// Job credits the node offers to the fleet.
    pub job_credits: u32,
    /// Node-log protocol version the node speaks.
    pub log_protocol: u32,
}

/// Signed runtime capacity measurements used by the placement planner.
///
/// The ordinary capacity hints remain intentionally small and compatible with
/// older node records. This optional block carries the totals and live counts
/// required to compare a node's usable headroom without guessing from host
/// totals on the receiving side.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodePlacementCapacity {
    /// Total memory the node reports.
    pub memory_capacity_bytes: u64,
    /// Total scratch disk the node reports.
    pub disk_capacity_bytes: u64,
    /// Cells the node currently owns.
    pub active_cells: u32,
    /// Cells the node admits.
    pub max_active_cells: u32,
    /// Jobs the node is running.
    pub running_jobs: u32,
    /// Jobs the node admits.
    pub job_capacity: u32,
    /// Publications waiting to be acknowledged.
    pub publication_backlog: u32,
    /// Hydrations waiting to run.
    pub hydration_backlog: u32,
    /// Primitive maintenance items waiting.
    pub primitive_backlog: u32,
}

impl NodePlacementCapacity {
    /// Validates and returns a placement snapshot with measured node totals.
    pub const fn validated(self) -> Result<Self> {
        if self.memory_capacity_bytes == 0
            || self.disk_capacity_bytes == 0
            || self.max_active_cells == 0
            || self.active_cells > self.max_active_cells
            || self.job_capacity == 0
            || self.running_jobs > self.job_capacity
        {
            return Err(Error::Node("placement capacity is invalid"));
        }
        Ok(self)
    }
}

/// Stable topology labels used only to prefer independent follower nodes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeFailureDomain {
    pub(super) zone: Option<String>,
    pub(super) host: Option<String>,
}

impl NodeFailureDomain {
    /// Validates optional zone and host labels advertised for one boot identity.
    pub fn new(zone: Option<String>, host: Option<String>) -> Result<Self> {
        let domain = Self { zone, host };
        domain.validate()?;
        Ok(domain)
    }

    /// Returns the availability zone, when the node declares one.
    #[must_use]
    pub fn zone(&self) -> Option<&str> {
        self.zone.as_deref()
    }

    /// Returns the host, when the node declares one.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub(super) fn validate(&self) -> Result<()> {
        if [self.zone.as_deref(), self.host.as_deref()]
            .into_iter()
            .flatten()
            .any(|value| {
                value.is_empty()
                    || value.len() > MAX_FAILURE_DOMAIN_BYTES
                    || !value.is_ascii()
                    || value.bytes().any(|byte| !byte.is_ascii_graphic())
            })
        {
            return Err(Error::Node("node failure domain is invalid"));
        }
        Ok(())
    }
}
