//! One worker-serialized measurement of durable work and logical database size.

use crate::primitives::maintenance::{PersistedWorkInventory, TransferWorkInventory};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerCellInventory {
    pub(crate) persisted_work: PersistedWorkInventory,
    pub(crate) transfer_work: TransferWorkInventory,
    pub(crate) maintenance_work: crate::primitives::maintenance_readiness::MaintenanceWorkInventory,
    pub(crate) database_bytes: u64,
    pub(crate) commit_sequence: u64,
    pub(crate) observed_at_ms: i64,
}
