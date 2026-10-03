//! LTX surface the runtime exposes to embedders.
//!
//! `cellule-host` and product servers may not depend on `cellule-ltx` directly,
//! so the runtime re-exports the exact LTX types its own API uses.

pub use cellule_ltx::{
    CaptureTiming, CellObjectKind, CellReplica, CellStorageLayout, DiskBudget, DiskReservation,
    Host, Limits, LtxPhase, LtxReadOrigin, LtxRequestOutcome, RootRef, ScratchMonitor,
};
