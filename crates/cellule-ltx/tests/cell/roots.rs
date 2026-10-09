#![cfg(feature = "replica")]

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use cellule_ltx::{
    CaptureBatch, CaptureTiming, CellObjectKind, CellReplica, CellStorageLayout, Db, DiskBudget,
    Host, Limits, RecoveryOverlay, RootRef, VerifiedPlan,
    bundle::{Bundle, BundleEntry},
    restore_exact,
};
use cellule_store::{StorageReadKind, Store};
use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};

fn checksum_path(database: &std::path::Path) -> std::path::PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(".cellule-ltx-checksums");
    path.into()
}

fn replica(store: Store, cell: [u8; 32], incarnation: [u8; 16]) -> CellReplica {
    CellReplica::new(
        CellStorageLayout::new(store, Path::from("runtime"), [3; 16]),
        cell,
        incarnation,
        Limits::default(),
    )
    .unwrap()
}

mod batch_cost;
mod coalescing;
mod compaction;
mod compaction_transfers;
mod directory;
mod lifecycle;
mod packed;
mod preparation;
mod prepare_cost;
mod sparse;
