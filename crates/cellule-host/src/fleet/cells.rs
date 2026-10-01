use super::FleetAdapterFuture;
use cellule_runtime::cell::catalog::CatalogProof;
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::fleet::operations::MoveAttemptSpec;
use cellule_runtime::ltx::CellReplica;
use std::path::PathBuf;

/// Trusted application inputs for one exact receiver or observation.
#[derive(Clone)]
pub struct FleetCellInputs {
    /// Verified catalog target; the host checks exact attempt scope.
    pub catalog: CatalogProof,
    /// Immutable storage operations bound to the Cell and incarnation.
    pub replica: CellReplica,
    /// Existing canonical Cell authority in that application's storage layout.
    pub authority: CellAuthority,
    /// Private local SQLite destination supplied during trusted composition.
    pub destination: PathBuf,
    /// Local leased session and advertised endpoint used by ordinary acquisition.
    pub owner: Owner,
}

/// Canonical failed-session proof and manifest access from ordinary recovery.
#[derive(Clone)]
pub struct FleetRecoveryInputs {
    /// Proof obtained only through the node directory/recovery coordinator.
    pub takeover: cellule_runtime::node::NodeTakeoverProof,
    /// Existing manifest store for the exact control-pinned recovery overlay.
    pub manifests: cellule_runtime::recovery::manifest::RecoveryManifestStore,
}

/// Application-owned lookup of catalog, storage and private local paths.
///
/// Install this trusted adapter at startup; remote actions never supply local
/// filesystem paths, credentials, or authority constructors. Lookup must not
/// mutate Cell authority, hydrate a database, or activate a writer. Actual
/// preparation, resource admission, and acquisition remain on the runtime path.
pub trait FleetCellProvider: Send + Sync + 'static {
    /// Resolves bounded inputs for the immutable movement specification.
    fn cell_inputs<'a>(
        &'a self,
        spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetCellInputs>;

    /// Resolves existing canonical recovery proof. This lookup must not fence
    /// a node, seal a log, publish an overlay, or acquire a Cell. The ordinary
    /// recovery coordinator establishes those prerequisites independently.
    fn recovery_inputs<'a>(
        &'a self,
        spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetRecoveryInputs>;
}
