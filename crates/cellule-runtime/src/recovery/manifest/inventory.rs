use super::{MAX_MANIFEST_BYTES, PinnedRecoveryCell, RecoveryManifest, RecoveryManifestStore};
use crate::identity::{Digest, SessionId};
use crate::{Error, Result};

/// Complete metadata from one immutable, digest-verified recovery manifest.
///
/// This inventories recovered suffixes across every application in the manifest.
/// It does not include object-covered Cells without a recovered suffix, verify
/// bundle availability, or establish current ownership, serving, or node closure.
/// The caller must obtain the manifest identity from canonical sealed-log authority.
pub struct RecoveryManifestInventory {
    leader_session: SessionId,
    log_epoch: u64,
    manifest_digest: Digest,
    cells: Vec<PinnedRecoveryCell>,
}

impl RecoveryManifestInventory {
    /// Returns the original failed leader session.
    #[must_use]
    pub const fn leader_session(&self) -> SessionId {
        self.leader_session
    }

    /// Returns the original node-log epoch.
    #[must_use]
    pub const fn log_epoch(&self) -> u64 {
        self.log_epoch
    }

    /// Returns the digest of the complete canonical manifest bytes.
    #[must_use]
    pub const fn manifest_digest(&self) -> Digest {
        self.manifest_digest
    }

    /// Returns every original pinned scope, including other applications.
    #[must_use]
    pub fn cells(&self) -> &[PinnedRecoveryCell] {
        &self.cells
    }
}

impl RecoveryManifestStore {
    /// Reads the complete original recovered-suffix set without starting recovery.
    ///
    /// Uses the same bounded, canonical, digest- and scope-checked read as
    /// [`Self::load_overlay`]. The store's application does not filter this set.
    /// Each row can subsequently be verified through the matching application's
    /// `load_overlay`; metadata alone does not verify the referenced bundle.
    pub async fn load_manifest(
        &self,
        leader_session: SessionId,
        log_epoch: u64,
        manifest_digest: Digest,
    ) -> Result<RecoveryManifestInventory> {
        let manifest = self
            .load_manifest_body(leader_session, log_epoch, manifest_digest)
            .await?;
        let cells = manifest
            .cells
            .iter()
            .map(|cell| cell.pinned(leader_session, log_epoch, manifest_digest))
            .collect();
        Ok(RecoveryManifestInventory {
            leader_session,
            log_epoch,
            manifest_digest,
            cells,
        })
    }

    pub(super) async fn load_manifest_body(
        &self,
        leader_session: SessionId,
        log_epoch: u64,
        manifest_digest: Digest,
    ) -> Result<RecoveryManifest> {
        if leader_session.as_bytes().iter().all(|byte| *byte == 0) || log_epoch == 0 {
            return Err(Error::Node("invalid recovery manifest scope"));
        }
        let path = self.layout.node_log_recovery_path(
            leader_session.as_bytes(),
            log_epoch,
            manifest_digest.as_bytes(),
        );
        let (body, _) = self
            .layout
            .store()
            .get_with_etag_bounded(&path, MAX_MANIFEST_BYTES)
            .await?;
        if *blake3::hash(&body).as_bytes() != *manifest_digest.as_bytes() {
            return Err(Error::Node("recovery manifest digest differs"));
        }
        let manifest = RecoveryManifest::decode(&body)?;
        if manifest.leader_session != leader_session || manifest.log_epoch != log_epoch {
            return Err(Error::Node("recovery manifest path scope differs"));
        }
        Ok(manifest)
    }
}
