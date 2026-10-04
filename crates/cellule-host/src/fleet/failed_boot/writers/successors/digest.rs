//! Local observation identity; this adds no persisted or transport codec.
use super::*;
use cellule_runtime::node::log_state::NodeLogPhase;

impl FleetOriginalWriterSuccessorInventory {
    /// Identifies the complete original set, current native successors, exact
    /// origin/suffix proofs and original capture barrier. This local producer
    /// identity grants no authentication, retention pin or settlement rights.
    pub fn digest(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.original-writer-successors.v1\0");
        for bytes in [
            self.original
                .snapshot()
                .head()
                .to_bytes()
                .map_err(operation)?,
            self.original
                .snapshot()
                .registry()
                .to_bytes()
                .map_err(operation)?,
        ] {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(&bytes);
        }
        hash.update(
            self.original
                .writers()
                .record()
                .digest()
                .map_err(operation)?
                .as_bytes(),
        );
        hash.update(self.original.process().witness().as_bytes());
        hash.update(self.original.process().request_digest().as_bytes());
        let recovered = self.original.recovered();
        let fence = recovered.fence();
        hash.update(fence.node().as_bytes());
        hash.update(fence.session().as_bytes());
        for n in [
            fence.expires_at_ms(),
            fence.retired_at_ms(),
            self.original.interval().0,
            self.original.interval().1,
            self.started_at_ms,
            self.finished_at_ms,
        ] {
            hash.update(&n.to_be_bytes());
        }
        hash.update(&[u8::from(recovered.log().is_some())]);
        if let Some(log) = recovered.log() {
            hash.update(&[
                match log.phase() {
                    NodeLogPhase::Open => 1,
                    NodeLogPhase::Recovering => 2,
                    NodeLogPhase::Sealed => 3,
                    NodeLogPhase::Retired => 4,
                },
                u8::from(log.active()),
            ]);
            hash.update(&log.epoch().to_be_bytes());
            hash.update(&log.tiered_through().to_be_bytes());
            hash.update(&(log.members().len() as u64).to_be_bytes());
            for member in log.members() {
                hash.update(member.as_bytes());
            }
            hash.update(&[u8::from(log.recovery_manifest().is_some())]);
            if let Some(digest) = log.recovery_manifest() {
                hash.update(digest.as_bytes());
            }
            hash.update(&[u8::from(log.recovery().is_some())]);
            if let Some(claim) = log.recovery() {
                hash.update(claim.claimant().as_bytes());
                hash.update(&claim.generation().to_be_bytes());
                hash.update(&claim.expires_at_ms().to_be_bytes());
            }
        }
        // Original record order binds every original epoch, including repeated
        // epochs of one Cell and targets outside the planner's application.
        hash.update(&(self.proofs.len() as u64).to_be_bytes());
        for proof in &self.proofs {
            hash.update(proof.original.target.cell_id().as_bytes());
            hash.update(&proof.original.control.epoch.to_be_bytes());
            hash.update(proof.node.as_bytes());
            hash.update(proof.release.as_bytes());
            let serving = &proof.serving;
            hash.update(serving.owner().session.as_bytes());
            hash.update(&(serving.owner().endpoint.len() as u64).to_be_bytes());
            hash.update(serving.owner().endpoint.as_bytes());
            hash.update(&serving.position().epoch.to_be_bytes());
            hash.update(serving.native().code.as_bytes());
            hash.update(&serving.native().schema.to_be_bytes());
            hash.update(&serving.native().generation.to_be_bytes());
            use cellule_runtime::cell::catalog::CatalogRole;
            hash.update(&[match serving.native().role {
                CatalogRole::Application => 1,
                CatalogRole::Sql => 2,
                CatalogRole::Kv => 3,
                CatalogRole::Queue => 4,
                CatalogRole::Workflow => 5,
                CatalogRole::Blob => 6,
                CatalogRole::Cron => 7,
            }]);
            prefix(&mut hash, &proof.origin);
            hash.update(&(proof.suffixes.len() as u64).to_be_bytes());
            for suffix in &proof.suffixes {
                let row = suffix.required();
                hash.update(row.application.as_bytes());
                hash.update(row.cell.as_bytes());
                hash.update(row.incarnation.as_bytes());
                hash.update(&row.cell_epoch.to_be_bytes());
                let overlay = &row.recovery;
                hash.update(overlay.leader_session.as_bytes());
                hash.update(overlay.manifest_digest.as_bytes());
                for n in [
                    overlay.log_epoch,
                    overlay.first_node_sequence,
                    overlay.last_node_sequence,
                    overlay.final_txid,
                    overlay.final_checksum,
                    overlay.final_commit_sequence,
                    suffix.acquisition_epoch(),
                ] {
                    hash.update(&n.to_be_bytes());
                }
                root(
                    &mut hash,
                    overlay.predecessor.to_ltx(row.cell, row.incarnation),
                );
                prefix(&mut hash, suffix.proof());
            }
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}

fn prefix(hash: &mut blake3::Hasher, proof: &VerifiedRootPrefix) {
    root(hash, proof.prefix());
    root(hash, proof.root());
    hash.update(&(proof.inspected_roots() as u64).to_be_bytes());
    hash.update(&(proof.dependency_count() as u64).to_be_bytes());
}
fn root(hash: &mut blake3::Hasher, root: cellule_runtime::ltx::RootRef) {
    hash.update(&root.cell);
    hash.update(&root.incarnation);
    hash.update(&root.digest);
    for n in [
        root.position.txid,
        root.position.checksum,
        root.commit_sequence,
    ] {
        hash.update(&n.to_be_bytes());
    }
}
