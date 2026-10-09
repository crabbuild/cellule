//! Local input identity; no persisted or signed wire contract is changed.
use super::*;
use cellule_runtime::Result;

impl FleetSourceReaderPolicies {
    /// Binds full original/current rows, closure evidence, exact roots, native
    /// successor identity, policy, ready replacements and the original barrier.
    pub fn digest(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-source-reader-policies.v2\0");
        for bytes in [
            self.snapshot.head().to_bytes().map_err(operation)?,
            self.snapshot.registry().to_bytes().map_err(operation)?,
        ] {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(&bytes);
        }
        hash.update(self.roster.as_bytes());
        hash.update(self.original.as_bytes());
        for time in [self.interval.0, self.interval.1] {
            hash.update(&time.to_be_bytes());
        }
        hash.update(&(self.checks.len() as u64).to_be_bytes());
        for check in &self.checks {
            let retirement = check.retirement();
            match retirement {
                FleetSourceReaderRetirement::Native(_) => {
                    hash.update(&[1]);
                }
                FleetSourceReaderRetirement::Failed(closure) => {
                    hash.update(&[2]);
                    hash.update(closure.digest().as_bytes());
                    hash.update(closure.request().digest().as_bytes());
                    hash.update(closure.process().request_digest().as_bytes());
                    hash.update(closure.process().witness().as_bytes());
                    for bytes in [
                        closure.snapshot().head().to_bytes().map_err(operation)?,
                        closure
                            .snapshot()
                            .registry()
                            .to_bytes()
                            .map_err(operation)?,
                    ] {
                        hash.update(&(bytes.len() as u64).to_be_bytes());
                        hash.update(&bytes);
                    }
                }
            }
            for row in [retirement.original(), retirement.retired()] {
                let bytes = row.to_bytes().map_err(operation)?;
                hash.update(&(bytes.len() as u64).to_be_bytes());
                hash.update(&bytes);
            }
            for time in [retirement.interval().0, retirement.interval().1] {
                hash.update(&time.to_be_bytes());
            }
            root(&mut hash, retirement.root()?);
            root(&mut hash, check.origin.prefix());
            root(&mut hash, check.origin.root());
            hash.update(&(check.origin.inspected_roots() as u64).to_be_bytes());
            hash.update(&(check.origin.dependency_count() as u64).to_be_bytes());
            hash.update(check.inputs.node.as_bytes());
            hash.update(check.writer_boot.as_bytes());
            hash.update(
                check
                    .inputs
                    .host
                    .application()
                    .registry()
                    .release_digest()
                    .as_bytes(),
            );
            let serving = &check.serving;
            hash.update(serving.owner().session.as_bytes());
            hash.update(&(serving.owner().endpoint.len() as u64).to_be_bytes());
            hash.update(serving.owner().endpoint.as_bytes());
            hash.update(&serving.position().epoch.to_be_bytes());
            hash.update(&serving.native().generation.to_be_bytes());
            hash.update(serving.native().code.as_bytes());
            hash.update(&serving.native().schema.to_be_bytes());
            hash.update(&[u8::from(check.policy_revision.is_some())]);
            if let Some(revision) = check.policy_revision {
                hash.update(&revision.to_be_bytes());
            }
            hash.update(&check.desired_readers.to_be_bytes());
            hash.update(&(check.replacements.len() as u64).to_be_bytes());
            for row in &check.replacements {
                hash.update(row.node.as_bytes());
                hash.update(row.session.as_bytes());
                hash.update(row.boot_identity.as_bytes());
                hash.update(row.enrollment_key.as_bytes());
                hash.update(row.enrollment_digest.as_bytes());
                hash.update(row.receipt.cell.as_bytes());
                hash.update(row.receipt.incarnation.as_bytes());
                hash.update(&row.receipt.commit_sequence.to_be_bytes());
            }
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
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
