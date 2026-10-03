//! Canonical verified-preparation links, independent of ownership authority.
use super::*;
use cellule_ltx::{CellReplica, PreparedRoot, RootRef};
use std::collections::BTreeMap;

mod codec;
#[cfg(test)]
mod tests;
mod verify;
pub use verify::VerifiedRootPrefix;

const MAX_LINEAGE_BYTES: u64 = 8 * 1024;
const MAX_PREDECESSORS: usize = 64;
pub(crate) const MAX_LINEAGE_ROOTS: usize = 10_000;

/// Retained verified preparations that produced one exact immutable root.
///
/// Only opaque native `PreparedRoot` values add links. Multiple verified inputs
/// can produce identical root bytes, including representation-only compaction.
/// Links accumulate by ETag CAS; delayed writers cannot replace existing links.
/// A proposal need not have won authority. Starting from a separately selected
/// root proves verified derivation, never which proposal won ownership or serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellRootLineage {
    root: RootRef,
    predecessors: Vec<RootRef>,
}
impl CellRootLineage {
    /// Exact root whose verified preparations are retained.
    #[must_use]
    pub const fn root(&self) -> RootRef {
        self.root
    }
    /// Every retained distinct input, in strictly increasing digest order.
    #[must_use]
    pub fn predecessors(&self) -> &[RootRef] {
        &self.predecessors
    }
    fn validate(&self) -> Result<()> {
        validate_root(&self.root)?;
        if self.predecessors.len() > MAX_PREDECESSORS {
            return Err(Error::Capacity(
                "Cell root lineage predecessor bound exceeded",
            ));
        }
        for (index, parent) in self.predecessors.iter().enumerate() {
            validate_root(parent)?;
            if parent.cell != self.root.cell
                || parent.incarnation != self.root.incarnation
                || parent.digest == self.root.digest
                || parent.position.txid > self.root.position.txid
                || (parent.position.txid == self.root.position.txid
                    && parent.position.checksum != self.root.position.checksum)
                || parent.commit_sequence > self.root.commit_sequence
                || (parent.commit_sequence == self.root.commit_sequence
                    && parent.position != self.root.position)
                || (index > 0 && self.predecessors[index - 1].digest >= parent.digest)
            {
                return Err(Error::Control("invalid Cell root lineage predecessor"));
            }
        }
        Ok(())
    }
}
fn validate_root(root: &RootRef) -> Result<()> {
    if root.position.txid == 0
        || root.position.checksum & cellule_ltx::types::CHECKSUM_FLAG == 0
        || root.commit_sequence > i64::MAX as u64
    {
        return Err(Error::Control("invalid Cell root lineage position"));
    }
    Ok(())
}

impl CellAuthority {
    /// Bounded origin read of retained verified preparations, without authority.
    /// Missing legacy/manual-publication metadata remains `None`.
    pub async fn root_lineage(&self, root: RootRef) -> Result<Option<CellRootLineage>> {
        Ok(self
            .load_root_lineage(root)
            .await?
            .map(|(record, _)| record))
    }

    async fn load_root_lineage(&self, root: RootRef) -> Result<Option<(CellRootLineage, ETag)>> {
        validate_root(&root)?;
        let path = self
            .layout
            .root_lineage_path(&root.cell, &root.incarnation, &root.digest);
        let (body, token) = match self
            .layout
            .store()
            .get_with_etag_bounded(&path, MAX_LINEAGE_BYTES)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(source.into()),
        };
        let record = CellRootLineage::decode(&body)?;
        if record.root != root {
            return Err(Error::Control(
                "Cell root lineage path differs from its root",
            ));
        }
        Ok(Some((record, token)))
    }

    pub(crate) async fn retain_root_lineage(&self, prepared: &PreparedRoot) -> Result<()> {
        self.retain_verified_link(prepared.root(), prepared.predecessor())
            .await
    }

    // Private: only the opaque PreparedRoot factory supplies production links.
    async fn retain_verified_link(&self, root: RootRef, parent: Option<RootRef>) -> Result<()> {
        validate_root(&root)?;
        if parent == Some(root) {
            return Ok(());
        }
        let previous = self.load_root_lineage(root).await?;
        let mut record = previous.as_ref().map_or_else(
            || CellRootLineage {
                root,
                predecessors: Vec::new(),
            },
            |(record, _)| record.clone(),
        );
        if let Some(parent) = parent {
            match record
                .predecessors
                .binary_search_by_key(&parent.digest, |root| root.digest)
            {
                Ok(index) if record.predecessors[index] == parent => return Ok(()),
                Ok(_) => return Err(Error::Control("Cell root lineage digest changed position")),
                Err(index) => record.predecessors.insert(index, parent),
            }
        } else if previous.is_some() {
            return Ok(());
        }
        let body = Bytes::from(record.encode()?);
        let path = self
            .layout
            .root_lineage_path(&root.cell, &root.incarnation, &root.digest);
        let written = match previous {
            Some((_, token)) => self.layout.store().update(&path, body, token).await,
            None => {
                self.layout
                    .store()
                    .create_strict_with_etag(&path, body)
                    .await
            }
        };
        match written {
            Ok(_) => Ok(()),
            Err(source) => {
                // Accept only a confirmed equal/superset link. No metadata loop
                // can outlive the existing publisher/acquisition work owner.
                if let Ok(Some(current)) = self.root_lineage(root).await
                    && parent.is_none_or(|parent| current.predecessors.contains(&parent))
                {
                    return Ok(());
                }
                Err(source.into())
            }
        }
    }
}
