//! Weak lookup of already admitted original live proofs, never an authority cache.
use super::*;
use crate::node::log::CellLogScope;
use crate::node::log_shipper::SelectedBundlePublication;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

const MAX_PREFIXES: usize = 4096;
// The fixed index fits inside the producer's existing metadata allowance.
// Hash collisions only evict an optimization; full scope equality is mandatory.
fn key(scope: CellLogScope) -> u64 {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    scope.hash(&mut hash);
    hash.finish()
}

pub(super) struct Prefixes {
    entries: HashMap<u64, Weak<Weak<SelectedBundle>>>,
}

impl Prefixes {
    pub(super) fn new() -> Result<Self> {
        let entries = HashMap::with_capacity(MAX_PREFIXES);
        let buckets = (entries.capacity() + 1).next_power_of_two();
        let bytes = storage_bytes(buckets);
        if bytes + 64 * 1024 > crate::node::bundle::LIVE_PREFIX_INDEX_BYTES {
            return Err(Error::Capacity("live prefix lookup metadata"));
        }
        Ok(Self { entries })
    }

    pub(super) fn for_captures(&self, captures: &[AssignedCapture]) -> Vec<Arc<SelectedBundle>> {
        let mut selected: Vec<Arc<SelectedBundle>> = Vec::with_capacity(captures.len());
        for capture in captures {
            let scope = capture.assignment().scope();
            if selected
                .iter()
                .any(|selected| selected.proof.scope() == scope)
            {
                continue;
            }
            if let Some(original) = self
                .entries
                .get(&key(scope))
                .and_then(Weak::upgrade)
                .and_then(|anchor| anchor.upgrade())
                .filter(|original| original.proof.scope() == scope)
            {
                selected.push(original);
            }
        }
        selected
    }

    pub(super) fn remember(&mut self, selected: &SelectedBundlePublication) {
        for original in &selected.selected {
            let scope = key(original.proof.scope());
            if self.entries.len() == MAX_PREFIXES && !self.entries.contains_key(&scope) {
                self.entries.retain(|_, proof| proof.strong_count() != 0);
            }
            if self.entries.len() < MAX_PREFIXES || self.entries.contains_key(&scope) {
                self.entries.insert(scope, original.weak_prefix());
            }
        }
    }
}

fn storage_bytes(buckets: usize) -> usize {
    let entry = std::mem::size_of::<(u64, Weak<Weak<SelectedBundle>>)>() + 1;
    // Two Arc counts plus conservative allocator/alignment overhead for every
    // expired small anchor. The large proof allocation is never retained here.
    let anchor = std::mem::size_of::<Weak<SelectedBundle>>() + 5 * std::mem::size_of::<usize>();
    buckets * entry + MAX_PREFIXES * anchor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_storage_fits_its_reserved_credit() {
        let index = Prefixes::new().unwrap();

        // Hash tables keep empty buckets beyond their reported usable capacity.
        let buckets = (index.entries.capacity() + 1).next_power_of_two();
        assert!(storage_bytes(buckets) + 64 * 1024 < crate::node::bundle::LIVE_PREFIX_INDEX_BYTES);
        let receipt_overhead = std::mem::size_of::<SelectedBundle>()
            - std::mem::size_of::<BundleCoverageProof>()
            + 8 * std::mem::size_of::<usize>();
        assert!(
            receipt_overhead <= 256,
            "receipt and anchor exceed admitted wrapper overhead"
        );
    }
}
