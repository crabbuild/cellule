//! Complete original capture validation and atomic receipt-memory transfer.
use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger, ResourceReservation};
use crate::node::bundle::BundleCoverageProof;
use crate::node::log_shipper::{AssignedCapture, SelectedBundle, SelectedBundlePublication};

pub(super) struct SelectedCaptures<'a> {
    captures: &'a [AssignedCapture],
    proofs: Vec<BundleCoverageProof>,
    bytes: Vec<usize>,
    total: usize,
}

impl<'a> SelectedCaptures<'a> {
    pub(super) fn new(
        durability: &NodeDurability,
        captures: &'a [AssignedCapture],
        proofs: Vec<BundleCoverageProof>,
    ) -> Result<Self> {
        durability.node_lease.check()?;
        if captures.is_empty() || captures.len() > 64 || proofs.len() > 64 {
            return Err(Error::Capacity("selected capture cohort"));
        }
        let assignments = proofs
            .iter()
            .try_fold(0_usize, |sum, proof| {
                sum.checked_add(proof.assignment_count())
            })
            .ok_or(Error::Capacity("selected capture assignments"))?;
        if assignments != captures.len() || proofs.iter().any(|proof| proof.assignment_count() == 0)
        {
            return Err(Error::Node("selection omits original captured assignments"));
        }
        for (index, capture) in captures.iter().enumerate() {
            let assignment = capture.assignment();
            if captures[..index]
                .iter()
                .any(|before| before.assignment() == assignment)
                || proofs
                    .iter()
                    .filter(|proof| proof.contains_assignment(&assignment))
                    .count()
                    != 1
            {
                return Err(Error::Node(
                    "selection differs from original captured cohort",
                ));
            }
        }
        let bytes = proofs
            .iter()
            .map(BundleCoverageProof::retained_metadata_bytes)
            .collect::<Result<Vec<_>>>()?;
        let total = bytes
            .iter()
            .try_fold(0_usize, |sum, bytes| sum.checked_add(*bytes))
            .ok_or(Error::Capacity("selected bundle metadata"))?;
        Ok(Self {
            captures,
            proofs,
            bytes,
            total,
        })
    }

    pub(super) fn resources<'b>(
        &self,
        durability: &'b NodeDurability,
    ) -> Result<&'b ResourceLedger> {
        durability.selection_resources.get().ok_or(Error::Node(
            "bundle publication has no installed runtime resource ledger",
        ))
    }

    pub(super) fn cost(&self) -> ResourceCost {
        ResourceCost::zero().with_retained_bytes(self.total)
    }

    pub(super) fn confirm(
        self,
        durability: &NodeDurability,
        mut memory: ResourceReservation,
    ) -> Result<SelectedBundlePublication> {
        // Transfer a single cohort admission before waking any sibling. Waiting
        // for credit may have joined older checkpoints, so revalidate the
        // original lease/gate against these exact live proofs after the wait.
        let memories = self
            .bytes
            .into_iter()
            .map(|bytes| memory.split_retained(bytes))
            .collect::<Result<Vec<_>>>()?;
        let through = crate::node::bundle::confirm_selected_coverage(
            &durability.gate,
            &durability.node_lease,
            &self.proofs,
        )?;
        let selected = self
            .proofs
            .into_iter()
            .zip(memories)
            .map(|(proof, memory)| {
                Arc::new(SelectedBundle {
                    proof,
                    _memory: memory,
                })
            })
            .collect::<Vec<_>>();
        for capture in self.captures {
            let proof = selected
                .iter()
                .find(|selected| selected.proof.contains_assignment(&capture.assignment()))
                .ok_or(Error::Node("selected capture lost original assignment"))?;
            capture.confirm_selection(Arc::clone(proof));
        }
        Ok(SelectedBundlePublication { through, selected })
    }
}
