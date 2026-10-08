//! Exact native assignment verification and shared authority selection.
use super::*;
use crate::node::lease::NodeLeaseGuard;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};

#[derive(Clone)]
pub(super) struct LiveBundleCoverage {
    node: crate::identity::NodeId,
    lease: NodeLeaseGuard,
    assignments: Vec<crate::node::log::AssignedCommitRange>,
}

impl LiveBundleCoverage {
    pub(super) fn check_assignment(
        &self,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> Result<()> {
        self.lease.check()?;
        if !self.contains_assignment(assignment) {
            return Err(Error::Node("bundle lacks original assigned capture"));
        }
        Ok(())
    }

    pub(super) fn assignment_count(&self) -> usize {
        self.assignments.len()
    }

    pub(super) fn contains_assignment(
        &self,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> bool {
        self.assignments.contains(assignment)
    }

    pub(super) fn retained_metadata_bytes(&self) -> usize {
        self.assignments.capacity() * std::mem::size_of::<crate::node::log::AssignedCommitRange>()
    }
}

impl BundleCoverageProof {
    /// Narrows an original selected cohort to one complete original capture,
    /// retaining the authenticated historical prefix and the original lease.
    /// An endpoint alone cannot create this capability.
    pub(crate) fn original_capture_prefix(
        &self,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> Result<Self> {
        self.check_live_assignment(assignment)?;
        let live = self.live.as_ref().ok_or(Error::Fenced)?;
        let index = live
            .assignments
            .iter()
            .position(|a| a == assignment)
            .ok_or(Error::Node("bundle lacks original assigned capture"))?;
        let count = |a: &crate::node::log::AssignedCommitRange| {
            usize::try_from(a.ticket().last_sequence() - a.ticket().first_sequence() + 1)
                .map_err(|_| Error::Capacity("bundle capture prefix"))
        };
        let frames = live.assignments.iter().try_fold(0_usize, |n, a| {
            n.checked_add(count(a)?)
                .ok_or(Error::Capacity("bundle capture prefix"))
        })?;
        let retained = live.assignments[..=index]
            .iter()
            .try_fold(0_usize, |n, a| {
                n.checked_add(count(a)?)
                    .ok_or(Error::Capacity("bundle capture prefix"))
            })?;
        let historical = self
            .binding
            .locators
            .len()
            .checked_sub(frames)
            .ok_or(Error::Node("bundle capture prefix omits assigned frames"))?;
        let mut binding = self.binding.clone();
        binding.locators.truncate(historical + retained);
        let (first, commit, position) = assignment.endpoint();
        binding.first_commit = first;
        binding.selected_commit = commit;
        binding.selected_position = position;
        binding.selected_sequence = assignment.ticket().last_sequence();
        let mut live = live.clone();
        live.assignments.truncate(index + 1);
        Ok(Self {
            pin: self.pin,
            binding,
            head: self.head,
            session: self.session,
            live: Some(live),
        })
    }
}

impl NodeDirectory {
    /// Verifies a contiguous complete native range and uploads one proposal for
    /// every participating Cell. Neither upload nor this value grants an ACK.
    pub async fn prepare_node_bundle(
        &self,
        observed: &VersionedNodeAdvertisement,
        frames: &[cellule_ltx::VerifiedNodeFrame],
        assignments: &[crate::node::log::AssignedCommitRange],
        now_ms: i64,
    ) -> Result<PreparedNodeBundle> {
        self.validate(&observed.advertisement, now_ms)?;
        let head = observed
            .advertisement
            .bundle
            .ok_or(Error::Node("bundle lane is absent"))?;
        if frames.is_empty() || frames.len() > MAX_FRAMES {
            return Err(Error::Capacity("bundle frame count"));
        }
        let cells = frames
            .iter()
            .map(|frame| {
                let scope = frame.scope();
                (scope.application, scope.cell)
            })
            .collect();
        let mut catalog =
            store::load_catalog_cells(&self.layout, observed.advertisement.session, head, &cells)
                .await?;
        let mut consumed = 0_usize;
        for assignment in assignments {
            let count = usize::try_from(
                assignment.ticket().last_sequence() - assignment.ticket().first_sequence() + 1,
            )
            .map_err(|_| Error::Capacity("assigned range count"))?;
            let end = consumed
                .checked_add(count)
                .ok_or(Error::Capacity("assigned range count"))?;
            assignment.verify(
                frames
                    .get(consumed..end)
                    .ok_or(Error::Node("bundle omits assigned capture"))?,
            )?;
            consumed = end;
        }
        if consumed != frames.len() {
            return Err(Error::Node("bundle contains unassigned native frames"));
        }
        for frame in frames {
            let scope = frame.scope();
            if scope.leader_session != *catalog.session.as_bytes()
                || scope.log_epoch != catalog.epoch
                || catalog.selected_through.checked_add(1) != Some(scope.node_sequence)
            {
                return Err(Error::Node(
                    "bundle native range is not contiguous in its lane",
                ));
            }
            let binding = catalog
                .bindings
                .iter_mut()
                .find(|binding| {
                    binding.application.as_bytes() == &scope.application
                        && binding.control.cell.as_bytes() == &scope.cell
                        && binding.control.incarnation.as_bytes() == &scope.incarnation
                        && binding.control.epoch == scope.cell_epoch
                })
                .ok_or(Error::Node("bundle row has no enrolled Cell binding"))?;
            if !matches!(binding.phase, BindingPhase::Open | BindingPhase::Closing)
                || binding.locators.len() >= MAX_LOCATORS
                || binding
                    .terminal
                    .is_some_and(|(sequence, commit, position)| {
                        scope.node_sequence > sequence
                            || scope.commit_sequence > commit
                            || frame.segment().max_txid > position.txid
                    })
            {
                return Err(Error::PendingPublication);
            }
            let continuation = binding.selected_commit == scope.commit_sequence;
            if (continuation && binding.first_commit != frame.first_commit_sequence())
                || (!continuation
                    && binding.selected_commit.checked_add(1)
                        != Some(frame.first_commit_sequence()))
                || binding.selected_position.txid.checked_add(1) != Some(frame.segment().min_txid)
                || binding.selected_position.checksum != frame.segment().pre_checksum
            {
                return Err(Error::Node(
                    "bundle Cell transaction or command range has a gap",
                ));
            }
            binding.first_commit = frame.first_commit_sequence();
            binding.selected_commit = scope.commit_sequence;
            binding.selected_position = frame.segment().position();
            binding.selected_sequence = scope.node_sequence;
            binding.locators.push(Locator {
                object: None,
                offset: 0,
                bytes: frame.encoded().len() as u64,
                frame_digest: Digest::from_bytes(frame.digest()),
            });
            catalog.selected_through = scope.node_sequence;
        }
        let mut prepared = self.upload_catalog(Some(head), catalog, frames).await?;
        prepared.assignments = assignments.to_vec();
        Ok(prepared)
    }

    /// Selects exactly the uploaded catalog/range after I/O, under the original
    /// live node lease. A changed catalog/head forces a new preparation; an
    /// unchanged head may rebase against heartbeat-only CAS conflicts.
    pub async fn select_node_bundle(
        &self,
        observed: &VersionedNodeAdvertisement,
        prepared: &PreparedNodeBundle,
        lease: &NodeLeaseGuard,
        limits: cellule_ltx::Limits,
        now_ms: i64,
    ) -> Result<(VersionedNodeAdvertisement, Vec<BundleCoverageProof>)> {
        lease.check()?;
        if prepared.assignments.is_empty() {
            return Err(Error::Node("native bundle has no complete assignments"));
        }
        let cells = prepared
            .catalog
            .bindings
            .iter()
            .filter(|binding| {
                binding
                    .locators
                    .iter()
                    .any(|locator| locator.object.is_none())
            })
            .map(|binding| {
                (
                    *binding.application.as_bytes(),
                    *binding.control.cell.as_bytes(),
                )
            })
            .collect();
        let catalog = store::load_catalog_cells(
            &self.layout,
            prepared.catalog.session,
            prepared.head,
            &cells,
        )
        .await?;
        let mut proofs = Vec::new();
        for binding in catalog.bindings {
            if !cells.contains(&(
                *binding.application.as_bytes(),
                *binding.control.cell.as_bytes(),
            )) {
                continue;
            }
            verify_binding(
                &self.layout,
                catalog.session,
                catalog.epoch,
                &binding,
                limits,
            )
            .await?;
            let pin = binding
                .control
                .bundle_binding
                .ok_or(Error::Node("selected binding lacks pin"))?;
            proofs.push(BundleCoverageProof {
                pin,
                binding,
                head: prepared.head,
                session: catalog.session,
                live: None,
            });
        }
        lease.check()?;
        let selected = self
            .select_native_catalog(observed, prepared, now_ms)
            .await?;
        // Expiry during verification must not revive the original writer.
        lease.check()?;
        // A boot without enrolled native log state supplies reconstruction
        // only. Enrollment and the selected coverage frontier are prerequisites
        // for waking this process's original durability gate.
        if selected.advertisement.log.is_some() {
            for proof in &mut proofs {
                let scope = crate::node::log::CellLogScope {
                    application: proof.binding.application,
                    cell: proof.binding.control.cell,
                    incarnation: proof.binding.control.incarnation,
                    cell_epoch: proof.binding.control.epoch,
                };
                let assignments = prepared
                    .assignments
                    .iter()
                    .filter(|assignment| assignment.scope() == scope)
                    .copied()
                    .collect::<Vec<_>>();
                if assignments.is_empty() {
                    return Err(Error::Node("selected binding lacks complete assignments"));
                }
                proof.live = Some(LiveBundleCoverage {
                    node: selected.advertisement.node,
                    lease: lease.clone(),
                    assignments,
                });
            }
        }
        Ok((selected, proofs))
    }
}

pub(crate) fn confirm_selected_coverage(
    gate: &crate::node::log::DurabilityGate,
    lease: &NodeLeaseGuard,
    proofs: &[BundleCoverageProof],
) -> Result<u64> {
    if proofs.is_empty() {
        return Err(Error::Node("empty selected bundle coverage"));
    }
    lease.check()?;
    let (session, node, epoch) = gate.identity()?;
    let mut assignments = Vec::new();
    for proof in proofs {
        let live = proof
            .live
            .as_ref()
            .ok_or(Error::Node("bundle proof grants reconstruction only"))?;
        if live.node != node
            || proof.session != session
            || proof.head.epoch != epoch
            || !live.lease.same_lease(lease)
        {
            return Err(Error::Fenced);
        }
        live.lease.check()?;
        assignments.extend_from_slice(&live.assignments);
    }
    // Validate every original capture before waking any sibling. The canonical
    // selection already persisted coverage; this is local confirmation only.
    let through = gate.confirm_bundle_ranges(&assignments)?;
    lease.check()?;
    Ok(through)
}
