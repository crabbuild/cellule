//! Exact native assignment verification and shared authority selection.
use super::*;
use crate::node::lease::NodeLeaseGuard;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};

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
        let mut catalog = load_catalog(&self.layout, observed.advertisement.session, head).await?;
        if frames.is_empty() || frames.len() > MAX_FRAMES {
            return Err(Error::Capacity("bundle frame count"));
        }
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
        self.upload_catalog(Some(head), catalog, frames).await
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
        let catalog = load_catalog(&self.layout, prepared.catalog.session, prepared.head).await?;
        let mut proofs = Vec::new();
        for binding in catalog.bindings {
            if !binding
                .locators
                .iter()
                .any(|locator| locator.object == Some(prepared.head.digest))
            {
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
            });
        }
        lease.check()?;
        let selected = self.select_catalog(observed, prepared, now_ms).await?;
        // Expiry during verification must not revive the original writer.
        lease.check()?;
        Ok((selected, proofs))
    }
}
