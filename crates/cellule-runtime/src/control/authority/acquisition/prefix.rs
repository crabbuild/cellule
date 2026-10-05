use super::*;
use crate::recovery::manifest::PinnedRecoveryCell;

/// Exact canonical suffix materialization and verified successor origin graph.
///
/// The caller obtains the required row from the original sealed-log manifest and
/// authenticates that complete scope and its backend mappings. This observation
/// grants no current ownership, native serving, storage pin or fleet settlement.
pub struct VerifiedRecoveryPrefix {
    required: PinnedRecoveryCell,
    acquisition_epoch: u64,
    proof: VerifiedRootPrefix,
}
impl VerifiedRecoveryPrefix {
    /// Exact original manifest row, without replacing any epoch or tail boundary.
    #[must_use]
    pub const fn required(&self) -> &PinnedRecoveryCell {
        &self.required
    }
    /// Original acquisition that materialized this precise recovery input.
    #[must_use]
    pub const fn acquisition_epoch(&self) -> u64 {
        self.acquisition_epoch
    }
    /// Materialized prefix and complete current successor origin observation.
    #[must_use]
    pub const fn proof(&self) -> &VerifiedRootPrefix {
        &self.proof
    }
}

impl CellAuthority {
    /// Proves materialization of one exact original sealed recovery row, followed
    /// by verified derivation and complete current successor origin availability.
    ///
    /// Binds the original closed owner and exact suffix, then searches bounded
    /// canonical acquisitions through the selected successor epoch. Interrupted
    /// claims may materialize later; counters or a different overlay cannot
    /// substitute. Rechecks selected authority after the origin walk. The caller
    /// owns admission and a finite deadline; use the runtime wrapper for shared
    /// memory/I/O admission. Missing legacy metadata refuses with a typed error.
    pub async fn verify_recovered_prefix(
        &self,
        required: &PinnedRecoveryCell,
        root: cellule_ltx::RootRef,
        replica: &cellule_ltx::CellReplica,
        limit: usize,
    ) -> Result<VerifiedRecoveryPrefix> {
        let first = self.recovered_prefix_start(required, root, replica, limit)?;
        let selected = self.load(required.cell).await?.ok_or(Error::Fenced)?;
        if selected.value().state != ControlState::Serving {
            return Err(Error::Fenced);
        }
        self.verify_recovered_prefix_selected(required, root, replica, limit, selected, first)
            .await
    }

    /// Verifies an exact unowned Idle rollback root against its original sealed
    /// suffix and native acquisition history. This grants no ownership, actor
    /// admission or serving proof. The complete selected control must still
    /// equal the caller's observation before and after the bounded origin walk.
    pub async fn verify_recovered_idle_prefix(
        &self,
        required: &PinnedRecoveryCell,
        observed: &VersionedControl,
        replica: &cellule_ltx::CellReplica,
        limit: usize,
    ) -> Result<VerifiedRecoveryPrefix> {
        if observed.value().state != ControlState::Idle
            || observed.value().owner.is_some()
            || observed.value().recovery.is_some()
            || observed.value().cell != required.cell
        {
            return Err(Error::Fenced);
        }
        let root = observed.value().ltx_root().ok_or(Error::Fenced)?;
        let first = self.recovered_prefix_start(required, root, replica, limit)?;
        let selected = self.load(required.cell).await?.ok_or(Error::Fenced)?;
        if selected.value() != observed.value() {
            return Err(Error::Fenced);
        }
        self.verify_recovered_prefix_selected(required, root, replica, limit, selected, first)
            .await
    }

    fn recovered_prefix_start(
        &self,
        required: &PinnedRecoveryCell,
        root: cellule_ltx::RootRef,
        replica: &cellule_ltx::CellReplica,
        limit: usize,
    ) -> Result<u64> {
        if limit == 0 || limit > MAX_LINEAGE_ROOTS {
            return Err(Error::Capacity("invalid Cell root lineage traversal bound"));
        }
        if required.application.as_bytes() != self.layout.application_id()
            || required.cell.as_bytes() != &root.cell
            || required.incarnation.as_bytes() != &root.incarnation
            || replica.scope() != (root.cell, root.incarnation)
            || required.cell_epoch == 0
        {
            return Err(Error::Control("recovered prefix scope differs"));
        }
        let first = required
            .cell_epoch
            .checked_add(1)
            .ok_or(Error::Control("recovered acquisition epoch overflow"))?;
        Ok(first)
    }

    async fn verify_recovered_prefix_selected(
        &self,
        required: &PinnedRecoveryCell,
        root: cellule_ltx::RootRef,
        replica: &cellule_ltx::CellReplica,
        limit: usize,
        selected: VersionedControl,
        first: u64,
    ) -> Result<VerifiedRecoveryPrefix> {
        if selected.value().incarnation != required.incarnation
            || selected.value().ltx_root() != Some(root)
            || selected.value().epoch < first
        {
            return Err(Error::Fenced);
        }
        let count = selected.value().epoch - required.cell_epoch;
        if count > limit as u64 {
            return Err(Error::Capacity(
                "recovery acquisition search bound exceeded",
            ));
        }
        let original = self
            .owner_observation(required.cell, required.incarnation, required.cell_epoch)
            .await?
            .ok_or(Error::OwnerHistoryIncomplete {
                cell: required.cell,
                incarnation: required.incarnation,
                epoch: required.cell_epoch,
            })?;
        if original.recovery.as_ref() != Some(&required.recovery)
            || original
                .owner
                .as_ref()
                .is_none_or(|owner| owner.session != required.recovery.leader_session)
        {
            return Err(Error::Control("original owner recovery scope differs"));
        }
        // An interrupted claim can retain the same original overlay at
        // a later ownership epoch. Choose its last canonical materialization;
        // owner/counter changes cannot substitute for the exact original suffix.
        let mut materialized = None;
        let mut missing = None;
        for epoch in first..=selected.value().epoch {
            let Some(record) = self
                .acquisition_record(required.cell, required.incarnation, epoch)
                .await?
            else {
                missing.get_or_insert(epoch);
                continue;
            };
            if record.input.recovery.as_ref() != Some(&required.recovery) {
                continue;
            }
            let prefix = record
                .materialized
                .ltx_root()
                .ok_or(Error::Control("recovery materialization lacks root"))?;
            if prefix.position.txid != required.recovery.final_txid
                || prefix.position.checksum != required.recovery.final_checksum
                || prefix.commit_sequence != required.recovery.final_commit_sequence
            {
                return Err(Error::Control("recovery materialization endpoint differs"));
            }
            materialized = Some((epoch, prefix));
        }
        let (epoch, prefix) = match materialized {
            Some(value) => value,
            None => {
                return Err(match missing {
                    Some(epoch) => Error::AcquisitionHistoryIncomplete {
                        cell: required.cell,
                        incarnation: required.incarnation,
                        epoch,
                    },
                    None => Error::Control("canonical acquisition recovery input differs"),
                });
            }
        };
        let proof = self
            .verify_root_prefix(prefix, root, replica, limit)
            .await?;
        let confirmed = self.load(required.cell).await?.ok_or(Error::Fenced)?;
        if confirmed.value().owner != selected.value().owner
            || confirmed.value().epoch != selected.value().epoch
            || confirmed.value().incarnation != required.incarnation
            || confirmed.value().state != selected.value().state
            || confirmed.value().ltx_root() != Some(root)
            || (selected.value().state == ControlState::Idle
                && confirmed.value() != selected.value())
        {
            return Err(Error::Fenced);
        }
        Ok(VerifiedRecoveryPrefix {
            required: PinnedRecoveryCell {
                application: required.application,
                cell: required.cell,
                incarnation: required.incarnation,
                cell_epoch: required.cell_epoch,
                recovery: required.recovery.clone(),
            },
            acquisition_epoch: epoch,
            proof,
        })
    }
}
