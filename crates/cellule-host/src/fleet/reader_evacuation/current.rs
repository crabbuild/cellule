//! Current policy/boot/native-prefix confirmation shared by donor and source checks.
use super::verification::FleetReaderEvacuationVerifier;
use crate::{
    fleet::{FleetJournal, FleetRoster},
    read_replicas::{
        ReaderReplacement,
        maintenance::{boot_identity, same_authority, validate_replacement},
    },
};
use cellule_runtime::{
    Error, Result,
    client::{CellDescription, Receipt},
    control::Control,
    fleet::operations::ReaderReplacementWitness,
    identity::CellTarget,
};
use tokio::time::Instant;

pub(super) struct CurrentReaderPolicy<'a> {
    pub target: &'a CellTarget,
    pub authority: &'a Control,
    pub minimum: Receipt,
    pub policy_revision: Option<u64>,
    pub desired_readers: u16,
    pub witnesses: &'a [ReaderReplacementWitness],
    pub operation_deadline_ms: i64,
}
impl FleetReaderEvacuationVerifier {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn confirm_current(
        &self,
        journal: &dyn FleetJournal,
        roster: &FleetRoster,
        basis: CurrentReaderPolicy<'_>,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<(Control, Vec<ReaderReplacement>)> {
        let target = basis.target;
        let minimum = basis.minimum;
        if basis.witnesses.len() != usize::from(basis.desired_readers) {
            return Err(Error::Fenced);
        }
        let expected = CellDescription {
            cell: minimum.cell,
            incarnation: minimum.incarnation,
            code: basis.authority.code,
            schema: basis.authority.schema,
        };
        let witnesses = basis.witnesses;
        let mut replacements = Vec::with_capacity(witnesses.len());
        let mut authority = basis.authority.clone();
        for _ in 0..2 {
            let now = clock()?;
            if now >= basis.operation_deadline_ms {
                return Err(Error::Deadline);
            }
            let observed = self
                .authority
                .load(minimum.cell)
                .await?
                .ok_or(Error::Fenced)?;
            let fresh = observed.value();
            if !same_authority(&authority, fresh) {
                return Err(Error::Fenced);
            }
            let policy = self.policy.load(minimum.cell).await?.map(|row| row.value());
            if policy.map(|policy| policy.revision()) != basis.policy_revision
                || policy.map_or(0, |policy| policy.desired_readers()) != basis.desired_readers
                || policy.is_some_and(|policy| {
                    policy.cell() != minimum.cell || policy.incarnation() != minimum.incarnation
                })
            {
                return Err(Error::Fenced);
            }
            let selected = self
                .directory
                .select_readers(
                    minimum.cell,
                    fresh.owner.as_ref().ok_or(Error::Fenced)?.session,
                    fresh.code,
                    witnesses.len(),
                    now,
                    10_000,
                )
                .await?;
            if selected.len() != witnesses.len() {
                return Err(Error::ReplicaUnavailable);
            }
            replacements.clear();
            for selected in selected {
                let witness = witnesses
                    .iter()
                    .find(|entry| {
                        entry.node == selected.node() && entry.session == selected.session()
                    })
                    .ok_or(Error::Fenced)?;
                let node = self
                    .directory
                    .load(witness.session, clock()?)
                    .await?
                    .ok_or(Error::Fenced)?;
                let node = node.advertisement();
                if node.node() != witness.node
                    || boot_identity(node)? != witness.boot_identity
                    || validate_replacement(roster, node, target, minimum)?
                        != (witness.enrollment_key, witness.enrollment_digest)
                {
                    return Err(Error::Fenced);
                }
                let (receipt, ready) = self.peer.status(target, node.clone(), expected).await?;
                if !ready
                    || receipt.cell != minimum.cell
                    || receipt.incarnation != minimum.incarnation
                    || receipt.commit_sequence
                        < minimum.commit_sequence.max(witness.commit_sequence)
                {
                    return Err(Error::ReplicaUnavailable);
                }
                replacements.push(ReaderReplacement {
                    node: witness.node,
                    session: witness.session,
                    boot_identity: witness.boot_identity,
                    enrollment_key: witness.enrollment_key,
                    enrollment_digest: witness.enrollment_digest,
                    receipt,
                });
            }
            authority = fresh.clone();
            // Probes can suspend; no authority/policy change is hidden behind
            // their replies or a stable native topology cursor.
            let after = self
                .authority
                .load(minimum.cell)
                .await?
                .ok_or(Error::Fenced)?;
            if !same_authority(&authority, after.value())
                || self.policy.load(minimum.cell).await?.map(|row| row.value()) != policy
            {
                return Err(Error::Fenced);
            }
            authority = after.value().clone();
            let selected = self
                .directory
                .select_readers(
                    minimum.cell,
                    authority.owner.as_ref().ok_or(Error::Fenced)?.session,
                    authority.code,
                    witnesses.len(),
                    clock()?,
                    10_000,
                )
                .await?;
            if selected.len() != witnesses.len() {
                return Err(Error::Fenced);
            }
            for node in selected {
                let witness = witnesses
                    .iter()
                    .find(|entry| entry.node == node.node() && entry.session == node.session())
                    .ok_or(Error::Fenced)?;
                let fresh = self
                    .directory
                    .load(witness.session, clock()?)
                    .await?
                    .ok_or(Error::Fenced)?;
                let fresh = fresh.advertisement();
                if fresh.node() != witness.node
                    || boot_identity(fresh)? != witness.boot_identity
                    || validate_replacement(roster, fresh, target, minimum)?
                        != (witness.enrollment_key, witness.enrollment_digest)
                {
                    return Err(Error::Fenced);
                }
            }
            roster.confirm(journal, deadline).await?;
        }
        Ok((authority, replacements))
    }
}
