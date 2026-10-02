//! Replacement-policy checks followed by the existing native reader closure.
use super::*;
use crate::fleet::FleetRoster;
use cellule_runtime::{
    cell::actor::NodeByteReservation,
    client::CellDescription,
    control::Control,
    fleet::operations::{
        EnrollmentRecord, EnrollmentRole, EnrollmentStatus, MAX_RECORD_BYTES, MaintenanceOperation,
    },
    identity::NodeId,
    node::{MAX_NODE_BYTES, NodeAdvertisement, NodeMode},
    peer::ReplicaPeerClient,
};

/// One fresh, authenticated ready reader outside the maintenance node.
/// Its receipt is an observed prefix, not authority to serve or acquire a Cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderReplacement {
    /// Exact physical replacement selected by current canonical policy.
    pub node: NodeId,
    /// Original probed boot, never replaced by another session in this result.
    pub session: SessionId,
    /// Hash of the probed signed boot's immutable identity.
    pub boot_identity: Digest,
    /// Ready native position returned through the existing peer protocol.
    pub receipt: Receipt,
}

/// Checked per-reader evacuation interval, retaining its native metadata charge.
/// This is not complete fleet role settlement. The controller must persist and
/// revalidate replacements, current authority, all remaining roles and inventory
/// barriers before terminal node shutdown.
pub struct ReaderEvacuation {
    original: EnrollmentRecord,
    retired: EnrollmentRecord,
    authority: Control,
    policy: Option<ReadPolicy>,
    minimum: Receipt,
    replacements: Vec<ReaderReplacement>,
    started_at_ms: i64,
    finished_at_ms: i64,
    _memory: [NodeByteReservation; 2],
}
impl ReaderEvacuation {
    /// Original immutable Established request supplied to this attempt.
    #[must_use]
    pub fn original(&self) -> &EnrollmentRecord {
        &self.original
    }
    /// Confirmed retirement of that exact request after canonical local joining.
    #[must_use]
    pub fn retired(&self) -> &EnrollmentRecord {
        &self.retired
    }
    /// Current Cell authority rechecked after closing, without ownership rights.
    #[must_use]
    pub fn authority(&self) -> &Control {
        &self.authority
    }
    /// Exact desired-count policy, rechecked after local retirement.
    #[must_use]
    pub const fn policy(&self) -> Option<ReadPolicy> {
        self.policy
    }
    /// Adequate prefix required from every replacement by this original attempt.
    #[must_use]
    pub const fn minimum(&self) -> Receipt {
        self.minimum
    }
    /// Fresh ready replacements on distinct physical nodes excluding the donor.
    #[must_use]
    pub fn replacements(&self) -> &[ReaderReplacement] {
        &self.replacements
    }
    /// Capture interval of this attempt. Replayed retirement retains its own
    /// original journal timestamps; fresh replacement probes have this interval.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

impl ReadReplicaManager {
    /// Evacuates one exact Established managed reader under retained maintenance.
    ///
    /// Authenticate the caller and use the current Evacuating operation. The
    /// normal live-owner recruiter creates replacements. This method probes them
    /// through the existing authenticated status protocol; no ready spare means
    /// no new local close. Both local and signed boot modes must be Draining.
    /// Canonical view closure and producer retirement use the same activation
    /// lane as ordinary removal. A deadline/cancelled waiter can leave a joined
    /// or fenced original view; retry the same request to resume its ownership.
    /// A changed policy, owner, boot or unresolved journal result yields no proof.
    pub async fn evacuate(
        &self,
        original: &EnrollmentRecord,
        operation: &MaintenanceOperation,
        peer: &ReplicaPeerClient,
        deadline: tokio::time::Instant,
    ) -> Result<ReaderEvacuation> {
        let deadline = evacuation_deadline(operation.deadline_ms(), deadline)?;
        tokio::time::timeout_at(
            deadline,
            self.evacuate_open(original, operation, peer, deadline),
        )
        .await
        .map_err(|source| Error::Facility {
            name: "reader-evacuation-deadline",
            source: Box::new(source),
        })?
    }

    async fn evacuate_open(
        &self,
        original: &EnrollmentRecord,
        operation: &MaintenanceOperation,
        peer: &ReplicaPeerClient,
        deadline: tokio::time::Instant,
    ) -> Result<ReaderEvacuation> {
        // Reserve before encoding or collecting authority and original-record
        // copies. These charges stay with the returned evidence after closure.
        let records = self
            .runtime
            .try_reserve_node_metadata_bytes(3 * MAX_RECORD_BYTES as usize + 4096)?;
        original.to_bytes().map_err(crate::fleet::operation)?;
        let EnrollmentRole::Reader { target, position } = &original.spec().role else {
            return Err(Error::Fenced);
        };
        if original.status() != EnrollmentStatus::Established
            || original.established_evidence().is_none()
            || original.spec().target.session != self.session
            || original.spec().target.node != operation.node()
            || self.session != operation.session()
            || target.application().as_bytes() != self.layout.application_id()
            || self.runtime.node_admission().mode()? != NodeMode::Draining
        {
            return Err(Error::Fenced);
        }
        let enrollment = self.bound_enrollment()?.ok_or(Error::Control(
            "reader evacuation requires managed enrollment",
        ))?;
        let _activation = self.activation.lock().await;
        self.ensure_open()?;
        let started_at_ms = now_ms()?;
        let roster = enrollment
            .maintenance_roster(original, operation, deadline, &self.runtime)
            .await?;
        let current = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(Error::CellNotActive)?;
        let authority = current.value().clone();
        let root = authority.root.as_ref().ok_or(Error::CellNotActive)?;
        let owner = authority.owner.as_ref().ok_or(Error::CellNotActive)?;
        if authority.state != ControlState::Serving
            || authority.recovery.is_some()
            || authority.incarnation != position.incarnation
        {
            return Err(Error::Fenced);
        }
        let local = self
            .active
            .read()
            .await
            .views
            .get(&target.cell_id())
            .cloned();
        let mut minimum = Receipt {
            cell: target.cell_id(),
            incarnation: authority.incarnation,
            commit_sequence: position.root.commit_sequence.max(root.commit_sequence),
        };
        if let Some(reader) = &local {
            let observed = reader.lifecycle_observation().await;
            let receipt = observed.receipt();
            if receipt.cell != minimum.cell || receipt.incarnation != minimum.incarnation {
                return Err(Error::Fenced);
            }
            minimum.commit_sequence = minimum.commit_sequence.max(receipt.commit_sequence);
        } else if roster
            .enrollments()
            .iter()
            .find(|row| row.spec() == original.spec())
            .is_none_or(|row| row.status() != EnrollmentStatus::Retired)
        {
            return Err(Error::Control(
                "reader evacuation lacks original local closure",
            ));
        }
        let policy = self.policy.load(minimum.cell).await?.map(|row| row.value());
        if policy.is_some_and(|policy| policy.incarnation() != minimum.incarnation) {
            return Err(Error::Fenced);
        }
        let desired = usize::from(policy.map_or(0, |policy| policy.desired_readers()));
        let memory = self.runtime.try_reserve_node_metadata_bytes(
            desired * std::mem::size_of::<ReaderReplacement>() + 4096,
        )?;
        // The canonical node codec bounds retained candidate bodies. Account
        // those temporary observations separately from the final fixed rows.
        let _candidates = self.runtime.try_reserve_node_metadata_bytes(
            (desired + 1)
                * (2 * MAX_NODE_BYTES as usize + std::mem::size_of::<NodeAdvertisement>())
                + 4096,
        )?;
        let own = self
            .directory
            .load(self.session, now_ms()?)
            .await?
            .ok_or(Error::Fenced)?;
        let own = own.advertisement();
        if !enrollment.matches_boot(own)
            || own
                .operational_sample()
                .is_none_or(|s| s.mode != NodeMode::Draining)
            || roster.boot(own.node(), own.session())?.intent().mode() != NodeMode::Draining
        {
            return Err(Error::Fenced);
        }
        let selected = self
            .directory
            .select_readers(
                minimum.cell,
                owner.session,
                authority.code,
                desired,
                now_ms()?,
                MAX_LIVE_NODES,
            )
            .await?;
        if selected.len() != desired || selected.iter().any(|ad| ad.node() == operation.node()) {
            return Err(Error::Capacity(
                "reader maintenance replacements unavailable",
            ));
        }
        let expected = CellDescription {
            cell: minimum.cell,
            incarnation: minimum.incarnation,
            code: authority.code,
            schema: authority.schema,
        };
        let mut replacements = Vec::with_capacity(desired);
        for node in selected {
            validate_replacement(&roster, &node, target, minimum)?;
            let (receipt, ready) = peer.status(target, node.clone(), expected).await?;
            if !ready
                || receipt.cell != minimum.cell
                || receipt.incarnation != minimum.incarnation
                || receipt.commit_sequence < minimum.commit_sequence
            {
                return Err(Error::ReplicaUnavailable);
            }
            replacements.push(ReaderReplacement {
                node: node.node(),
                session: node.session(),
                boot_identity: boot_identity(&node)?,
                receipt,
            });
        }
        enrollment
            .confirm_maintenance_roster(&roster, deadline)
            .await?;
        self.recheck_replacements(target, &authority, policy, &replacements)
            .await?;
        drop(roster);
        self.remove_locked(minimum.cell).await?;
        if let Some(reader) = &local {
            let closed = reader.lifecycle_observation().await;
            let receipt = closed.receipt();
            if !closed.locally_joined()
                || receipt.cell != minimum.cell
                || receipt.incarnation != minimum.incarnation
            {
                return Err(Error::Fenced);
            }
            // A retained peer clone may finish a refresh after the first probe.
            // Every final replacement must also cover this real closed prefix.
            minimum.commit_sequence = minimum.commit_sequence.max(receipt.commit_sequence);
        }
        let after = enrollment
            .maintenance_roster(original, operation, deadline, &self.runtime)
            .await?;
        let retired = after
            .enrollments()
            .iter()
            .find(|row| row.spec() == original.spec())
            .filter(|row| row.status() == EnrollmentStatus::Retired)
            .cloned()
            .ok_or(Error::Control(
                "reader evacuation retirement is unconfirmed",
            ))?;
        for replacement in &mut replacements {
            let node = self
                .directory
                .load(replacement.session, now_ms()?)
                .await?
                .ok_or(Error::Fenced)?;
            let node = node.advertisement();
            if node.node() != replacement.node || boot_identity(node)? != replacement.boot_identity
            {
                return Err(Error::Fenced);
            }
            validate_replacement(&after, node, target, minimum)?;
            let (receipt, ready) = peer.status(target, node.clone(), expected).await?;
            if !ready
                || receipt.cell != minimum.cell
                || receipt.incarnation != minimum.incarnation
                || receipt.commit_sequence < minimum.commit_sequence
            {
                return Err(Error::ReplicaUnavailable);
            }
            replacement.receipt = receipt;
        }
        let current = self
            .recheck_replacements(target, &authority, policy, &replacements)
            .await?;
        enrollment
            .confirm_maintenance_roster(&after, deadline)
            .await?;
        Ok(ReaderEvacuation {
            original: original.clone(),
            retired,
            authority: current,
            policy,
            minimum,
            replacements,
            started_at_ms,
            finished_at_ms: now_ms()?,
            _memory: [records, memory],
        })
    }

    async fn recheck_replacements(
        &self,
        target: &CellTarget,
        before: &Control,
        policy: Option<ReadPolicy>,
        replacements: &[ReaderReplacement],
    ) -> Result<Control> {
        let current = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(Error::Fenced)?;
        let current = current.value();
        // Publication may advance while a foreign writer serves traffic. Its
        // exact lifetime and nonregressing published prefix must remain bound.
        if current.state != ControlState::Serving
            || current.recovery.is_some()
            || current.incarnation != before.incarnation
            || current.epoch != before.epoch
            || current.owner != before.owner
            || current.code != before.code
            || current.schema != before.schema
            || current.root.as_ref().is_none_or(|root| {
                before
                    .root
                    .as_ref()
                    .is_none_or(|old| root.commit_sequence < old.commit_sequence)
            })
            || self.policy.load(current.cell).await?.map(|row| row.value()) != policy
        {
            return Err(Error::Fenced);
        }
        let owner = current.owner.as_ref().ok_or(Error::Fenced)?;
        let selected = self
            .directory
            .select_readers(
                current.cell,
                owner.session,
                current.code,
                replacements.len(),
                now_ms()?,
                MAX_LIVE_NODES,
            )
            .await?;
        if selected.len() != replacements.len() {
            return Err(Error::Fenced);
        }
        for (node, replacement) in selected.iter().zip(replacements) {
            let fresh = self
                .directory
                .load(node.session(), now_ms()?)
                .await?
                .ok_or(Error::Fenced)?;
            let node = fresh.advertisement();
            if node.node() != replacement.node
                || node.session() != replacement.session
                || boot_identity(node)? != replacement.boot_identity
                || !node.accepts_new_roles(now_ms()?)
            {
                return Err(Error::Fenced);
            }
        }
        Ok(current.clone())
    }
}

fn evacuation_deadline(
    operation_deadline_ms: i64,
    requested: tokio::time::Instant,
) -> Result<tokio::time::Instant> {
    let captured = tokio::time::Instant::now();
    let remaining = operation_deadline_ms
        .checked_sub(now_ms()?)
        .filter(|remaining| *remaining > 0)
        .ok_or(Error::Node("reader maintenance operation deadline elapsed"))?;
    if captured >= requested {
        return Err(Error::Node("reader evacuation deadline elapsed"));
    }
    let limit = captured
        .checked_add(Duration::from_millis(remaining as u64))
        .ok_or(Error::Node("reader maintenance deadline overflow"))?;
    Ok(requested.min(limit))
}

fn validate_replacement(
    roster: &FleetRoster,
    node: &NodeAdvertisement,
    target: &CellTarget,
    minimum: Receipt,
) -> Result<()> {
    if node.fleet() != roster.snapshot().head().scope().fleet
        || !node.accepts_new_roles(now_ms()?)
        || roster.boot(node.node(), node.session())?.intent().mode() != NodeMode::Active
    {
        return Err(Error::Fenced);
    }
    if !roster.enrollments().iter().any(|row| {
        row.status() == EnrollmentStatus::Established
            && row.spec().target.node == node.node()
            && row.spec().target.session == node.session()
            && matches!(&row.spec().role, EnrollmentRole::Reader {target: enrolled,position}
            if enrolled == target && position.incarnation == minimum.incarnation)
    }) {
        return Err(Error::Control(
            "reader replacement lacks Established enrollment",
        ));
    }
    Ok(())
}

fn boot_identity(node: &NodeAdvertisement) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.reader-maintenance-boot.v1\0");
    hash.update(node.node().as_bytes());
    hash.update(node.session().as_bytes());
    hash.update(node.fleet().as_bytes());
    hash.update(node.certificate().as_bytes());
    hash.update(node.image().as_bytes());
    hash.update(node.release().as_bytes());
    hash.update(&node.verifying_key()?.to_bytes());
    hash.update(node.endpoint().as_bytes());
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
