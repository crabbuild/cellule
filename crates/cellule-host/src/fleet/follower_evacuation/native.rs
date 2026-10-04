//! Fresh opaque native inventories through the canonical snapshot task owner.
use super::*;
use crate::NodeDurabilitySupervisorState;
use cellule_runtime::{
    Error, Result,
    identity::{NodeId, SessionId},
    node::NodeMode,
};
use tokio::time::Instant;

impl FleetFollowerEvacuationVerifier {
    pub(super) async fn collect_native(
        &self,
        roster: &FleetRoster,
        record: &FollowerEvacuationRecord,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<Vec<FleetNodeInventory>> {
        let source = record.source().map_err(operation)?;
        let mut inventories = Vec::with_capacity(3);
        inventories.push(
            self.inventory(roster, source.node, source.session, deadline, clock)
                .await?,
        );
        let leader = &inventories[0];
        if leader.mode() != NodeMode::Active
            || !leader.bindings().follower_producer
            || !leader.bindings().durability_supervisor
            || leader.node_log() != Some((source.session, source.node, record.replacement_epoch()))
            || leader
                .follower_producer_state()
                .is_none_or(|(busy, draining, pending)| busy || draining || pending.is_some())
            || leader.supervisor().is_none_or(|supervisor| {
                supervisor.state != NodeDurabilitySupervisorState::Running
                    || supervisor.cancellation_requested
                    || supervisor.supervisor_error.is_some()
                    || supervisor.requests_error.is_some()
            })
        {
            return Err(Error::Fenced);
        }
        leader.validate_enrollments(roster)?;
        let progress = leader
            .follower_enrollments()
            .iter()
            .find(|progress| progress.epoch == record.replacement_epoch())
            .ok_or(Error::Fenced)?;
        if !progress.delivered
            || !progress.native_started
            || progress.native_closed
            || progress.refusal.is_some()
            || progress.enrollment.is_none()
            || progress.members.len() != record.replacements().len()
            || progress
                .members
                .iter()
                .zip(record.replacements())
                .any(|(member, entry)| {
                    member.spec != *entry.enrollment.spec()
                        || !member.published
                        || member.accepted.as_ref().is_none_or(|accepted| {
                            accepted.accepted_at_ms() != entry.enrollment.accepted_at_ms()
                        })
                })
        {
            return Err(Error::Fenced);
        }
        for entry in record.replacements() {
            let endpoint = entry.enrollment.spec().target;
            let receiver = self
                .inventory(roster, endpoint.node, endpoint.session, deadline, clock)
                .await?;
            if receiver.mode() != NodeMode::Active
                || !receiver.bindings().follower_store
                || receiver
                    .follower_store_state()
                    .is_none_or(|(_, quarantined)| quarantined != 0)
            {
                return Err(Error::Fenced);
            }
            // The original native producer and current canonical complete ensemble
            // account for a member before its first persisted append lane exists.
            receiver.validate_enrollments_with(roster, |row| {
                record
                    .replacements()
                    .iter()
                    .any(|entry| &entry.enrollment == row)
            })?;
            inventories.push(receiver);
        }
        Ok(inventories)
    }
    async fn inventory(
        &self,
        roster: &FleetRoster,
        node: NodeId,
        session: SessionId,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<FleetNodeInventory> {
        let mut scan = FleetNodeInventoryScan::new(roster, node, session)?;
        while let Some(subject) = scan.next_subject()? {
            let request = self.request(roster, node, session, subject, deadline, clock)?;
            let page = self
                .transport
                .capture(&request, deadline)
                .await
                .map_err(|source| Error::Facility {
                    name: "fleet-native-snapshot-transport",
                    source,
                })?;
            scan.accept(&request, &page, clock()?)?;
        }
        scan.finish()
    }
    pub(super) async fn recheck_native(
        &self,
        roster: &FleetRoster,
        inventories: &mut [FleetNodeInventory],
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<()> {
        for inventory in inventories {
            let node = inventory.node();
            let session = inventory.session();
            let mut recheck = inventory.recheck();
            while let Some(subject) = recheck.next_subject()? {
                let request = self.request(roster, node, session, subject, deadline, clock)?;
                let page = self
                    .transport
                    .capture(&request, deadline)
                    .await
                    .map_err(|source| Error::Facility {
                        name: "fleet-native-snapshot-transport",
                        source,
                    })?;
                recheck.accept(&request, &page, clock()?)?;
            }
            recheck.finish()?;
        }
        Ok(())
    }
    fn request(
        &self,
        roster: &FleetRoster,
        node: NodeId,
        session: SessionId,
        subject: FleetSnapshotSubject,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
    ) -> Result<FleetSnapshotRequest> {
        let now = clock()?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Deadline)?;
        let millis = i64::try_from(remaining.as_millis()).map_err(|_| Error::Deadline)?;
        let end = now
            .checked_add(millis.min(30_000))
            .ok_or(Error::Deadline)?
            .min(
                roster
                    .snapshot()
                    .head()
                    .controller()
                    .ok_or(Error::Fenced)?
                    .expires_at_ms,
            );
        let mut nonce = blake3::Hasher::new();
        nonce.update(b"cellule.follower-policy-native.v1\0");
        nonce.update(uuid::Uuid::now_v7().as_bytes());
        let limit = if matches!(subject, FleetSnapshotSubject::FollowerEnrollments(_)) {
            32
        } else {
            128
        };
        FleetSnapshotRequest::new(
            roster.snapshot().clone(),
            Digest::from_bytes(*nonce.finalize().as_bytes()),
            node,
            session,
            subject,
            limit,
            now,
            end,
        )
        .map_err(operation)
    }
}
