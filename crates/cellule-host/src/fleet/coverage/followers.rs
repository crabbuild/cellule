use super::*;
use cellule_runtime::fleet::operations::{EnrollmentEvent, EnrollmentRole};
use cellule_runtime::identity::{NodeId, SessionId};
use cellule_runtime::node::{FollowerLogObservation, LogLeaderState, log_state::NodeLogPhase};

pub(super) fn match_authority(
    roster: &FleetRoster,
    native: &HashMap<(NodeId, SessionId), &FleetNodeInventory>,
    foreign: &HashMap<NodeId, &FleetFollowerReferences>,
) -> Result<HashSet<Digest>> {
    let mut observed = HashMap::<(NodeId, SessionId), &FollowerLogObservation>::new();
    let mut empty_lanes = HashSet::new();
    for references in foreign.values() {
        for reference in references.entries() {
            if reference.log.phase() == NodeLogPhase::Retired {
                // Terminal directory rows remain visible through their existing
                // grace boundary. They grant no failed-process join or deletion.
                continue;
            }
            if reference.leader_state != LogLeaderState::Live
                || reference.log.phase() != NodeLogPhase::Open
            {
                return Err(Error::Control(
                    "failed follower owner requires canonical recovery",
                ));
            }
            let source = native
                .get(&(reference.leader_node, reference.leader))
                .ok_or(Error::Control("authoritative follower owner is unobserved"))?;
            if source.node_log()
                != Some((
                    reference.leader,
                    reference.leader_node,
                    reference.log.epoch(),
                ))
                || !source.bindings().follower_producer
                || !source.bindings().durability_supervisor
            {
                return Err(Error::Control(
                    "authoritative follower owner has no managed binding",
                ));
            }
            if let Some(previous) =
                observed.insert((reference.leader_node, reference.leader), reference)
                && previous != reference
            {
                return Err(Error::Control(
                    "foreign follower authority intervals differ",
                ));
            }
            let progress = source
                .follower_enrollments()
                .iter()
                .find(|progress| progress.epoch == reference.log.epoch())
                .ok_or(Error::Fenced)?;
            if !progress.delivered
                || !progress.native_started
                || progress.native_closed
                || progress.enrollment.is_none()
                || progress.refusal.is_some()
                || progress.members.len() != reference.log.members().len()
            {
                return Err(Error::Control(
                    "authoritative follower producer is incomplete",
                ));
            }
            let selected = progress
                .members
                .iter()
                .map(|member| member.spec.target.node)
                .collect::<Vec<_>>();
            if selected != reference.log.members() {
                return Err(Error::Fenced);
            }
            for member in &progress.members {
                let spec = &member.spec;
                let row = roster
                    .enrollments()
                    .iter()
                    .find(|row| row.spec() == spec)
                    .ok_or(Error::Fenced)?;
                if row.status() != EnrollmentStatus::Established
                    || !member.published
                    || !matches!(member.event, Some(EnrollmentEvent::Established(evidence)) if Some(evidence) == row.established_evidence())
                    || member.accepted.as_ref().is_none_or(|accepted| {
                        accepted.spec() != spec || accepted.accepted_at_ms() != row.accepted_at_ms()
                    })
                    || spec.source.is_none_or(|source| {
                        source.node != reference.leader_node || source.session != reference.leader
                    })
                    || !matches!(spec.role, EnrollmentRole::Follower { log_epoch } if log_epoch == reference.log.epoch())
                {
                    return Err(Error::Fenced);
                }
                let target =
                    native
                        .get(&(spec.target.node, spec.target.session))
                        .ok_or(Error::Control(
                            "authoritative follower receiver is unobserved",
                        ))?;
                if !target.bindings().follower_store
                    || target
                        .follower_store_state()
                        .is_none_or(|(_, quarantine)| quarantine != 0)
                {
                    return Err(Error::Control(
                        "authoritative follower receiver store is incomplete",
                    ));
                }
                let counterpart = foreign.get(&spec.target.node).ok_or(Error::Fenced)?;
                if !counterpart.entries().iter().any(|other| other == reference) {
                    return Err(Error::Control(
                        "complete follower ensemble authority is missing",
                    ));
                }
                // The directory enrollment installs the obligation before any
                // append opens a persisted lane. This exact cross-node witness
                // supplies observation of that obligation, never its absence.
                empty_lanes.insert(spec.key().map_err(super::super::operation)?);
            }
        }
    }
    for inventory in native.values() {
        if let Some((session, node, epoch)) = inventory.node_log()
            && observed
                .get(&(node, session))
                .is_none_or(|reference| reference.log.epoch() != epoch)
        {
            return Err(Error::Control(
                "native bound follower log has no current authority",
            ));
        }
        if inventory
            .follower_store_state()
            .is_some_and(|(_, quarantine)| quarantine != 0)
        {
            return Err(Error::Control(
                "native follower inventory has quarantined entries",
            ));
        }
    }
    Ok(empty_lanes)
}
