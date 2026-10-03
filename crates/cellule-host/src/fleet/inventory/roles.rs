use super::*;
use cellule_runtime::fleet::operations::{
    EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentStatus,
};
use cellule_runtime::follower::FollowerLaneState;

impl FleetNodeInventory {
    /// Matches original producer inputs and local roles to the exact full roster.
    /// Missing bindings/requests refuse coverage; failed boots, remote authority,
    /// producer preparation, replacement policy and finalization still require
    /// their own evidence. This performs no enrollment or settlement effect.
    pub fn validate_enrollments(&self, roster: &FleetRoster) -> Result<()> {
        self.validate_enrollments_with(roster, |_| false)
    }
    pub(crate) fn validate_enrollments_with(
        &self,
        roster: &FleetRoster,
        empty_enrolled_lane: impl Fn(&EnrollmentRecord) -> bool,
    ) -> Result<()> {
        if roster.snapshot() != &self.snapshot || roster.digest()? != self.roster {
            return Err(Error::Fenced);
        }
        let host = self.host;
        for (index, required) in [
            (2, host.bindings.readers),
            (3, host.bindings.readers),
            (4, host.bindings.follower_store),
            (5, host.bindings.follower_producer),
            (6, host.bindings.durability_supervisor),
        ] {
            if self.headers[index].is_none_or(|header| header.bound != required) {
                return Err(Error::Control("native inventory owner is unbound"));
            }
        }
        let mut reader_keys = HashSet::new();
        for completion in &self.reader_enrollments {
            let spec = &completion.spec;
            let EnrollmentRole::Reader { target, position } = &spec.role else {
                return Err(Error::Fenced);
            };
            let source = spec.source.ok_or(Error::Fenced)?;
            let original = &completion.source;
            if spec.scope != roster.snapshot().head().scope()
                || spec.target.node != self.node
                || spec.target.session != self.session
                || original.target() != target
                || original.description().incarnation != position.incarnation
                || original.epoch() != position.epoch
                || original.root() != &position.root
                || original.fleet() != spec.scope.fleet
                || original.node() != source.node
                || original.owner().session != source.session
            {
                return Err(Error::Fenced);
            }
            let current = roster
                .enrollments()
                .iter()
                .find(|row| row.spec() == spec)
                .ok_or(Error::Control(
                    "native reader request is missing from the roster",
                ))?;
            check_publication(
                current,
                completion.accepted.as_ref(),
                completion.event,
                completion.published,
            )?;
            if !reader_keys.insert(spec.key().map_err(operation)?) {
                return Err(Error::Fenced);
            }
            let view = self
                .readers
                .iter()
                .find(|r| r.receipt().cell == target.cell_id());
            if let Some(view) = view {
                let receipt = view.receipt();
                if !completion.opening_started
                    || !completion.opening_joined
                    || completion.execution_error.is_some()
                    || receipt.incarnation != position.incarnation
                    || receipt.commit_sequence < position.root.commit_sequence
                    || (matches!(completion.event, Some(EnrollmentEvent::Retired(_)))
                        && !view.locally_joined())
                {
                    return Err(Error::Fenced);
                }
            } else if matches!(completion.event, Some(EnrollmentEvent::Established(_))) {
                return Err(Error::Control(
                    "Established reader has no observed native view",
                ));
            }
        }
        for view in &self.readers {
            if !self.reader_enrollments.iter().any(|r| {
                matches!(&r.spec.role, EnrollmentRole::Reader {target, position}
                    if target.cell_id() == view.receipt().cell && position.incarnation == view.receipt().incarnation)
            }) { return Err(Error::Control("native reader lacks its original producer")); }
        }
        let mut follower_keys = HashSet::new();
        for progress in &self.follower_enrollments {
            if progress.epoch == 0 || progress.members.is_empty() || progress.members.len() > 16 {
                return Err(Error::Fenced);
            }
            for member in &progress.members {
                let spec = &member.spec;
                let source = spec.source.ok_or(Error::Fenced)?;
                if spec.scope != roster.snapshot().head().scope()
                    || source.node != self.node
                    || source.session != self.session
                    || !matches!(spec.role, EnrollmentRole::Follower { log_epoch } if log_epoch == progress.epoch)
                {
                    return Err(Error::Fenced);
                }
                let current = roster
                    .enrollments()
                    .iter()
                    .find(|row| row.spec() == spec)
                    .ok_or(Error::Control(
                        "native follower request is missing from the roster",
                    ))?;
                check_publication(
                    current,
                    member.accepted.as_ref(),
                    member.event,
                    member.published,
                )?;
                if !follower_keys.insert(spec.key().map_err(operation)?) {
                    return Err(Error::Fenced);
                }
            }
        }
        for lane in &self.follower_lanes {
            // Reopened stores can retain an earlier receiving boot's lane. Keep
            // that original target session in the roster; never substitute this
            // envelope's current boot as evidence that the old process joined.
            if !roster.enrollments().iter().any(|row| {
                row.spec().target.node == self.node
                    && row.spec().source.is_some_and(|s| s.session == lane.leader)
                    && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch } if log_epoch == lane.epoch)
                    && (row.unresolved() || lane.state == FollowerLaneState::Retired)
            }) { return Err(Error::Control("persisted follower lane lacks original enrollment")); }
        }
        for row in roster.enrollments().iter().filter(|row| row.unresolved()) {
            let spec = row.spec();
            let key = spec.key().map_err(operation)?;
            match spec.role {
                EnrollmentRole::Reader { .. }
                    if spec.target.node == self.node && spec.target.session == self.session =>
                {
                    if !reader_keys.contains(&key) {
                        return Err(Error::Control("registered reader is unobserved"));
                    }
                }
                EnrollmentRole::Follower { log_epoch } => {
                    if spec
                        .source
                        .is_some_and(|s| s.node == self.node && s.session == self.session)
                        && !follower_keys.contains(&key)
                    {
                        return Err(Error::Control("registered follower producer is unobserved"));
                    }
                    if row.status() == EnrollmentStatus::Established
                        && spec.target.node == self.node
                        && spec.target.session == self.session
                        && !self.follower_lanes.iter().any(|lane| {
                            spec.source.is_some_and(|s| s.session == lane.leader)
                                && lane.epoch == log_epoch
                        })
                        && !empty_enrolled_lane(row)
                    {
                        return Err(Error::Control(
                            "Established follower has no persisted native lane",
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn check_publication(
    current: &EnrollmentRecord,
    accepted: Option<&EnrollmentRecord>,
    event: Option<EnrollmentEvent>,
    published: bool,
) -> Result<()> {
    if let Some(accepted) = accepted {
        if accepted.spec() != current.spec()
            || accepted.accepted_at_ms() != current.accepted_at_ms()
        {
            return Err(Error::Fenced);
        }
    } else if published {
        return Err(Error::Fenced);
    }
    if published {
        let agrees = match event {
            Some(EnrollmentEvent::Established(d)) => {
                current.status() == EnrollmentStatus::Established
                    && current.established_evidence() == Some(d)
            }
            Some(EnrollmentEvent::Retired(d)) => {
                current.status() == EnrollmentStatus::Retired
                    && current.settlement_evidence() == Some(d)
            }
            Some(EnrollmentEvent::Refused(d)) => {
                current.status() == EnrollmentStatus::Refused
                    && current.settlement_evidence() == Some(d)
            }
            None => false,
        };
        if !agrees {
            return Err(Error::Fenced);
        }
    }
    Ok(())
}
