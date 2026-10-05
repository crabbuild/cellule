use super::registry::{read_version, write_version};
use super::*;

fn write_receiver_route(e: &mut BoundedEncoder, route: &ReceiverRoute) -> Result<()> {
    e.write_u8(
        u8::try_from(route.hops.len())
            .map_err(|_| OperationError::Invalid("receiver route length overflow"))?,
    )?;
    for hop in &route.hops {
        e.write_bytes(hop.previous_node.as_bytes())?;
        e.write_bytes(hop.previous_session.as_bytes())?;
        e.write_bytes(hop.target_node.as_bytes())?;
        e.write_bytes(hop.target_session.as_bytes())?;
        e.write_bytes(hop.process_closure.as_bytes())?;
        write_version(e, hop.registry)?;
    }
    Ok(())
}

fn read_receiver_route(d: &mut BoundedDecoder<'_>) -> Result<ReceiverRoute> {
    let count = usize::from(d.read_u8()?);
    if count == 0 || count > MAX_RECEIVER_HANDOFFS {
        return Err(OperationError::Invalid("invalid receiver route length"));
    }
    let mut hops = Vec::with_capacity(count);
    for _ in 0..count {
        hops.push(ReceiverHandoff {
            previous_node: NodeId::from_bytes(fixed(d)?),
            previous_session: SessionId::from_bytes(fixed(d)?),
            target_node: NodeId::from_bytes(fixed(d)?),
            target_session: SessionId::from_bytes(fixed(d)?),
            process_closure: Digest::from_bytes(fixed(d)?),
            registry: read_version(d)?,
        });
    }
    Ok(ReceiverRoute { hops })
}

fn movement(d: &mut BoundedDecoder<'_>) -> Result<MovementAction> {
    match d.read_u8()? {
        1 => Ok(MovementAction::Prepare),
        2 => Ok(MovementAction::Release),
        3 => Ok(MovementAction::Activate),
        4 => Ok(MovementAction::Inspect),
        5 => Ok(MovementAction::Cancel),
        7 => Ok(MovementAction::Recover),
        8 => Ok(MovementAction::ReleaseMaintenance),
        _ => Err(OperationError::Invalid("unknown remote movement action")),
    }
}

fn maintenance(d: &mut BoundedDecoder<'_>) -> Result<MaintenanceAction> {
    match d.read_u8()? {
        1 => Ok(MaintenanceAction::Cordon),
        2 => Ok(MaintenanceAction::SettleRoles),
        3 => Ok(MaintenanceAction::Finalize),
        4 => Ok(MaintenanceAction::Inspect),
        _ => Err(OperationError::Invalid("unknown maintenance action")),
    }
}

impl FleetAction {
    /// Encodes a bounded canonical envelope for an authenticated management adapter.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ACTION)?;
        write_scope(&mut e, self.scope)?;
        e.write_u64(self.journal_revision)?;
        e.write_bytes(self.controller.as_bytes())?;
        e.write_u64(self.controller_epoch)?;
        e.write_i64(self.issued_at_ms)?;
        match &self.kind {
            FleetActionKind::Movement { action, attempt }
                if let Some(route) = &self.receiver_route =>
            {
                e.write_u8(3)?;
                e.write_u8(*action as u8)?;
                write_attempt(&mut e, attempt)?;
                write_receiver_route(&mut e, route)?;
            }
            FleetActionKind::Movement { action, attempt } => {
                e.write_u8(1)?;
                e.write_u8(*action as u8)?;
                write_attempt(&mut e, attempt)?;
            }
            FleetActionKind::Maintenance { action, operation } => {
                e.write_u8(2)?;
                e.write_u8(*action as u8)?;
                write_maintenance(&mut e, operation)?;
            }
        }
        Ok(e.finish())
    }

    /// Decodes shape only. Receiving applications still authenticate and call
    /// `authorize_against` with a fresh journal head before accepting an effect.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ACTION)?;
        let scope = read_scope(&mut d)?;
        let journal_revision = d.read_u64()?;
        let controller = SessionId::from_bytes(fixed(&mut d)?);
        let controller_epoch = d.read_u64()?;
        let issued_at_ms = d.read_i64()?;
        let (kind, receiver_route) = match d.read_u8()? {
            1 => (
                FleetActionKind::Movement {
                    action: movement(&mut d)?,
                    attempt: Box::new(read_attempt(&mut d)?),
                },
                None,
            ),
            2 => (
                FleetActionKind::Maintenance {
                    action: maintenance(&mut d)?,
                    operation: Box::new(read_maintenance(&mut d)?),
                },
                None,
            ),
            3 => (
                FleetActionKind::Movement {
                    action: movement(&mut d)?,
                    attempt: Box::new(read_attempt(&mut d)?),
                },
                Some(read_receiver_route(&mut d)?),
            ),
            _ => return Err(OperationError::Invalid("unknown fleet action family")),
        };
        d.finish()?;
        let action = Self {
            scope,
            journal_revision,
            controller,
            controller_epoch,
            issued_at_ms,
            kind,
            receiver_route,
        };
        action.validate()?;
        Ok(action)
    }
}

impl FleetActionOutcome {
    /// Encodes a typed reply while source errors stay in correlated adapter diagnostics.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(OUTCOME)?;
        write_scope(&mut e, self.scope)?;
        e.write_bytes(self.action_key.as_bytes())?;
        e.write_bytes(self.node.as_bytes())?;
        e.write_bytes(self.session.as_bytes())?;
        e.write_i64(self.observed_at_ms)?;
        match &self.outcome {
            FleetOutcome::Reserved(r) => {
                e.write_u8(1)?;
                e.write_bytes(r.session.as_bytes())?;
                e.write_i64(r.expires_at_ms)?;
            }
            FleetOutcome::Released(p) => {
                e.write_u8(2)?;
                write_position(&mut e, p)?;
            }
            FleetOutcome::Activated(a) => {
                e.write_u8(3)?;
                e.write_bytes(a.node.as_bytes())?;
                e.write_bytes(a.session.as_bytes())?;
                write_position(&mut e, &a.position)?;
            }
            FleetOutcome::Rejected(blocker) => {
                e.write_u8(4)?;
                write_blocker(&mut e, Some(*blocker))?;
            }
            FleetOutcome::Blocked(blocker) => {
                e.write_u8(5)?;
                write_blocker(&mut e, Some(*blocker))?;
            }
            FleetOutcome::Unknown => e.write_u8(6)?,
            FleetOutcome::ReceiverCleaned => e.write_u8(7)?,
            FleetOutcome::Cordoned => e.write_u8(8)?,
            FleetOutcome::RolesSettled { inventory } => {
                e.write_u8(9)?;
                e.write_bytes(inventory.as_bytes())?;
            }
            FleetOutcome::RolesSettledAt {
                inventory,
                head_revision,
                registry,
            } => {
                e.write_u8(12)?;
                e.write_bytes(inventory.as_bytes())?;
                e.write_u64(*head_revision)?;
                write_version(&mut e, *registry)?;
            }
            FleetOutcome::Recovered(evidence) => {
                e.write_u8(11)?;
                super::recovery::write_recovered(&mut e, evidence)?;
            }
            FleetOutcome::Stopped(evidence) => {
                e.write_u8(10)?;
                write_drain_evidence(&mut e, *evidence)?;
            }
        }
        Ok(e.finish())
    }

    /// Restores one complete bounded reply without trusting its claimed origin.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, OUTCOME)?;
        let scope = read_scope(&mut d)?;
        let action_key = Digest::from_bytes(fixed(&mut d)?);
        let node = NodeId::from_bytes(fixed(&mut d)?);
        let session = SessionId::from_bytes(fixed(&mut d)?);
        let observed_at_ms = d.read_i64()?;
        let outcome = match d.read_u8()? {
            1 => FleetOutcome::Reserved(ReceiverReservation {
                session: SessionId::from_bytes(fixed(&mut d)?),
                expires_at_ms: d.read_i64()?,
            }),
            2 => FleetOutcome::Released(read_position(&mut d)?),
            3 => FleetOutcome::Activated(ActivationEvidence {
                node: NodeId::from_bytes(fixed(&mut d)?),
                session: SessionId::from_bytes(fixed(&mut d)?),
                position: read_position(&mut d)?,
            }),
            4 => FleetOutcome::Rejected(
                read_blocker(&mut d)?.ok_or(OperationError::Invalid("rejection lacks a reason"))?,
            ),
            5 => FleetOutcome::Blocked(
                read_blocker(&mut d)?.ok_or(OperationError::Invalid("blocker lacks a reason"))?,
            ),
            6 => FleetOutcome::Unknown,
            7 => FleetOutcome::ReceiverCleaned,
            8 => FleetOutcome::Cordoned,
            9 => FleetOutcome::RolesSettled {
                inventory: Digest::from_bytes(fixed(&mut d)?),
            },
            10 => FleetOutcome::Stopped(read_drain_evidence(&mut d)?),
            11 => FleetOutcome::Recovered(Box::new(super::recovery::read_recovered(&mut d)?)),
            12 => FleetOutcome::RolesSettledAt {
                inventory: Digest::from_bytes(fixed(&mut d)?),
                head_revision: d.read_u64()?,
                registry: read_version(&mut d)?,
            },
            _ => return Err(OperationError::Invalid("unknown fleet action outcome")),
        };
        d.finish()?;
        let result = Self {
            scope,
            action_key,
            node,
            session,
            observed_at_ms,
            outcome,
        };
        result.validate()?;
        Ok(result)
    }
}
