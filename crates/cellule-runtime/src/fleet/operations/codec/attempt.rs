use super::recovery::{read_recovered, write_recovered};
use super::*;

pub(super) fn write_spec(e: &mut BoundedEncoder, spec: &MoveAttemptSpec) -> Result<()> {
    write_operation_id(e, spec.id.operation)?;
    e.write_u64(spec.id.sequence)?;
    e.write_bytes(spec.target.tenant().as_bytes())?;
    e.write_bytes(spec.target.application().as_bytes())?;
    e.write_bytes(spec.target.namespace().as_bytes())?;
    e.write_bytes(spec.target.partition())?;
    e.write_bytes(spec.incarnation.as_bytes())?;
    e.write_bytes(spec.source_node.as_bytes())?;
    e.write_bytes(spec.source.as_bytes())?;
    e.write_u64(spec.generation)?;
    e.write_u64(spec.source_epoch)?;
    e.write_bytes(spec.destination_node.as_bytes())?;
    e.write_bytes(spec.destination.as_bytes())?;
    e.write_u64(spec.cost.memory_bytes)?;
    e.write_u64(spec.cost.disk_bytes)?;
    e.write_u32(spec.cost.file_descriptors)?;
    e.write_u32(spec.cost.job_credits)?;
    e.write_bytes(spec.snapshot_digest.as_bytes())?;
    e.write_i64(spec.deadline_ms)?;
    Ok(())
}

pub(super) fn read_spec(d: &mut BoundedDecoder<'_>) -> Result<MoveAttemptSpec> {
    let id = AttemptId {
        operation: read_operation_id(d)?,
        sequence: d.read_u64()?,
    };
    let tenant = TenantId::from_bytes(fixed(d)?);
    let application = ApplicationId::from_bytes(fixed(d)?);
    let namespace = NamespaceId::from_bytes(fixed(d)?);
    let partition = d.read_bytes()?;
    let target = CellTarget::new(tenant, application, namespace, partition)
        .map_err(|source| OperationError::Identity(Box::new(source)))?;
    Ok(MoveAttemptSpec {
        id,
        target,
        incarnation: IncarnationId::from_bytes(fixed(d)?),
        source_node: NodeId::from_bytes(fixed(d)?),
        source: SessionId::from_bytes(fixed(d)?),
        generation: d.read_u64()?,
        source_epoch: d.read_u64()?,
        destination_node: NodeId::from_bytes(fixed(d)?),
        destination: SessionId::from_bytes(fixed(d)?),
        cost: TransferCost {
            memory_bytes: d.read_u64()?,
            disk_bytes: d.read_u64()?,
            file_descriptors: d.read_u32()?,
            job_credits: d.read_u32()?,
        },
        snapshot_digest: Digest::from_bytes(fixed(d)?),
        deadline_ms: d.read_i64()?,
    })
}

pub(super) fn write_attempt(e: &mut BoundedEncoder, attempt: &MoveAttempt) -> Result<()> {
    write_spec(e, &attempt.spec)?;
    e.write_u8(attempt.phase as u8)?;
    e.write_bool(attempt.reservation.is_some())?;
    if let Some(reservation) = attempt.reservation {
        e.write_bytes(reservation.session.as_bytes())?;
        e.write_i64(reservation.expires_at_ms)?;
    }
    e.write_bool(attempt.released.is_some())?;
    if let Some(position) = &attempt.released {
        write_position(e, position)?;
    }
    e.write_bool(attempt.activated.is_some())?;
    if let Some(evidence) = &attempt.activated {
        e.write_bytes(evidence.node.as_bytes())?;
        e.write_bytes(evidence.session.as_bytes())?;
        write_position(e, &evidence.position)?;
    }
    e.write_bool(attempt.receiver_cleaned)?;
    write_blocker(e, attempt.blocker)?;
    e.write_bool(attempt.completed_at_ms.is_some())?;
    if let Some(at) = attempt.completed_at_ms {
        e.write_i64(at)?;
    }
    if let Some(evidence) = &attempt.recovered {
        write_recovered(e, evidence)?;
    }
    Ok(())
}

pub(super) fn read_attempt(d: &mut BoundedDecoder<'_>) -> Result<MoveAttempt> {
    let spec = read_spec(d)?;
    let phase = match d.read_u8()? {
        1 => AttemptPhase::Planned,
        2 => AttemptPhase::Preparing,
        3 => AttemptPhase::Reserved,
        4 => AttemptPhase::Releasing,
        5 => AttemptPhase::Released,
        6 => AttemptPhase::Activating,
        7 => AttemptPhase::Activated,
        8 => AttemptPhase::Cancelling,
        9 => AttemptPhase::Cancelled,
        10 => AttemptPhase::CleaningReceiver,
        11 => AttemptPhase::Recovering,
        12 => AttemptPhase::Recovered,
        _ => return Err(OperationError::Invalid("unknown movement phase")),
    };
    let reservation = if d.read_bool()? {
        Some(ReceiverReservation {
            session: SessionId::from_bytes(fixed(d)?),
            expires_at_ms: d.read_i64()?,
        })
    } else {
        None
    };
    let released = if d.read_bool()? {
        Some(read_position(d)?)
    } else {
        None
    };
    let activated = if d.read_bool()? {
        Some(ActivationEvidence {
            node: NodeId::from_bytes(fixed(d)?),
            session: SessionId::from_bytes(fixed(d)?),
            position: read_position(d)?,
        })
    } else {
        None
    };
    let mut attempt = MoveAttempt {
        spec,
        phase,
        reservation,
        released,
        activated,
        recovered: None,
        receiver_cleaned: d.read_bool()?,
        blocker: read_blocker(d)?,
        completed_at_ms: if d.read_bool()? {
            Some(d.read_i64()?)
        } else {
            None
        },
    };
    if phase == AttemptPhase::Recovered {
        attempt.recovered = Some(Box::new(read_recovered(d)?));
    }
    attempt.validate()?;
    Ok(attempt)
}
