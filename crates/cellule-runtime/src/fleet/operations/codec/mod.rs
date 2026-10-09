//! Canonical bounded journal records. No serde defaults can invent missing proof.

use crate::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use crate::control::RootRef;
use crate::identity::{
    ApplicationId, CellTarget, Digest, IncarnationId, NamespaceId, NodeId, SessionId, TenantId,
};

use super::*;

mod accepted;
mod acquisition;
mod attempt;
mod follower_evacuation;
mod inspection;
mod maintenance_enrollments;
mod reader_evacuation;
mod receiver_recovery;
mod recovery;
mod registry;
mod writer_inventory;
use attempt::{read_attempt, write_attempt};
mod actions;
mod pages;
use pages::{read_scope, write_scope};

const DOMAIN: &[u8] = b"cellule.fleet-operation\0";
const HEAD: u8 = 1;
const ATTEMPT: u8 = 2;
const MAINTENANCE: u8 = 3;
const INTENT: u8 = 4;
const PROGRESS: u8 = 5;
const ACTION: u8 = 6;
const OUTCOME: u8 = 7;
const ACCEPTED: u8 = 8;
const ACQUISITION: u8 = 9;
const RECOVERY_BASIS: u8 = 10;
const RECOVERY_EVIDENCE: u8 = 11;
const REGISTRY_VERSION: u8 = 12;
const INTENT_PAGE: u8 = 13;
const ENROLLMENT: u8 = 14;
const ENROLLMENT_PAGE: u8 = 15;
const ENROLLMENT_SPEC: u8 = 16;
const INSPECTION_REQUEST: u8 = 17;
const INSPECTION_OBSERVATION: u8 = 18;
const READER_EVACUATION: u8 = 19;
const READER_EVACUATION_PAGE: u8 = 20;
const READER_EVACUATION_BASIS: u8 = 21;
const FOLLOWER_POLICY: u8 = 22;
const FOLLOWER_EVACUATION: u8 = 23;
const WRITER_INVENTORY: u8 = 24;
const WRITER_INVENTORY_PAGE: u8 = 25;
const WRITER_INVENTORY_BASIS: u8 = 26;
const MAINTENANCE_ENROLLMENTS: u8 = 27;
const MAINTENANCE_ENROLLMENTS_PAGE: u8 = 28;
const MAINTENANCE_ENROLLMENTS_BASIS: u8 = 29;
const RECEIVER_RECOVERY_BASIS: u8 = 30;
const RECEIVER_RECOVERY_EVIDENCE: u8 = 31;

fn encoder(kind: u8) -> Result<BoundedEncoder> {
    encoder_limited(kind, MAX_RECORD_BYTES)
}

fn encoder_limited(kind: u8, limit: u32) -> Result<BoundedEncoder> {
    let mut e = BoundedEncoder::new(limit)?;
    e.write_bytes(DOMAIN)?;
    e.write_u8(FORMAT_VERSION)?;
    e.write_u8(kind)?;
    Ok(e)
}

fn decoder(bytes: &[u8], kind: u8) -> Result<BoundedDecoder<'_>> {
    decoder_limited(bytes, kind, MAX_RECORD_BYTES)
}

fn decoder_limited(bytes: &[u8], kind: u8, limit: u32) -> Result<BoundedDecoder<'_>> {
    let mut d = BoundedDecoder::new(bytes, limit)?;
    if d.read_bytes()? != DOMAIN || d.read_u8()? != FORMAT_VERSION || d.read_u8()? != kind {
        return Err(OperationError::Invalid("unsupported fleet record envelope"));
    }
    Ok(d)
}

fn fixed<const N: usize>(d: &mut BoundedDecoder<'_>) -> Result<[u8; N]> {
    d.read_bytes()?
        .try_into()
        .map_err(|_| OperationError::Invalid("fleet identity width"))
}

fn write_operation_id(e: &mut BoundedEncoder, id: OperationId) -> Result<()> {
    e.write_bytes(id.as_bytes())?;
    Ok(())
}

fn read_operation_id(d: &mut BoundedDecoder<'_>) -> Result<OperationId> {
    OperationId::from_bytes(fixed(d)?)
}

fn write_blocker(e: &mut BoundedEncoder, value: Option<DrainBlocker>) -> Result<()> {
    e.write_u8(value.map_or(0, |b| b as u8))?;
    Ok(())
}

fn read_blocker(d: &mut BoundedDecoder<'_>) -> Result<Option<DrainBlocker>> {
    Ok(Some(match d.read_u8()? {
        0 => return Ok(None),
        1 => DrainBlocker::IncompleteObservation,
        2 => DrainBlocker::StaleObservation,
        3 => DrainBlocker::IncompatibleRelease,
        4 => DrainBlocker::ReceiverCapacity,
        5 => DrainBlocker::BusyExecution,
        6 => DrainBlocker::ExternalLease,
        7 => DrainBlocker::PendingPublication,
        8 => DrainBlocker::FollowerObligation,
        9 => DrainBlocker::UnknownInventory,
        10 => DrainBlocker::MovementBudget,
        11 => DrainBlocker::OutcomeUnknown,
        12 => DrainBlocker::Deadline,
        13 => DrainBlocker::FacilityFailure,
        14 => DrainBlocker::ReaderObligation,
        _ => return Err(OperationError::Invalid("unknown fleet blocker")),
    }))
}

fn write_position(e: &mut BoundedEncoder, position: &PublishedPosition) -> Result<()> {
    e.write_bytes(position.incarnation.as_bytes())?;
    e.write_u64(position.epoch)?;
    e.write_bytes(position.root.digest.as_bytes())?;
    e.write_u64(position.root.txid)?;
    e.write_u64(position.root.checksum)?;
    e.write_u64(position.root.commit_sequence)?;
    Ok(())
}

fn read_position(d: &mut BoundedDecoder<'_>) -> Result<PublishedPosition> {
    let position = PublishedPosition {
        incarnation: IncarnationId::from_bytes(fixed(d)?),
        epoch: d.read_u64()?,
        root: RootRef {
            digest: Digest::from_bytes(fixed(d)?),
            txid: d.read_u64()?,
            checksum: d.read_u64()?,
            commit_sequence: d.read_u64()?,
        },
    };
    position.validate()?;
    Ok(position)
}

fn write_drain_evidence(e: &mut BoundedEncoder, evidence: DrainEvidence) -> Result<()> {
    e.write_bytes(evidence.node.as_bytes())?;
    e.write_bytes(evidence.session.as_bytes())?;
    e.write_u64(evidence.remaining_cells)?;
    e.write_u32(evidence.unresolved_attempts)?;
    e.write_bool(evidence.relocated)?;
    e.write_bool(evidence.readers_settled)?;
    e.write_bool(evidence.followers_settled)?;
    e.write_bool(evidence.facilities_closed)?;
    e.write_bool(evidence.stopped)?;
    e.write_bool(evidence.withdrawn)?;
    Ok(())
}

fn read_drain_evidence(d: &mut BoundedDecoder<'_>) -> Result<DrainEvidence> {
    Ok(DrainEvidence {
        node: NodeId::from_bytes(fixed(d)?),
        session: SessionId::from_bytes(fixed(d)?),
        remaining_cells: d.read_u64()?,
        unresolved_attempts: d.read_u32()?,
        relocated: d.read_bool()?,
        readers_settled: d.read_bool()?,
        followers_settled: d.read_bool()?,
        facilities_closed: d.read_bool()?,
        stopped: d.read_bool()?,
        withdrawn: d.read_bool()?,
    })
}

fn write_maintenance(e: &mut BoundedEncoder, operation: &MaintenanceOperation) -> Result<()> {
    write_operation_id(e, operation.id)?;
    e.write_bytes(operation.request_digest.as_bytes())?;
    e.write_bytes(operation.node.as_bytes())?;
    e.write_bytes(operation.session.as_bytes())?;
    e.write_u64(operation.intent_revision)?;
    e.write_i64(operation.created_at_ms)?;
    e.write_i64(operation.deadline_ms)?;
    e.write_u8(operation.phase as u8)?;
    write_blocker(e, operation.blocker)?;
    e.write_bool(operation.drain_evidence.is_some())?;
    if let Some(evidence) = operation.drain_evidence {
        write_drain_evidence(e, evidence)?;
    }
    Ok(())
}

fn read_maintenance(d: &mut BoundedDecoder<'_>) -> Result<MaintenanceOperation> {
    let id = read_operation_id(d)?;
    let request_digest = Digest::from_bytes(fixed(d)?);
    let node = NodeId::from_bytes(fixed(d)?);
    let session = SessionId::from_bytes(fixed(d)?);
    let intent_revision = d.read_u64()?;
    let created_at_ms = d.read_i64()?;
    let deadline_ms = d.read_i64()?;
    let phase = match d.read_u8()? {
        1 => MaintenancePhase::Requested,
        2 => MaintenancePhase::Cordoned,
        3 => MaintenancePhase::Evacuating,
        4 => MaintenancePhase::Closing,
        5 => MaintenancePhase::Completed,
        _ => return Err(OperationError::Invalid("unknown maintenance phase")),
    };
    let operation = MaintenanceOperation {
        id,
        request_digest,
        node,
        session,
        intent_revision,
        created_at_ms,
        deadline_ms,
        phase,
        blocker: read_blocker(d)?,
        drain_evidence: if d.read_bool()? {
            Some(read_drain_evidence(d)?)
        } else {
            None
        },
    };
    operation.validate()?;
    Ok(operation)
}

impl FleetHead {
    /// Encodes a validated bounded head for atomic publication by an adapter.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(HEAD)?;
        e.write_bytes(self.scope.fleet.as_bytes())?;
        e.write_bytes(self.scope.application.as_bytes())?;
        e.write_u64(self.revision)?;
        e.write_i64(self.last_observed_ms)?;
        e.write_bool(self.controller.is_some())?;
        if let Some(lease) = self.controller {
            e.write_bytes(lease.claimant.as_bytes())?;
            e.write_u64(lease.epoch)?;
            e.write_i64(lease.expires_at_ms)?;
        }
        e.write_bool(self.maintenance.is_some())?;
        if let Some(operation) = &self.maintenance {
            write_maintenance(&mut e, operation)?;
        }
        e.write_u64(self.next_sequence)?;
        e.write_count(self.attempts.len())?;
        for attempt in &self.attempts {
            write_attempt(&mut e, attempt)?;
        }
        e.write_bool(self.progress.is_some())?;
        if let Some(progress) = self.progress {
            e.write_bytes(progress.digest.as_bytes())?;
            e.write_u64(progress.sequence)?;
        }
        Ok(e.finish())
    }

    /// Restores a complete canonical head, rejecting trailing and oversized data.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, HEAD)?;
        let scope = FleetScope {
            fleet: Digest::from_bytes(fixed(&mut d)?),
            application: ApplicationId::from_bytes(fixed(&mut d)?),
        };
        let revision = d.read_u64()?;
        let last_observed_ms = d.read_i64()?;
        let controller = if d.read_bool()? {
            Some(ControllerLease {
                claimant: SessionId::from_bytes(fixed(&mut d)?),
                epoch: d.read_u64()?,
                expires_at_ms: d.read_i64()?,
            })
        } else {
            None
        };
        let maintenance = if d.read_bool()? {
            Some(read_maintenance(&mut d)?)
        } else {
            None
        };
        let next_sequence = d.read_u64()?;
        let count = d.read_count()?;
        if count > MAX_ACTIVE_ATTEMPTS {
            return Err(CodecError::Limit.into());
        }
        let mut attempts = Vec::with_capacity(count);
        for _ in 0..count {
            attempts.push(read_attempt(&mut d)?);
        }
        let progress = if d.read_bool()? {
            Some(ProgressHead {
                digest: Digest::from_bytes(fixed(&mut d)?),
                sequence: d.read_u64()?,
            })
        } else {
            None
        };
        d.finish()?;
        let head = Self {
            scope,
            revision,
            last_observed_ms,
            controller,
            maintenance,
            next_sequence,
            attempts,
            progress,
        };
        head.validate()?;
        Ok(head)
    }
}

impl MoveAttempt {
    /// Encodes an exact attempt for immutable historical progress pages.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ATTEMPT)?;
        write_attempt(&mut e, self)?;
        Ok(e.finish())
    }

    /// Restores a validated attempt from its canonical bounded record.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ATTEMPT)?;
        let attempt = read_attempt(&mut d)?;
        d.finish()?;
        Ok(attempt)
    }
}

impl MaintenanceOperation {
    /// Encodes the durable physical-node intent and lifecycle progress.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(MAINTENANCE)?;
        write_maintenance(&mut e, self)?;
        Ok(e.finish())
    }

    /// Restores a canonical bounded maintenance record.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, MAINTENANCE)?;
        let operation = read_maintenance(&mut d)?;
        d.finish()?;
        Ok(operation)
    }
}
