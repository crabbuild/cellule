use super::attempt::{read_spec, write_spec};
use super::*;

fn write_control(e: &mut BoundedEncoder, control: &crate::control::Control) -> Result<()> {
    e.write_bytes(
        &control
            .encode()
            .map_err(|e| OperationError::Control(Box::new(e)))?,
    )?;
    Ok(())
}
fn read_control(d: &mut BoundedDecoder<'_>) -> Result<crate::control::Control> {
    crate::control::Control::decode(d.read_bytes()?)
        .map_err(|e| OperationError::Control(Box::new(e)))
}
fn write_basis(e: &mut BoundedEncoder, basis: &RecoveryBasis) -> Result<()> {
    write_spec(e, &basis.spec)?;
    write_scope(e, basis.scope)?;
    e.write_bytes(basis.action_key.as_bytes())?;
    e.write_bytes(basis.node.as_bytes())?;
    e.write_bytes(basis.session.as_bytes())?;
    e.write_i64(basis.accepted_at_ms)?;
    write_control(e, &basis.control)?;
    e.write_i64(basis.observed_at_ms)?;
    Ok(())
}
fn read_basis(d: &mut BoundedDecoder<'_>) -> Result<RecoveryBasis> {
    let basis = RecoveryBasis {
        spec: read_spec(d)?,
        scope: read_scope(d)?,
        action_key: Digest::from_bytes(fixed(d)?),
        node: NodeId::from_bytes(fixed(d)?),
        session: SessionId::from_bytes(fixed(d)?),
        accepted_at_ms: d.read_i64()?,
        control: read_control(d)?,
        observed_at_ms: d.read_i64()?,
    };
    basis.validate()?;
    Ok(basis)
}
fn write_evidence(e: &mut BoundedEncoder, evidence: &RecoveryEvidence) -> Result<()> {
    write_basis(e, &evidence.basis)?;
    write_control(e, &evidence.restored)?;
    e.write_i64(evidence.recorded_at_ms)?;
    Ok(())
}
fn read_evidence(d: &mut BoundedDecoder<'_>) -> Result<RecoveryEvidence> {
    RecoveryEvidence::new(read_basis(d)?, read_control(d)?, d.read_i64()?)
}
pub(super) fn write_recovered(
    e: &mut BoundedEncoder,
    evidence: &RecoveredActivation,
) -> Result<()> {
    write_evidence(e, &evidence.recovery)?;
    e.write_bytes(evidence.serving.node.as_bytes())?;
    e.write_bytes(evidence.serving.session.as_bytes())?;
    write_position(e, &evidence.serving.position)
}
pub(super) fn read_recovered(d: &mut BoundedDecoder<'_>) -> Result<RecoveredActivation> {
    let evidence = RecoveredActivation {
        recovery: read_evidence(d)?,
        serving: ActivationEvidence {
            node: NodeId::from_bytes(fixed(d)?),
            session: SessionId::from_bytes(fixed(d)?),
            position: read_position(d)?,
        },
    };
    evidence.validate()?;
    Ok(evidence)
}

impl RecoveryBasis {
    /// Encodes immutable recovery input separately from the accepted-action record.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(RECOVERY_BASIS)?;
        write_basis(&mut e, self)?;
        Ok(e.finish())
    }
    /// Checks bounded canonical shape; decoding cannot create takeover authority.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, RECOVERY_BASIS)?;
        let basis = read_basis(&mut d)?;
        d.finish()?;
        Ok(basis)
    }
}
impl RecoveryEvidence {
    /// Encodes the exact recovered position retained before successor admission.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(RECOVERY_EVIDENCE)?;
        write_evidence(&mut e, self)?;
        Ok(e.finish())
    }
    /// Checks canonical transition shape; the journal provides execution provenance.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, RECOVERY_EVIDENCE)?;
        let evidence = read_evidence(&mut d)?;
        d.finish()?;
        Ok(evidence)
    }
}
