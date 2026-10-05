//! Separate envelopes preserve all existing source-recovery record bytes.
use super::*;

impl ReceiverRecoveryBasis {
    /// Encodes bounded receiver recovery input; it grants no takeover authority.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(RECEIVER_RECOVERY_BASIS)?;
        e.write_bytes(&self.accepted.to_bytes()?)?;
        e.write_bytes(
            &self
                .control
                .encode()
                .map_err(|e| OperationError::Control(Box::new(e)))?,
        )?;
        e.write_i64(self.observed_at_ms)?;
        Ok(e.finish())
    }
    /// Validates shape only; the trusted journal retains checked provenance.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, RECEIVER_RECOVERY_BASIS)?;
        let value = Self {
            accepted: AcceptedFleetAction::from_bytes(d.read_bytes()?)?,
            control: crate::control::Control::decode(d.read_bytes()?)
                .map_err(|e| OperationError::Control(Box::new(e)))?,
            observed_at_ms: d.read_i64()?,
        };
        d.finish()?;
        value.validate()?;
        Ok(value)
    }
}
impl ReceiverRecoveryEvidence {
    /// Encodes the canonical recovered receiver position before actor admission.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(RECEIVER_RECOVERY_EVIDENCE)?;
        e.write_bytes(&self.basis.to_bytes()?)?;
        e.write_bytes(
            &self
                .restored
                .encode()
                .map_err(|e| OperationError::Control(Box::new(e)))?,
        )?;
        e.write_i64(self.recorded_at_ms)?;
        Ok(e.finish())
    }
    /// Validates transition shape without creating runtime recovery permission.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, RECEIVER_RECOVERY_EVIDENCE)?;
        let value = Self::new(
            ReceiverRecoveryBasis::from_bytes(d.read_bytes()?)?,
            crate::control::Control::decode(d.read_bytes()?)
                .map_err(|e| OperationError::Control(Box::new(e)))?,
            d.read_i64()?,
        )?;
        d.finish()?;
        Ok(value)
    }
}
