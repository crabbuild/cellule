use super::*;

impl AcquisitionBasis {
    /// Encodes a bounded immutable basis without changing Cell control format.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ACQUISITION)?;
        e.write_bytes(&self.accepted.to_bytes()?)?;
        e.write_bytes(
            &self
                .control
                .encode()
                .map_err(|error| OperationError::Control(Box::new(error)))?,
        )?;
        e.write_i64(self.observed_at_ms)?;
        Ok(e.finish())
    }

    /// Decodes shape only; the journal and checked host supply provenance.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ACQUISITION)?;
        let accepted = AcceptedFleetAction::from_bytes(d.read_bytes()?)?;
        let control = crate::control::Control::decode(d.read_bytes()?)
            .map_err(|error| OperationError::Control(Box::new(error)))?;
        let observed_at_ms = d.read_i64()?;
        d.finish()?;
        Self::new(accepted, control, observed_at_ms)
    }
}
