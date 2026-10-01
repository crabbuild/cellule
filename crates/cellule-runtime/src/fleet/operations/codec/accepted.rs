use super::*;

impl AcceptedFleetAction {
    /// Encodes the immutable acceptance within the ordinary record bound.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ACCEPTED)?;
        e.write_bytes(&self.action.to_bytes()?)?;
        e.write_bytes(self.node.as_bytes())?;
        e.write_bytes(self.session.as_bytes())?;
        e.write_i64(self.accepted_at_ms)?;
        Ok(e.finish())
    }

    /// Decodes a complete bounded acceptance without trusting its origin.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ACCEPTED)?;
        let action = FleetAction::from_bytes(d.read_bytes()?)?;
        let node = NodeId::from_bytes(fixed(&mut d)?);
        let session = SessionId::from_bytes(fixed(&mut d)?);
        let accepted_at_ms = d.read_i64()?;
        d.finish()?;
        let accepted = Self {
            action,
            node,
            session,
            accepted_at_ms,
        };
        accepted.validate()?;
        Ok(accepted)
    }
}
