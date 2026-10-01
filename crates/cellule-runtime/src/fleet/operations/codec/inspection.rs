use super::*;

impl FleetInspectionRequest {
    /// Encodes one complete bounded request; older record kinds are unchanged.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(INSPECTION_REQUEST)?;
        e.write_bytes(&self.action.to_bytes()?)?;
        e.write_bytes(&self.registry.to_bytes()?)?;
        e.write_bytes(self.nonce.as_bytes())?;
        e.write_bytes(self.node.as_bytes())?;
        e.write_bytes(self.session.as_bytes())?;
        e.write_i64(self.deadline_ms)?;
        Ok(e.finish())
    }
    /// Decodes shape only; authentication and current journal checks are required.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, INSPECTION_REQUEST)?;
        let action = FleetAction::from_bytes(d.read_bytes()?)?;
        let registry = RegistryVersion::from_bytes(d.read_bytes()?)?;
        let nonce = Digest::from_bytes(fixed(&mut d)?);
        let node = NodeId::from_bytes(fixed(&mut d)?);
        let session = SessionId::from_bytes(fixed(&mut d)?);
        let deadline_ms = d.read_i64()?;
        d.finish()?;
        Self::new(action, registry, nonce, node, session, deadline_ms)
    }
}

impl FleetInspectionObservation {
    /// Encodes original capture times; replay/delivery must not restamp them.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(INSPECTION_OBSERVATION)?;
        e.write_bytes(&self.request.to_bytes()?)?;
        e.write_i64(self.capture_started_at_ms)?;
        e.write_bytes(&self.outcome.to_bytes()?)?;
        Ok(e.finish())
    }
    /// Restores one complete bounded request-bound observation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, INSPECTION_OBSERVATION)?;
        let request = FleetInspectionRequest::from_bytes(d.read_bytes()?)?;
        let started = d.read_i64()?;
        let outcome = FleetActionOutcome::from_bytes(d.read_bytes()?)?;
        d.finish()?;
        Self::new(request, started, outcome)
    }
}
