use super::*;
use crate::node::NodeMode;

pub(super) fn write_scope(e: &mut BoundedEncoder, scope: FleetScope) -> Result<()> {
    e.write_bytes(scope.fleet.as_bytes())?;
    e.write_bytes(scope.application.as_bytes())?;
    Ok(())
}

pub(super) fn read_scope(d: &mut BoundedDecoder<'_>) -> Result<FleetScope> {
    let scope = FleetScope {
        fleet: Digest::from_bytes(fixed(d)?),
        application: ApplicationId::from_bytes(fixed(d)?),
    };
    scope.validate()?;
    Ok(scope)
}

impl NodeIntent {
    /// Encodes bounded physical-node intent for atomic journal publication.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(INTENT)?;
        write_scope(&mut e, self.scope)?;
        e.write_bytes(self.node.as_bytes())?;
        e.write_bytes(self.session.as_bytes())?;
        e.write_u64(self.revision)?;
        e.write_u8(match self.mode {
            NodeMode::Active => 1,
            NodeMode::Cordoned => 2,
            NodeMode::Draining => 3,
        })?;
        e.write_bool(self.operation.is_some())?;
        if let Some(operation) = self.operation {
            write_operation_id(&mut e, operation)?;
        }
        Ok(e.finish())
    }

    /// Restores an exact canonical desired-mode record without reopening it.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, INTENT)?;
        let intent = Self {
            scope: read_scope(&mut d)?,
            node: NodeId::from_bytes(fixed(&mut d)?),
            session: SessionId::from_bytes(fixed(&mut d)?),
            revision: d.read_u64()?,
            mode: match d.read_u8()? {
                1 => NodeMode::Active,
                2 => NodeMode::Cordoned,
                3 => NodeMode::Draining,
                _ => return Err(OperationError::Invalid("unknown desired node mode")),
            },
            operation: if d.read_bool()? {
                Some(read_operation_id(&mut d)?)
            } else {
                None
            },
        };
        d.finish()?;
        intent.validate()?;
        Ok(intent)
    }
}

impl ProgressPage {
    /// Encodes at most 128 terminal records within the one-MiB page limit.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(PROGRESS, MAX_PAGE_BYTES)?;
        write_scope(&mut e, self.scope)?;
        write_operation_id(&mut e, self.operation)?;
        e.write_u64(self.sequence)?;
        e.write_bool(self.previous.is_some())?;
        if let Some(previous) = self.previous {
            e.write_bytes(previous.as_bytes())?;
        }
        e.write_count(self.entries.len())?;
        for attempt in &self.entries {
            write_attempt(&mut e, attempt)?;
        }
        Ok(e.finish())
    }

    /// Restores a bounded complete page, rejecting an oversized count before allocation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, PROGRESS, MAX_PAGE_BYTES)?;
        let scope = read_scope(&mut d)?;
        let operation = read_operation_id(&mut d)?;
        let sequence = d.read_u64()?;
        let previous = if d.read_bool()? {
            Some(Digest::from_bytes(fixed(&mut d)?))
        } else {
            None
        };
        let count = d.read_count()?;
        if count == 0 || count > MAX_PAGE_ENTRIES {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(read_attempt(&mut d)?);
        }
        d.finish()?;
        Self::new(scope, operation, sequence, previous, entries)
    }
}
