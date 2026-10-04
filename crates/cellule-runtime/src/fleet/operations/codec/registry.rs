use super::*;
use crate::node::NodeMode;

fn write_version(e: &mut BoundedEncoder, version: RegistryVersion) -> Result<()> {
    write_scope(e, version.scope)?;
    e.write_u64(version.revision)?;
    e.write_bool(version.bootstrap_revision.is_some())?;
    if let Some(revision) = version.bootstrap_revision {
        e.write_u64(revision)?;
    }
    e.write_bool(version.scheduling_enabled)?;
    Ok(())
}

fn read_version(d: &mut BoundedDecoder<'_>) -> Result<RegistryVersion> {
    let version = RegistryVersion {
        scope: read_scope(d)?,
        revision: d.read_u64()?,
        bootstrap_revision: if d.read_bool()? {
            Some(d.read_u64()?)
        } else {
            None
        },
        scheduling_enabled: d.read_bool()?,
    };
    version.validate()?;
    Ok(version)
}

fn write_endpoint(e: &mut BoundedEncoder, endpoint: EnrollmentEndpoint) -> Result<()> {
    e.write_bytes(endpoint.node.as_bytes())?;
    e.write_bytes(endpoint.session.as_bytes())?;
    e.write_u64(endpoint.intent_revision)?;
    Ok(())
}

fn read_endpoint(d: &mut BoundedDecoder<'_>) -> Result<EnrollmentEndpoint> {
    let endpoint = EnrollmentEndpoint {
        node: NodeId::from_bytes(fixed(d)?),
        session: SessionId::from_bytes(fixed(d)?),
        intent_revision: d.read_u64()?,
    };
    endpoint.validate()?;
    Ok(endpoint)
}

fn write_optional_digest(e: &mut BoundedEncoder, value: Option<Digest>) -> Result<()> {
    e.write_bool(value.is_some())?;
    if let Some(value) = value {
        e.write_bytes(value.as_bytes())?;
    }
    Ok(())
}

fn read_optional_digest(d: &mut BoundedDecoder<'_>) -> Result<Option<Digest>> {
    Ok(if d.read_bool()? {
        Some(Digest::from_bytes(fixed(d)?))
    } else {
        None
    })
}

fn write_spec(e: &mut BoundedEncoder, spec: &EnrollmentSpec) -> Result<()> {
    write_scope(e, spec.scope)?;
    e.write_bytes(spec.request.as_bytes())?;
    match &spec.role {
        EnrollmentRole::Node { mode } => {
            e.write_u8(1)?;
            e.write_u8(match mode {
                NodeMode::Active => 1,
                NodeMode::Cordoned => 2,
                NodeMode::Draining => 3,
            })?;
        }
        EnrollmentRole::Reader { target, position } => {
            e.write_u8(2)?;
            e.write_bytes(target.tenant().as_bytes())?;
            e.write_bytes(target.application().as_bytes())?;
            e.write_bytes(target.namespace().as_bytes())?;
            e.write_bytes(target.partition())?;
            write_position(e, position)?;
        }
        EnrollmentRole::Follower { log_epoch } => {
            e.write_u8(3)?;
            e.write_u64(*log_epoch)?;
        }
    }
    e.write_bool(spec.source.is_some())?;
    if let Some(source) = spec.source {
        write_endpoint(e, source)?;
    }
    write_endpoint(e, spec.target)?;
    Ok(())
}

fn write_record(e: &mut BoundedEncoder, record: &EnrollmentRecord) -> Result<()> {
    write_spec(e, &record.spec)?;
    e.write_i64(record.accepted_at_ms)?;
    e.write_i64(record.updated_at_ms)?;
    e.write_u8(record.status as u8)?;
    write_optional_digest(e, record.established)?;
    write_optional_digest(e, record.settlement)?;
    Ok(())
}

fn read_spec(d: &mut BoundedDecoder<'_>) -> Result<EnrollmentSpec> {
    let scope = read_scope(d)?;
    let request = Digest::from_bytes(fixed(d)?);
    let role = match d.read_u8()? {
        1 => EnrollmentRole::Node {
            mode: match d.read_u8()? {
                1 => NodeMode::Active,
                2 => NodeMode::Cordoned,
                3 => NodeMode::Draining,
                _ => return Err(OperationError::Invalid("unknown boot enrollment mode")),
            },
        },
        2 => {
            let tenant = TenantId::from_bytes(fixed(d)?);
            let application = ApplicationId::from_bytes(fixed(d)?);
            let namespace = NamespaceId::from_bytes(fixed(d)?);
            let target = CellTarget::new(tenant, application, namespace, d.read_bytes()?)
                .map_err(|error| OperationError::Identity(Box::new(error)))?;
            EnrollmentRole::Reader {
                target,
                position: read_position(d)?,
            }
        }
        3 => EnrollmentRole::Follower {
            log_epoch: d.read_u64()?,
        },
        _ => return Err(OperationError::Invalid("unknown enrollment role")),
    };
    let source = if d.read_bool()? {
        Some(read_endpoint(d)?)
    } else {
        None
    };
    let target = read_endpoint(d)?;
    let spec = EnrollmentSpec {
        scope,
        request,
        role,
        source,
        target,
    };
    spec.validate()?;
    Ok(spec)
}

fn read_record(d: &mut BoundedDecoder<'_>) -> Result<EnrollmentRecord> {
    let spec = read_spec(d)?;
    let accepted_at_ms = d.read_i64()?;
    let updated_at_ms = d.read_i64()?;
    let status = match d.read_u8()? {
        1 => EnrollmentStatus::Pending,
        2 => EnrollmentStatus::Established,
        3 => EnrollmentStatus::Refused,
        4 => EnrollmentStatus::Retired,
        _ => return Err(OperationError::Invalid("unknown enrollment status")),
    };
    let record = EnrollmentRecord {
        spec,
        accepted_at_ms,
        updated_at_ms,
        status,
        established: read_optional_digest(d)?,
        settlement: read_optional_digest(d)?,
    };
    record.validate()?;
    Ok(record)
}

impl EnrollmentSpec {
    /// Encodes bounded immutable request inputs before journal acceptance.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ENROLLMENT_SPEC)?;
        write_spec(&mut e, self)?;
        Ok(e.finish())
    }
    /// Restores a request without implying it has a pending permit or authority.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ENROLLMENT_SPEC)?;
        let spec = read_spec(&mut d)?;
        d.finish()?;
        Ok(spec)
    }
}

impl RegistryVersion {
    /// Encodes the exact shared registry revision and coverage barrier.
    pub fn to_bytes(self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(REGISTRY_VERSION)?;
        write_version(&mut e, self)?;
        Ok(e.finish())
    }
    /// Restores the strict record without inventing a bootstrap marker.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, REGISTRY_VERSION)?;
        let version = read_version(&mut d)?;
        d.finish()?;
        Ok(version)
    }
}

impl EnrollmentRecord {
    /// Encodes immutable inputs and checked original progress in 64 KiB.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(ENROLLMENT)?;
        write_record(&mut e, self)?;
        Ok(e.finish())
    }
    /// Restores a retained obligation, including failed or pending sessions.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, ENROLLMENT)?;
        let record = read_record(&mut d)?;
        d.finish()?;
        Ok(record)
    }
}

impl IntentPage {
    /// Encodes at most 128 retained intents within one MiB.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(INTENT_PAGE, MAX_PAGE_BYTES)?;
        write_version(&mut e, self.version)?;
        e.write_bool(self.after.is_some())?;
        if let Some(after) = self.after {
            e.write_bytes(after.as_bytes())?;
        }
        e.write_count(self.entries.len())?;
        for intent in &self.entries {
            e.write_bytes(&intent.to_bytes()?)?;
        }
        e.write_bool(self.next.is_some())?;
        if let Some(next) = self.next {
            e.write_bytes(next.as_bytes())?;
        }
        Ok(e.finish())
    }
    /// Rejects an oversized row count before allocating the page vector.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, INTENT_PAGE, MAX_PAGE_BYTES)?;
        let version = read_version(&mut d)?;
        let after = if d.read_bool()? {
            Some(NodeId::from_bytes(fixed(&mut d)?))
        } else {
            None
        };
        let count = d.read_count()?;
        if count > MAX_PAGE_ENTRIES {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(NodeIntent::from_bytes(d.read_bytes()?)?);
        }
        let next = if d.read_bool()? {
            Some(NodeId::from_bytes(fixed(&mut d)?))
        } else {
            None
        };
        d.finish()?;
        Self::new(version, after, entries, next)
    }
}

impl EnrollmentPage {
    /// Encodes every included obligation without filtering failed-session rows.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(ENROLLMENT_PAGE, MAX_PAGE_BYTES)?;
        write_version(&mut e, self.version)?;
        write_optional_digest(&mut e, self.after)?;
        e.write_count(self.entries.len())?;
        for record in &self.entries {
            write_record(&mut e, record)?;
        }
        write_optional_digest(&mut e, self.next)?;
        Ok(e.finish())
    }
    /// Restores a bounded sorted page, rejecting incomplete progress shapes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, ENROLLMENT_PAGE, MAX_PAGE_BYTES)?;
        let version = read_version(&mut d)?;
        let after = read_optional_digest(&mut d)?;
        let count = d.read_count()?;
        if count > MAX_PAGE_ENTRIES {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(read_record(&mut d)?);
        }
        let next = read_optional_digest(&mut d)?;
        d.finish()?;
        Self::new(version, after, entries, next)
    }
}
