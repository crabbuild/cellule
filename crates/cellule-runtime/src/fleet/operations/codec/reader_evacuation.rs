use super::*;
use crate::control::Control;

fn write_basis(e: &mut BoundedEncoder, record: &ReaderEvacuationRecord) -> Result<()> {
    e.write_bytes(&record.operation.to_bytes()?)?;
    e.write_bytes(record.head_digest.as_bytes())?;
    e.write_bytes(&record.registry.to_bytes()?)?;
    e.write_bytes(&record.retired.to_bytes()?)?;
    e.write_bytes(record.original_digest.as_bytes())?;
    e.write_bytes(
        &record
            .authority
            .encode()
            .map_err(|error| OperationError::Control(Box::new(error)))?,
    )?;
    e.write_bool(record.policy_revision.is_some())?;
    if let Some(revision) = record.policy_revision {
        e.write_u64(revision)?;
    }
    e.write_u32(u32::from(record.desired_readers))?;
    e.write_u64(record.minimum_sequence)?;
    e.write_i64(record.started_at_ms)?;
    e.write_i64(record.finished_at_ms)?;
    Ok(())
}
impl ReaderEvacuationRecord {
    /// Immutable capture basis, independent of its page digests.
    pub fn basis_digest(&self) -> Result<Digest> {
        self.validate_basis()?;
        let mut e = encoder_limited(READER_EVACUATION_BASIS, MAX_PAGE_BYTES)?;
        write_basis(&mut e, self)?;
        Ok(Digest::from_bytes(*blake3::hash(&e.finish()).as_bytes()))
    }
    /// Canonical bounded manifest. Complete page verification remains required.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(READER_EVACUATION, MAX_PAGE_BYTES)?;
        write_basis(&mut e, self)?;
        e.write_count(self.pages.len())?;
        for digest in &self.pages {
            e.write_bytes(digest.as_bytes())?;
        }
        Ok(e.finish())
    }
    /// Decodes historical shape only; native provenance and freshness are separate.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, READER_EVACUATION, MAX_PAGE_BYTES)?;
        let operation = MaintenanceOperation::from_bytes(d.read_bytes()?)?;
        let head_digest = Digest::from_bytes(fixed(&mut d)?);
        let registry = RegistryVersion::from_bytes(d.read_bytes()?)?;
        let retired = EnrollmentRecord::from_bytes(d.read_bytes()?)?;
        let original_digest = Digest::from_bytes(fixed(&mut d)?);
        let authority = Control::decode(d.read_bytes()?)
            .map_err(|error| OperationError::Control(Box::new(error)))?;
        let policy_revision = if d.read_bool()? {
            Some(d.read_u64()?)
        } else {
            None
        };
        let desired_readers = u16::try_from(d.read_u32()?)
            .map_err(|_| OperationError::Invalid("reader policy count width"))?;
        let minimum_sequence = d.read_u64()?;
        let started_at_ms = d.read_i64()?;
        let finished_at_ms = d.read_i64()?;
        let count = d.read_count()?;
        if count > super::super::reader_evacuation::MAX_READER_PAGES {
            return Err(CodecError::Limit.into());
        }
        let mut pages = Vec::with_capacity(count);
        for _ in 0..count {
            pages.push(Digest::from_bytes(fixed(&mut d)?));
        }
        d.finish()?;
        let record = Self {
            operation,
            head_digest,
            registry,
            retired,
            original_digest,
            authority,
            policy_revision,
            desired_readers,
            minimum_sequence,
            started_at_ms,
            finished_at_ms,
            pages,
        };
        record.validate()?;
        Ok(record)
    }
}
impl ReaderEvacuationPage {
    /// Canonical page, bounded by the existing 128-entry and 64-KiB limits.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(READER_EVACUATION_PAGE)?;
        e.write_bytes(self.basis.as_bytes())?;
        e.write_u32(self.ordinal)?;
        e.write_count(self.entries.len())?;
        for entry in &self.entries {
            e.write_bytes(entry.node.as_bytes())?;
            e.write_bytes(entry.session.as_bytes())?;
            e.write_bytes(entry.boot_identity.as_bytes())?;
            e.write_bytes(entry.enrollment_key.as_bytes())?;
            e.write_bytes(entry.enrollment_digest.as_bytes())?;
            e.write_u64(entry.commit_sequence)?;
        }
        Ok(e.finish())
    }
    /// Rejects unknown envelopes, oversized/count/trailing data before allocation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, READER_EVACUATION_PAGE)?;
        let basis = Digest::from_bytes(fixed(&mut d)?);
        let ordinal = d.read_u32()?;
        let count = d.read_count()?;
        if count == 0 || count > MAX_PAGE_ENTRIES {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(ReaderReplacementWitness {
                node: NodeId::from_bytes(fixed(&mut d)?),
                session: SessionId::from_bytes(fixed(&mut d)?),
                boot_identity: Digest::from_bytes(fixed(&mut d)?),
                enrollment_key: Digest::from_bytes(fixed(&mut d)?),
                enrollment_digest: Digest::from_bytes(fixed(&mut d)?),
                commit_sequence: d.read_u64()?,
            });
        }
        d.finish()?;
        let page = Self {
            basis,
            ordinal,
            entries,
        };
        page.validate()?;
        Ok(page)
    }
}
