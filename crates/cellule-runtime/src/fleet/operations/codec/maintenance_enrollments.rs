use super::*;

fn write_basis(e: &mut BoundedEncoder, record: &MaintenanceEnrollmentInventory) -> Result<()> {
    e.write_bytes(&record.operation.to_bytes()?)?;
    e.write_bytes(record.head_digest.as_bytes())?;
    e.write_bytes(&record.registry.to_bytes()?)?;
    e.write_i64(record.captured_at_ms)?;
    e.write_count(record.count)?;
    Ok(())
}
impl MaintenanceEnrollmentInventory {
    /// Original basis identity, independent of its dependent page digests.
    pub fn basis_digest(&self) -> Result<Digest> {
        self.validate_basis()?;
        let mut e = encoder(MAINTENANCE_ENROLLMENTS_BASIS)?;
        write_basis(&mut e, self)?;
        Ok(Digest::from_bytes(*blake3::hash(&e.finish()).as_bytes()))
    }
    /// Encodes a complete bounded original manifest; old record tags are unchanged.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(MAINTENANCE_ENROLLMENTS)?;
        write_basis(&mut e, self)?;
        e.write_count(self.pages.len())?;
        for digest in &self.pages {
            e.write_bytes(digest.as_bytes())?;
        }
        Ok(e.finish())
    }
    /// Decodes historical shape only; every page and committed pointer are required.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, MAINTENANCE_ENROLLMENTS)?;
        let operation = MaintenanceOperation::from_bytes(d.read_bytes()?)?;
        let head_digest = Digest::from_bytes(fixed(&mut d)?);
        let registry = RegistryVersion::from_bytes(d.read_bytes()?)?;
        let captured_at_ms = d.read_i64()?;
        let count = d.read_count()?;
        let page_count = d.read_count()?;
        if count > MAX_MAINTENANCE_ENROLLMENTS
            || page_count > super::super::maintenance_enrollments::MAX_ENROLLMENT_INVENTORY_PAGES
        {
            return Err(CodecError::Limit.into());
        }
        let mut pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            pages.push(Digest::from_bytes(fixed(&mut d)?));
        }
        d.finish()?;
        let record = Self {
            operation,
            head_digest,
            registry,
            captured_at_ms,
            count,
            pages,
        };
        record.validate()?;
        Ok(record)
    }
}
impl MaintenanceEnrollmentPage {
    /// Encodes an original acceptance page in the existing one-MiB envelope.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(MAINTENANCE_ENROLLMENTS_PAGE, MAX_PAGE_BYTES)?;
        e.write_bytes(self.basis.as_bytes())?;
        e.write_u32(self.ordinal)?;
        e.write_count(self.entries.len())?;
        for row in &self.entries {
            e.write_bytes(&row.to_bytes()?)?;
        }
        Ok(e.finish())
    }
    /// Decodes bounded original acceptances; full manifest validation is separate.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, MAINTENANCE_ENROLLMENTS_PAGE, MAX_PAGE_BYTES)?;
        let basis = Digest::from_bytes(fixed(&mut d)?);
        let ordinal = d.read_u32()?;
        let count = d.read_count()?;
        if count > MAX_PAGE_ENTRIES {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(EnrollmentRecord::from_bytes(d.read_bytes()?)?);
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
