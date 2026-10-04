use super::*;
impl FollowerReplacementPolicy {
    /// Canonical scoped policy; shape does not authorize its publication.
    pub fn to_bytes(self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(FOLLOWER_POLICY)?;
        write_scope(&mut e, self.scope)?;
        e.write_u64(self.revision)?;
        e.write_u8(self.minimum_members)?;
        Ok(e.finish())
    }
    /// Rejects unknown envelope, invalid scope/count and trailing data.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder(bytes, FOLLOWER_POLICY)?;
        let value = Self::new(read_scope(&mut d)?, d.read_u64()?, d.read_u8()?)?;
        d.finish()?;
        Ok(value)
    }
}
impl FollowerEvacuationRecord {
    /// Bounded manifest retaining every original and replacement member.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(FOLLOWER_EVACUATION, MAX_PAGE_BYTES)?;
        e.write_bytes(&self.operation.to_bytes()?)?;
        e.write_bytes(self.head_digest.as_bytes())?;
        e.write_bytes(&self.registry.to_bytes()?)?;
        e.write_bytes(&self.policy.to_bytes()?)?;
        e.write_bytes(self.original_key.as_bytes())?;
        e.write_bytes(self.original_digest.as_bytes())?;
        e.write_count(self.retired.len())?;
        for row in &self.retired {
            e.write_bytes(&row.to_bytes()?)?;
        }
        e.write_u64(self.covered_through)?;
        e.write_bytes(self.source_boot.as_bytes())?;
        e.write_u64(self.replacement_epoch)?;
        e.write_bytes(self.replacement_evidence.as_bytes())?;
        e.write_count(self.replacements.len())?;
        for entry in &self.replacements {
            e.write_bytes(&entry.enrollment.to_bytes()?)?;
            e.write_bytes(entry.boot_identity.as_bytes())?;
        }
        e.write_i64(self.started_at_ms)?;
        e.write_i64(self.finished_at_ms)?;
        Ok(e.finish())
    }
    /// Historical shape only; the journal and native verifier authenticate it.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(bytes, FOLLOWER_EVACUATION, MAX_PAGE_BYTES)?;
        let operation = MaintenanceOperation::from_bytes(d.read_bytes()?)?;
        let head_digest = Digest::from_bytes(fixed(&mut d)?);
        let registry = RegistryVersion::from_bytes(d.read_bytes()?)?;
        let policy = FollowerReplacementPolicy::from_bytes(d.read_bytes()?)?;
        let original_key = Digest::from_bytes(fixed(&mut d)?);
        let original_digest = Digest::from_bytes(fixed(&mut d)?);
        let count = d.read_count()?;
        if count == 0 || count > 2 {
            return Err(CodecError::Limit.into());
        }
        let mut retired = Vec::with_capacity(count);
        for _ in 0..count {
            retired.push(EnrollmentRecord::from_bytes(d.read_bytes()?)?);
        }
        let covered_through = d.read_u64()?;
        let source_boot = Digest::from_bytes(fixed(&mut d)?);
        let replacement_epoch = d.read_u64()?;
        let replacement_evidence = Digest::from_bytes(fixed(&mut d)?);
        let count = d.read_count()?;
        if count == 0 || count > 2 {
            return Err(CodecError::Limit.into());
        }
        let mut replacements = Vec::with_capacity(count);
        for _ in 0..count {
            replacements.push(FollowerReplacementWitness {
                enrollment: EnrollmentRecord::from_bytes(d.read_bytes()?)?,
                boot_identity: Digest::from_bytes(fixed(&mut d)?),
            });
        }
        let record = Self {
            operation,
            head_digest,
            registry,
            policy,
            original_key,
            original_digest,
            retired,
            covered_through,
            source_boot,
            replacement_epoch,
            replacement_evidence,
            replacements,
            started_at_ms: d.read_i64()?,
            finished_at_ms: d.read_i64()?,
        };
        d.finish()?;
        record.validate()?;
        Ok(record)
    }
}
