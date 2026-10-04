use super::*;
use crate::control::Control;

fn write_basis(e: &mut BoundedEncoder, record: &OriginalWriterInventoryRecord) -> Result<()> {
    let basis = record.basis();
    e.write_bytes(&basis.operation.to_bytes()?)?;
    e.write_bytes(basis.head_digest.as_bytes())?;
    e.write_bytes(&basis.registry.to_bytes()?)?;
    e.write_bytes(&basis.boot.to_bytes()?)?;
    for digest in [
        basis.process_request,
        basis.process_witness,
        basis.catalog_witness,
    ] {
        e.write_bytes(digest.as_bytes())?;
    }
    e.write_i64(basis.interval.0)?;
    e.write_i64(basis.interval.1)?;
    e.write_count(record.catalogs.len())?;
    for row in &record.catalogs {
        e.write_bytes(row.application.as_bytes())?;
        e.write_bytes(row.tenant.as_bytes())?;
        for digest in [row.source, row.heads, row.histories] {
            e.write_bytes(digest.as_bytes())?;
        }
        e.write_u64(row.cells)?;
        e.write_u64(row.owners)?;
    }
    e.write_count(record.count)?;
    Ok(())
}
impl OriginalWriterInventoryRecord {
    /// Complete canonical basis, independent of its dependent page digests.
    pub fn basis_digest(&self) -> Result<Digest> {
        self.validate_basis()?;
        let mut e = encoder(WRITER_INVENTORY_BASIS)?;
        write_basis(&mut e, self)?;
        Ok(Digest::from_bytes(*blake3::hash(&e.finish()).as_bytes()))
    }
    /// Canonical manifest bounded by the existing 64-KiB record envelope.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder(WRITER_INVENTORY)?;
        write_basis(&mut e, self)?;
        e.write_count(self.pages.len())?;
        for digest in &self.pages {
            e.write_bytes(digest.as_bytes())?;
        }
        Ok(e.finish())
    }
    /// Decodes historical metadata only; completion requires every verified page.
    pub fn from_bytes(body: &[u8]) -> Result<Self> {
        let mut d = decoder(body, WRITER_INVENTORY)?;
        let operation = MaintenanceOperation::from_bytes(d.read_bytes()?)?;
        let head_digest = Digest::from_bytes(fixed(&mut d)?);
        let registry = RegistryVersion::from_bytes(d.read_bytes()?)?;
        let boot = EnrollmentRecord::from_bytes(d.read_bytes()?)?;
        let process_request = Digest::from_bytes(fixed(&mut d)?);
        let process_witness = Digest::from_bytes(fixed(&mut d)?);
        let catalog_witness = Digest::from_bytes(fixed(&mut d)?);
        let interval = (d.read_i64()?, d.read_i64()?);
        let count = d.read_count()?;
        if count > MAX_ORIGINAL_CATALOGS {
            return Err(CodecError::Limit.into());
        }
        let mut catalogs = Vec::with_capacity(count);
        for _ in 0..count {
            catalogs.push(OriginalCatalogWitness {
                application: ApplicationId::from_bytes(fixed(&mut d)?),
                tenant: TenantId::from_bytes(fixed(&mut d)?),
                source: Digest::from_bytes(fixed(&mut d)?),
                heads: Digest::from_bytes(fixed(&mut d)?),
                histories: Digest::from_bytes(fixed(&mut d)?),
                cells: d.read_u64()?,
                owners: d.read_u64()?,
            });
        }
        let count = d.read_count()?;
        if count > MAX_ORIGINAL_WRITERS {
            return Err(CodecError::Limit.into());
        }
        let page_count = d.read_count()?;
        if page_count > super::super::writer_inventory::MAX_WRITER_PAGES {
            return Err(CodecError::Limit.into());
        }
        let mut pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            pages.push(Digest::from_bytes(fixed(&mut d)?));
        }
        d.finish()?;
        let record = Self {
            basis: OriginalWriterInventoryBasis {
                operation,
                head_digest,
                registry,
                boot,
                process_request,
                process_witness,
                catalog_witness,
                interval,
            },
            catalogs,
            count,
            pages,
        };
        record.validate()?;
        Ok(record)
    }
}
impl OriginalWriterInventoryPage {
    /// Full original Controls and targets, bounded by the one-MiB page envelope.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut e = encoder_limited(WRITER_INVENTORY_PAGE, MAX_PAGE_BYTES)?;
        e.write_bytes(self.basis.as_bytes())?;
        e.write_u32(self.ordinal)?;
        e.write_count(self.entries.len())?;
        for row in &self.entries {
            e.write_bytes(row.target.tenant().as_bytes())?;
            e.write_bytes(row.target.application().as_bytes())?;
            e.write_bytes(row.target.namespace().as_bytes())?;
            e.write_bytes(row.target.partition())?;
            e.write_bytes(
                &row.control
                    .encode()
                    .map_err(|source| OperationError::Control(Box::new(source)))?,
            )?;
        }
        Ok(e.finish())
    }
    /// Refuses counts, envelopes, malformed Controls and trailing data before use.
    pub fn from_bytes(body: &[u8]) -> Result<Self> {
        let mut d = decoder_limited(body, WRITER_INVENTORY_PAGE, MAX_PAGE_BYTES)?;
        let basis = Digest::from_bytes(fixed(&mut d)?);
        let ordinal = d.read_u32()?;
        let count = d.read_count()?;
        if count == 0 || count > super::super::writer_inventory::WRITERS_PER_PAGE {
            return Err(CodecError::Limit.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let tenant = TenantId::from_bytes(fixed(&mut d)?);
            let application = ApplicationId::from_bytes(fixed(&mut d)?);
            let namespace = NamespaceId::from_bytes(fixed(&mut d)?);
            let target = CellTarget::new(tenant, application, namespace, d.read_bytes()?)
                .map_err(|source| OperationError::Identity(Box::new(source)))?;
            let encoded = d.read_bytes()?;
            if encoded.len() > 8 * 1024 {
                return Err(CodecError::Limit.into());
            }
            let control = Control::decode(encoded)
                .map_err(|source| OperationError::Control(Box::new(source)))?;
            entries.push(OriginalWriterObservation { target, control });
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
