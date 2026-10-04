use super::*;

const DOMAIN: &[u8] = b"cellule.root-lineage.v1\0";
impl CellRootLineage {
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut body = DOMAIN.to_vec();
        body.extend_from_slice(&self.root.cell);
        body.extend_from_slice(&self.root.incarnation);
        write_root(&mut body, &self.root);
        body.extend_from_slice(&(self.predecessors.len() as u16).to_be_bytes());
        for parent in &self.predecessors {
            write_root(&mut body, parent);
        }
        let checksum = *blake3::hash(&body).as_bytes();
        body.extend_from_slice(&checksum);
        if body.len() as u64 > MAX_LINEAGE_BYTES {
            return Err(Error::Control("Cell root lineage envelope exceeds bound"));
        }
        Ok(body)
    }
    pub(super) fn decode(body: &[u8]) -> Result<Self> {
        if body.len() as u64 > MAX_LINEAGE_BYTES || body.len() < DOMAIN.len() + 32 {
            return Err(Error::Control("invalid Cell root lineage envelope"));
        }
        let (payload, checksum) = body.split_at(body.len() - 32);
        if blake3::hash(payload).as_bytes().as_slice() != checksum {
            return Err(Error::Control("Cell root lineage checksum differs"));
        }
        let mut remaining = payload
            .strip_prefix(DOMAIN)
            .ok_or(Error::Control("unknown Cell root lineage version"))?;
        let cell = take::<32>(&mut remaining)?;
        let incarnation = take::<16>(&mut remaining)?;
        let root = read_root(&mut remaining, cell, incarnation)?;
        let count = u16::from_be_bytes(take::<2>(&mut remaining)?) as usize;
        if count > MAX_PREDECESSORS {
            return Err(Error::Capacity(
                "Cell root lineage predecessor bound exceeded",
            ));
        }
        let mut predecessors = Vec::with_capacity(count);
        for _ in 0..count {
            predecessors.push(read_root(&mut remaining, cell, incarnation)?);
        }
        if !remaining.is_empty() {
            return Err(Error::Control("trailing Cell root lineage bytes"));
        }
        let record = Self { root, predecessors };
        record.validate()?;
        Ok(record)
    }
}
fn take<const N: usize>(remaining: &mut &[u8]) -> Result<[u8; N]> {
    let bytes = remaining
        .get(..N)
        .ok_or(Error::Control("truncated Cell root lineage"))?;
    let value = bytes
        .try_into()
        .map_err(|_| Error::Control("truncated Cell root lineage"))?;
    *remaining = &remaining[N..];
    Ok(value)
}
fn write_root(body: &mut Vec<u8>, root: &RootRef) {
    body.extend_from_slice(&root.digest);
    body.extend_from_slice(&root.position.txid.to_be_bytes());
    body.extend_from_slice(&root.position.checksum.to_be_bytes());
    body.extend_from_slice(&root.commit_sequence.to_be_bytes());
}
fn read_root(remaining: &mut &[u8], cell: [u8; 32], incarnation: [u8; 16]) -> Result<RootRef> {
    Ok(RootRef {
        cell,
        incarnation,
        digest: take::<32>(remaining)?,
        position: cellule_ltx::Position {
            txid: u64::from_be_bytes(take::<8>(remaining)?),
            checksum: u64::from_be_bytes(take::<8>(remaining)?),
        },
        commit_sequence: u64::from_be_bytes(take::<8>(remaining)?),
    })
}
