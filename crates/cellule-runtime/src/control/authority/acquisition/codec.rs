use super::*;

const DOMAIN: &[u8] = b"cellule.cell-acquisition.v1\0";
impl CellAcquisitionRecord {
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut body = DOMAIN.to_vec();
        for control in [&self.input, &self.materialized] {
            let bytes = control.encode()?;
            // Control's canonical envelope is at most 8 KiB.
            body.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            body.extend_from_slice(&bytes);
        }
        if body.len() as u64 > MAX_ACQUISITION_BYTES {
            return Err(Error::Control("acquisition record exceeds bound"));
        }
        Ok(body)
    }
    pub(super) fn decode(body: &[u8]) -> Result<Self> {
        if body.len() as u64 > MAX_ACQUISITION_BYTES {
            return Err(Error::Control("acquisition record exceeds bound"));
        }
        let mut remaining = body
            .strip_prefix(DOMAIN)
            .ok_or(Error::Control("unknown acquisition record version"))?;
        let mut control = || -> Result<Control> {
            let prefix = remaining
                .get(..4)
                .ok_or(Error::Control("truncated acquisition record"))?;
            let length = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
            if length > MAX_CONTROL_BYTES as usize {
                return Err(Error::Control("acquisition Control exceeds bound"));
            }
            let bytes = remaining
                .get(4..4 + length)
                .ok_or(Error::Control("truncated acquisition Control"))?;
            let value = Control::decode(bytes)?;
            remaining = &remaining[4 + length..];
            Ok(value)
        };
        let record = Self {
            input: control()?,
            materialized: control()?,
        };
        if !remaining.is_empty() {
            return Err(Error::Control("trailing acquisition record bytes"));
        }
        record.validate()?;
        Ok(record)
    }
}
