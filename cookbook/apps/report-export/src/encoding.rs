use crate::{Chunk, MAX_BYTES, MAX_CHUNK, MAX_ROWS, PAGE_ROWS, Request, Row};
use cellule_runtime::Error;
use std::io::Write;
/// Canonical digest of all ordered rows, independent of mutable draft state.
pub fn dataset_digest(rows: &[Row]) -> cellule_runtime::Result<[u8; 32]> {
    if rows.len() > MAX_ROWS as usize {
        return Err(Error::Command("dataset exceeds 512 rows"));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.report.dataset.v1\0");
    hash.update(&(rows.len() as u32).to_be_bytes());
    let mut previous = 0;
    for row in rows {
        row.validate()?;
        if row.id <= previous {
            return Err(Error::Command("dataset row IDs repeat or regress"));
        }
        previous = row.id;
        hash.update(&row.id.to_be_bytes());
        hash.update(&(row.label.len() as u32).to_be_bytes());
        hash.update(row.label.as_bytes());
        hash.update(&row.units.to_be_bytes());
    }
    Ok(*hash.finalize().as_bytes())
}
struct Bounded(Vec<u8>, usize);
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > self.1 {
            return Err(std::io::Error::other("CSV output exceeds declared bound"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn write(rows: &[Row], header: bool, limit: usize) -> Result<Vec<u8>, crate::BoxError> {
    dataset_digest(rows)?;
    let mut buffer = Bounded(Vec::new(), limit);
    {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Always)
            .terminator(csv::Terminator::CRLF)
            .from_writer(&mut buffer);
        if header {
            writer.write_record(["id", "label", "units"])?;
        }
        for row in rows {
            writer.write_record([row.id.to_string(), row.label.clone(), row.units.to_string()])?;
        }
        writer.flush()?;
    }
    Ok(buffer.0)
}
/// Encodes one page as fully quoted UTF-8 CSV records with CRLF terminators.
pub fn encode_page(rows: &[Row]) -> Result<Vec<u8>, crate::BoxError> {
    if rows.is_empty() || rows.len() > PAGE_ROWS as usize {
        return Err("CSV page must contain 1..32 rows".into());
    }
    write(rows, false, MAX_CHUNK)
}
/// Encodes the complete bounded CSV, including a fixed canonical header.
pub fn encode_report(rows: &[Row]) -> Result<Vec<u8>, crate::BoxError> {
    write(rows, true, MAX_BYTES)
}
/// Parses and checks canonical page bytes, IDs, numeric fields, and row count.
pub fn decode_page(bytes: &[u8]) -> Result<Vec<Row>, crate::BoxError> {
    if bytes.is_empty() || bytes.len() > MAX_CHUNK {
        return Err("CSV page exceeds bound".into());
    }
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(bytes);
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        if record.len() != 3 || rows.len() >= PAGE_ROWS as usize {
            return Err("CSV page record shape or count differs".into());
        }
        let row = Row {
            id: record.get(0).ok_or("CSV row ID absent")?.parse()?,
            label: record.get(1).ok_or("CSV label absent")?.into(),
            units: record.get(2).ok_or("CSV units absent")?.parse()?,
        };
        row.validate()?;
        rows.push(row);
    }
    if encode_page(&rows)? != bytes {
        return Err("CSV page encoding is not canonical".into());
    }
    Ok(rows)
}
pub(crate) fn validate_chunks(
    request: &Request,
    chunks: &[Chunk],
    complete: bool,
) -> cellule_runtime::Result<()> {
    if chunks.len() > MAX_ROWS.div_ceil(PAGE_ROWS) as usize {
        return Err(Error::Command("export chunk count exceeds bound"));
    }
    let mut after = 0;
    let mut rows = 0;
    for (index, chunk) in chunks.iter().enumerate() {
        chunk.validate(request)?;
        if chunk.after != after || index + 1 < chunks.len() && !chunk.more {
            return Err(Error::Command(
                "export chunks repeat, skip, or continue past completion",
            ));
        }
        after = chunk.last;
        rows += chunk.rows;
    }
    if rows > request.snapshot.rows
        || complete
            && (rows != request.snapshot.rows || chunks.last().is_some_and(|chunk| chunk.more))
        || chunks.last().is_some_and(|chunk| !chunk.more) && rows != request.snapshot.rows
    {
        return Err(Error::Command(
            "export chunk total differs from sealed count",
        ));
    }
    Ok(())
}
/// Reconstructs ordered rows from verified page bytes and proves the exact sealed content digest.
pub fn reconstruct(
    request: &Request,
    chunks: &[(Chunk, Vec<u8>)],
) -> Result<Vec<Row>, crate::BoxError> {
    request.validate()?;
    validate_chunks(
        request,
        &chunks
            .iter()
            .map(|(chunk, _)| chunk.clone())
            .collect::<Vec<_>>(),
        true,
    )?;
    let mut rows = Vec::with_capacity(request.snapshot.rows as usize);
    for (chunk, bytes) in chunks {
        if bytes.len() != chunk.artifact.bytes as usize
            || *blake3::hash(bytes).as_bytes() != chunk.artifact.digest
        {
            return Err("CSV page bytes differ from immutable manifest pin".into());
        }
        let page = decode_page(bytes)?;
        if page.len() != chunk.rows as usize
            || page.first().is_none_or(|row| row.id <= chunk.after)
            || page.last().is_none_or(|row| row.id != chunk.last)
        {
            return Err("CSV page row boundaries differ from progress".into());
        }
        rows.extend(page);
    }
    if rows.len() != request.snapshot.rows as usize
        || dataset_digest(&rows)? != request.snapshot.digest
    {
        return Err("reconstructed rows contain duplicates, gaps, or changed sealed data".into());
    }
    Ok(rows)
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #[test]
    fn csv_roundtrips_unicode_quotes_and_embedded_newlines() {
        let rows = vec![
            crate::Row {
                id: 3,
                label: "Café, \"west\"\nqueue".into(),
                units: 7,
            },
            crate::Row {
                id: 8,
                label: "South".into(),
                units: 0,
            },
        ];
        let bytes = super::encode_page(&rows).unwrap();
        assert_eq!(super::decode_page(&bytes).unwrap(), rows);
        assert!(bytes.ends_with(b"\r\n"));
        assert!(super::decode_page(b"3,West,7\n").is_err());
    }
    #[test]
    fn duplicates_and_excess_page_records_are_rejected() {
        let row = crate::Row {
            id: 1,
            label: "West".into(),
            units: 7,
        };
        assert!(super::dataset_digest(&[row.clone(), row.clone()]).is_err());
        assert!(super::encode_page(&vec![row; 33]).is_err());
    }
    #[test]
    fn empty_report_retains_a_canonical_header() {
        assert_eq!(
            super::encode_report(&[]).unwrap(),
            b"\"id\",\"label\",\"units\"\r\n"
        );
    }
}
