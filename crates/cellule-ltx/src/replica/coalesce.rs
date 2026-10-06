//! Bounded local delta merging before immutable publication.

use std::{collections::BTreeMap, io};

use bytes::Bytes;
use cellule_store::MultipartUploadSource as _;

use super::{
    AppendBody, CellReplica, PreparedSegment, SegmentDescriptor, upload::SINGLE_PUT_BYTES,
};
use crate::{LtxError, Result, SegmentInfo, codec, ltx};

pub(super) async fn run(
    replica: &CellReplica,
    segments: Vec<PreparedSegment>,
) -> Result<Vec<PreparedSegment>> {
    if segments.len() < 2
        || segments.iter().any(|segment| {
            !matches!(segment.body, AppendBody::Native(_))
                || segment.descriptor.info.size_bytes > SINGLE_PUT_BYTES
                || segment.index.len() as u64 > SINGLE_PUT_BYTES
        })
    {
        return Ok(segments);
    }
    let mut state = MergeState::default();
    let original_bytes = segments.iter().try_fold(0_u64, |sum, segment| {
        sum.checked_add(segment.descriptor.info.size_bytes)
            .and_then(|sum| sum.checked_add(segment.index.len() as u64))
            .ok_or(LtxError::LTXCorrupted)
    })?;
    for segment in &segments {
        let AppendBody::Native(source) = &segment.body else {
            return Err(LtxError::LTXCorrupted);
        };
        let info = segment.descriptor.info.clone();
        if source.byte_len().await? != info.size_bytes {
            return Err(LtxError::ChecksumMismatch);
        }
        let bytes = cellule_store::MultipartUploadSource::read_exact(
            source.as_ref(),
            0,
            info.size_bytes as usize,
        )
        .await?;
        if source.byte_len().await? != info.size_bytes {
            return Err(LtxError::ChecksumMismatch);
        }
        let index = segment.index.clone();
        // Read outside the blocking callback: pinned sources use the same job
        // pool, so nested admitted file jobs could exhaust its entire cohort.
        let merged = replica
            .host
            .run(move || state.apply(bytes, &info, &index))
            .await??;
        let Some(merged) = merged else {
            return Ok(segments);
        };
        state = merged;
    }
    let limits = replica.limits;
    let merged = replica
        .host
        .run(move || state.finish(limits.max_file_bytes.min(SINGLE_PUT_BYTES)))
        .await??;
    let Some(merged) = merged else {
        return Ok(segments);
    };
    if merged.descriptor.info.size_bytes + merged.index.len() as u64 > original_bytes {
        return Ok(segments);
    }
    match replica.admit_segment_representation(&merged.descriptor.info, merged.index.len()) {
        Ok(()) => {}
        Err(LtxError::Limit(crate::LimitKind::CapturedCellLtxBytes)) => return Ok(segments),
        Err(error) => return Err(error),
    }
    Ok(vec![merged])
}

#[derive(Default)]
struct MergeState {
    first: Option<ltx::Header>,
    last: Option<ltx::Header>,
    checksum: u64,
    minimum_commit: Option<u32>,
    pages: BTreeMap<u32, Vec<u8>>,
}

impl MergeState {
    fn apply(mut self, bytes: Bytes, info: &SegmentInfo, index: &Bytes) -> Result<Option<Self>> {
        if bytes.len() as u64 != info.size_bytes || *blake3::hash(&bytes).as_bytes() != info.blake3
        {
            return Err(LtxError::ChecksumMismatch);
        }
        let mut decoder = codec::Decoder::new_with_index(bytes.as_ref());
        decoder.decode_header()?;
        let header = decoder.header;
        if header.no_checksum() {
            return Err(LtxError::LTXCorrupted);
        }
        self.pages.retain(|page, _| *page <= header.commit);
        self.minimum_commit = Some(
            self.minimum_commit
                .map_or(header.commit, |minimum| minimum.min(header.commit)),
        );
        let mut page = vec![0; header.page_size as usize];
        while let Some(entry) = decoder.decode_page(&mut page)? {
            if !self.pages.contains_key(&entry.pgno)
                && (self.pages.len() as u64 + 1) * u64::from(header.page_size) > SINGLE_PUT_BYTES
            {
                return Ok(None);
            }
            self.pages.insert(entry.pgno, page.clone());
        }
        decoder.close()?;
        let (length, digest) = decoder.artifact()?;
        let file = ltx::DecodedFile {
            header,
            trailer: decoder.trailer,
        };
        if SegmentInfo::from_inspected(&file, length, digest) != *info
            || crate::paged::encode_index_from_pages(&decoder.into_replica_index()?)?.as_slice()
                != index.as_ref()
        {
            return Err(LtxError::ChecksumMismatch);
        }
        self.first.get_or_insert(header);
        self.last = Some(header);
        self.checksum = info.post_checksum;
        Ok(Some(self))
    }

    fn finish(self, byte_limit: u64) -> Result<Option<PreparedSegment>> {
        let first = self.first.ok_or(LtxError::TxNotAvailable)?;
        let last = self.last.ok_or(LtxError::TxNotAvailable)?;
        let minimum = self.minimum_commit.ok_or(LtxError::TxNotAvailable)?;
        if minimum < last.commit {
            // A merged header cannot describe an intermediate truncation.
            // Every regrown page must replace its predecessor locator so an
            // old base page cannot reappear when only the final commit remains.
            let lock = ltx::lock_pgno(last.page_size);
            let expected =
                u64::from(last.commit - minimum) - u64::from(minimum < lock && lock <= last.commit);
            if self.pages.range((minimum + 1)..=last.commit).count() as u64 != expected {
                return Ok(None);
            }
        }
        let mut encoder = codec::Encoder::new_block(BoundedBytes {
            bytes: Vec::new(),
            limit: byte_limit,
            overflow: false,
        });
        encoder.encode_header(ltx::Header {
            version: ltx::VERSION,
            page_size: last.page_size,
            commit: last.commit,
            min_txid: first.min_txid,
            max_txid: last.max_txid,
            timestamp: last.timestamp,
            pre_apply_checksum: first.pre_apply_checksum,
            ..ltx::Header::default()
        })?;
        let mut index = Vec::new();
        for (page, bytes) in self.pages {
            let encoded = encoder.encode_page(
                ltx::PageHeader {
                    pgno: page,
                    flags: 0,
                },
                &bytes,
            );
            if encoder.writer.overflow {
                return Ok(None);
            }
            index.push(encoded?);
        }
        let closed = encoder.close(self.checksum);
        if encoder.writer.overflow {
            return Ok(None);
        }
        closed?;
        let file = ltx::DecodedFile {
            header: encoder.header,
            trailer: encoder.trailer,
        };
        let bytes = Bytes::from(encoder.into_writer().bytes);
        let info = SegmentInfo::from_inspected(
            &file,
            bytes.len() as u64,
            *blake3::hash(&bytes).as_bytes(),
        );
        let index = Bytes::from(crate::paged::encode_index_from_pages(&index)?);
        let descriptor =
            SegmentDescriptor::native(info, *blake3::hash(&index).as_bytes(), index.len() as u64);
        Ok(Some(PreparedSegment {
            descriptor,
            index,
            body: AppendBody::Frozen(bytes),
        }))
    }
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: u64,
    overflow: bool,
}

impl io::Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.bytes.len() as u64) {
            self.overflow = true;
            return Err(io::Error::other(
                "coalesced capture exceeds small upload bound",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
