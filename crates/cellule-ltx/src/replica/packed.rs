//! One bounded, content-addressed segment/index object. Native LTX bytes stay exact.
use bytes::{Bytes, BytesMut};
use cellule_store::MultipartUploadSource;
use std::sync::Arc;

use super::{
    AppendBody, CellReplica, PreparedSegment, SegmentDescriptor, upload::SINGLE_PUT_BYTES,
};
use crate::{LtxError, Result};

pub(super) const HEADER_BYTES: u64 = 64;
const MAGIC: &[u8; 8] = b"CRBPACK1";

pub(super) async fn freeze(
    replica: &CellReplica,
    mut segment: PreparedSegment,
) -> Result<PreparedSegment> {
    let length = HEADER_BYTES
        .checked_add(segment.descriptor.info.size_bytes)
        .and_then(|n| n.checked_add(segment.index.len() as u64))
        .ok_or(LtxError::LTXCorrupted)?;
    if length > SINGLE_PUT_BYTES || matches!(segment.body, AppendBody::Bundle) {
        return Ok(segment);
    }
    let body = match &segment.body {
        AppendBody::Native(source) => {
            let source = source.clone();
            let size = segment.descriptor.info.size_bytes;
            replica.host.run(move || source.read_small(size)).await??
        }
        AppendBody::Frozen(bytes) => bytes.clone(),
        AppendBody::Bundle | AppendBody::Packed(_) => return Err(LtxError::LTXCorrupted),
    };
    if body.len() as u64 != segment.descriptor.info.size_bytes
        || *blake3::hash(&body).as_bytes() != segment.descriptor.info.blake3
        || *blake3::hash(&segment.index).as_bytes() != segment.descriptor.index_digest
    {
        return Err(LtxError::ChecksumMismatch);
    }
    // Freeze before provider I/O; retained memory is bounded by the existing
    // single-PUT limit and survives cancellation with its preparation owner.
    let mut bytes = BytesMut::with_capacity(length as usize);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(body.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&(segment.index.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(&segment.descriptor.info.blake3);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&segment.index);
    let bytes = bytes.freeze();
    let level = segment.descriptor.level();
    segment.descriptor = SegmentDescriptor::packed(
        segment.descriptor.info,
        segment.descriptor.index_digest,
        segment.descriptor.index_length,
        *blake3::hash(&bytes).as_bytes(),
    )
    .with_level(level);
    let source: Arc<dyn MultipartUploadSource> = match segment.body {
        AppendBody::Native(source) => source,
        AppendBody::Frozen(bytes) => Arc::new(super::upload::FrozenCapture(bytes)),
        AppendBody::Bundle | AppendBody::Packed(_) => return Err(LtxError::LTXCorrupted),
    };
    // Retain the pinned source and shared index, rather than all frozen bodies
    // across a preparation cohort. Upload buffers only while holding host I/O.
    segment.body = AppendBody::Packed(Arc::new(PackedCapture {
        header: Bytes::copy_from_slice(&bytes[..HEADER_BYTES as usize]),
        source,
        body_length: segment.descriptor.info.size_bytes,
        index: segment.index.clone(),
    }));
    Ok(segment)
}

pub(super) fn verify(bytes: &Bytes, descriptor: &SegmentDescriptor) -> Result<()> {
    let body_end = HEADER_BYTES
        .checked_add(descriptor.info.size_bytes)
        .ok_or(LtxError::LTXCorrupted)?;
    let total = body_end
        .checked_add(descriptor.index_length)
        .ok_or(LtxError::LTXCorrupted)?;
    if total > SINGLE_PUT_BYTES
        || bytes.len() as u64 != total
        || bytes.len() < HEADER_BYTES as usize
    {
        return Err(LtxError::LTXCorrupted);
    }
    if &bytes[..8] != MAGIC
        || bytes[8..16] != descriptor.info.size_bytes.to_be_bytes()
        || bytes[16..24] != descriptor.index_length.to_be_bytes()
        || bytes[24..32] != [0; 8]
        || bytes[32..64] != descriptor.info.blake3
        || *blake3::hash(bytes).as_bytes() != descriptor.object_digest()
        || *blake3::hash(&bytes[HEADER_BYTES as usize..body_end as usize]).as_bytes()
            != descriptor.info.blake3
        || *blake3::hash(&bytes[body_end as usize..]).as_bytes() != descriptor.index_digest
    {
        return Err(LtxError::ChecksumMismatch);
    }
    for entry in
        crate::paged::validated_index_entries(&bytes[body_end as usize..], &descriptor.info)?
    {
        entry?;
    }
    Ok(())
}

struct PackedCapture {
    header: Bytes,
    source: Arc<dyn MultipartUploadSource>,
    body_length: u64,
    index: Bytes,
}

#[async_trait::async_trait]
impl MultipartUploadSource for PackedCapture {
    async fn byte_len(&self) -> cellule_store::Result<u64> {
        // Propagate source length changes to the existing exact-upload guard.
        Ok(HEADER_BYTES + self.source.byte_len().await? + self.index.len() as u64)
    }

    async fn read_exact(&self, offset: u64, length: usize) -> cellule_store::Result<Bytes> {
        let total = HEADER_BYTES + self.body_length + self.index.len() as u64;
        if offset != 0 || length as u64 != total {
            return Err(cellule_store::StorageError::ReadRejected {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "packed object requires one bounded exact read",
                )),
            });
        }
        let body = self.source.read_exact(0, self.body_length as usize).await?;
        let mut bytes = BytesMut::with_capacity(length);
        bytes.extend_from_slice(&self.header);
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&self.index);
        Ok(bytes.freeze())
    }
}
