//! Source spooling for one compaction pass.

use super::*;
use futures_util::FutureExt as _;

pub(super) async fn spool_selected(
    replica: &CellReplica,
    descriptors: &[SegmentDescriptor],
    scratch: &Arc<ScratchFiles>,
    body_destination: &Path,
    index_destination: &Path,
) -> Result<(Vec<SpoolInput>, Vec<BodySpoolInput>)> {
    let mut planned = Vec::with_capacity(descriptors.len());
    let (mut body_bytes, mut index_bytes) = (0_u64, 0_u64);
    for descriptor in descriptors {
        if descriptor.index_length == 0
            || descriptor.index_length % crate::paged::ENTRY_BYTES as u64 != 0
        {
            return Err(LtxError::LTXCorrupted);
        }
        planned.push((descriptor.clone(), body_bytes, index_bytes));
        body_bytes = body_bytes
            .checked_add(descriptor.info.size_bytes)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionBodySpool))?;
        index_bytes = index_bytes
            .checked_add(descriptor.index_length)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionIndexSpool))?;
    }
    let results = stream::iter(planned.into_iter().enumerate().map(
        |(order, (descriptor, body_start, index_start))| {
            async move {
                if matches!(
                    descriptor.object_kind(),
                    CellObjectKind::Packed | CellObjectKind::SharedPacked
                ) {
                    return spool_packed(
                        replica,
                        descriptor,
                        scratch,
                        body_destination,
                        index_destination,
                        body_start,
                        index_start,
                    )
                    .await;
                }
                // Separate large objects retain bounded streaming. Join both
                // transfers even on error so dispatched file jobs keep ownership.
                let (index, body) = futures_util::future::join(
                    spool_index(
                        replica,
                        descriptor.clone(),
                        scratch,
                        index_destination,
                        index_start,
                    ),
                    spool_body(replica, descriptor, scratch, body_destination, body_start),
                )
                .await;
                Ok((index?, body?))
            }
            .map(move |result| (order, result))
        },
    ))
    .buffer_unordered(SEGMENT_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    let (indexes, bodies) = ordered_results(results)?.into_iter().unzip();
    let (body_sync, index_sync) = futures_util::future::join(
        sync_spool(scratch, body_destination, body_bytes),
        sync_spool(scratch, index_destination, index_bytes),
    )
    .await;
    body_sync?;
    index_sync?;
    Ok((indexes, bodies))
}

#[allow(
    clippy::too_many_arguments,
    reason = "one verified object supplies two independently bounded spool extents"
)]
async fn spool_packed(
    replica: &CellReplica,
    descriptor: SegmentDescriptor,
    scratch: &Arc<ScratchFiles>,
    body_destination: &Path,
    index_destination: &Path,
    body_start: u64,
    index_start: u64,
) -> Result<(SpoolInput, BodySpoolInput)> {
    let path = replica.layout.incarnation_object_path(
        &replica.cell,
        &replica.incarnation,
        &descriptor.object_digest(),
        descriptor.object_kind(),
    );
    let permit = replica.host.io_permit().await?;
    let (bytes, _) = replica
        .layout
        .store()
        .get_with_etag_bounded(&path, super::super::upload::SINGLE_PUT_BYTES)
        .await?;
    if descriptor.object_kind() == CellObjectKind::SharedPacked {
        super::super::shared::verify(&bytes, &descriptor, &replica.cell, &replica.incarnation)?;
    } else {
        super::super::packed::verify(&bytes, &descriptor)?;
    }
    let body_offset = descriptor.offset() as usize;
    let index_offset = (descriptor.offset() + descriptor.info.size_bytes) as usize;
    let body = bytes.slice(body_offset..index_offset);
    let index = bytes.slice(index_offset..index_offset + descriptor.index_length as usize);
    let mut body_file = scratch.open(body_destination).await?;
    let mut index_file = scratch.open(index_destination).await?;
    replica
        .host
        .run(move || {
            // One I/O slot bounds the pack's memory until both dispatched writes
            // finish, including cancellation. ScratchFile retains its file owner.
            let _permit = permit;
            body_file.write_all_at(body_start, &body)?;
            index_file.write_all_at(index_start, &index)?;
            Ok::<_, LtxError>(())
        })
        .await??;
    let length = descriptor.index_length;
    Ok((
        SpoolInput {
            descriptor: descriptor.clone(),
            start: index_start,
            length,
        },
        BodySpoolInput {
            descriptor,
            start: body_start,
        },
    ))
}

async fn spool_body(
    replica: &CellReplica,
    descriptor: SegmentDescriptor,
    scratch: &Arc<ScratchFiles>,
    destination: &Path,
    output_start: u64,
) -> Result<BodySpoolInput> {
    let mut file = scratch.open(destination).await?;
    let source_start = descriptor.offset();
    let source_end = source_start
        .checked_add(descriptor.info.size_bytes)
        .ok_or(LtxError::LTXCorrupted)?;
    let path = replica.layout.incarnation_object_path(
        &replica.cell,
        &replica.incarnation,
        &descriptor.object_digest(),
        descriptor.object_kind(),
    );
    let mut source_offset = source_start;
    let mut hasher = blake3::Hasher::new();
    while source_offset < source_end {
        let next = source_offset
            .checked_add((source_end - source_offset).min(FRAME_READ_BYTES))
            .ok_or(LtxError::LTXCorrupted)?;
        let permit = replica.host.io_permit().await?;
        let bytes = replica
            .layout
            .store()
            .range_get(&path, source_offset..next)
            .await?;
        drop(permit);
        if bytes.len() as u64 != next - source_offset {
            return Err(LtxError::ChecksumMismatch);
        }
        hasher.update(&bytes);
        let output_offset = output_start
            .checked_add(source_offset - source_start)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionBodySpool))?;
        file = replica
            .host
            .run(move || {
                file.write_all_at(output_offset, &bytes)?;
                Ok::<_, LtxError>(file)
            })
            .await??;
        source_offset = next;
    }
    if *hasher.finalize().as_bytes() != descriptor.info.blake3 {
        return Err(LtxError::ChecksumMismatch);
    }
    Ok(BodySpoolInput {
        descriptor,
        start: output_start,
    })
}

async fn spool_index(
    replica: &CellReplica,
    descriptor: SegmentDescriptor,
    scratch: &Arc<ScratchFiles>,
    destination: &Path,
    output_start: u64,
) -> Result<SpoolInput> {
    let mut file = scratch.open(destination).await?;
    let (object, kind, index_start) = descriptor.index_extent();
    let path =
        replica
            .layout
            .incarnation_object_path(&replica.cell, &replica.incarnation, &object, kind);
    let mut source_offset = 0_u64;
    let mut hasher = blake3::Hasher::new();
    let mut validator = crate::paged::IndexValidator::new(&descriptor.info);
    while source_offset < descriptor.index_length {
        let length = (descriptor.index_length - source_offset).min(INDEX_READ_BYTES);
        let permit = replica.host.io_permit().await?;
        let bytes = replica
            .layout
            .store()
            .range_get(
                &path,
                index_start + source_offset..index_start + source_offset + length,
            )
            .await?;
        drop(permit);
        if bytes.len() as u64 != length {
            return Err(LtxError::ChecksumMismatch);
        }
        hasher.update(&bytes);
        let output_offset = output_start
            .checked_add(source_offset)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionIndexSpool))?;
        let returned = replica
            .host
            .run(move || {
                for entry in bytes.as_chunks::<{ crate::paged::ENTRY_BYTES }>().0 {
                    validator.validate(crate::paged::decode_index_entry(entry)?)?;
                }
                file.write_all_at(output_offset, &bytes)?;
                Ok::<_, LtxError>((file, validator))
            })
            .await??;
        file = returned.0;
        validator = returned.1;
        source_offset += length;
    }
    if *hasher.finalize().as_bytes() != descriptor.index_digest {
        return Err(LtxError::ChecksumMismatch);
    }
    let length = descriptor.index_length;
    Ok(SpoolInput {
        descriptor,
        start: output_start,
        length,
    })
}

// Finished transfers refill the same bounded window immediately. Restore
// descriptor order, including first-error selection, before consuming results.
fn ordered_results<T>(mut results: Vec<(usize, Result<T>)>) -> Result<Vec<T>> {
    results.sort_unstable_by_key(|(order, _)| *order);
    results.into_iter().map(|(_, result)| result).collect()
}

async fn sync_spool(scratch: &Arc<ScratchFiles>, path: &Path, expected_bytes: u64) -> Result<()> {
    let mut file = scratch.open(path).await?;
    scratch
        .host
        .run(move || {
            if file.file_len()? != expected_bytes {
                return Err(LtxError::LTXCorrupted);
            }
            file.sync_all()?;
            Ok::<_, LtxError>(())
        })
        .await??;
    Ok(())
}

pub(super) struct SpoolCursor {
    input: SpoolInput,
    offset: u64,
    buffer: Vec<u8>,
    buffered_offset: usize,
    buffer_bytes: usize,
    pub(super) current: Option<DirectoryEntry>,
}

impl SpoolCursor {
    pub(super) fn new(
        input: SpoolInput,
        source: &mut dyn FileIo,
        buffer_entries: usize,
    ) -> Result<Self> {
        let mut cursor = Self {
            offset: input.start,
            buffer: Vec::new(),
            buffered_offset: 0,
            buffer_bytes: buffer_entries * crate::paged::ENTRY_BYTES,
            input,
            current: None,
        };
        cursor.advance(source)?;
        Ok(cursor)
    }

    pub(super) fn advance(&mut self, source: &mut dyn FileIo) -> Result<()> {
        let end = self
            .input
            .start
            .checked_add(self.input.length)
            .ok_or(LtxError::LTXCorrupted)?;
        if self.offset == end {
            self.current = None;
            return Ok(());
        }
        if self.offset > end || end - self.offset < crate::paged::ENTRY_BYTES as u64 {
            return Err(LtxError::LTXCorrupted);
        }
        if self.buffered_offset == self.buffer.len() {
            let length = (end - self.offset).min(self.buffer_bytes as u64) as usize;
            self.buffer = source.read_exact_at(self.offset, length)?;
            if self.buffer.len() != length {
                return Err(LtxError::LTXCorrupted);
            }
            self.buffered_offset = 0;
        }
        let next = self.buffered_offset + crate::paged::ENTRY_BYTES;
        let bytes = self
            .buffer
            .get(self.buffered_offset..next)
            .ok_or(LtxError::LTXCorrupted)?;
        let entry = crate::paged::decode_index_entry(bytes)?;
        self.buffered_offset = next;
        let descriptor = &self.input.descriptor;
        self.current = Some(DirectoryEntry {
            page: entry.page,
            object: descriptor.object_digest(),
            offset: descriptor
                .offset()
                .checked_add(entry.offset)
                .ok_or(LtxError::LTXCorrupted)?,
            length: u32::try_from(entry.size).map_err(|_| LtxError::LTXCorrupted)?,
            frame_hash: entry.hash,
            checksum: entry.checksum,
        });
        self.offset += crate::paged::ENTRY_BYTES as u64;
        Ok(())
    }
}
