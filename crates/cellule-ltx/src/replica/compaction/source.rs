//! Source spooling for one compaction pass.

use super::*;
use futures_util::FutureExt as _;

pub(super) async fn spool_selected_bodies(
    replica: &CellReplica,
    descriptors: &[SegmentDescriptor],
    scratch: &Arc<ScratchFiles>,
    destination: &Path,
) -> Result<Vec<BodySpoolInput>> {
    let mut planned = Vec::with_capacity(descriptors.len());
    let mut total_bytes = 0_u64;
    for descriptor in descriptors {
        let start = total_bytes;
        total_bytes = total_bytes
            .checked_add(descriptor.info.size_bytes)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionBodySpool))?;
        planned.push((descriptor.clone(), start));
    }

    let results = stream::iter(planned.into_iter().enumerate().map(
        |(order, (descriptor, output_start))| {
            async move {
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
                    let _permit = replica.host.io_permit().await?;
                    let bytes = replica
                        .layout
                        .store()
                        .range_get(&path, source_offset..next)
                        .await?;
                    drop(_permit);
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
            .map(move |result| (order, result))
        },
    ))
    .buffer_unordered(SEGMENT_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    let spooled = ordered_results(results)?;
    sync_spool(scratch, destination, total_bytes).await?;
    Ok(spooled)
}

pub(super) async fn spool_indexes(
    replica: &CellReplica,
    descriptors: &[SegmentDescriptor],
    scratch: &Arc<ScratchFiles>,
    destination: &Path,
) -> Result<Vec<SpoolInput>> {
    let mut planned = Vec::with_capacity(descriptors.len());
    let mut total_bytes = 0_u64;
    for descriptor in descriptors {
        if descriptor.index_length == 0
            || descriptor.index_length % crate::paged::ENTRY_BYTES as u64 != 0
        {
            return Err(LtxError::LTXCorrupted);
        }
        let start = total_bytes;
        total_bytes = total_bytes
            .checked_add(descriptor.index_length)
            .ok_or(LtxError::Limit(crate::LimitKind::CompactionIndexSpool))?;
        planned.push((descriptor.clone(), start));
    }

    let results = stream::iter(planned.into_iter().enumerate().map(
        |(order, (descriptor, output_start))| {
            async move {
                let mut file = scratch.open(destination).await?;
                let path = replica.layout.incarnation_object_path(
                    &replica.cell,
                    &replica.incarnation,
                    &descriptor.index_digest,
                    CellObjectKind::Index,
                );
                let mut source_offset = 0_u64;
                let mut hasher = blake3::Hasher::new();
                let mut validator = crate::paged::IndexValidator::new(&descriptor.info);
                while source_offset < descriptor.index_length {
                    let length = (descriptor.index_length - source_offset).min(INDEX_READ_BYTES);
                    let _permit = replica.host.io_permit().await?;
                    let bytes = replica
                        .layout
                        .store()
                        .range_get(&path, source_offset..source_offset + length)
                        .await?;
                    drop(_permit);
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
            .map(move |result| (order, result))
        },
    ))
    .buffer_unordered(SEGMENT_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    let inputs = ordered_results(results)?;
    sync_spool(scratch, destination, total_bytes).await?;
    Ok(inputs)
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
