//! Bounded application-scoped capture sharing. Upload supplies no authority.

use std::{collections::BTreeSet, path::Path, sync::Arc};

use bytes::Bytes;

use super::{
    AppendBaseState, AppendBody, AppendInput, BodyLocation, CellPagedDatabase, CellReplica,
    PreparedRoot, PreparedSegment, RootRef, SegmentDescriptor, coalesce, compaction::scratch,
    packed, upload::SINGLE_PUT_BYTES,
};
use crate::{CaptureBatch, CellObjectKind, LtxError, Position, Result};

const MAGIC: &[u8; 8] = b"CRBSH001";
const HEADER_BYTES: u64 = 16;
const SCOPE_BYTES: u64 = 48;
/// Bound shared data to one verified small-object transfer.
pub const SHARED_PUBLICATION_BYTES: u64 = SINGLE_PUT_BYTES;
/// Bound cohort ownership tables independently of compressed body size.
pub const SHARED_PUBLICATION_ROWS: usize = 64;
pub(super) const FIRST_BODY_OFFSET: u64 = HEADER_BYTES + SCOPE_BYTES + packed::HEADER_BYTES;

/// Verified, pinned captures awaiting one bounded shared upload.
///
/// Construction admits every original segment before representation reduction.
/// This owns no Cell preparation permit and confers no root or writer authority.
pub struct SharedCaptures {
    replica: CellReplica,
    position: Position,
    segments: Vec<PreparedSegment>,
    original: Vec<SegmentDescriptor>,
    encoded_bytes: u64,
}

impl CellPagedDatabase {
    pub(super) async fn verify_shared_range(
        &self,
        object: [u8; 32],
        start: u64,
        end: u64,
        origin: crate::LtxReadOrigin,
    ) -> Result<()> {
        for descriptor in self.shared_segments.iter().filter(|descriptor| {
            descriptor.object_digest() == object
                && descriptor.offset() < end
                && descriptor.offset() + descriptor.info.size_bytes > start
        }) {
            let mut expected = [0; (SCOPE_BYTES + packed::HEADER_BYTES) as usize];
            expected[..32].copy_from_slice(&self.replica.cell);
            expected[32..48].copy_from_slice(&self.replica.incarnation);
            expected[48..].copy_from_slice(&packed::header(descriptor));
            let offset = descriptor
                .offset()
                .checked_sub(expected.len() as u64)
                .ok_or(LtxError::LTXCorrupted)?;
            if super::cache::shared_header(&self.replica.layout, object, offset, &expected, false)?
            {
                continue;
            }
            let path = self.replica.layout.incarnation_object_path(
                &self.replica.cell,
                &self.replica.incarnation,
                &object,
                CellObjectKind::SharedPacked,
            );
            let _permit = self.replica.host.io_permit().await?;
            let bytes = self
                .replica
                .layout
                .store()
                .range_get(&path, offset..descriptor.offset())
                .await;
            self.replica.host.observe_ltx_origin_request(
                origin,
                bytes.is_ok(),
                bytes.as_ref().map_or(0, Bytes::len),
            );
            if bytes?.as_ref() != expected {
                return Err(LtxError::LTXCorrupted);
            }
            super::cache::shared_header(&self.replica.layout, object, offset, &expected, true)?;
        }
        Ok(())
    }
}

impl SharedCaptures {
    /// Exact encoded row bytes, excluding the one shared header.
    #[must_use]
    pub const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    /// Number of scoped rows retained by this input.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.segments.len()
    }

    /// Immutable namespace and transport binding; supplies no authority.
    #[must_use]
    pub fn storage_binding(&self) -> (u64, String) {
        (
            self.replica.layout.immutable_cache_identity(),
            self.replica.layout.application_prefix().to_string(),
        )
    }
}

impl super::RecoveryOverlay {
    /// Conservative encoded-byte and row bounds for this Cell's shared input.
    ///
    /// Includes the one object header, every original scoped row/body and its
    /// maximum index. This allocates no bodies and grants no verified root or
    /// authority; use it to reserve working memory before preparation.
    pub fn shared_input_upper_bound(&self) -> Result<(u64, usize)> {
        let (repository, epoch) =
            crate::bundle::cell_identity(&self.predecessor.cell, &self.predecessor.incarnation);
        let mut count = 0_usize;
        let mut upper = HEADER_BYTES;
        for row in self
            .bundle
            .rows()
            .iter()
            .filter(|row| row.repository == repository && row.epoch == epoch)
        {
            count += 1;
            upper = upper
                .checked_add(SCOPE_BYTES + packed::HEADER_BYTES)
                .and_then(|bytes| bytes.checked_add(row.info.size_bytes))
                .and_then(|bytes| bytes.checked_add(u64::from(row.info.database_pages) * 60))
                .ok_or(LtxError::Limit(crate::LimitKind::CellBundleBytes))?;
        }
        Ok((upper, count))
    }
}

/// One Cell's verified inputs from a bounded publication cohort.
///
/// Multi-row inputs contain uploaded shared extents. A singleton retains its
/// native captures for the canonical pack factory. Neither grants authority;
/// the runtime must select a prepared root under the Cell's writer fence.
#[derive(Clone)]
pub struct SharedAppend {
    replica: CellReplica,
    position: Position,
    segments: Vec<PreparedSegment>,
    original: Vec<SegmentDescriptor>,
}

impl CellReplica {
    /// Pins and verifies captures eligible for one bounded shared publication.
    /// Large captures retain the ordinary streaming preparation path.
    pub async fn shared_captures(&self, cuts: &CaptureBatch) -> Result<Option<SharedCaptures>> {
        self.admit_capture_batch(cuts)?;
        let upper = cuts
            .segments
            .iter()
            .try_fold(HEADER_BYTES, |bytes, segment| {
                bytes
                    .checked_add(SCOPE_BYTES + packed::HEADER_BYTES)
                    .and_then(|bytes| bytes.checked_add(segment.info().size_bytes))
                    .and_then(|bytes| {
                        bytes.checked_add(u64::from(segment.info().database_pages) * 60)
                    })
                    .ok_or(LtxError::Limit(crate::LimitKind::CellBundleBytes))
            })?;
        if cuts.segments.len() > SHARED_PUBLICATION_ROWS || upper > SINGLE_PUT_BYTES {
            return Ok(None);
        }
        let inputs = self.prepare_captured_inputs(&cuts.segments).await?;
        self.shared_inputs(inputs, cuts.position).await.map(Some)
    }

    /// Verifies small recovered rows through the canonical bundle-input reader.
    ///
    /// Original scopes and endpoint must match the overlay. Every original cut
    /// remains in the returned input's admission facts before coalescing; the
    /// later canonical root factory verifies the fresh base and complete chain.
    /// Large tails return `None` for ordinary recovery preparation. The caller
    /// retains memory and artifact admission through this operation and upload.
    pub async fn shared_recovered_captures(
        &self,
        overlay: &super::RecoveryOverlay,
    ) -> Result<Option<SharedCaptures>> {
        self.validate_recovery_overlay(overlay)?;
        if overlay.bundle.len() > self.limits.max_plan_bytes {
            return Err(LtxError::Limit(crate::LimitKind::CellBundleBytes));
        }
        let (upper, count) = overlay.shared_input_upper_bound()?;
        if count > SHARED_PUBLICATION_ROWS || upper > SINGLE_PUT_BYTES {
            return Ok(None);
        }
        let inputs = self.read_bundle_inputs(&overlay.bundle, true)?;
        if !inputs.independent || inputs.target != overlay.final_position {
            return Err(LtxError::ChecksumMismatch);
        }
        self.shared_inputs(inputs.inputs, inputs.target)
            .await
            .map(Some)
    }

    async fn shared_inputs(
        &self,
        inputs: Vec<AppendInput>,
        position: Position,
    ) -> Result<SharedCaptures> {
        let segments: Vec<_> = inputs
            .into_iter()
            .map(|input| PreparedSegment {
                descriptor: SegmentDescriptor::native(
                    input.info,
                    *blake3::hash(&input.index).as_bytes(),
                    input.index.len() as u64,
                ),
                index: input.index,
                body: input.body,
            })
            .collect();
        let original = segments
            .iter()
            .map(|segment: &PreparedSegment| segment.descriptor.clone())
            .collect();
        let segments = coalesce::run(self, segments).await?;
        let encoded_bytes = segments.iter().try_fold(0_u64, |bytes, segment| {
            bytes
                .checked_add(SCOPE_BYTES + packed::HEADER_BYTES)
                .and_then(|bytes| bytes.checked_add(segment.descriptor.info.size_bytes))
                .and_then(|bytes| bytes.checked_add(segment.descriptor.index_length))
                .ok_or(LtxError::LTXCorrupted)
        })?;
        Ok(SharedCaptures {
            replica: self.clone(),
            position,
            segments,
            original,
            encoded_bytes,
        })
    }

    /// Uploads a multi-row cohort once and returns independently scoped inputs.
    /// A singleton retains its verified native inputs: `prepare_shared` uses the
    /// ordinary pack factory without a redundant shared file or scratch upload.
    /// The caller owns retained-memory admission for row/index tables and the
    /// bounded upload buffer. Scratch uses the same host ledger as compaction.
    pub async fn upload_shared(
        inputs: Vec<SharedCaptures>,
        scratch_directory: &Path,
    ) -> Result<Vec<SharedAppend>> {
        let first = inputs
            .first()
            .ok_or(LtxError::InvalidState("empty shared publication"))?;
        let binding = first.storage_binding();
        let replica = first.replica.clone();
        let size = inputs.iter().try_fold(HEADER_BYTES, |size, input| {
            if input.storage_binding() != binding {
                return Err(LtxError::InvalidState(
                    "shared publication crosses storage binding",
                ));
            }
            size.checked_add(input.encoded_bytes)
                .ok_or(LtxError::LTXCorrupted)
        })?;
        let rows: usize = inputs.iter().map(SharedCaptures::rows).sum();
        if size > SINGLE_PUT_BYTES || rows == 0 || rows > SHARED_PUBLICATION_ROWS {
            return Err(LtxError::Limit(crate::LimitKind::CellBundleBytes));
        }
        if inputs.len() == 1 && rows == 1 {
            return Ok(inputs
                .into_iter()
                .map(|input| SharedAppend {
                    replica: input.replica,
                    position: input.position,
                    segments: input.segments,
                    original: input.original,
                })
                .collect());
        }
        let host = replica.host.for_scratch(size).await?;
        let disk = host.reserve_local_disk(size)?;
        let (cleaned, cleanup) = tokio::sync::oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let directory = scratch_directory.to_owned();
        let work_host = host.clone();
        let built = host
            .run(move || -> Result<_> {
                let mut scratch =
                    scratch::ScratchFiles::new(work_host, &directory, runtime, cleaned)
                        .with_disk_reservation(disk);
                let destination = scratch.create("shared-publication")?;
                let work = Arc::new(scratch);
                let mut file = work.host.filesystem.open_rw(&destination)?;
                let mut hasher = blake3::Hasher::new();
                let mut offset = 0_u64;
                let mut write = |bytes: &[u8]| -> Result<()> {
                    file.write_all(bytes)?;
                    hasher.update(bytes);
                    offset += bytes.len() as u64;
                    Ok(())
                };
                let mut header = [0; HEADER_BYTES as usize];
                header[..8].copy_from_slice(MAGIC);
                header[8..12].copy_from_slice(&(rows as u32).to_be_bytes());
                write(&header)?;
                let mut appends = Vec::with_capacity(inputs.len());
                let mut identities = BTreeSet::new();
                let mut body_offset = HEADER_BYTES;
                for input in inputs {
                    let mut segments = Vec::with_capacity(input.segments.len());
                    for mut segment in input.segments {
                        let info = segment.descriptor.info.clone();
                        if !identities.insert((
                            input.replica.cell,
                            input.replica.incarnation,
                            info.min_txid,
                            info.max_txid,
                        )) {
                            return Err(LtxError::InvalidState("duplicate shared capture range"));
                        }
                        let body = match &segment.body {
                            AppendBody::Native(source) => source.read_small(info.size_bytes)?,
                            AppendBody::Frozen(bytes) => bytes.clone(),
                            _ => {
                                return Err(LtxError::InvalidState(
                                    "shared input is not a native capture",
                                ));
                            }
                        };
                        if body.len() as u64 != info.size_bytes
                            || *blake3::hash(&body).as_bytes() != info.blake3
                        {
                            return Err(LtxError::ChecksumMismatch);
                        }
                        write(&input.replica.cell)?;
                        write(&input.replica.incarnation)?;
                        write(&packed::header(&segment.descriptor))?;
                        body_offset += SCOPE_BYTES + packed::HEADER_BYTES;
                        write(&body)?;
                        write(&segment.index)?;
                        segment.descriptor = SegmentDescriptor::shared(
                            info.clone(),
                            segment.descriptor.index_digest,
                            segment.descriptor.index_length,
                            [0; 32],
                            body_offset,
                        );
                        body_offset += info.size_bytes + segment.index.len() as u64;
                        segment.body = AppendBody::SharedUploaded;
                        segments.push(segment);
                    }
                    appends.push(SharedAppend {
                        replica: input.replica,
                        position: input.position,
                        segments,
                        original: input.original,
                    });
                }
                if offset != size || file.file_len()? != size {
                    return Err(LtxError::LTXCorrupted);
                }
                // This file is only a transient upload source, never restart
                // state or a durability proof. Close the writer before origin
                // reads; put_source still verifies its exact length and digest.
                // Native captures and follower logs retain their own barriers.
                drop(file);
                let digest = *hasher.finalize().as_bytes();
                for append in &mut appends {
                    for segment in &mut append.segments {
                        segment.descriptor = SegmentDescriptor::shared(
                            segment.descriptor.info.clone(),
                            segment.descriptor.index_digest,
                            segment.descriptor.index_length,
                            digest,
                            segment.descriptor.offset(),
                        );
                    }
                }
                Ok((appends, digest, work, destination))
            })
            .await;
        let result = match built {
            Ok(Ok((appends, digest, scratch, path))) => scratch::upload(
                &replica,
                &scratch,
                &path,
                size,
                &digest,
                CellObjectKind::SharedPacked,
            )
            .await
            .map(|()| appends),
            Ok(Err(error)) | Err(error) => Err(error),
        };
        drop(host);
        let _ = cleanup.await;
        result
    }

    /// Prepares scoped cohort inputs through the canonical root factory.
    pub async fn prepare_shared(
        &self,
        base: Option<&RootRef>,
        append: &SharedAppend,
        commit_sequence: u64,
        schema: u32,
    ) -> Result<PreparedRoot> {
        if self.scope() != append.replica.scope()
            || self.layout.immutable_cache_identity()
                != append.replica.layout.immutable_cache_identity()
            || self.layout.application_prefix() != append.replica.layout.application_prefix()
        {
            return Err(LtxError::InvalidState(
                "shared append belongs to another Cell or store",
            ));
        }
        self.validate_metadata(commit_sequence, schema)?;
        let mut replica = self.clone();
        replica.host = self.host.for_dirty().await?;
        let graph = match base {
            Some(root) => Some(replica.load_graph(root).await?),
            None => None,
        };
        replica.validate_append_sequence(&graph, commit_sequence)?;
        // Check the original chain and budget before reduced representations.
        // Coalescing cannot hide a missing cut or rescue an over-budget append.
        let mut original = graph
            .as_ref()
            .map(|graph| graph.descriptors.clone())
            .unwrap_or_default();
        original.extend(append.original.iter().cloned());
        replica.validate_chain(&original, append.position)?;
        let inputs = append
            .segments
            .iter()
            .map(|segment| {
                let location = match &segment.body {
                    AppendBody::SharedUploaded => BodyLocation::Shared {
                        digest: segment.descriptor.object_digest(),
                        offset: segment.descriptor.offset(),
                    },
                    AppendBody::Native(_) | AppendBody::Frozen(_) => BodyLocation::Native,
                    _ => return Err(LtxError::InvalidState("invalid cohort input body")),
                };
                Ok(AppendInput {
                    info: segment.descriptor.info.clone(),
                    location,
                    index: segment.index.clone(),
                    body: segment.body.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        replica
            .prepare_append(
                base,
                graph.map(AppendBaseState::from),
                inputs,
                append.position,
                commit_sequence,
                schema,
                None,
            )
            .await
    }
}

pub(super) fn verify(
    bytes: &Bytes,
    descriptor: &SegmentDescriptor,
    cell: &[u8; 32],
    incarnation: &[u8; 16],
) -> Result<()> {
    if bytes.len() < HEADER_BYTES as usize
        || bytes.len() as u64 > SINGLE_PUT_BYTES
        || &bytes[..8] != MAGIC
        || bytes[12..16] != [0; 4]
    {
        return Err(LtxError::LTXCorrupted);
    }
    if *blake3::hash(bytes).as_bytes() != descriptor.object_digest() {
        return Err(LtxError::ChecksumMismatch);
    }
    let rows = u32::from_be_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| LtxError::LTXCorrupted)?,
    ) as usize;
    if rows == 0 || rows > SHARED_PUBLICATION_ROWS {
        return Err(LtxError::LTXCorrupted);
    }
    let mut offset = HEADER_BYTES as usize;
    let mut found = false;
    let mut identities = BTreeSet::new();
    for _ in 0..rows {
        let start = offset
            .checked_add(SCOPE_BYTES as usize)
            .ok_or(LtxError::LTXCorrupted)?;
        let body = start
            .checked_add(packed::HEADER_BYTES as usize)
            .ok_or(LtxError::LTXCorrupted)?;
        let header = bytes.get(start..body).ok_or(LtxError::LTXCorrupted)?;
        if &header[..8] != b"CRBPACK1" || header[24..32] != [0; 8] {
            return Err(LtxError::LTXCorrupted);
        }
        let length = u64::from_be_bytes(
            header[8..16]
                .try_into()
                .map_err(|_| LtxError::LTXCorrupted)?,
        );
        let index = u64::from_be_bytes(
            header[16..24]
                .try_into()
                .map_err(|_| LtxError::LTXCorrupted)?,
        );
        let end = (body as u64)
            .checked_add(length)
            .and_then(|end| end.checked_add(index))
            .filter(|end| *end <= bytes.len() as u64)
            .ok_or(LtxError::LTXCorrupted)? as usize;
        if length < 128
            || index == 0
            || !index.is_multiple_of(crate::paged::ENTRY_BYTES as u64)
            || !identities.insert((bytes.slice(offset..start), bytes.slice(start + 32..body)))
        {
            return Err(LtxError::LTXCorrupted);
        }
        if blake3::hash(&bytes[body..body + length as usize])
            .as_bytes()
            .as_slice()
            != &header[32..64]
        {
            return Err(LtxError::ChecksumMismatch);
        }
        if body as u64 == descriptor.offset() {
            if &bytes[offset..offset + 32] != cell || &bytes[offset + 32..start] != incarnation {
                return Err(LtxError::LTXCorrupted);
            }
            let record = bytes.slice(start..end);
            let local = SegmentDescriptor::packed(
                descriptor.info.clone(),
                descriptor.index_digest,
                descriptor.index_length,
                *blake3::hash(&record).as_bytes(),
            );
            packed::verify(&record, &local)?;
            found = true;
        }
        offset = end;
    }
    if offset != bytes.len() || !found {
        return Err(LtxError::LTXCorrupted);
    }
    Ok(())
}
