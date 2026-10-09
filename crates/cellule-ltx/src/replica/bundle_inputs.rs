//! Canonical verified Cell inputs before bundle representation reduction.
use super::*;

pub(super) struct BundleInputs {
    pub inputs: Vec<AppendInput>,
    pub target: Position,
    pub independent: bool,
}

impl CellReplica {
    pub(super) fn validate_recovery_overlay(&self, overlay: &RecoveryOverlay) -> Result<()> {
        if overlay.predecessor.cell != self.cell
            || overlay.predecessor.incarnation != self.incarnation
            || overlay.final_commit_sequence <= overlay.predecessor.commit_sequence
        {
            return Err(LtxError::InvalidState("recovery overlay scope"));
        }
        let (repository, epoch) = crate::bundle::cell_identity(&self.cell, &self.incarnation);
        let final_position = overlay
            .bundle
            .rows()
            .iter()
            .rfind(|row| row.repository == repository && row.epoch == epoch)
            .map(|row| row.info.position())
            .ok_or(LtxError::TxNotAvailable)?;
        if final_position != overlay.final_position {
            return Err(LtxError::ChecksumMismatch);
        }
        Ok(())
    }

    pub(super) fn read_bundle_inputs(
        &self,
        bundle: &crate::bundle::Bundle,
        mut independent: bool,
    ) -> Result<BundleInputs> {
        if bundle.len() > self.limits.max_plan_bytes {
            return Err(LtxError::Limit(crate::LimitKind::CellBundleBytes));
        }
        let (repository, epoch) = crate::bundle::cell_identity(&self.cell, &self.incarnation);
        let bundle_digest = bundle.digest();
        let mut inputs: Vec<AppendInput> = Vec::new();
        let mut selected_bytes = 0_u64;
        let mut independent_bytes = 0_u64;
        for (index, row) in bundle.rows().iter().enumerate() {
            if row.repository != repository || row.epoch != epoch {
                continue;
            }
            selected_bytes = selected_bytes
                .checked_add(row.info.size_bytes)
                .ok_or(LtxError::Limit(crate::LimitKind::CapturedCellBundleBytes))?;
            if row.info.size_bytes > self.limits.max_file_bytes
                || selected_bytes > self.limits.max_plan_bytes
            {
                return Err(LtxError::Limit(crate::LimitKind::CapturedCellBundleBytes));
            }
            let bytes = bundle.read_segment(index)?;
            let (file, size, digest, pages) = crate::ltx::inspect_bytes_with_index(&bytes)?;
            if size != row.info.size_bytes
                || digest != row.info.blake3
                || crate::SegmentInfo::from_inspected(&file, size, digest) != row.info
            {
                return Err(LtxError::ChecksumMismatch);
            }
            self.admit_segment_representation(&row.info, pages.len() * crate::paged::ENTRY_BYTES)?;
            let index_bytes = Bytes::from(crate::paged::encode_index_from_pages(&pages)?);
            // Release earlier frozen rows as soon as the original small-pack
            // allowance fails. A shared producer cannot widen this bound.
            if independent {
                match independent_bytes
                    .checked_add(packed::HEADER_BYTES)
                    .and_then(|size| size.checked_add(row.info.size_bytes))
                    .and_then(|size| size.checked_add(index_bytes.len() as u64))
                    .filter(|size| *size <= upload::SINGLE_PUT_BYTES)
                {
                    Some(size) => independent_bytes = size,
                    None => {
                        independent = false;
                        for input in &mut inputs {
                            input.body = AppendBody::Bundle;
                        }
                    }
                }
            }
            inputs.push(AppendInput {
                info: row.info.clone(),
                location: BodyLocation::Bundle {
                    digest: bundle_digest,
                    offset: row.offset,
                },
                index: index_bytes,
                body: if independent {
                    AppendBody::Frozen(bytes)
                } else {
                    AppendBody::Bundle
                },
            });
        }
        let target = inputs
            .last()
            .map(|input| input.info.position())
            .ok_or(LtxError::TxNotAvailable)?;
        Ok(BundleInputs {
            inputs,
            target,
            independent,
        })
    }
}
