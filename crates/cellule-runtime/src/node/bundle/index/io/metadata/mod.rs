//! Bounded windows retain compact indices and authenticate every original extent.
use super::*;
use std::ops::Range;

pub(super) struct Window {
    pub(super) indices: Range<usize>,
    object: Digest,
    range: Range<u64>,
}

pub(super) fn sort<'a>(
    indices: &mut [u16],
    extent: impl Fn(u16) -> Result<&'a Locator>,
) -> Result<()> {
    for index in indices.iter().copied() {
        let extent = extent(index)?;
        codec::validate_extent(extent)?;
        extent
            .object
            .ok_or(Error::Node("unresolved bundle metadata"))?;
    }
    let mut failed = None;
    indices.sort_unstable_by(|left, right| {
        let keys = extent(*left).and_then(|left| {
            extent(*right).map(|right| {
                (left.object.map(|object| *object.as_bytes()), left.offset)
                    .cmp(&(right.object.map(|object| *object.as_bytes()), right.offset))
            })
        });
        match keys {
            Ok(order) => order,
            Err(error) => {
                failed = Some(error);
                std::cmp::Ordering::Equal
            }
        }
    });
    if let Some(error) = failed {
        return Err(error);
    }
    Ok(())
}

pub(super) fn cohort<'a>(
    indices: &[u16],
    next: &mut usize,
    padding: &mut u64,
    extent: impl Fn(u16) -> Result<&'a Locator>,
) -> Result<Vec<Window>> {
    let mut windows = Vec::with_capacity(READ_CONCURRENCY);
    while windows.len() < READ_CONCURRENCY && *next < indices.len() {
        let first = *next;
        let initial = extent(indices[first])?;
        let object = initial
            .object
            .ok_or(Error::Node("unresolved bundle metadata"))?;
        let mut range = initial.offset
            ..initial
                .offset
                .checked_add(initial.bytes)
                .ok_or(Error::Node("bundle metadata extent overflow"))?;
        *next += 1;
        while let Some(index) = indices.get(*next) {
            let candidate = extent(*index)?;
            let gap = candidate.offset.saturating_sub(range.end);
            if candidate.object != Some(object)
                || gap > *padding
                || gap > history::MAX_HISTORY_BYTES
            {
                break;
            }
            let end = candidate
                .offset
                .checked_add(candidate.bytes)
                .ok_or(Error::Node("bundle metadata extent overflow"))?;
            // A gap consumes byte credit once. Overlapping extents cannot buy
            // more padding, and every requested extent remains in the window.
            *padding -= gap;
            range.end = range.end.max(end);
            *next += 1;
        }
        windows.push(Window {
            indices: first..*next,
            object,
            range,
        });
    }
    Ok(windows)
}

pub(super) async fn read(
    layout: &cellule_ltx::CellStorageLayout,
    root: &Root,
    origin: Option<&super::super::super::origin::ProposalMetadata<'_>>,
    window: Window,
) -> Result<(Window, Bytes)> {
    let bytes = super::super::super::origin::read_metadata_range(
        layout,
        root.session,
        root.epoch,
        window.object,
        window.range.clone(),
        origin,
    )
    .await?;
    if bytes.len() as u64 != window.range.end - window.range.start {
        return Err(Error::Node("bundle metadata window is truncated"));
    }
    Ok((window, bytes))
}

impl Window {
    pub(super) fn slice(&self, body: &Bytes, extent: &Locator) -> Result<Bytes> {
        let start = extent
            .offset
            .checked_sub(self.range.start)
            .and_then(|start| usize::try_from(start).ok());
        let end = start.and_then(|start| {
            usize::try_from(extent.bytes)
                .ok()
                .and_then(|bytes| start.checked_add(bytes))
        });
        let (start, end) = start
            .zip(end)
            .filter(|(_, end)| {
                *end <= body.len() && *end as u64 <= self.range.end - self.range.start
            })
            .ok_or(Error::Node("bundle metadata extent is truncated"))?;
        if extent.object != Some(self.object) {
            return Err(Error::Node("bundle metadata window scope differs"));
        }
        Ok(body.slice(start..end))
    }
}

#[cfg(test)]
mod tests;
