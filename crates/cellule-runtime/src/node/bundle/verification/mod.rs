//! Fresh cohort reads; exact frame and Cell-chain validation stays canonical.
use super::*;
use futures_util::{StreamExt, stream};
use std::ops::Range;
use std::sync::Mutex;
use tokio::sync::Semaphore;

pub(super) const READ_CONCURRENCY: usize = 8;
mod base;
pub(super) use base::verify as verify_bases;
// Together scratch and metadata replace the released fresh-origin buffer;
// they never add to the producer's original 20-MiB working reservation.
const SCRATCH_BYTES: u64 = MAX_BUNDLE_BYTES / 2;
#[cfg(test)]
const METADATA_BYTES: usize = (MAX_BUNDLE_BYTES / 2) as usize;

#[derive(Clone, Copy)]
struct ReadIndex {
    binding: u16,
    locator: u16,
}

struct Window {
    object: Digest,
    range: Range<u64>,
    useful_bytes: u64,
    reads: Vec<ReadIndex>,
}

fn locator<'a>(bindings: &[&'a Binding], read: ReadIndex) -> &'a Locator {
    // Indices are constructed from these immutable slices inside this module.
    &bindings[usize::from(read.binding)].locators[usize::from(read.locator)]
}

struct Windows<'a> {
    bindings: &'a [&'a Binding],
    reads: std::iter::Peekable<std::vec::IntoIter<ReadIndex>>,
}

fn windows<'a>(bindings: &'a [&'a Binding]) -> Result<Windows<'a>> {
    if bindings.len() > MAX_FRAMES
        || bindings
            .iter()
            .any(|binding| binding.locators.len() > MAX_LOCATORS)
    {
        return Err(Error::Capacity("bundle cohort verification metadata"));
    }
    let count = bindings.iter().map(|binding| binding.locators.len()).sum();
    let mut reads = Vec::with_capacity(count);
    for (binding, value) in bindings.iter().enumerate() {
        for (index, value) in value.locators.iter().enumerate() {
            value
                .object
                .ok_or(Error::Node("bundle locator is unresolved"))?;
            value
                .offset
                .checked_add(value.bytes)
                .ok_or(Error::Node("bundle locator overflow"))?;
            if value.bytes == 0 || value.bytes > MAX_BUNDLE_BYTES {
                return Err(Error::Capacity("bundle historical window bytes"));
            }
            reads.push(ReadIndex {
                binding: u16::try_from(binding)
                    .map_err(|_| Error::Capacity("bundle cohort verification metadata"))?,
                locator: u16::try_from(index)
                    .map_err(|_| Error::Capacity("bundle cohort verification metadata"))?,
            });
        }
    }
    reads.sort_unstable_by_key(|read| {
        let value = locator(bindings, *read);
        (value.object.map(|object| *object.as_bytes()), value.offset)
    });
    // Keep only compact sorted indices. Windows are assembled lazily, bounded
    // by the reader concurrency rather than all 64 * 256 possible extents.
    Ok(Windows {
        bindings,
        reads: reads.into_iter().peekable(),
    })
}

impl Iterator for Windows<'_> {
    type Item = Result<Window>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reads.next().map(|read| self.window(read))
    }
}

impl Windows<'_> {
    fn window(&mut self, read: ReadIndex) -> Result<Window> {
        let value = locator(self.bindings, read);
        let mut window = Window {
            object: value
                .object
                .ok_or(Error::Node("bundle locator is unresolved"))?,
            range: value.offset..value.offset + value.bytes,
            useful_bytes: value.bytes,
            reads: vec![read],
        };
        while let Some(read) = self.reads.peek().copied() {
            let value = locator(self.bindings, read);
            if value.object != Some(window.object) {
                break;
            }
            // Every end was checked before this immutable plan was formed.
            let end = value.offset + value.bytes;
            let useful = window
                .useful_bytes
                .checked_add(end.saturating_sub(window.range.end.max(value.offset)))
                .ok_or(Error::Node("bundle locator overflow"))?;
            let span = end.max(window.range.end) - window.range.start;
            // Overlapping extents buy no padding; sparse windows cannot read
            // more than twice the useful requested union or exceed scratch.
            if span > SCRATCH_BYTES || span > useful.saturating_mul(2) {
                break;
            }
            self.reads.next();
            window.range.end = end.max(window.range.end);
            window.useful_bytes = useful;
            window.reads.push(read);
        }
        Ok(window)
    }
}

pub(super) async fn verify_cohort(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    bindings: &[&Binding],
    limits: cellule_ltx::Limits,
    origin: &origin::OriginBundle,
) -> Result<()> {
    if bindings.iter().any(|binding| {
        binding
            .locators
            .iter()
            .any(|locator| locator.bytes > SCRATCH_BYTES)
    }) {
        // A protocol-valid individual extent can exceed shared scratch. Keep
        // the canonical serial path and allocate no cohort facts or windows.
        for binding in bindings {
            proof::verify_selected_binding(layout, session, epoch, binding, limits, origin).await?;
        }
        return Ok(());
    }
    // Base and historical verification occupy the released origin allowance
    // in disjoint phases. No historical facts or windows coexist with bases.
    verify_bases(layout, bindings, limits).await?;
    let windows = windows(bindings)?;
    let facts = Mutex::new(
        bindings
            .iter()
            .map(|binding| vec![None; binding.locators.len()])
            .collect::<Vec<_>>(),
    );
    let scratch = Semaphore::new(SCRATCH_BYTES as usize);
    let mut reads = stream::iter(windows)
        .map(|window| async {
            read_window(
                layout, session, epoch, bindings, limits, origin, &scratch, &facts, window?,
            )
            .await
        })
        .buffer_unordered(READ_CONCURRENCY);
    while let Some(result) = reads.next().await {
        result?;
    }
    drop(reads);
    let facts = facts
        .into_inner()
        .map_err(|_| Error::Node("bundle cohort facts lock poisoned"))?;
    for (binding, facts) in bindings.iter().zip(facts) {
        let mut chain = proof::BindingChain::new(binding)?;
        for step in facts {
            chain.accept(step.ok_or(Error::Node("bundle cohort omits locator"))?)?;
        }
        chain.finish(binding)?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one read retains its exact scope and shared scratch admission"
)]
async fn read_window(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    bindings: &[&Binding],
    limits: cellule_ltx::Limits,
    origin: &origin::OriginBundle,
    scratch: &Semaphore,
    facts: &Mutex<Vec<Vec<Option<proof::FrameStep>>>>,
    window: Window,
) -> Result<()> {
    let span = window.range.end - window.range.start;
    let current = origin.range(session, epoch, window.object, &window.range)?;
    let _permit = if current.is_none() {
        Some(
            scratch
                .acquire_many(
                    u32::try_from(span)
                        .map_err(|_| Error::Capacity("bundle historical window bytes"))?,
                )
                .await
                .map_err(|_| Error::RuntimeClosed)?,
        )
    } else {
        None
    };
    let bytes = match current {
        Some(bytes) => bytes,
        None => {
            origin::read_range(
                layout,
                session,
                epoch,
                window.object,
                window.range.clone(),
                None,
            )
            .await?
        }
    };
    if bytes.len() as u64 != span {
        return Err(Error::Node("bundle extent is truncated"));
    }
    // Verification is synchronous in this one reader task. Only I/O overlaps;
    // one decoder's scratch is live at a time, as on the original serial path.
    // No body or duplicate vector of facts escapes this window.
    let mut facts = facts
        .lock()
        .map_err(|_| Error::Node("bundle cohort facts lock poisoned"))?;
    for read in window.reads {
        let value = locator(bindings, read);
        let start = usize::try_from(value.offset - window.range.start)
            .map_err(|_| Error::Node("bundle extent overflow"))?;
        let length =
            usize::try_from(value.bytes).map_err(|_| Error::Node("bundle extent overflow"))?;
        let frame = proof::checked_frame(
            session,
            epoch,
            bindings[usize::from(read.binding)],
            value,
            bytes.slice(start..start + length),
            limits,
        )?;
        if facts[usize::from(read.binding)][usize::from(read.locator)]
            .replace(proof::FrameStep::from_frame(&frame))
            .is_some()
        {
            return Err(Error::Node("bundle cohort repeats locator"));
        }
    }
    // No native body escapes this operation; the shared scratch permit covers
    // each fresh read until every frame in that window has been inspected.
    Ok(())
}

#[cfg(test)]
mod tests;
