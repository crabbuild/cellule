//! Bounded canonical publication observation after the offered-load window.

use cellule_runtime::control::{Control, ControlState, authority::CellAuthority};
use cellule_runtime::{Error, Result};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

pub(super) const DRAIN_GRACE_US: u64 = 2_000_000;

pub(super) struct CapturedRoots {
    pub(super) values: Vec<Control>,
    pub(super) elapsed_us: u64,
    pub(super) reads: usize,
}

pub(super) async fn capture(
    authority: &CellAuthority,
    expected: &[Control],
    minimum: &[u64],
) -> Result<CapturedRoots> {
    let started = Instant::now();
    let deadline = started + Duration::from_micros(DRAIN_GRACE_US);
    let mut cells = HashSet::new();
    if expected.is_empty()
        || expected.len() > 80
        || expected.len() != minimum.len()
        || expected.iter().any(|original| {
            original.state != ControlState::Serving
                || original.owner.is_none()
                || original.recovery.is_some()
                || original.root.is_none()
                || !cells.insert(original.cell)
        })
    {
        return Err(Error::Control("invalid qualified root capture inputs"));
    }
    let mut reads = 0;
    loop {
        let mut values = Vec::with_capacity(expected.len());
        let mut covered = true;
        for (original, minimum) in expected.iter().zip(minimum) {
            let observed = tokio::time::timeout_at(deadline.into(), authority.load(original.cell))
                .await
                .map_err(|_| Error::Deadline)??
                .ok_or(Error::CellNotActive)?;
            let current = observed.value();
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            if current.cell != original.cell
                || current.incarnation != original.incarnation
                || current.epoch != original.epoch
                || current.owner != original.owner
                || current.code != original.code
                || current.schema != original.schema
                || current.state != ControlState::Serving
                || current.recovery.is_some()
                || current.revision < original.revision
                || current.progress < original.progress
            {
                return Err(Error::Fenced);
            }
            let root = current.root.as_ref().ok_or(Error::Fenced)?;
            covered &= root.commit_sequence >= *minimum;
            reads += 1;
            values.push(current.clone());
        }
        if covered {
            let elapsed_us = started.elapsed().as_micros() as u64;
            if elapsed_us >= DRAIN_GRACE_US {
                return Err(Error::Deadline);
            }
            return Ok(CapturedRoots {
                reads,
                values,
                elapsed_us,
            });
        }
        // A follower proof can return before object publication. Observe the
        // canonical publisher within one deadline for the entire original roster;
        // do not rotate authority or charge this readback to arrival latency.
        tokio::time::timeout_at(
            deadline.into(),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .map_err(|_| Error::Deadline)?;
    }
}

mod tests;
