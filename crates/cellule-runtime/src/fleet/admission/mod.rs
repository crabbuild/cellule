//! Shared node role-admission reasons and the locally measured stable tier.
//!
//! Pressure recovery only clears pressure. Cordon and terminal drain stay
//! closed for this runtime; a return to service needs a validated new session.

use std::sync::{Arc, RwLock};

use crate::node::{NodeMode, NodeOperationalSample, NodePressure};
use crate::{Error, Result};

use super::pressure::PressureState;

#[derive(Clone, Copy, Debug)]
struct State {
    mode: NodeMode,
    pressure: NodePressure,
    sequence: u64,
    observed_at_ms: Option<i64>,
}

/// One node's local writer, new-reader, and new-follower admission gate.
/// Clones share reasons; this state does not authorize or fence Cell ownership.
#[derive(Clone, Debug)]
pub struct NodeAdmission {
    state: Arc<RwLock<State>>,
}

impl Default for NodeAdmission {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(State {
                mode: NodeMode::Active,
                pressure: NodePressure::Normal,
                sequence: 0,
                observed_at_ms: None,
            })),
        }
    }
}

impl NodeAdmission {
    /// Returns the lifecycle reason independent of pressure.
    pub fn mode(&self) -> Result<NodeMode> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?
            .mode)
    }

    /// Checks current local admission; a stale peer advertisement cannot reopen it.
    pub fn check_new_role(&self) -> Result<()> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.mode != NodeMode::Active {
            return Err(Error::CellDraining);
        }
        if state.pressure != NodePressure::Normal {
            return Err(Error::Capacity("node pressure"));
        }
        Ok(())
    }

    /// Closes new role admission while preserving all existing obligations.
    pub fn cordon(&self) -> Result<()> {
        self.advance_mode(NodeMode::Cordoned)
    }

    /// Closes new role admission for terminal lifecycle drain.
    pub fn begin_drain(&self) -> Result<()> {
        self.advance_mode(NodeMode::Draining)
    }

    fn advance_mode(&self, requested: NodeMode) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.mode == requested || state.mode == NodeMode::Draining {
            return Ok(());
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(Error::Node("node admission sequence overflow"))?;
        state.mode = requested;
        state.sequence = sequence;
        // A lifecycle change does not refresh an old pressure measurement.
        Ok(())
    }

    /// Returns the latest real sample, or unknown before the first observation.
    /// Repeated reads do not advance its sequence or measurement timestamp.
    pub fn sample(&self) -> Result<Option<NodeOperationalSample>> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        Ok(state
            .observed_at_ms
            .map(|observed_at_ms| NodeOperationalSample {
                mode: state.mode,
                pressure: state.pressure,
                sequence: state.sequence,
                observed_at_ms,
            }))
    }

    pub(crate) fn observe(&self, pressure: PressureState, at_ms: i64) -> Result<()> {
        let pressure = NodePressure::try_from(pressure)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if at_ms < 0
            || state
                .observed_at_ms
                .is_some_and(|previous| at_ms < previous)
        {
            return Err(Error::Node("node admission sample time regressed"));
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(Error::Node("node admission sequence overflow"))?;
        state.pressure = pressure;
        state.observed_at_ms = Some(at_ms);
        state.sequence = sequence;
        Ok(())
    }

    /// Performs a short synchronous admission effect atomically with cordon.
    /// Used to enroll a new follower lane; existing lanes bypass this new-role
    /// gate so their acknowledged tails can continue to settle under pressure.
    pub(crate) fn admit<T>(&self, effect: impl FnOnce() -> Result<T>) -> Result<T> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.mode != NodeMode::Active {
            return Err(Error::CellDraining);
        }
        if state.pressure != NodePressure::Normal {
            return Err(Error::Capacity("node pressure"));
        }
        effect()
    }
}

#[cfg(test)]
mod tests;
