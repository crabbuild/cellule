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
    startup: Startup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Startup {
    Unconfigured,
    Held,
    Confirmed,
}

impl State {
    fn effective_mode(self) -> NodeMode {
        if self.startup == Startup::Held && self.mode == NodeMode::Active {
            NodeMode::Cordoned
        } else {
            self.mode
        }
    }
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
                startup: Startup::Unconfigured,
            })),
        }
    }
}

impl NodeAdmission {
    /// Reports whether boot confirmation still holds this runtime's startup.
    /// Cordon and pressure remain separate; outbound replacement of existing
    /// responsibilities may proceed after a maintenance boot is confirmed.
    pub fn startup_held(&self) -> Result<bool> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?
            .startup
            == Startup::Held)
    }
    /// Returns the lifecycle reason independent of pressure.
    pub fn mode(&self) -> Result<NodeMode> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?
            .effective_mode())
    }

    /// Checks current local admission; a stale peer advertisement cannot reopen it.
    pub fn check_new_role(&self) -> Result<()> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.effective_mode() != NodeMode::Active {
            return Err(Error::CellDraining);
        }
        if state.pressure != NodePressure::Normal {
            return Err(Error::Capacity("node pressure"));
        }
        Ok(())
    }

    /// Holds every new role before a fleet boot is enrolled. Install before
    /// exposing the runtime or installing its required node lease. The hold
    /// reports Cordoned until confirmation; it does not refresh measurements.
    pub fn hold_startup(&self) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.startup != Startup::Unconfigured {
            return Err(Error::Node("node startup admission already configured"));
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(Error::Node("node admission sequence overflow"))?;
        state.startup = Startup::Held;
        state.sequence = sequence;
        Ok(())
    }

    /// Confirms application-checked durable boot enrollment in its retained
    /// mode. This only removes the startup hold: cordon, drain and pressure
    /// remain independent. Returning to service requires a new runtime/session.
    pub fn confirm_startup(&self, mode: NodeMode) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::Node("node admission lock poisoned"))?;
        if state.startup == Startup::Unconfigured {
            return Err(Error::Node("node startup admission is not configured"));
        }
        let mode = match (state.mode, mode) {
            (NodeMode::Draining, _) | (_, NodeMode::Draining) => NodeMode::Draining,
            (NodeMode::Cordoned, _) | (_, NodeMode::Cordoned) => NodeMode::Cordoned,
            _ => NodeMode::Active,
        };
        if state.startup == Startup::Confirmed && state.mode == mode {
            return Ok(());
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(Error::Node("node admission sequence overflow"))?;
        state.mode = mode;
        state.startup = Startup::Confirmed;
        state.sequence = sequence;
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
                mode: state.effective_mode(),
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
        if state.effective_mode() != NodeMode::Active {
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
