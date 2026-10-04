//! Closed-receiver continuation through the real host and SQLite journal.
use super::*;
use crate::scenario::receiver_loss::adapters::{ClosedBootObserver, LoseRoutedActivationReply};
use cellule_host::fleet::{FleetActionAcceptance, FleetObserver, FleetTransport};
use cellule_runtime::control::{ControlState, Transition};
mod fixture;
mod recovery_faults;
mod tests;
