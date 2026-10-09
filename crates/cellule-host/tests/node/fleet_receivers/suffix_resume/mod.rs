//! Retained original input resumes an inherited suffix without another claim.
use super::*;
use bytes::Bytes;
use cellule_host::fleet::FleetActionAcceptance;
use cellule_runtime::cell::actor::{AcquisitionObservation, AcquisitionObserver};
use cellule_runtime::control::Control;

mod interrupted;
mod materialized;
