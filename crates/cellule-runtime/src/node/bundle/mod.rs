//! Verified node-wide coverage and bounded per-Cell reconstruction locators.
//!
//! Upload is a proposal. The canonical node-record CAS selects its complete
//! binding catalog and native range together. Cell authority pins each binding
//! and refuses departure until its complete issued range has a materialized root.
//!
//! The caller owns host admission and the original node lease. This selection
//! helper operates on complete captures assigned by the canonical shipper;
//! live selection can confirm the original assigned captures locally without
//! another CAS. Ordinary actor bundle responses remain disabled until their
//! command/read/retry visibility and capture release consume that exact proof.
//!
//! ```no_run
//! use cellule_runtime::node::{NodeDirectory, VersionedNodeAdvertisement};
//! use cellule_runtime::node::bundle::BundleCoverageProof;
//! use cellule_runtime::node::lease::NodeLeaseGuard;
//! use cellule_runtime::node::log::AssignedCommitRange;
//!
//! async fn select_complete_captures(
//!     directory: &NodeDirectory,
//!     observed: &VersionedNodeAdvertisement,
//!     lease: &NodeLeaseGuard,
//!     frames: &[cellule_ltx::VerifiedNodeFrame],
//!     assignments: &[AssignedCommitRange],
//!     now_ms: i64,
//! ) -> cellule_runtime::Result<(VersionedNodeAdvertisement, Vec<BundleCoverageProof>)> {
//!     let proposal = directory
//!         .prepare_node_bundle(observed, frames, assignments, now_ms).await?;
//!     directory.select_node_bundle(
//!         observed, &proposal, lease, cellule_ltx::Limits::default(), now_ms,
//!     ).await
//! }
//! ```

use crate::control::{BundleBindingRef, Control};
use crate::identity::{Digest, SessionId};
use crate::{Error, Result};
use bytes::Bytes;

mod binding;
mod closure;
mod codec;
mod index;
mod origin;
mod proof;
pub(crate) mod recovery;
mod selection;
#[cfg(test)]
use proof::checkpoint_prefix;
use proof::verify_base;
pub(crate) use selection::confirm_selected_coverage;
#[cfg(test)]
use store::load_catalog;
pub(crate) mod store;
#[cfg(test)]
mod tests;

pub(crate) const MAX_BUNDLE_BYTES: u64 = 4 << 20;
const MAX_BINDINGS: usize = 4_096;
const MAX_LOCATORS: usize = 256;
const MAX_INLINE_LOCATORS: usize = 32;
const MAX_FRAMES: usize = 64;
// Reconstruction retains at most one Cell suffix. Selection streams checked
// historical frames and shares one fresh cohort body for its new extents.
const MAX_SUFFIX_BYTES: u64 = MAX_BUNDLE_BYTES;
const MAX_BASE_OBJECTS: usize = 65_536;

/// Canonical node-record pointer; observing it alone grants no response proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeBundleHead {
    pub(crate) epoch: u64,
    pub(crate) digest: Digest,
    pub(crate) selected_through: u64,
}
impl NodeBundleHead {
    /// Publication lane epoch.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Digest authenticating the immutable catalog index and exact range.
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Exact contiguous selected native range endpoint.
    pub const fn selected_through(&self) -> u64 {
        self.selected_through
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.epoch == 0 || self.digest.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(Error::Node("invalid node bundle head"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BindingPhase {
    Provisional,
    Open,
    Closing,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Locator {
    // None in a newly encoded manifest refers to that very immutable object.
    // Resolving it on load avoids a self-referential digest in the byte format.
    object: Option<Digest>,
    offset: u64,
    bytes: u64,
    frame_digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    application: crate::identity::ApplicationId,
    first_commit: u64,
    control: Control,
    phase: BindingPhase,
    terminal: Option<(u64, u64, cellule_ltx::Position)>,
    selected_sequence: u64,
    selected_commit: u64,
    selected_position: cellule_ltx::Position,
    locators: Vec<Locator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Catalog {
    session: SessionId,
    epoch: u64,
    predecessor: Option<Digest>,
    selected_through: u64,
    bindings: Vec<Binding>,
    // Authenticated unchanged shards survive a partial load. Loaded rows are
    // snapshots for copy-on-write comparison, never a second authority.
    index: Option<index::LoadedIndex>,
}

/// Uploaded exact proposal. It cannot release an ACK or prune a capture.
pub struct PreparedNodeBundle {
    original: Option<NodeBundleHead>,
    catalog: Catalog,
    body: Bytes,
    head: NodeBundleHead,
    assignments: Vec<crate::node::log::AssignedCommitRange>,
}

/// Selected, dependency-verified coverage of one exact Cell writer.
///
/// The bounded locators retain no frame bodies. Creation is restricted to a
/// successful/reconciled canonical node CAS with prior complete range verification.
/// Live selections also retain the original process lease and complete native
/// assignments. Cold reconstruction proofs cannot confirm a live ACK gate.
pub struct BundleCoverageProof {
    pin: BundleBindingRef,
    binding: Binding,
    head: NodeBundleHead,
    session: SessionId,
    // Cold reconstruction must never revive the original process's ACK gate.
    live: Option<selection::LiveBundleCoverage>,
}
impl BundleCoverageProof {
    pub(crate) fn check_live_assignment(
        &self,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> Result<()> {
        self.live
            .as_ref()
            .ok_or(Error::Node("cold bundle cannot release live capture"))?
            .check_assignment(assignment)
    }

    /// Original Cell authority pin.
    pub fn binding(&self) -> BundleBindingRef {
        self.pin
    }
    /// Exact covered logical command endpoint.
    pub const fn commit_sequence(&self) -> u64 {
        self.binding.selected_commit
    }
    /// Exact covered SQLite position.
    pub const fn position(&self) -> cellule_ltx::Position {
        self.binding.selected_position
    }
    /// Number of bounded authenticated frame locators retained by the proof.
    pub fn locator_count(&self) -> usize {
        self.binding.locators.len()
    }
    /// Exact immutable base required for reconstruction.
    pub fn base(&self) -> Result<cellule_ltx::RootRef> {
        self.binding
            .control
            .ltx_root()
            .ok_or(Error::Node("bundle binding has no base"))
    }

    pub(crate) fn contains_assignment(
        &self,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.contains_assignment(assignment))
    }

    pub(crate) fn assignment_count(&self) -> usize {
        self.live.as_ref().map_or(0, |live| live.assignment_count())
    }

    pub(crate) fn retained_metadata_bytes(&self) -> Result<usize> {
        let control = self.binding.control.encode()?.len();
        self.binding
            .locators
            .capacity()
            .checked_mul(std::mem::size_of::<Locator>())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.live
                        .as_ref()
                        .map_or(0, |live| live.retained_metadata_bytes()),
                )
            })
            .and_then(|bytes| bytes.checked_add(control.checked_mul(4)?))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() + 256))
            .ok_or(Error::Capacity("selected bundle metadata"))
    }
}

impl Catalog {
    fn validate(&self) -> Result<()> {
        if self.epoch == 0
            || self.session.as_bytes().iter().all(|byte| *byte == 0)
            || self.bindings.len() > MAX_BINDINGS
        {
            return Err(Error::Node("invalid bundle catalog bounds"));
        }
        let mut previous = None;
        let mut scopes = std::collections::HashSet::new();
        for binding in &self.bindings {
            index::validate_deferred(self, binding)?;
            binding.control.encode()?;
            let pin = binding
                .control
                .bundle_binding
                .ok_or(Error::Node("bundle catalog lacks Cell pin"))?;
            let base = binding
                .control
                .ltx_root()
                .ok_or(Error::Node("bundle catalog lacks base"))?;
            if pin.session != self.session
                || pin.epoch != self.epoch
                // A freshly published runtime bootstrap has no logical command
                // or assigned frame yet. Only that empty binding may use zero.
                || (binding.first_commit == 0
                    && (binding.selected_commit != 0 || binding.selected_sequence != 0))
                || binding.first_commit > binding.selected_commit
                || !scopes.insert((
                    binding.application,
                    binding.control.cell,
                    binding.control.incarnation,
                    binding.control.epoch,
                ))
                || previous.is_some_and(|digest| digest >= *pin.digest.as_bytes())
                || binding.locators.len() > MAX_LOCATORS
                || binding.selected_sequence > self.selected_through
                || binding.selected_commit < base.commit_sequence
                || binding.selected_position.txid < base.position.txid
                || ((binding.selected_commit == base.commit_sequence)
                    != binding.locators.is_empty())
                || (binding.locators.is_empty() && binding.selected_position != base.position)
                || binding
                    .locators
                    .iter()
                    .try_fold(0_u64, |bytes, locator| bytes.checked_add(locator.bytes))
                    .is_none_or(|bytes| bytes > MAX_SUFFIX_BYTES)
                || binding.locators.iter().any(|locator| {
                    locator.bytes == 0
                        || locator.bytes > MAX_BUNDLE_BYTES
                        || locator
                            .offset
                            .checked_add(locator.bytes)
                            .is_none_or(|end| end > MAX_BUNDLE_BYTES)
                })
            {
                return Err(Error::Node("invalid bundle binding coverage"));
            }
            match (binding.phase, binding.terminal) {
                (BindingPhase::Provisional, None)
                    if binding.locators.is_empty() && binding.selected_sequence == 0 => {}
                (BindingPhase::Open, None) => {}
                (BindingPhase::Closing, Some((sequence, commit, position)))
                    if sequence >= binding.selected_sequence
                        && commit >= binding.selected_commit
                        && position.txid >= binding.selected_position.txid => {}
                (BindingPhase::Closed, Some((sequence, commit, position)))
                    if sequence == binding.selected_sequence
                        && commit == binding.selected_commit
                        && position == binding.selected_position => {}
                _ => return Err(Error::Node("invalid bundle binding closure")),
            }
            previous = Some(*pin.digest.as_bytes());
        }
        Ok(())
    }
    fn binding_mut(&mut self, digest: Digest) -> Result<&mut Binding> {
        self.bindings
            .iter_mut()
            .find(|binding| {
                binding
                    .control
                    .bundle_binding
                    .is_some_and(|pin| pin.digest == digest)
            })
            .ok_or(Error::Node("bundle binding missing"))
    }
}
