//! Complete original captures retained by the same bounded native issuance lane.

use super::*;
use std::sync::{Mutex, OnceLock, Weak};
use tokio::sync::watch;

/// Selected coverage and its node-ledger reservation, shared by complete
/// captures from the same Cell in one selection. No native bodies are retained.
pub(crate) struct SelectedBundle {
    pub(crate) proof: crate::node::bundle::BundleCoverageProof,
    pub(crate) _memory: crate::fleet::resource::ResourceReservation,
    // Weak indexes retain only this small anchor allocation after the final
    // strong proof drops, not the much larger inline SelectedBundle allocation.
    prefix: Arc<Weak<Self>>,
}

impl SelectedBundle {
    pub(crate) fn new(
        proof: crate::node::bundle::BundleCoverageProof,
        memory: crate::fleet::resource::ResourceReservation,
    ) -> Arc<Self> {
        Arc::new_cyclic(|original| Self {
            proof,
            _memory: memory,
            prefix: Arc::new(original.clone()),
        })
    }

    pub(crate) fn weak_prefix(&self) -> Weak<Weak<Self>> {
        Arc::downgrade(&self.prefix)
    }
}

#[derive(Clone)]
pub(crate) struct CaptureSelection {
    receiver: watch::Receiver<Option<Arc<SelectedBundle>>>,
}

impl CaptureSelection {
    /// A scheduling hint only; consumers still verify the original receipt.
    pub(crate) fn is_ready(&self) -> bool {
        self.receiver.borrow().is_some()
    }

    pub(crate) async fn selected(&self) -> Result<Arc<SelectedBundle>> {
        let mut receiver = self.receiver.clone();
        loop {
            if let Some(selected) = receiver.borrow_and_update().clone() {
                return Ok(selected);
            }
            receiver.changed().await.map_err(|_| Error::RuntimeClosed)?;
        }
    }
}

#[derive(Clone)]
pub(crate) struct SubmittedCapture {
    pub(crate) assignment: crate::node::log::AssignedCommitRange,
    pub(crate) selection: Option<CaptureSelection>,
}

/// Admitted selected metadata retained through command confirmation and root
/// materialization. Every consumer shares the same reservation for a Cell.
pub struct SelectedBundlePublication {
    pub(crate) through: u64,
    pub(crate) selected: Vec<Arc<SelectedBundle>>,
}

impl SelectedBundlePublication {
    /// Contiguous native frontier selected by the original canonical node CAS.
    pub const fn selected_through(&self) -> u64 {
        self.through
    }

    /// Exact Cell proofs; keep this publication alive through joined I/O so its
    /// metadata remains charged to the serving node's original resource ledger.
    pub fn proofs(&self) -> impl Iterator<Item = &crate::node::bundle::BundleCoverageProof> {
        self.selected.iter().map(|selected| &selected.proof)
    }
}

/// One complete captured assignment, in the original node-log order.
///
/// The native lane constructed and verified these frames before issuance. This
/// value retains the existing byte admission until dropped. It is a proposal,
/// never selected authority or an ACK, and must not be split into prefix proofs.
pub struct AssignedCapture {
    assignment: crate::node::log::AssignedCommitRange,
    frames: Vec<cellule_ltx::VerifiedNodeFrame>,
    _reservation: Arc<OutstandingBytes>,
    selection: watch::Sender<Option<Arc<SelectedBundle>>>,
}

impl AssignedCapture {
    pub(super) fn new(
        assignment: crate::node::log::AssignedCommitRange,
        frames: Vec<cellule_ltx::VerifiedNodeFrame>,
        reservation: Arc<OutstandingBytes>,
    ) -> (Self, CaptureSelection) {
        let (selection, receiver) = watch::channel(None);
        (
            Self {
                assignment,
                frames,
                _reservation: reservation,
                selection,
            },
            CaptureSelection { receiver },
        )
    }

    /// Exact original complete-capture witness accepted by bundle selection.
    pub const fn assignment(&self) -> crate::node::log::AssignedCommitRange {
        self.assignment
    }

    /// Complete verified frames. Borrow them while retaining this admission;
    /// copies retained beyond it require the consumer's separate accounting.
    pub fn frames(&self) -> &[cellule_ltx::VerifiedNodeFrame] {
        &self.frames
    }

    pub(crate) fn confirm_selection(&self, selected: Arc<SelectedBundle>) {
        // Selection confirmation is retained even if a cancelled command has
        // dropped its receiver. Publication/drain still owns the original cut.
        self.selection.send_replace(Some(selected));
    }
}

/// Sole ordered consumer for this original native epoch's publication work.
///
/// Complete captures and their bookkeeping share the shipper's outstanding-byte
/// limit. Receiving does not release admission: the consumer
/// retains each value through joined selection or verified object fallback.
/// Publication has no independent slot wait in the global issuance lane;
/// exhausted native byte credit applies backpressure before that lane.
pub struct NodePublicationFeed {
    receiver: mpsc::UnboundedReceiver<AssignedCapture>,
    stopping: watch::Receiver<bool>,
}

impl NodePublicationFeed {
    pub(crate) fn try_recv(&mut self) -> Option<AssignedCapture> {
        self.receiver.try_recv().ok()
    }

    /// Receives the next complete capture, including accepted work after close.
    /// None means all producers closed and every queued capture was received.
    pub async fn recv(&mut self) -> Option<AssignedCapture> {
        if *self.stopping.borrow() {
            self.receiver.close();
            return self.receiver.recv().await;
        }
        tokio::select! {
            capture = self.receiver.recv() => capture,
            _ = self.stopping.changed() => {
                self.receiver.close();
                self.receiver.recv().await
            }
        }
    }
}

#[derive(Default)]
pub(super) struct PublicationState {
    sender: OnceLock<Mutex<Option<mpsc::UnboundedSender<AssignedCapture>>>>,
}

impl PublicationState {
    pub(super) fn take_feed(&self, stopping: watch::Receiver<bool>) -> Result<NodePublicationFeed> {
        // Every queued value owns charged native credit, including control and
        // frame-vector bookkeeping. The byte window bounds this FIFO even when
        // the origin publisher is held; a second slot budget would couple Fleet
        // progress to bucket latency again.
        let (sender, receiver) = mpsc::unbounded_channel();
        self.sender
            .set(Mutex::new(Some(sender)))
            .map_err(|_| Error::Node("node publication feed already installed"))?;
        Ok(NodePublicationFeed { receiver, stopping })
    }

    pub(super) fn sender(&self) -> Result<Option<mpsc::UnboundedSender<AssignedCapture>>> {
        let Some(sender) = self.sender.get() else {
            return Ok(None);
        };
        let sender = sender
            .lock()
            .map_err(|_| Error::Node("node publication feed lock poisoned"))?
            .clone()
            .ok_or(Error::RuntimeClosed)?;
        if sender.is_closed() {
            return Err(Error::RuntimeClosed);
        }
        Ok(Some(sender))
    }

    pub(super) fn close(&self) -> Result<()> {
        if let Some(sender) = self.sender.get() {
            sender
                .lock()
                .map_err(|_| Error::Node("node publication feed lock poisoned"))?
                .take();
        }
        Ok(())
    }
}

/// Body and bounded capture/queue control allocations consume the same original
/// byte window. The fixed allowance covers watch/Arc/channel bookkeeping;
/// frame vectors are charged by their concrete element sizes. No independent
/// count limit or larger native window is introduced.
pub(super) fn retained_bytes(body: u64, frames: u64, members: usize) -> Result<u64> {
    let member_vectors = members
        .checked_mul(std::mem::size_of::<Bytes>() + std::mem::size_of::<Arc<OutstandingBytes>>())
        .ok_or(Error::Capacity("node-log retained member bytes"))?;
    let control = (members as u64)
        .checked_mul(128)
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or(Error::Capacity("node-log retained member bytes"))?;
    let per_frame = 2 * std::mem::size_of::<cellule_ltx::VerifiedNodeFrame>()
        + std::mem::size_of::<QueuedFrame>()
        + member_vectors;
    frames
        .checked_mul(per_frame as u64)
        .and_then(|metadata| metadata.checked_add(control))
        .and_then(|metadata| body.checked_add(metadata))
        .ok_or(Error::Capacity("node-log retained capture bytes"))
}
