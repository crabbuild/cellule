//! Complete original captures retained by the same bounded native issuance lane.

use super::*;
use std::sync::{Mutex, OnceLock};
use tokio::sync::watch;

/// One complete captured assignment, in the original node-log order.
///
/// The native lane constructed and verified these frames before issuance. This
/// value retains the existing byte admission until dropped. It is a proposal,
/// never selected authority or an ACK, and must not be split into prefix proofs.
pub struct AssignedCapture {
    assignment: crate::node::log::AssignedCommitRange,
    frames: Vec<cellule_ltx::VerifiedNodeFrame>,
    _reservation: Arc<OutstandingBytes>,
}

impl AssignedCapture {
    pub(super) fn new(
        assignment: crate::node::log::AssignedCommitRange,
        frames: Vec<cellule_ltx::VerifiedNodeFrame>,
        reservation: Arc<OutstandingBytes>,
    ) -> Self {
        Self {
            assignment,
            frames,
            _reservation: reservation,
        }
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
}

/// Sole ordered consumer for this original native epoch's publication work.
///
/// Complete captures share the shipper's outstanding-byte limit and a queue of
/// at most 512 submissions. Receiving does not release admission: the consumer
/// retains each value through joined selection or verified object fallback.
/// A slow consumer applies backpressure before new sequence issuance.
pub struct NodePublicationFeed {
    receiver: mpsc::Receiver<AssignedCapture>,
    stopping: watch::Receiver<bool>,
}

impl NodePublicationFeed {
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
    sender: OnceLock<Mutex<Option<mpsc::Sender<AssignedCapture>>>>,
}

impl PublicationState {
    pub(super) fn take_feed(&self, stopping: watch::Receiver<bool>) -> Result<NodePublicationFeed> {
        let (sender, receiver) = mpsc::channel(MAX_QUEUED_SUBMISSIONS);
        self.sender
            .set(Mutex::new(Some(sender)))
            .map_err(|_| Error::Node("node publication feed already installed"))?;
        Ok(NodePublicationFeed { receiver, stopping })
    }

    pub(super) async fn reserve(
        &self,
        stopping: &watch::Sender<bool>,
    ) -> Result<Option<mpsc::OwnedPermit<AssignedCapture>>> {
        let Some(sender) = self.sender.get() else {
            return Ok(None);
        };
        let sender = sender
            .lock()
            .map_err(|_| Error::Node("node publication feed lock poisoned"))?
            .clone()
            .ok_or(Error::RuntimeClosed)?;
        let mut stopping = stopping.subscribe();
        if *stopping.borrow() {
            return Err(Error::RuntimeClosed);
        }
        tokio::select! {
            permit = sender.reserve_owned() => permit.map(Some).map_err(|_| Error::RuntimeClosed),
            _ = stopping.changed() => Err(Error::RuntimeClosed),
        }
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
