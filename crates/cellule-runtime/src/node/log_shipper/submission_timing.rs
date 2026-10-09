//! Optional pre-enqueue phase observation, independent of durability authority.

use std::time::{Duration, Instant};

use super::{CommitTicket, NodeLogSubmission};
use crate::fleet::telemetry::{CellTelemetryHandle, NodeLogSubmissionTiming};
use crate::{CellId, Result};

#[derive(Clone, Copy)]
pub(super) enum SubmissionStage {
    Bytes,
    Queue,
    Load,
    Order,
    Encode,
}

pub(super) struct SubmissionTrace<'a> {
    telemetry: &'a CellTelemetryHandle,
    cell: CellId,
    origin: Option<Instant>,
    stage: Option<(SubmissionStage, Instant)>,
    timing: NodeLogSubmissionTiming,
}

impl<'a> SubmissionTrace<'a> {
    pub(super) fn new(telemetry: &'a CellTelemetryHandle, submission: &NodeLogSubmission) -> Self {
        Self {
            telemetry,
            cell: submission.cell,
            origin: telemetry.is_enabled().then(Instant::now),
            stage: None,
            timing: NodeLogSubmissionTiming {
                commit_sequence: submission.commit_sequence,
                first_sequence: None,
                encoded_bytes: submission.encoded_bytes,
                byte_admission: None,
                queue_admission: None,
                capture_load: None,
                ticket_order: None,
                encoding: None,
                total: Duration::ZERO,
                enqueued: None,
            },
        }
    }

    pub(super) fn begin(&mut self, stage: SubmissionStage) {
        if self.origin.is_some() {
            self.stage = Some((stage, Instant::now()));
        }
    }

    pub(super) fn complete(&mut self) {
        if let Some((stage, started)) = self.stage.take() {
            let elapsed = Some(started.elapsed());
            match stage {
                SubmissionStage::Bytes => self.timing.byte_admission = elapsed,
                SubmissionStage::Queue => self.timing.queue_admission = elapsed,
                SubmissionStage::Load => self.timing.capture_load = elapsed,
                SubmissionStage::Order => self.timing.ticket_order = elapsed,
                SubmissionStage::Encode => self.timing.encoding = elapsed,
            }
        }
    }

    pub(super) fn finish(&mut self, result: &Result<CommitTicket>) {
        self.timing.enqueued = Some(result.is_ok());
        self.timing.first_sequence = result.as_ref().ok().map(|ticket| ticket.first_sequence());
    }
}

impl Drop for SubmissionTrace<'_> {
    fn drop(&mut self) {
        if let Some(origin) = self.origin {
            self.timing.total = origin.elapsed();
            self.telemetry.node_log_submission(self.cell, self.timing);
        }
    }
}
