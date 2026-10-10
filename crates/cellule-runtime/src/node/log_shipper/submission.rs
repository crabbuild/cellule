//! One partitioned submission lifetime, including cancellation during admission.
use std::time::Instant;

use crate::CellId;
use crate::fleet::telemetry::{CellTelemetryHandle, NodeLogSubmissionTiming};

pub(super) enum Stage {
    Validation,
    NativeBytes,
    ShippingSlot,
    LocalLoad,
    OrderedLane,
    PublicationSlot,
    Assignment,
}

pub(super) struct Observation {
    telemetry: CellTelemetryHandle,
    cell: CellId,
    started: Instant,
    phase_started: Instant,
    stage: Stage,
    timing: NodeLogSubmissionTiming,
}

impl Observation {
    pub(super) fn new(
        telemetry: CellTelemetryHandle,
        cell: CellId,
        first_commit_sequence: u64,
        commit_sequence: u64,
        bytes: u64,
    ) -> Self {
        let started = Instant::now();
        Self {
            telemetry,
            cell,
            started,
            phase_started: started,
            stage: Stage::Validation,
            timing: NodeLogSubmissionTiming {
                first_commit_sequence,
                commit_sequence,
                bytes,
                encoded_bytes: bytes,
                cancelled: true,
                ..NodeLogSubmissionTiming::default()
            },
        }
    }

    pub(super) fn frames(&mut self, frames: u64) {
        self.timing.frames = frames;
    }

    pub(super) fn assigned(&mut self, first_sequence: u64) {
        self.timing.first_sequence = Some(first_sequence);
    }

    pub(super) fn enter(&mut self, stage: Stage) {
        let now = Instant::now();
        self.observe_phase(now);
        self.phase_started = now;
        self.stage = stage;
    }

    pub(super) fn finish(&mut self, succeeded: bool) {
        self.timing.succeeded = succeeded;
        self.timing.cancelled = false;
        self.timing.enqueued = Some(succeeded);
    }

    fn observe_phase(&mut self, now: Instant) {
        let duration = now.saturating_duration_since(self.phase_started);
        let phase = match self.stage {
            Stage::Validation => &mut self.timing.validation,
            Stage::NativeBytes => &mut self.timing.native_bytes,
            Stage::ShippingSlot => &mut self.timing.shipping_slot,
            Stage::LocalLoad => &mut self.timing.local_load,
            Stage::OrderedLane => &mut self.timing.ordered_lane,
            Stage::PublicationSlot => &mut self.timing.publication_slot,
            Stage::Assignment => &mut self.timing.assignment,
        };
        *phase += duration;
        let optional = match self.stage {
            Stage::NativeBytes => Some(&mut self.timing.byte_admission),
            Stage::ShippingSlot => Some(&mut self.timing.queue_admission),
            Stage::LocalLoad => Some(&mut self.timing.capture_load),
            Stage::OrderedLane => Some(&mut self.timing.ticket_order),
            Stage::Assignment => Some(&mut self.timing.encoding),
            Stage::Validation | Stage::PublicationSlot => None,
        };
        if let Some(optional) = optional {
            *optional = Some(optional.unwrap_or_default() + duration);
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        let now = Instant::now();
        self.observe_phase(now);
        self.timing.total = now.saturating_duration_since(self.started);
        self.telemetry.node_log_submission(self.cell, self.timing);
    }
}
