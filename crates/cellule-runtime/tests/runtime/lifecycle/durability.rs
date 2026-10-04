//! Node-log durability, fleet proofs, and byte admission.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, CommandResponseSource, PublicationTiming};

mod admission;
mod group;
mod proofs;
mod recovery;

#[derive(Default)]
pub(super) struct RecordingResponses(
    pub(super) Mutex<Vec<CommandResponseSource>>,
    pub(super) Mutex<Vec<PublicationTiming>>,
);

impl CellTelemetry for RecordingResponses {
    fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: std::time::Duration,
        confirmation: std::time::Duration,
    ) {
        assert!(confirmation <= elapsed);
        if source == CommandResponseSource::Recorded {
            assert!(confirmation.is_zero());
        }
        self.0.lock().unwrap().push(source);
    }

    fn publication_completed(&self, _cell: cellule_runtime::CellId, timing: PublicationTiming) {
        self.1.lock().unwrap().push(timing);
    }
}
