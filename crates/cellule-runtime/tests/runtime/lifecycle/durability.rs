//! Node-log durability, fleet proofs, and byte admission.

use super::*;
use cellule_runtime::fleet::telemetry::{CellTelemetry, CommandResponseSource, PublicationTiming};

mod admission;
mod admission_batching;
mod group;
mod proofs;
mod recovery;
mod retirement;

#[derive(Default)]
pub(super) struct RecordingResponses(
    pub(super) Mutex<Vec<CommandResponseSource>>,
    pub(super) Mutex<Vec<PublicationTiming>>,
    tokio::sync::Notify,
);

impl RecordingResponses {
    pub(super) async fn wait_for_responses(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.2.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.lock().unwrap().len() >= count {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}

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
        self.2.notify_waiters();
    }

    fn publication_completed(&self, _cell: cellule_runtime::CellId, timing: PublicationTiming) {
        self.1.lock().unwrap().push(timing);
    }
}
