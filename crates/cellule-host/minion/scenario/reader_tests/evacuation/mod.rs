//! Actual managed boots and nonzero reader replacement under retained intent.
use super::super::startup;
use super::*;
use cellule_host::fleet::FleetEnrollmentAcceptance;
use cellule_host::read_replicas::ReaderEvacuation;
use cellule_runtime::{
    client::{CellDescription, CellReadReplica},
    fleet::operations::{
        EnrollmentEndpoint, EnrollmentEvent, EnrollmentRole, EnrollmentSpec, JournalTransition,
        MaintenanceEvent, MaintenanceOperation, OperationId, PublishedPosition,
    },
    peer::{PeerPrincipal, PeerSigner, ReplicaPeerClient},
};
use ed25519_dalek::SigningKey;

mod fixture;
mod inventory_tests;
mod persisted;
mod source;
mod tests;
use crate::scenario::native_peers as transport;

struct Fixture {
    layout: CellStorageLayout,
    root: tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    description: CellDescription,
    directory: NodeDirectory,
    nodes: Vec<Arc<CellNode>>,
    managers: Vec<ReadReplicaManager>,
    records: Arc<HashMap<CellId, Record>>,
    boots: Vec<startup::BootOwner>,
    handle: CellHandle,
    target: CellTarget,
    original: EnrollmentRecord,
    reader: CellReadReplica,
    operation: MaintenanceOperation,
    peer: ReplicaPeerClient,
    transport: Arc<transport::NativePeers>,
}

impl Fixture {
    async fn spare(&self) {
        self.boots[2]
            .refresh_capacity(
                2,
                self.journal.as_ref(),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let selected = self
                    .directory
                    .select_readers(
                        self.target.cell_id(),
                        session(0),
                        self.description.code,
                        1,
                        clock().unwrap(),
                        10_000,
                    )
                    .await
                    .unwrap();
                if selected.len() == 1 && selected[0].session() == session(2) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let node = self
            .directory
            .load(session(2), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        self.peer
            .activate(
                &self.target,
                &self.directory,
                node.advertisement().clone(),
                self.description,
            )
            .await
            .unwrap();
    }

    async fn evacuate(&self) -> cellule_runtime::Result<ReaderEvacuation> {
        self.managers[1]
            .evacuate(
                &self.original,
                &self.operation,
                &self.peer,
                Instant::now() + Duration::from_secs(3),
            )
            .await
    }

    async fn original_row(&self) -> EnrollmentRecord {
        self.journal
            .load_enrollment(scope(), self.original.spec().key().unwrap())
            .await
            .unwrap()
            .unwrap()
    }

    async fn read_original(&self, value: i64) {
        let result = self
            .reader
            .query::<application::ReadValue>(Some(self.reader.receipt().await), 0)
            .await
            .unwrap();
        assert_eq!(result.output, value);
    }

    async fn finish(self) {
        for node in self.nodes.iter().rev() {
            node.shutdown().await.unwrap();
            assert_eq!(node.state(), NodeState::Stopped);
            let stats = node.stats();
            assert_eq!(stats.active_cells(), 0);
            assert_eq!(stats.retained_bytes(), 0);
            assert_eq!(stats.resident_bytes(), 0);
            assert_eq!(stats.worker_jobs(), 0);
            assert_eq!(stats.local_disk_reserved_bytes(), 0);
        }
        assert_eq!(
            self.original_row().await.status(),
            EnrollmentStatus::Retired
        );
        for boot in &self.boots {
            assert!(
                self.directory
                    .is_withdrawn(boot.spec.target.session)
                    .await
                    .unwrap()
            );
            let row = self
                .journal
                .load_enrollment(scope(), boot.spec.key().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.status(), EnrollmentStatus::Retired);
        }
        self.journal.close().await.unwrap();
    }
}
