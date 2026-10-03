//! Real native evacuation, immutable local transactions and fresh status proofs.
use super::*;
use cellule_host::fleet::{
    FleetReaderEvacuationJournal, FleetReaderEvacuationPublication, FleetReaderEvacuationVerifier,
};
use cellule_runtime::{fleet::operations::ReaderEvacuationRecord, read_policy::ReadPolicyStore};

mod races;
mod tests;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
impl Fixture {
    fn verifier(&self) -> FleetReaderEvacuationVerifier {
        FleetReaderEvacuationVerifier::new(
            self.directory.clone(),
            CellAuthority::new(self.layout.clone()),
            ReadPolicyStore::new(self.layout.clone()),
            self.peer.clone(),
        )
    }
    async fn stored(&self, digest: Digest) -> ReaderEvacuationRecord {
        self.journal
            .load_reader_evacuation(scope(), digest)
            .await
            .unwrap()
            .unwrap()
    }
    async fn latest(&self) -> ReaderEvacuationRecord {
        let snapshot = self.journal.load_snapshot(scope()).await.unwrap();
        self.journal
            .latest_reader_evacuation(
                &snapshot,
                self.operation.id(),
                self.original.spec().key().unwrap(),
            )
            .await
            .unwrap()
            .unwrap()
    }
    async fn client(&self) -> SqliteJournal {
        SqliteJournal::open(
            self.root.path().join("evacuation.sqlite"),
            scope(),
            FleetProfile::default(),
            clock().unwrap(),
        )
        .await
        .unwrap()
    }
    async fn publish(&self, capture: &ReaderEvacuation) -> FleetReaderEvacuationPublication {
        FleetReaderEvacuationPublication::publish(
            capture,
            self.journal.as_ref(),
            &self.verifier(),
            deadline(),
            clock,
        )
        .await
        .unwrap()
    }
}
