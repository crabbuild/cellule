//! Native live-owner rotation with durable history and reconstructed confirmation.
use super::evacuation::{maintenance, rotated, spare};
use super::*;
use cellule_host::{
    FollowerEvacuation,
    fleet::{
        FleetFollowerEvacuationJournal, FleetFollowerEvacuationPublication,
        FleetFollowerEvacuationVerifier,
    },
};
use cellule_runtime::fleet::operations::{FollowerEvacuationRecord, FollowerReplacementPolicy};
mod cancelled_settlement;
mod maintenance;
mod observation;
mod races;
mod restart;
mod tests;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn verifier(fixture: &ManagedFixture) -> FleetFollowerEvacuationVerifier {
    FleetFollowerEvacuationVerifier::new(
        fixture.native.directory.clone(),
        Arc::new(Snapshots::new(fixture)),
    )
}
struct Snapshots {
    nodes: Vec<Arc<CellNode>>,
    pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl Snapshots {
    fn new(fixture: &ManagedFixture) -> Self {
        Self {
            nodes: fixture.nodes.clone(),
            pause: Mutex::new(None),
        }
    }
    fn pause_next(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (resume, release) = tokio::sync::oneshot::channel();
        *self.pause.lock().unwrap() = Some((entered, release));
        (captured, resume)
    }
}
impl cellule_host::fleet::FleetSnapshotTransport for Snapshots {
    fn capture<'a>(
        &'a self,
        request: &'a cellule_host::fleet::FleetSnapshotRequest,
        _deadline: Instant,
    ) -> cellule_host::fleet::FleetAdapterFuture<'a, Arc<cellule_host::fleet::FleetNodeSnapshot>>
    {
        Box::pin(async move {
            let gate = self.pause.lock().unwrap().take();
            if let Some((entered, resume)) = gate {
                let _ = entered.send(());
                let _ = resume.await;
            }
            let node = self
                .nodes
                .iter()
                .enumerate()
                .find(|(index, _)| {
                    node_id(*index) == request.node() && session(*index) == request.session()
                })
                .map(|(_, node)| node)
                .ok_or(Error::Fenced)?;
            node.fleet_snapshot(request.clone())
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}
async fn setup() -> (
    ManagedFixture,
    FollowerEvacuation,
    FollowerReplacementPolicy,
) {
    setup_epochs(2).await
}
async fn setup_epochs(
    epochs: usize,
) -> (
    ManagedFixture,
    FollowerEvacuation,
    FollowerReplacementPolicy,
) {
    let fixture = ManagedFixture::with_members(4, epochs).await;
    let (original, operation) = maintenance(&fixture).await;
    spare(&fixture).await;
    rotated(&fixture).await;
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let policy = FollowerReplacementPolicy::new(scope(), 1, 2).unwrap();
    fixture
        .native
        .journal
        .set_follower_replacement_policy(&snapshot, policy, clock().unwrap())
        .await
        .unwrap();
    let capture = fixture
        .native
        .node
        .follower_evacuation(&original, &operation, 2, deadline())
        .await
        .unwrap();
    (fixture, capture, policy)
}
async fn publish(
    fixture: &ManagedFixture,
    capture: &FollowerEvacuation,
    policy: FollowerReplacementPolicy,
) -> FleetFollowerEvacuationPublication {
    FleetFollowerEvacuationPublication::publish(
        capture,
        policy,
        fixture.native.journal.as_ref(),
        &verifier(fixture),
        deadline(),
        clock,
    )
    .await
    .unwrap()
}
async fn client(fixture: &ManagedFixture) -> SqliteJournal {
    SqliteJournal::open(
        fixture.native.root.path().join("journal.sqlite"),
        scope(),
        FleetProfile::default(),
        clock().unwrap(),
    )
    .await
    .unwrap()
}
async fn stored(fixture: &ManagedFixture, digest: Digest) -> FollowerEvacuationRecord {
    fixture
        .native
        .journal
        .load_follower_evacuation(scope(), digest)
        .await
        .unwrap()
        .unwrap()
}
async fn latest(
    fixture: &ManagedFixture,
    record: &FollowerEvacuationRecord,
) -> FollowerEvacuationRecord {
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .latest_follower_evacuation(&snapshot, record.operation().id(), record.original_key())
        .await
        .unwrap()
        .unwrap()
}
