use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::control::{ControlState, Owner, RootRef, Transition};
use crate::identity::{ApplicationId, CellId, IncarnationId, NodeId};
use crate::node::lease::NodeLeaseGuard;
use crate::node::log::{AssignedCommitRange, CellLogScope, DurabilityGate, DurabilitySource};
use crate::node::{
    NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain, VersionedNodeAdvertisement,
};
use cellule_ltx::{CellReplica, CellStorageLayout, Db, Limits};
use cellule_store::{Store, test_support::CountingObjectStore};
use ed25519_dalek::{Signer, SigningKey};
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

const NOW: i64 = 1_000_000;
const EPOCH: u64 = 2;
mod actor;
mod coverage;
mod faults;
mod index;
mod lifecycle;
mod managed;
mod ranges;
mod readiness;
mod receipts;
mod recovery;
struct Fixture {
    count: Arc<CountingObjectStore>,
    layout: CellStorageLayout,
    directory: NodeDirectory,
    node: VersionedNodeAdvertisement,
    lease: NodeLeaseGuard,
    gate: DurabilityGate,
    scratch: tempfile::TempDir,
    capture_host: cellule_ltx::Host,
}
struct Cell {
    db: Db,
    replica: CellReplica,
    authority: CellAuthority,
    control: VersionedControl,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_store(Arc::new(InMemory::new())).await
    }
    async fn with_store(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self::with_capture_host(store, cellule_ltx::Host::default()).await
    }
    async fn with_capture_host(
        store: Arc<dyn object_store::ObjectStore>,
        capture_host: cellule_ltx::Host,
    ) -> Self {
        let count = Arc::new(CountingObjectStore::new(store));
        let layout = CellStorageLayout::new(
            Store::new(count.clone()),
            Path::from("bundle-test"),
            [9; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            Digest::from_bytes([6; 32]),
            Digest::from_bytes([7; 32]),
            Digest::from_bytes([8; 32]),
        );
        let key = SigningKey::from_bytes(&[10; 32]);
        let advertisement = NodeAdvertisement::sign(
            NodeId::from_bytes([1; 16]),
            SessionId::from_bytes([1; 16]),
            "https://bundle.internal:8081".into(),
            Digest::from_bytes([6; 32]),
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([7; 32]),
            Digest::from_bytes([8; 32]),
            &key,
            1,
            NOW,
            NOW + 30_000,
            vec![Digest::from_bytes([12; 32])],
            vec![1],
            NodeFailureDomain::new(None, None).unwrap(),
            NodeCapacity {
                free_memory_bytes: 1 << 30,
                free_disk_bytes: 1 << 30,
                follower_free_bytes: 1 << 30,
                follower_retained_bytes: 0,
                job_credits: 8,
                log_protocol: 2,
            },
        )
        .unwrap();
        let node = directory.create(advertisement, NOW).await.unwrap();
        let node = directory
            .initialize_bundle_lane(&node, EPOCH, NOW)
            .await
            .unwrap();
        let lease = NodeLeaseGuard::new(NOW, NOW + 30_000).unwrap();
        let gate = DurabilityGate::new(
            SessionId::from_bytes([1; 16]),
            NodeId::from_bytes([1; 16]),
            EPOCH,
            [NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
        )
        .unwrap();
        Self {
            count,
            layout,
            directory,
            node,
            lease,
            gate,
            scratch: tempfile::tempdir().unwrap(),
            capture_host,
        }
    }
    async fn heartbeat(&mut self) -> i64 {
        // Density tests deliberately perform hundreds of selections. Renew
        // only after canonical heartbeat CAS, preserving the original head
        // and production lease duration even on a busy verification host.
        let mut next = self.node.advertisement().clone();
        next.issued_at_ms += 1;
        next.expires_at_ms += 1;
        next.progress += 1;
        let key = SigningKey::from_bytes(&[10; 32]);
        next.signature = key.sign(&next.signing_bytes().unwrap()).to_bytes();
        let now = next.issued_at_ms;
        let refreshed = self.directory.refresh(&self.node, next, now).await.unwrap();
        assert_eq!(
            refreshed.advertisement().bundle_head(),
            self.node.advertisement().bundle_head()
        );
        self.lease
            .renew(now, refreshed.advertisement().expires_at_ms())
            .unwrap();
        self.node = refreshed;
        now
    }
    async fn cell(&mut self, byte: u8) -> Cell {
        self.cell_for_application(byte, [9; 16]).await
    }
    async fn cell_for_application(&mut self, byte: u8, application: [u8; 16]) -> Cell {
        let mut cell = self.unbound_cell_for_application(byte, application).await;
        let (node, control) = self
            .directory
            .bind_bundle_cell(&self.node, &cell.authority, &cell.control, NOW)
            .await
            .unwrap();
        self.node = node;
        cell.control = control;
        cell
    }
    async fn unbound_cell_for_application(&mut self, byte: u8, application: [u8; 16]) -> Cell {
        self.unbound_cell_at_commit(byte, application, 1).await
    }
    async fn unbound_cell_at_commit(
        &mut self,
        byte: u8,
        application: [u8; 16],
        commit_sequence: u64,
    ) -> Cell {
        let layout = self.layout.for_application(application);
        let cell = CellId::from_bytes([byte; 32]);
        let incarnation = IncarnationId::from_bytes([byte + 10; 16]);
        let mut db = Db::open_with_host(
            &self.scratch.path().join(format!(
                "{byte}-{}.sqlite",
                crate::identity::encode_hex(&application)
            )),
            Limits::default(),
            self.capture_host.clone(),
        )
        .unwrap();
        db.transaction(|tx| tx.execute_batch("CREATE TABLE outcomes(request TEXT PRIMARY KEY, result TEXT); INSERT INTO outcomes VALUES ('seed','original')")).unwrap();
        let cuts = db.capture().unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *cell.as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let prepared = replica
            .prepare(None, &cuts, commit_sequence, 1)
            .await
            .unwrap();
        let mut control = Control::initial(
            cell,
            incarnation,
            Owner {
                session: SessionId::from_bytes([1; 16]),
                endpoint: "https://bundle.internal:8081".into(),
            },
            Digest::from_bytes([12; 32]),
            1,
        )
        .unwrap();
        control.state = ControlState::Serving;
        control.root = Some(RootRef::from_ltx(cell, incarnation, prepared.root()).unwrap());
        layout
            .store()
            .create_strict(
                &layout.control_path(cell.as_bytes()),
                Bytes::from(control.encode().unwrap()),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout);
        authority.retain_root_lineage(&prepared).await.unwrap();
        let observed = authority.load(cell).await.unwrap().unwrap();
        Cell {
            db,
            replica,
            authority,
            control: observed,
        }
    }
    fn append(
        &self,
        cell: &mut Cell,
        commit: u64,
    ) -> (
        cellule_ltx::CaptureBatch,
        Vec<cellule_ltx::VerifiedNodeFrame>,
        AssignedCommitRange,
    ) {
        cell.db
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO outcomes VALUES (?1,?2)",
                    [format!("request-{commit}"), format!("result-{commit}")],
                )
            })
            .unwrap();
        let cuts = cell.db.capture().unwrap();
        let ticket = self.gate.preview(cuts.segments.len() as u64).unwrap();
        let frames = cuts
            .segments
            .iter()
            .enumerate()
            .map(|(offset, segment)| {
                cellule_ltx::encode_node_frame(
                    cellule_ltx::NodeFrameScope {
                        leader_session: [1; 16],
                        log_epoch: EPOCH,
                        node_sequence: ticket.first_sequence() + offset as u64,
                        application: *cell.authority.layout().application_id(),
                        cell: *cell.control.value().cell.as_bytes(),
                        incarnation: *cell.control.value().incarnation.as_bytes(),
                        cell_epoch: cell.control.value().epoch,
                        commit_sequence: commit,
                    },
                    segment.info().clone(),
                    Bytes::from(std::fs::read(segment.path()).unwrap()),
                    Limits::default(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let assignment = self.gate.commit_frames(ticket, &frames).unwrap().unwrap();
        (cuts, frames, assignment)
    }
    fn scope(cell: &Cell) -> CellLogScope {
        CellLogScope {
            application: ApplicationId::from_bytes(*cell.authority.layout().application_id()),
            cell: cell.control.value().cell,
            incarnation: cell.control.value().incarnation,
            cell_epoch: cell.control.value().epoch,
        }
    }
    fn publisher(&self, cell: &Cell) -> crate::publication::CellPublisher {
        crate::publication::CellPublisher::new(
            cell.replica.clone(),
            cell.authority.clone(),
            cell.control.clone(),
            self.scratch.path().to_owned(),
        )
        .with_node_lease(self.lease.clone())
    }
}

#[tokio::test]
async fn one_selection_covers_two_cells_while_roots_lag_and_materializes_identical_bytes() {
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (a_cuts, mut frames, a_range) = f.append(&mut a, 2);
    let (b_cuts, b_frames, b_range) = f.append(&mut b, 2);
    frames.extend(b_frames);
    let before_a = a.control.value().encode().unwrap();
    let before_b = b.control.value().encode().unwrap();
    f.count.reset();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[a_range, b_range], NOW)
        .await
        .unwrap();
    assert_eq!(f.count.put_requests(), 1);
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        f.count.put_requests(),
        2,
        "one immutable upload and one shared selector CAS"
    );
    assert_eq!(proofs.len(), 2);
    assert_eq!(
        a.authority
            .load(a.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .encode()
            .unwrap(),
        before_a
    );
    assert_eq!(
        b.authority
            .load(b.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .encode()
            .unwrap(),
        before_b
    );
    assert_eq!(
        node.advertisement()
            .bundle_head()
            .unwrap()
            .selected_through(),
        frames.len() as u64
    );
    for (cell, cuts) in [(&a, &a_cuts), (&b, &b_cuts)] {
        let proof = proofs
            .iter()
            .find(|proof| proof.binding() == cell.control.value().bundle_binding.unwrap())
            .unwrap();
        assert_eq!(proof.locator_count(), cuts.segments.len());
        let expected = cell
            .replica
            .prepare(cell.control.value().ltx_root().as_ref(), cuts, 2, 1)
            .await
            .unwrap();
        let actual = f.publisher(cell).materialize_bundle(proof).await.unwrap();
        assert_eq!(actual.position, cuts.position);
        let expected_path = f.scratch.path().join(format!(
            "expected-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        let actual_path = f.scratch.path().join(format!(
            "actual-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&expected.root())
            .await
            .unwrap()
            .restore(&expected_path)
            .await
            .unwrap();
        cell.replica
            .open_root(&actual)
            .await
            .unwrap()
            .restore(&actual_path)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(actual_path).unwrap(),
            std::fs::read(expected_path).unwrap()
        );
    }
}

#[tokio::test]
async fn closure_drains_prior_fleet_ack_rejects_old_cas_and_blocks_transfer_until_materialized() {
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, frames, assignment) = f.append(&mut a, 2);
    for member in [2, 3] {
        f.gate
            .acknowledge(
                NodeId::from_bytes([member; 16]),
                assignment.ticket().last_sequence(),
            )
            .unwrap();
    }
    f.gate.activate_fleet().unwrap();
    assert_eq!(
        f.gate.prove(assignment.ticket()).await.unwrap().source(),
        DurabilitySource::Fleet
    );
    assert_eq!(
        f.node
            .advertisement()
            .bundle_head()
            .unwrap()
            .selected_through(),
        0
    );
    let old = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let issued = f
        .gate
        .close_cell_issuance(Fixture::scope(&a), a.control.value().ltx_root().unwrap())
        .unwrap();
    assert_eq!(issued.commit_sequence(), 2);
    let pin = a.control.value().bundle_binding.unwrap();
    let closing = f
        .directory
        .begin_bundle_close(&f.node, pin, issued, NOW)
        .await
        .unwrap();
    assert!(
        f.directory
            .select_node_bundle(&closing, &old, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert!(matches!(
        f.directory
            .finish_bundle_close(&closing, pin, issued, NOW)
            .await,
        Err(Error::PendingPublication)
    ));
    let successor = a
        .control
        .value()
        .takeover(Owner {
            session: SessionId::from_bytes([6; 16]),
            endpoint: "https://successor.internal:8081".into(),
        })
        .unwrap();
    assert!(matches!(
        a.authority
            .transition(&a.control, successor, Transition::Takeover)
            .await,
        Err(Error::PendingPublication)
    ));
    let prepared = f
        .directory
        .prepare_node_bundle(&closing, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&closing, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let closed = f
        .directory
        .finish_bundle_close(&selected, pin, issued, NOW)
        .await
        .unwrap();
    assert!(matches!(
        a.authority
            .transition(
                &a.control,
                a.control.value().release().unwrap(),
                Transition::Release
            )
            .await,
        Err(Error::PendingPublication)
    ));
    let mut publisher = f.publisher(&a);
    publisher.materialize_bundle(&proofs[0]).await.unwrap();
    let materialized = publisher.control().clone();
    assert!(matches!(
        a.authority
            .transition(
                &materialized,
                materialized.value().release().unwrap(),
                Transition::Release
            )
            .await,
        Err(Error::PendingPublication)
    ));
    // Checkpoint before clearing the Cell pin; otherwise the immutable catalog
    // would retain locators that the departed writer can no longer release.
    let closed = f
        .directory
        .checkpoint_bundle_cell(&closed, &a.authority, &proofs[0], Limits::default(), NOW)
        .await
        .unwrap();
    let released = a
        .authority
        .transition(
            &materialized,
            materialized.value().release().unwrap(),
            Transition::Release,
        )
        .await
        .unwrap();
    assert!(released.value().bundle_binding.is_none());
    assert!(
        f.directory
            .bind_bundle_cell(&closed, &a.authority, &a.control, NOW)
            .await
            .is_err()
    );
    let (_, sibling_frames, sibling_assignment) = f.append(&mut b, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&closed, &sibling_frames, &[sibling_assignment], NOW)
        .await
        .unwrap();
    f.directory
        .select_node_bundle(&closed, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let ticket = f.gate.preview(1).unwrap();
    let late = frames[0]
        .clone()
        .with_node_sequence(ticket.first_sequence())
        .unwrap();
    assert!(matches!(
        f.gate.commit_frames(ticket, &[late]),
        Err(Error::Fenced)
    ));
    assert_eq!(
        f.gate.issued_through(),
        sibling_assignment.ticket().last_sequence()
    );
}

#[tokio::test]
async fn missing_and_unassigned_rows_never_mint_proof_or_advance_head() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &[], NOW)
            .await
            .is_err()
    );
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &[], &[assignment], NOW)
            .await
            .is_err()
    );
    let mut wrong = frames.clone();
    wrong[0] = wrong[0].clone().with_node_sequence(2).unwrap();
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &wrong, &[assignment], NOW)
            .await
            .is_err()
    );
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    f.layout.store().delete(&path).await.unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    let node = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.advertisement().bundle_head(),
        f.node.advertisement().bundle_head()
    );
    assert_eq!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .root,
        cell.control.value().root
    );
}

#[tokio::test]
async fn lease_loss_and_heartbeat_rebase_preserve_bundle_authority() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let key = SigningKey::from_bytes(&[10; 32]);
    let mut next = f.node.advertisement().clone();
    next.issued_at_ms += 1_000;
    next.expires_at_ms += 1_000;
    next.progress += 1;
    next.signature = key.sign(&next.signing_bytes().unwrap()).to_bytes();
    let refreshed = f
        .directory
        .refresh(&f.node, next, NOW + 1_000)
        .await
        .unwrap();
    assert_eq!(
        refreshed.advertisement().bundle_head(),
        f.node.advertisement().bundle_head()
    );
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW + 1_000)
        .await
        .unwrap();
    assert_eq!(selected.advertisement().issued_at_ms(), NOW + 1_000);
    f.lease.fence();
    assert!(matches!(
        f.directory
            .select_node_bundle(
                &selected,
                &prepared,
                &f.lease,
                Limits::default(),
                NOW + 1_000
            )
            .await,
        Err(Error::Fenced)
    ));
}
