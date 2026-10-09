//! Exercise callback scheduling through the original complete Cell close path.
use super::*;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::control::{BundleBindingRef, Control, ControlState, Owner, RootRef};
use crate::identity::Digest;
use crate::node::log::CellIssuedRange;
use futures_util::{future::join_all, poll};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FifoClosures {
    state: tokio::sync::Mutex<()>,
    lease: NodeLeaseGuard,
    entered: AtomicUsize,
    peak: AtomicUsize,
    closed: Mutex<Vec<CellIssuedRange>>,
}

struct Callback<'a>(&'a AtomicUsize);
impl Drop for Callback<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl NodeBundleAuthority for FifoClosures {
    fn bind<'a>(
        &'a self,
        _: &'a CellAuthority,
        _: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        Box::pin(async { Err(Error::Node("unused closure test bind")) })
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let entered = self.entered.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(entered, Ordering::SeqCst);
            let _callback = Callback(&self.entered);
            let _state = self.state.lock().await;
            self.lease.check()?;
            let scope = issued.scope();
            assert_eq!(
                scope.application.as_bytes(),
                authority.layout().application_id()
            );
            assert_eq!(scope.cell, observed.value().cell);
            assert_eq!(scope.incarnation, observed.value().incarnation);
            assert_eq!(scope.cell_epoch, observed.value().epoch);
            assert_eq!(issued.leader_session(), session(1));
            assert_eq!(issued.log_epoch(), 2);
            assert_eq!(issued.commit_sequence(), 1);
            assert_eq!(
                issued.position(),
                observed.value().ltx_root().unwrap().position
            );
            assert_eq!(issued.last_node_sequence(), 0);
            self.closed.lock().unwrap().push(issued);
            Ok(())
        })
    }
}

async fn fixture(
    count: usize,
) -> (
    Arc<NodeDurability>,
    Arc<FifoClosures>,
    Vec<(CellAuthority, VersionedControl)>,
) {
    let guard = lease();
    let callbacks = Arc::new(FifoClosures {
        state: tokio::sync::Mutex::new(()),
        lease: guard.clone(),
        entered: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        closed: Mutex::new(Vec::new()),
    });
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(ImmediateTransport);
    let shipper = NodeLogShipper::new(
        gate.clone(),
        transport.clone(),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    let durability = Arc::new(NodeDurability::new(
        gate,
        shipper,
        Arc::new(RecordingAuthority::default()),
        transport,
        guard,
    ));
    // This seam verifies scheduling, not catalog publication or reconstruction.
    // Real close callbacks share this FIFO mutex with their heartbeat authority.
    let _feed = durability
        .enable_bundle_publication(callbacks.clone())
        .unwrap();
    let layout = cellule_ltx::CellStorageLayout::new(
        cellule_store::Store::new(Arc::new(object_store::memory::InMemory::new())),
        object_store::path::Path::from("close-scheduling"),
        [1; 16],
    );
    let authority = CellAuthority::new(layout.clone());
    let mut cells = Vec::with_capacity(count);
    for index in 1..=count {
        let mut id = [0; 32];
        id[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let cell = CellId::from_bytes(id);
        let mut control = Control::initial(
            cell,
            IncarnationId::from_bytes([3; 16]),
            Owner {
                session: session(1),
                endpoint: "https://original.test".into(),
            },
            Digest::from_bytes([5; 32]),
            1,
        )
        .unwrap();
        control.state = ControlState::Serving;
        control.root = Some(RootRef {
            digest: Digest::from_bytes([7; 32]),
            txid: 1,
            checksum: cellule_ltx::types::CHECKSUM_FLAG | 1,
            commit_sequence: 1,
        });
        control.bundle_binding = Some(BundleBindingRef {
            session: session(1),
            epoch: 2,
            digest: Digest::from_bytes(id),
        });
        layout
            .store()
            .create_strict(
                &layout.control_path(cell.as_bytes()),
                Bytes::from(control.encode().unwrap()),
            )
            .await
            .unwrap();
        cells.push((
            authority.clone(),
            authority.load(cell).await.unwrap().unwrap(),
        ));
    }
    (durability, callbacks, cells)
}

#[tokio::test]
async fn two_thousand_cell_closures_leave_a_bounded_heartbeat_queue() {
    let (durability, callbacks, cells) = fixture(2000).await;
    let held = callbacks.state.lock().await;
    let mut closures: Vec<_> = cells
        .iter()
        .map(|(authority, observed)| Box::pin(durability.close_bundle_cell(authority, observed)))
        .collect();
    // Poll every original caller once, including those outside the callback
    // cohort. A join_all implementation may otherwise yield before all callers.
    for close in &mut closures {
        assert!(poll!(close.as_mut()).is_pending());
    }
    let heartbeat = async {
        let _state = callbacks.state.lock().await;
        let before = callbacks.closed.lock().unwrap().len();
        callbacks.lease.renew(1001, 61001).unwrap();
        before
    };
    tokio::pin!(heartbeat);
    assert!(poll!(heartbeat.as_mut()).is_pending());
    drop(held);
    let (results, before_heartbeat) = tokio::join!(join_all(closures), heartbeat);
    assert!(results.into_iter().all(|result| result.is_ok()));
    assert!(
        before_heartbeat <= 8,
        "heartbeat queued behind {before_heartbeat} Cell callbacks"
    );
    assert!(callbacks.peak.load(Ordering::SeqCst) <= 8);
    let closed = callbacks.closed.lock().unwrap();
    assert_eq!(closed.len(), 2000);
    assert_eq!(
        closed
            .iter()
            .map(|issued| issued.scope().cell)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2000
    );
    assert_eq!(callbacks.entered.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fencing_wakes_closures_waiting_outside_authority() {
    let (durability, callbacks, cells) = fixture(16).await;
    let held = callbacks.state.lock().await;
    let mut closures: Vec<_> = cells
        .iter()
        .map(|(authority, observed)| Box::pin(durability.close_bundle_cell(authority, observed)))
        .collect();
    for close in &mut closures {
        assert!(poll!(close.as_mut()).is_pending());
    }
    callbacks.lease.fence();
    for close in &mut closures[8..] {
        assert!(matches!(
            poll!(close.as_mut()),
            std::task::Poll::Ready(Err(Error::Fenced))
        ));
    }
    closures.truncate(8);
    drop(held);
    assert!(
        join_all(closures)
            .await
            .into_iter()
            .all(|result| matches!(result, Err(Error::Fenced)))
    );
    assert!(callbacks.closed.lock().unwrap().is_empty());
    assert_eq!(callbacks.entered.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_queued_closures_releases_callback_admission() {
    let (durability, callbacks, cells) = fixture(16).await;
    let held = callbacks.state.lock().await;
    let mut closures: Vec<_> = cells
        .iter()
        .map(|(authority, observed)| Box::pin(durability.close_bundle_cell(authority, observed)))
        .collect();
    for close in &mut closures {
        assert!(poll!(close.as_mut()).is_pending());
    }
    drop(closures);
    assert_eq!(callbacks.entered.load(Ordering::SeqCst), 0);
    let mut retry = Box::pin(durability.close_bundle_cell(&cells[0].0, &cells[0].1));
    assert!(poll!(retry.as_mut()).is_pending());
    assert_eq!(callbacks.entered.load(Ordering::SeqCst), 1);
    drop(held);
    retry.await.unwrap();
    assert_eq!(callbacks.entered.load(Ordering::SeqCst), 0);
    assert_eq!(callbacks.closed.lock().unwrap().len(), 1);
}
