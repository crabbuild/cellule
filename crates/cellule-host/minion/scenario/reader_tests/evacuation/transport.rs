use super::*;
use cellule_runtime::peer::{
    PeerAuthorizer, PeerDispatcher, PeerRoundTrip, ResidentPeerCellResolver, VerifiedPeerRequest,
    wire,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::oneshot;

#[derive(Clone)]
pub(super) struct NativePeers {
    directory: NodeDirectory,
    origin: usize,
    dispatchers: Vec<Arc<PeerDispatcher>>,
    pub(super) probes: Arc<AtomicUsize>,
    pause: Arc<Mutex<Option<Pause>>>,
}
struct Pause {
    number: usize,
    captured: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}
struct Authorizer {
    origin: SessionId,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.origin_session() != self.origin
            || request.principal().issuer != "managed-owner"
            || request.principal().subject != "live-owner"
            || !request
                .principal()
                .actions
                .iter()
                .any(|action| action == "replica-maintenance")
        {
            return Err(Error::PeerAuthorization(
                "maintenance fixture principal differs",
            ));
        }
        Ok(())
    }
}
impl NativePeers {
    pub(super) fn new(
        nodes: &[Arc<CellNode>],
        managers: &[ReadReplicaManager],
        layout: &CellStorageLayout,
        directory: NodeDirectory,
    ) -> Self {
        Self::for_origin(nodes, managers, layout, directory, 0)
    }
    pub(super) fn for_origin(
        nodes: &[Arc<CellNode>],
        managers: &[ReadReplicaManager],
        layout: &CellStorageLayout,
        directory: NodeDirectory,
        origin: usize,
    ) -> Self {
        let dispatchers = nodes
            .iter()
            .zip(managers)
            .map(|(node, manager)| {
                let registry = node.application().registry();
                Arc::new(
                    PeerDispatcher::new(
                        registry.clone(),
                        Arc::new(ResidentPeerCellResolver::new(
                            node.runtime().clone(),
                            layout.clone(),
                            registry,
                        )),
                        Arc::new(Authorizer {
                            origin: session(origin),
                        }),
                    )
                    .with_replica_control(Arc::new(manager.clone()))
                    .with_replica_resolver(Arc::new(manager.clone())),
                )
            })
            .collect();
        Self {
            directory,
            origin,
            dispatchers,
            probes: Arc::new(AtomicUsize::new(0)),
            pause: Arc::new(Mutex::new(None)),
        }
    }
    pub(super) fn pause_probe(
        &self,
        number: usize,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        self.probes.store(0, Ordering::SeqCst);
        let (captured, waiting) = oneshot::channel();
        let (resume, paused) = oneshot::channel();
        *self.pause.lock().unwrap() = Some(Pause {
            number,
            captured,
            resume: paused,
        });
        (waiting, resume)
    }
}
impl PeerRoundTrip for NativePeers {
    fn send(
        &self,
        _: CellTarget,
        _: Vec<u8>,
        _: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        Box::pin(async { Err(Error::Peer("fixture requires explicit replica routing")) })
    }
    fn send_to_node(
        &self,
        target: CellTarget,
        node: cellule_runtime::node::NodeAdvertisement,
        request: Vec<u8>,
        _: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peers = self.clone();
        Box::pin(async move {
            let index = (0..3)
                .find(|index| node.node() == node_id(*index) && node.session() == session(*index))
                .ok_or(Error::Fenced)?;
            let now = clock()?;
            peers
                .directory
                .load(node.session(), now)
                .await?
                .ok_or(Error::Fenced)?;
            // Trusted in-process routing pins the actual enrolled sender's
            // certificate/key, then uses the same verifier as an mTLS adapter.
            let verified = peers
                .directory
                .verify_peer_request(
                    &request,
                    Digest::from_bytes([30; 32]),
                    SigningKey::from_bytes(&[peers.origin as u8 + 1; 32])
                        .verifying_key()
                        .to_bytes(),
                    now,
                )
                .await?;
            if verified.target() != &target {
                return Err(Error::Fenced);
            }
            let status = matches!(
                verified.operation(),
                Some(wire::peer_request::Operation::Read(wire::ReadRequest {
                    operation: Some(wire::read_request::Operation::ReplicaStatus(true)),
                    ..
                }))
            );
            let reply = peers.dispatchers[index]
                .dispatch_bytes(&verified, clock()?)
                .await?;
            let pause = if status {
                let number = peers.probes.fetch_add(1, Ordering::SeqCst) + 1;
                let mut pending = peers.pause.lock().unwrap();
                if pending.as_ref().is_some_and(|pause| pause.number == number) {
                    pending.take()
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(pause) = pause {
                let _ = pause.captured.send(());
                let _ = pause.resume.await;
            }
            Ok(reply)
        })
    }
}
