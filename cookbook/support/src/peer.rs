use cellule_runtime::{
    CellTarget, Error, Registry, SessionId,
    cell::actor::CellHandle,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal,
        PeerRoundTrip, PeerSigner, PeerVerifier, VerifiedPeerRequest,
    },
};
use ed25519_dalek::SigningKey;
use rand::RngCore as _;
use std::{future::Future, pin::Pin, sync::Arc};

struct Destination {
    cells: Vec<(CellTarget, CellHandle)>,
}
impl PeerCellResolver for Destination {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<CellHandle>> + Send + 'static>> {
        let handle = self
            .cells
            .iter()
            .find(|(expected, _)| expected == &target)
            .map(|(_, handle)| handle.clone());
        Box::pin(async move {
            if let Some(handle) = handle {
                Ok(handle)
            } else {
                Err(Error::PeerAuthorization("foreign local peer destination"))
            }
        })
    }
}
/// Signed process-local peer transport using the real destination dispatcher and inbox.
///
/// It pins one destination and one ephemeral signer. The embedding application
/// supplies its authorizer and principal; this adapter owns no domain policy.
/// Network deployments supply authenticated transport and durable trust discovery.
#[derive(Clone)]
pub struct LocalPeer {
    signer: Arc<PeerSigner>,
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
    registry: Arc<Registry>,
    authorizer: Arc<dyn PeerAuthorizer>,
}
impl LocalPeer {
    /// Binds one destination and an application-owned authorization policy.
    pub fn new(
        registry: Arc<Registry>,
        target: CellTarget,
        handle: CellHandle,
        authorizer: Arc<dyn PeerAuthorizer>,
    ) -> Self {
        Self::build(registry, vec![(target, handle)], authorizer)
    }
    /// Pins an explicit roster of 1..4 distinct same-application destinations.
    /// No destination is inferred from storage listings or incoming requests.
    pub fn for_destinations(
        registry: Arc<Registry>,
        cells: Vec<(CellTarget, CellHandle)>,
        authorizer: Arc<dyn PeerAuthorizer>,
    ) -> cellule_runtime::Result<Self> {
        let Some((scope, _)) = cells.first() else {
            return Err(Error::PeerAuthorization("empty local destination roster"));
        };
        if cells.len() > 4
            || cells.iter().enumerate().any(|(index, (target, handle))| {
                target.tenant() != scope.tenant()
                    || target.application() != scope.application()
                    || target.cell_id() != handle.cell_id()
                    || cells[..index]
                        .iter()
                        .any(|(previous, _)| previous == target)
            })
        {
            return Err(Error::PeerAuthorization("invalid local destination roster"));
        }
        Ok(Self::build(registry, cells, authorizer))
    }
    fn build(
        registry: Arc<Registry>,
        cells: Vec<(CellTarget, CellHandle)>,
        authorizer: Arc<dyn PeerAuthorizer>,
    ) -> Self {
        let mut key = [0; 32];
        rand::rng().fill_bytes(&mut key);
        let session = SessionId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let signer = Arc::new(PeerSigner::new(
            session,
            registry.release_digest(),
            SigningKey::from_bytes(&key),
        ));
        let verifier = Arc::new(PeerVerifier::new(
            session,
            registry.release_digest(),
            signer.verifying_key(),
        ));
        let dispatcher = Arc::new(PeerDispatcher::new(
            registry.clone(),
            Arc::new(Destination { cells }),
            authorizer.clone(),
        ));
        Self {
            signer,
            verifier,
            dispatcher,
            registry,
            authorizer,
        }
    }
    /// Returns the pinned signer for an application transport decorator.
    pub fn signer(&self) -> Arc<PeerSigner> {
        self.signer.clone()
    }
    /// Authenticates an envelope for application diagnostics or exercised fault injection.
    pub fn verify_request(&self, bytes: &[u8]) -> cellule_runtime::Result<VerifiedPeerRequest> {
        self.verifier.verify(bytes, clock()?)
    }
    /// Runs application policy before a dynamic receiver acquires any Cell.
    pub fn authorize_request(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        self.authorizer.authorize(request)
    }
    /// Dispatches an authenticated request to an explicitly acquired matching Cell.
    /// The original application authorizer still runs; no new unsigned wire path exists.
    /// The caller owns that Cell's node and must retain it through reply and drain.
    pub async fn dispatch_to(
        &self,
        request: &VerifiedPeerRequest,
        handle: CellHandle,
    ) -> cellule_runtime::Result<Vec<u8>> {
        if request.target().cell_id() != handle.cell_id() {
            return Err(Error::PeerAuthorization(
                "dynamic destination handle differs",
            ));
        }
        PeerDispatcher::new(
            self.registry.clone(),
            Arc::new(Destination {
                cells: vec![(request.target().clone(), handle)],
            }),
            self.authorizer.clone(),
        )
        .dispatch_bytes(request, clock()?)
        .await
    }
    /// Builds the ordinary effect client over this exact signed transport.
    pub fn effect_client(&self, principal: PeerPrincipal) -> EffectPeerClient {
        EffectPeerClient::new(self.signer(), principal, Arc::new(self.clone()))
    }
}
fn clock() -> cellule_runtime::Result<i64> {
    crate::now_ms().map_err(|source| Error::PeerTransport {
        context: "local peer wall clock",
        source: Box::new(source),
    })
}
impl PeerRoundTrip for LocalPeer {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        _: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peer = self.clone();
        Box::pin(async move {
            let request = peer.verify_request(&bytes)?;
            if request.target() != &target {
                return Err(Error::PeerAuthorization("changed local peer target"));
            }
            peer.dispatcher.dispatch_bytes(&request, clock()?).await
        })
    }
}
