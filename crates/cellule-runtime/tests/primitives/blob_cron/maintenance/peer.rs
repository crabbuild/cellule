//! Signed in-process transport to the actual destination command and Inbox.

use super::*;
use cellule_runtime::cell::actor::CellHandle;
use cellule_runtime::peer::{
    EffectPeerClient, PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal,
    PeerRoundTrip, PeerSigner, PeerVerifier, VerifiedPeerRequest,
};
use std::{future::Future, pin::Pin};

pub(super) fn client(
    registry: Arc<cellule_runtime::Registry>,
    target: CellTarget,
    handle: CellHandle,
) -> EffectPeerClient {
    let session = SessionId::from_bytes([70; 16]);
    let signer = Arc::new(PeerSigner::new(
        session,
        registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[71; 32]),
    ));
    EffectPeerClient::new(
        signer.clone(),
        PeerPrincipal {
            issuer: "cellule:test".into(),
            subject: "cron-source".into(),
            actions: vec!["cron.invoke".into()],
        },
        Arc::new(Loopback {
            verifier: Arc::new(PeerVerifier::new(
                session,
                registry.release_digest(),
                signer.verifying_key(),
            )),
            dispatcher: Arc::new(PeerDispatcher::new(
                registry,
                Arc::new(Resolver { target, handle }),
                Arc::new(Authorizer),
            )),
        }),
    )
}

struct Resolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for Resolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<CellHandle>> + Send + 'static>> {
        let matches = target == self.target;
        let handle = self.handle.clone();
        Box::pin(async move {
            if matches {
                Ok(handle)
            } else {
                Err(Error::CellNotActive)
            }
        })
    }
}

struct Authorizer;

impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.permits("cron.invoke") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization("missing Cron invocation action"))
        }
    }
}

struct Loopback {
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
}

impl PeerRoundTrip for Loopback {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        _remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let verifier = self.verifier.clone();
        let dispatcher = self.dispatcher.clone();
        Box::pin(async move {
            let now = now_ms();
            let verified = verifier.verify(&request, now)?;
            if verified.target() != &target {
                return Err(Error::Peer("round trip target changed"));
            }
            dispatcher.dispatch_bytes(&verified, now).await
        })
    }
}
