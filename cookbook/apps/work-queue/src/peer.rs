use cellule_cookbook_support::LocalPeer;
use cellule_runtime::{
    CellTarget, Error, Registry,
    cell::actor::CellHandle,
    peer::{EffectPeerClient, PeerAuthorizer, PeerPrincipal, VerifiedPeerRequest},
};
use std::sync::Arc;
struct Authorizer;
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.permits("cookbook.dead-letter.deliver") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization(
                "dead-letter delivery not permitted",
            ))
        }
    }
}
pub(crate) fn client(
    registry: Arc<Registry>,
    target: CellTarget,
    handle: CellHandle,
) -> EffectPeerClient {
    LocalPeer::new(registry, target, handle, Arc::new(Authorizer)).effect_client(PeerPrincipal {
        issuer: "work-queue".into(),
        subject: "local-effects".into(),
        actions: vec![
            "cell.read".into(),
            "cell.write".into(),
            "cookbook.dead-letter.deliver".into(),
        ],
    })
}
