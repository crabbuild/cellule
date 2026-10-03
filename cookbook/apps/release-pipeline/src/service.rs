use crate::{
    ARTIFACTS, Acknowledgment, Artifacts, FLOWS, Flows, Projection, RECORDS, Records,
    ReleaseApplication, ReleaseClient, wire::decode_wire,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error,
    peer::{PeerAuthorizer, PeerPrincipal, VerifiedPeerRequest, wire},
    primitives::{
        effects::{EffectModule, EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
        workflow::{ActivityRunOutcome, ActivitySupervisor, ActivitySupervisorError},
    },
};
use std::{sync::Arc, time::Duration};
/// Concrete lifecycle, native Activity, and signed delivery error sources.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node enrollment, provider, or drain failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, routing, codec, or authorization failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Owned native Activity execution or completion failure.
    #[error(transparent)]
    Activity(#[from] ActivitySupervisorError),
    /// Owned native Effect execution or settlement failure.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
}
/// Opens current code normally; an older stored Workflow requires the explicit rollout path.
pub async fn open_release<const V: u8>(
    node: &LocalNode,
    handle: &ApplicationHandle<ReleaseApplication<V>>,
) -> Result<ReleaseClient<V>, ServiceError> {
    let client = ReleaseClient::new(handle.clone());
    node.open_cell(&client.target(ARTIFACTS)?, &Artifacts)
        .await?;
    node.open_cell(&client.target(RECORDS)?, &Records).await?;
    node.open_cell(&client.target(FLOWS)?, &Flows::<V>::new()?)
        .await?;
    Ok(client)
}
/// Explicit version-one to version-two code introduction; drain the predecessor under its own release first.
pub async fn open_release_after_rollout(
    node: &LocalNode,
    handle: &ApplicationHandle<ReleaseApplication<2>>,
) -> Result<ReleaseClient<2>, ServiceError> {
    let predecessor = crate::compile_release::<1>()?;
    let client = ReleaseClient::new(handle.clone());
    node.open_cell(&client.target(ARTIFACTS)?, &Artifacts)
        .await?;
    node.open_cell(&client.target(RECORDS)?, &Records).await?;
    node.open_cell_after_rollout(
        &client.target(FLOWS)?,
        &Flows::<2>::new()?,
        predecessor.as_ref(),
    )
    .await?;
    Ok(client)
}
struct Authorizer {
    source: CellTarget,
    destination: CellTarget,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.target() != &self.destination || !request.permits("cookbook.release.deliver") {
            return Err(Error::PeerAuthorization(
                "release signed destination or permission differs",
            ));
        }
        match request.operation() {
            Some(wire::peer_request::Operation::Read(read))
                if matches!(
                    read.operation,
                    Some(wire::read_request::Operation::Describe(true))
                ) =>
            {
                Ok(())
            }
            Some(wire::peer_request::Operation::ResolveEffect(r))
                if r.identity
                    .as_ref()
                    .is_some_and(|i| i.source_cell == self.source.cell_id().as_bytes()) =>
            {
                Ok(())
            }
            Some(wire::peer_request::Operation::DeliverEffect(e)) => {
                let id = e
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing release Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(c)) = &e.operation else {
                    return Err(Error::PeerAuthorization(
                        "release delivery requires typed command",
                    ));
                };
                if id.source_cell != self.source.cell_id().as_bytes() || c.codec_version != 1 {
                    return Err(Error::PeerAuthorization(
                        "release signed source or codec differs",
                    ));
                }
                match (
                    self.source.namespace(),
                    self.destination.namespace(),
                    c.command_id,
                ) {
                    (FLOWS, RECORDS, 1) => decode_wire::<Projection>(&c.input, 8192)?.validate(),
                    (RECORDS, FLOWS, 14) => decode_wire::<Acknowledgment>(&c.input, 8192)?
                        .projection
                        .validate(),
                    _ => Err(Error::PeerAuthorization("unsupported release signed route")),
                }
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported release peer operation",
            )),
        }
    }
}
async fn runner<const V: u8, M: EffectModule>(
    node: &LocalNode,
    handle: &ApplicationHandle<ReleaseApplication<V>>,
    source: CellTarget,
    destination: CellTarget,
) -> Result<(), ServiceError> {
    let cell = if destination.namespace() == RECORDS {
        node.open_cell(&destination, &Records).await?
    } else if destination.namespace() == FLOWS {
        node.open_cell(&destination, &Flows::<V>::new()?).await?
    } else {
        return Err(Error::PeerAuthorization("undeclared release destination").into());
    };
    let peer = LocalPeer::new(
        handle.compiled().registry(),
        destination.clone(),
        cell,
        Arc::new(Authorizer {
            source: source.clone(),
            destination,
        }),
    );
    let source = handle.effects::<M>(source)?;
    node.spawn_worker(move |cancel|async move {
        let supervisor=EffectSupervisor::new(source,peer.effect_client(PeerPrincipal {issuer:"release-pipeline".into(),subject:"release-record-delivery".into(),actions:vec!["cell.read".into(),"cell.write".into(),"cookbook.release.deliver".into()]}),15000)?;
        let mut delay=50;while !cancel.is_cancelled() {
            let result=supervisor.run_once().await?;delay=if matches!(result,EffectRunOutcome::Idle {..}) {(delay*2).min(500)}else {50};
            if matches!(result,EffectRunOutcome::Failed {..}) {return Err(ServiceError::Runtime(Error::Command("release signed delivery failed; native evidence retained")));}
            tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(delay))=>{}}
        }Ok::<_,ServiceError>(())
    })?;
    Ok(())
}
/// Installs only the two signed progress routes, useful when an embedding owns its Activity adapter separately.
pub async fn spawn_record_delivery<const V: u8>(
    node: &LocalNode,
    handle: ApplicationHandle<ReleaseApplication<V>>,
) -> Result<(), ServiceError> {
    let client = ReleaseClient::new(handle.clone());
    runner::<V, Flows<V>>(
        node,
        &handle,
        client.target(FLOWS)?,
        client.target(RECORDS)?,
    )
    .await?;
    runner::<V, Records>(
        node,
        &handle,
        client.target(RECORDS)?,
        client.target(FLOWS)?,
    )
    .await
}
/// Owns signed progress delivery and the native bounded build/target/rebuild Activity supervisor.
pub async fn spawn_release_workers<const V: u8>(
    node: &LocalNode,
    handle: ApplicationHandle<ReleaseApplication<V>>,
) -> Result<(), ServiceError> {
    spawn_record_delivery(node, handle.clone()).await?;
    let supervisor = ActivitySupervisor::new(handle.activities::<Flows<V>>()?, 30000)?;
    node.spawn_worker(move |cancel|async move {
        let mut delay=50;while !cancel.is_cancelled() {
            let result=supervisor.run_once(0,None).await?;delay=if matches!(result,ActivityRunOutcome::Idle {..}) {(delay*2).min(500)}else {50};
            if matches!(result,ActivityRunOutcome::IdentityConflict {..}) {return Err(ServiceError::Runtime(Error::Command("release Activity completion identity conflict")));}
            tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(delay))=>{}}
        }Ok::<_,ServiceError>(())
    })?;
    Ok(())
}
