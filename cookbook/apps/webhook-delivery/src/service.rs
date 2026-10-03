use crate::{
    DELIVERIES, Deliveries, DeliveryTicket, Feed, Receiver, StartDelivery, WebhookApplication,
    WebhookClient, model::decode_wire,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error, partition_for_shard,
    peer::{PeerAuthorizer, PeerPrincipal, VerifiedPeerRequest, wire},
    primitives::{
        effects::{EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
        workflow::{
            ActivityRunOutcome, ActivitySupervisor, ActivitySupervisorError, WorkflowOutcome,
        },
    },
    registry::Command,
};
use std::{sync::Arc, time::Duration};
/// Service installation and owned worker failures retain originating errors and uncertainty.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Provider, node, acquisition, or worker lifecycle failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, authorization, or typed contract failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native source Effect claim, peer receipt, or source settlement failure.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Native HTTP Activity lease or completion failure, including uncertain completion evidence.
    #[error(transparent)]
    Activity(#[from] ActivitySupervisorError),
}
struct SourceAuthorizer {
    feed: CellTarget,
    destinations: Vec<CellTarget>,
}
impl PeerAuthorizer for SourceAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if !request.permits("cookbook.webhook.fanout")
            || !self.destinations.contains(request.target())
        {
            return Err(Error::PeerAuthorization(
                "webhook source permission or destination differs",
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
            Some(wire::peer_request::Operation::ResolveEffect(_)) => Ok(()),
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing webhook source Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "webhook requires a committed source command Effect",
                    ));
                };
                if command.command_id != StartDelivery::ID
                    || command.codec_version != 1
                    || identity.source_cell != self.feed.cell_id().as_bytes()
                {
                    return Err(Error::PeerAuthorization(
                        "webhook source Effect operation differs",
                    ));
                }
                let ticket: DeliveryTicket = decode_wire(&command.input, 4096)?;
                let expected = CellTarget::new(
                    self.feed.tenant(),
                    self.feed.application(),
                    DELIVERIES,
                    &partition_for_shard(cellule_runtime::shard_for_scope(
                        DELIVERIES,
                        &ticket.key(),
                        2,
                    )?),
                )?;
                if ticket.source_cell != self.feed.cell_id().as_bytes()
                    || request.target() != &expected
                {
                    return Err(Error::PeerAuthorization(
                        "webhook frozen ticket scope differs",
                    ));
                }
                Ok(())
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported webhook peer operation",
            )),
        }
    }
}
/// Opens all four declared Cells. Caller drains the node if startup cannot finish.
pub async fn open(
    node: &LocalNode,
    handle: ApplicationHandle<WebhookApplication>,
) -> Result<WebhookClient, ServiceError> {
    let client = WebhookClient::new(handle)?;
    node.open_cell(client.feed_target(), &Feed).await?;
    node.open_cell(client.receiver_target(), &Receiver).await?;
    for shard in 0..2 {
        node.open_cell(
            &CellTarget::new(
                client.feed_target().tenant(),
                client.feed_target().application(),
                DELIVERIES,
                &partition_for_shard(shard),
            )?,
            &Deliveries,
        )
        .await?;
    }
    Ok(client)
}
/// Installs one source fan-out runner with authenticated native Effect inbox delivery.
/// HTTP ingress is installed separately by the embedding before admitting source work.
pub async fn spawn_fanout(
    node: &LocalNode,
    handle: ApplicationHandle<WebhookApplication>,
) -> Result<(), ServiceError> {
    let client = WebhookClient::new(handle.clone())?;
    let mut cells = Vec::with_capacity(2);
    let mut destinations = Vec::with_capacity(2);
    for shard in 0..2 {
        let target = CellTarget::new(
            client.feed_target().tenant(),
            client.feed_target().application(),
            DELIVERIES,
            &partition_for_shard(shard),
        )?;
        cells.push((target.clone(), node.open_cell(&target, &Deliveries).await?));
        destinations.push(target);
    }
    let peer = LocalPeer::for_destinations(
        handle.compiled().registry(),
        cells,
        Arc::new(SourceAuthorizer {
            feed: client.feed_target().clone(),
            destinations,
        }),
    )?;
    let source = handle.effects::<Feed>(client.feed_target().clone())?;
    let principal = PeerPrincipal {
        issuer: "webhook-delivery".into(),
        subject: "publisher-fanout".into(),
        actions: vec![
            "cell.read".into(),
            "cell.write".into(),
            "cookbook.webhook.fanout".into(),
        ],
    };
    let supervisor = EffectSupervisor::new(source, peer.effect_client(principal), 15_000)?;
    node.spawn_worker(move|cancel|async move{
        while !cancel.is_cancelled(){
            match supervisor.run_once().await?{
                EffectRunOutcome::Failed{..}=>return Err(ServiceError::Runtime(Error::Command("webhook source fan-out exhausted native attempts"))),
                EffectRunOutcome::Delivered{destination,..}=>{
                    let outcome:WorkflowOutcome=decode_wire(destination.result(),64)?;
                    if !matches!(outcome,WorkflowOutcome::Applied{..}|WorkflowOutcome::AlreadyExists){return Err(ServiceError::Runtime(Error::Command("webhook delivery start was durably rejected")));}
                }
                _=>{},
            }
            tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(50))=>{}}
        }
        Ok::<_,ServiceError>(())
    })?;
    Ok(())
}
/// Installs one owned HTTP Activity runner per Workflow shard.
/// Accepted HTTP calls finish native completion before the worker exits on graceful drain.
pub fn spawn_http_activities(
    node: &LocalNode,
    handle: ApplicationHandle<WebhookApplication>,
) -> Result<(), ServiceError> {
    for shard in 0..2 {
        let supervisor = ActivitySupervisor::new(handle.activities::<Deliveries>()?, 15_000)?;
        node.spawn_worker(move|cancel|async move{
            while !cancel.is_cancelled(){
                if matches!(supervisor.run_once(shard,None).await?,ActivityRunOutcome::IdentityConflict{..}){return Err(ServiceError::Runtime(Error::Command("webhook HTTP completion identity conflict")));}
                tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(50))=>{}}
            }
            Ok::<_,ServiceError>(())
        })?;
    }
    Ok(())
}
