use crate::{
    Call, CheckoutApplication, CheckoutClient, INVENTORY, Inventory, ORDERS, Operation, OrderSpec,
    Orders, Reply, SAGAS, Sagas, wire::decode_wire,
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
/// Owned startup, native settlement, and worker source errors.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Provider, ownership, and node drain errors.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, authorization, and domain errors.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native Effect settlement errors retaining original pending evidence.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Native Activity execution and completion errors.
    #[error(transparent)]
    Activity(#[from] ActivitySupervisorError),
}
/// Opens the three checkout writer domains. The simulator separately opens Payments.
pub async fn open(
    node: &LocalNode,
    handle: &ApplicationHandle<CheckoutApplication>,
) -> Result<CheckoutClient, ServiceError> {
    let client = CheckoutClient::new(handle.clone());
    node.open_cell(&client.target(ORDERS)?, &Orders).await?;
    node.open_cell(&client.target(INVENTORY)?, &Inventory)
        .await?;
    node.open_cell(&client.target(SAGAS)?, &Sagas).await?;
    Ok(client)
}
struct Authorizer {
    source: CellTarget,
    destinations: Vec<CellTarget>,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if !self.destinations.contains(request.target())
            || !request.permits("cookbook.checkout.deliver")
        {
            return Err(Error::PeerAuthorization(
                "checkout destination or permission differs",
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
            Some(wire::peer_request::Operation::ResolveEffect(resolve))
                if resolve
                    .identity
                    .as_ref()
                    .is_some_and(|id| id.source_cell == self.source.cell_id().as_bytes()) =>
            {
                Ok(())
            }
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing checkout Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "checkout delivery requires a typed command",
                    ));
                };
                if identity.source_cell != self.source.cell_id().as_bytes()
                    || command.codec_version != 1
                {
                    return Err(Error::PeerAuthorization("checkout source or codec differs"));
                }
                match (
                    self.source.namespace(),
                    request.target().namespace(),
                    command.command_id,
                ) {
                    (ORDERS, SAGAS, 11) => {
                        decode_wire::<OrderSpec>(&command.input, 2048)?.validate()
                    }
                    (ORDERS | INVENTORY, SAGAS, 12) => {
                        let reply: Reply = decode_wire(&command.input, 4096)?;
                        reply.validate()?;
                        let stock = matches!(
                            reply.call.operation,
                            Operation::Reserve | Operation::Commit | Operation::Release
                        );
                        if stock != (self.source.namespace() == INVENTORY) {
                            return Err(Error::PeerAuthorization(
                                "checkout callback receiver differs",
                            ));
                        }
                        Ok(())
                    }
                    (SAGAS, ORDERS | INVENTORY, 8) => {
                        let call: Call = decode_wire(&command.input, 4096)?;
                        call.validate()?;
                        let stock = matches!(
                            call.operation,
                            Operation::Reserve | Operation::Commit | Operation::Release
                        );
                        if stock != (request.target().namespace() == INVENTORY) {
                            return Err(Error::PeerAuthorization("checkout call receiver differs"));
                        }
                        Ok(())
                    }
                    _ => Err(Error::PeerAuthorization(
                        "unsupported checkout signed route",
                    )),
                }
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported checkout peer operation",
            )),
        }
    }
}
async fn runner<M: EffectModule>(
    node: &LocalNode,
    handle: &ApplicationHandle<CheckoutApplication>,
    source: CellTarget,
    destinations: Vec<CellTarget>,
) -> Result<(), ServiceError> {
    let mut cells = Vec::new();
    for target in &destinations {
        let cell = match target.namespace() {
            ORDERS => node.open_cell(target, &Orders).await?,
            INVENTORY => node.open_cell(target, &Inventory).await?,
            SAGAS => node.open_cell(target, &Sagas).await?,
            _ => return Err(Error::PeerAuthorization("undeclared checkout destination").into()),
        };
        cells.push((target.clone(), cell));
    }
    let peer = LocalPeer::for_destinations(
        handle.compiled().registry(),
        cells,
        Arc::new(Authorizer {
            source: source.clone(),
            destinations,
        }),
    )?;
    let source = handle.effects::<M>(source)?;
    node.spawn_worker(move|cancel|async move{
        let supervisor=EffectSupervisor::new(source,peer.effect_client(PeerPrincipal{issuer:"checkout".into(),subject:"local-checkout-delivery".into(),actions:vec!["cell.read".into(),"cell.write".into(),"cookbook.checkout.deliver".into()]}),15_000)?;
        let mut delay=200;
        while !cancel.is_cancelled(){
            let outcome=supervisor.run_once().await?;
            delay=if matches!(outcome,EffectRunOutcome::Idle{..}){(delay*2).min(2000)}else{200};
            if matches!(outcome,EffectRunOutcome::Failed{..}){return Err(ServiceError::Runtime(Error::Command("checkout delivery failed; native source evidence retained")));}
            tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(delay))=>{}}
        }Ok::<_,ServiceError>(())
    })?;
    Ok(())
}
/// Owns three signed Effect runners and one native payment Activity supervisor.
/// Accepted cycles settle before cancellation is observed and node drain finishes.
pub async fn spawn_workers(
    node: &LocalNode,
    handle: ApplicationHandle<CheckoutApplication>,
) -> Result<(), ServiceError> {
    let client = CheckoutClient::new(handle.clone());
    runner::<Orders>(
        node,
        &handle,
        client.target(ORDERS)?,
        vec![client.target(SAGAS)?],
    )
    .await?;
    runner::<Inventory>(
        node,
        &handle,
        client.target(INVENTORY)?,
        vec![client.target(SAGAS)?],
    )
    .await?;
    runner::<Sagas>(
        node,
        &handle,
        client.target(SAGAS)?,
        vec![client.target(ORDERS)?, client.target(INVENTORY)?],
    )
    .await?;
    let supervisor = ActivitySupervisor::new(handle.activities::<Sagas>()?, 30_000)?;
    node.spawn_worker(move|cancel|async move{
        let mut delay=200;
        while !cancel.is_cancelled(){
            let outcome=supervisor.run_once(0,None).await?;
            delay=if matches!(outcome,ActivityRunOutcome::Idle{..}){(delay*2).min(2000)}else{200};
            if matches!(outcome,ActivityRunOutcome::IdentityConflict{..}){return Err(ServiceError::Runtime(Error::Command("checkout native Activity completion conflict")));}
            tokio::select!{()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(delay))=>{}}
        }Ok::<_,ServiceError>(())
    })?;
    Ok(())
}
