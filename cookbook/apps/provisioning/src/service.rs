use crate::{
    Advance, Advanced, Call, DIRECTORY, Directory, FLOWS, Flows, ProvisioningApplication,
    ProvisioningClient, Reply, Spec, wire::decode_wire,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer, new_identity, now_ms};
use cellule_runtime::{
    CellTarget, Error, InvocationError,
    peer::{PeerAuthorizer, PeerPrincipal, VerifiedPeerRequest, wire},
    primitives::{
        effects::{EffectModule, EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
        workflow::{ActivityRunOutcome, ActivitySupervisor, ActivitySupervisorError},
    },
};
use std::{sync::Arc, time::Duration};
/// Owned node, native publication, and background settlement errors.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Provider, ownership, and node lifecycle failures.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, authorization, and domain failures.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native Effect delivery and settlement failures retaining pending evidence.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Native Activity execution and completion failures.
    #[error(transparent)]
    Activity(#[from] ActivitySupervisorError),
    /// Provider lifecycle mutation error, including its original native evidence.
    #[error(transparent)]
    ProviderAdvance(#[from] InvocationError<Advanced>),
    /// Read-only scheduling failure retaining its query source error.
    #[error("provider scheduling query failed")]
    ProviderDue(#[source] crate::BoxError),
}
/// Opens directory and Workflow writers; the simulator opens Provider independently.
pub async fn open(
    node: &LocalNode,
    handle: &ApplicationHandle<ProvisioningApplication>,
) -> Result<ProvisioningClient, ServiceError> {
    let client = ProvisioningClient::new(handle.clone());
    node.open_cell(&client.target(DIRECTORY)?, &Directory)
        .await?;
    node.open_cell(&client.target(FLOWS)?, &Flows).await?;
    Ok(client)
}
struct Authorizer {
    source: CellTarget,
    destination: CellTarget,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.target() != &self.destination
            || !request.permits("cookbook.provisioning.deliver")
        {
            return Err(Error::PeerAuthorization(
                "provisioning destination or permission differs",
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
                let id = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing provisioning Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "provisioning delivery requires a typed command",
                    ));
                };
                if id.source_cell != self.source.cell_id().as_bytes() || command.codec_version != 1
                {
                    return Err(Error::PeerAuthorization(
                        "provisioning source or codec differs",
                    ));
                }
                match (
                    self.source.namespace(),
                    self.destination.namespace(),
                    command.command_id,
                ) {
                    (DIRECTORY, FLOWS, 11 | 12) => {
                        decode_wire::<Spec>(&command.input, 2048)?.validate()
                    }
                    (DIRECTORY, FLOWS, 13) => {
                        decode_wire::<Reply>(&command.input, 4096)?.validate()
                    }
                    (FLOWS, DIRECTORY, 8) => decode_wire::<Call>(&command.input, 4096)?.validate(),
                    _ => Err(Error::PeerAuthorization(
                        "unsupported provisioning signed route",
                    )),
                }
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported provisioning peer operation",
            )),
        }
    }
}
async fn runner<M: EffectModule>(
    node: &LocalNode,
    handle: &ApplicationHandle<ProvisioningApplication>,
    source: CellTarget,
    destination: CellTarget,
) -> Result<(), ServiceError> {
    let cell = match destination.namespace() {
        DIRECTORY => node.open_cell(&destination, &Directory).await?,
        FLOWS => node.open_cell(&destination, &Flows).await?,
        _ => return Err(Error::PeerAuthorization("undeclared provisioning destination").into()),
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
    node.spawn_worker(move |cancel| async move {
        let supervisor = EffectSupervisor::new(
            source,
            peer.effect_client(PeerPrincipal {
                issuer: "provisioning".into(),
                subject: "local-resource-delivery".into(),
                actions: vec![
                    "cell.read".into(),
                    "cell.write".into(),
                    "cookbook.provisioning.deliver".into(),
                ],
            }),
            15000,
        )?;
        let mut delay = 200;
        while !cancel.is_cancelled() {
            let outcome = supervisor.run_once().await?;
            delay = if matches!(outcome, EffectRunOutcome::Idle { .. }) {
                (delay * 2).min(2000)
            } else {
                200
            };
            if matches!(outcome, EffectRunOutcome::Failed { .. }) {
                return Err(ServiceError::Runtime(Error::Command(
                    "resource delivery failed; native source evidence retained",
                )));
            }
            tokio::select! {
                () = cancel.cancelled() => {},
                () = tokio::time::sleep(Duration::from_millis(delay)) => {},
            }
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
/// Owns two signed Effect runners and one native provider Activity supervisor.
pub async fn spawn_workers(
    node: &LocalNode,
    handle: ApplicationHandle<ProvisioningApplication>,
) -> Result<(), ServiceError> {
    let client = ProvisioningClient::new(handle.clone());
    runner::<Directory>(
        node,
        &handle,
        client.target(DIRECTORY)?,
        client.target(FLOWS)?,
    )
    .await?;
    runner::<Flows>(
        node,
        &handle,
        client.target(FLOWS)?,
        client.target(DIRECTORY)?,
    )
    .await?;
    let supervisor = ActivitySupervisor::new(handle.activities::<Flows>()?, 30000)?;
    node.spawn_worker(move |cancel| async move {
        let mut delay = 200;
        while !cancel.is_cancelled() {
            let outcome = supervisor.run_once(0, None).await?;
            delay = if matches!(outcome, ActivityRunOutcome::Idle { .. }) {
                (delay * 2).min(2000)
            } else {
                200
            };
            if matches!(outcome, ActivityRunOutcome::IdentityConflict { .. }) {
                return Err(ServiceError::Runtime(Error::Command(
                    "provider Activity completion identity conflict",
                )));
            }
            tokio::select! {
                () = cancel.cancelled() => {},
                () = tokio::time::sleep(Duration::from_millis(delay)) => {},
            }
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
/// Independently progresses due virtual provider allocations and cleanup in bounded transactions.
/// HTTP GET does not advance lifecycles; restart resumes due work from durable deadlines.
pub fn spawn_provider_lifecycle(
    node: &LocalNode,
    client: ProvisioningClient,
) -> Result<(), ServiceError> {
    node.spawn_worker(move |cancel| async move {
        let mut delay = 200;
        while !cancel.is_cancelled() {
            let due = client
                .next_provider_due(None)
                .await
                .map_err(ServiceError::ProviderDue)?
                .output
                .at_ms;
            let now = now_ms()?;
            // A concurrent create/delete may invalidate this hint. Advancement
            // rechecks durable deadlines and native time in its own transaction.
            let advanced = if due.is_some_and(|due| due <= now) {
                client
                    .prepare_advance(new_identity()?, Advance { limit: 16 })
                    .await?
                    .execute()
                    .await?
                    .output
                    .resources
            } else {
                0
            };
            delay = if advanced == 0 {
                (delay * 2).min(2000)
            } else {
                200
            };
            tokio::select! {
                () = cancel.cancelled() => {},
                () = tokio::time::sleep(Duration::from_millis(delay)) => {},
            }
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
