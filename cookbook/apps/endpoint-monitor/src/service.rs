use crate::{
    ALERTS, Alerts, CHECKS, Check, Checks, MonitorApplication, MonitorClient, PROBES, Probes,
    RecordAlert, RecordCheck, RecordOutcome, SCHEDULES, Schedules, StartProbe, wire::decode_wire,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire,
    },
    primitives::{
        cron::CronInvocation,
        effects::{EffectModule, EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
        workflow::{ActivityRunOutcome, ActivitySupervisor, ActivitySupervisorError},
    },
    registry::Command,
};
use prost::Message as _;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
// Empty native claims still publish outcomes. Back off empty queues to leave
// maintenance room to advance Cron and reclaim leases in the shared probe Cell.
const POLL_MS: u64 = 200;
const MAX_IDLE_POLL_MS: u64 = 2000;
fn poll_delay(previous: u64, idle: bool) -> u64 {
    if idle {
        (previous * 2).min(MAX_IDLE_POLL_MS)
    } else {
        POLL_MS
    }
}
/// Assembly and owned worker failures preserve native source errors and pending evidence.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Infrastructure or lifecycle failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, signed authorization, or domain invariant failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native effect lease, publication, or unresolved settlement.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Native Activity execution or completion failure.
    #[error(transparent)]
    Activity(#[from] ActivitySupervisorError),
}
/// Receipt-bearing checkpoint after durable SQL projection, before its source Effect ack.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Progress {
    /// Stable native effect identity, preserved across redelivery.
    pub effect_id: String,
    /// Workflow source publication sequence, scoped to its Cell.
    pub source_commit_sequence: u64,
    /// Exact immutable observation projected to SQL.
    pub check: Check,
    /// Original SQL row and incident edge, reused by duplicate delivery.
    pub outcome: RecordOutcome,
    /// SQL destination publication sequence, scoped to its Cell.
    pub destination_commit_sequence: u64,
}
/// Bounded checkpoint and actual reply-loss injection; omitted in ordinary operation.
#[derive(Clone, Default)]
pub struct DeliveryOptions {
    /// Delay the first successful projection reply by at most ten seconds.
    pub after_publication: Duration,
    /// Drop that first reply after SQL publication; native resolution reuses it.
    pub drop_reply_once: bool,
    /// Nonblocking bounded observer; slow readers never delay publication.
    pub progress: Option<mpsc::Sender<Progress>>,
}
/// Opens all four fixed writer domains before application ingress.
/// The embedding must drain the node if startup cannot finish.
pub async fn open(
    node: &LocalNode,
    handle: &ApplicationHandle<MonitorApplication>,
) -> Result<MonitorClient, ServiceError> {
    let client = MonitorClient::new(handle.clone());
    node.open_cell(&client.target(SCHEDULES)?, &Schedules)
        .await?;
    node.open_cell(&client.target(PROBES)?, &Probes).await?;
    node.open_cell(&client.target(CHECKS)?, &Checks).await?;
    node.open_cell(&client.target(ALERTS)?, &Alerts).await?;
    Ok(client)
}
struct Authorizer {
    source: CellTarget,
    destination: CellTarget,
    command: u32,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.target() != &self.destination || !request.permits("cookbook.monitor.deliver") {
            return Err(Error::PeerAuthorization(
                "monitor destination or permission differs",
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
                if resolve.identity.as_ref().is_some_and(|identity| {
                    identity.source_cell == self.source.cell_id().as_bytes()
                }) =>
            {
                Ok(())
            }
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing monitor Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "monitor delivery requires a typed command",
                    ));
                };
                if identity.source_cell != self.source.cell_id().as_bytes()
                    || command.command_id != self.command
                    || command.codec_version != 1
                {
                    return Err(Error::PeerAuthorization(
                        "monitor source or command differs",
                    ));
                }
                if self.destination.namespace() == PROBES {
                    let value: CronInvocation = decode_wire(&command.input, 2048)?;
                    crate::schedules::definition(&value.payload)?;
                    if value.occurrence == 0 || value.generation == 0 {
                        return Err(Error::PeerAuthorization("invalid monitor occurrence"));
                    }
                } else if self.destination.namespace() == CHECKS {
                    let value: Check = decode_wire(&command.input, 4096)?;
                    value.validate()?;
                    let cron = CellTarget::new(
                        self.source.tenant(),
                        self.source.application(),
                        SCHEDULES,
                        &cellule_runtime::partition_for_shard(0),
                    )?;
                    if value.ticket.source_cell != *cron.cell_id().as_bytes() {
                        return Err(Error::PeerAuthorization("foreign frozen probe origin"));
                    }
                } else {
                    let edge: crate::Edge = decode_wire(&command.input, 2048)?;
                    edge.validate()?;
                }
                Ok(())
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported monitor peer operation",
            )),
        }
    }
}
#[derive(Clone)]
struct ObservedTransport {
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
}
impl PeerRoundTrip for ObservedTransport {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let transport = self.clone();
        Box::pin(async move {
            let request = transport.peer.verify_request(&bytes)?;
            let observed = match request.operation() {
                Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(Error::Peer("missing projection identity"))?;
                    let Some(wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(Error::Peer("missing projection command"));
                    };
                    let check: Check = decode_wire(&command.input, 4096)?;
                    let key: [u8; 32] = identity
                        .effect_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::Peer("invalid projection effect key"))?;
                    Some((
                        check,
                        blake3::Hash::from_bytes(key).to_hex().to_string(),
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            let reply = transport.peer.send(target, bytes, remaining_ms).await?;
            if let Some((check, effect_id, source_commit_sequence)) = observed {
                let decoded = wire::PeerReply::decode(reply.as_slice())?;
                if let Some(wire::peer_reply::Outcome::Mutation(mutation)) = decoded.outcome
                    && let Some(wire::mutation_reply::Outcome::Result(result)) = mutation.outcome
                    && let Some(wire::mutation_result::Result::CommandOutput(bytes)) = result.result
                {
                    let outcome: RecordOutcome = decode_wire(&bytes, 4096)?;
                    if matches!(outcome, RecordOutcome::Recorded { .. }) {
                        let receipt = mutation
                            .receipt
                            .ok_or(Error::Peer("projection receipt absent"))?;
                        if let Some(progress) = &transport.options.progress {
                            let _ = progress.try_send(Progress {
                                effect_id,
                                source_commit_sequence,
                                check,
                                outcome,
                                destination_commit_sequence: receipt.commit_sequence,
                            });
                        }
                        if transport
                            .used
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            // Preserve the exact receipt through graceful drain. Only the explicit
                            // fault drops a reply that already names a published SQL observation.
                            tokio::select! {()=transport.cancel.cancelled()=>{},()=tokio::time::sleep(transport.options.after_publication)=>{}}
                            if transport.options.drop_reply_once {
                                return Err(Error::PeerTransportUnknown {
                                    context: "injected lost monitor projection reply",
                                    source: Box::new(std::io::Error::new(
                                        std::io::ErrorKind::ConnectionReset,
                                        "SQL check was published before reply loss",
                                    )),
                                });
                            }
                        }
                    }
                }
            }
            Ok(reply)
        })
    }
}
async fn runner<M: EffectModule>(
    node: &LocalNode,
    handle: &ApplicationHandle<MonitorApplication>,
    source: CellTarget,
    destination: CellTarget,
    command: u32,
    options: Option<DeliveryOptions>,
) -> Result<(), ServiceError> {
    let destination_handle = if destination.namespace() == PROBES {
        node.open_cell(&destination, &Probes).await?
    } else if destination.namespace() == CHECKS {
        node.open_cell(&destination, &Checks).await?
    } else {
        node.open_cell(&destination, &Alerts).await?
    };
    let peer = LocalPeer::new(
        handle.compiled().registry(),
        destination.clone(),
        destination_handle,
        Arc::new(Authorizer {
            source: source.clone(),
            destination,
            command,
        }),
    );
    let source = handle.effects::<M>(source)?;
    node.spawn_worker(move |cancel| async move {
        let principal = PeerPrincipal {
            issuer: "endpoint-monitor".into(),
            subject: "local-monitor-delivery".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "cookbook.monitor.deliver".into(),
            ],
        };
        let transport: Arc<dyn PeerRoundTrip> = match options {
            Some(options) => Arc::new(ObservedTransport {
                peer: peer.clone(),
                options,
                used: Arc::new(AtomicBool::new(false)),
                cancel: cancel.clone(),
            }),
            None => Arc::new(peer.clone()),
        };
        let supervisor = EffectSupervisor::new(
            source,
            EffectPeerClient::new(peer.signer(), principal, transport),
            15_000,
        )?;
        let mut delay = POLL_MS;
        while !cancel.is_cancelled() {
            let outcome = supervisor.run_once().await?;
            delay = poll_delay(delay, matches!(outcome, EffectRunOutcome::Idle { .. }));
            if matches!(outcome, EffectRunOutcome::Failed { .. }) {
                return Err(ServiceError::Runtime(Error::Command(
                    "monitor delivery failed; source evidence retained",
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
/// Installs three signed Effect runners and one native HTTP Activity supervisor.
/// Accepted probe, delivery, and settlement cycles finish before graceful worker drain.
pub async fn spawn_workers(
    node: &LocalNode,
    handle: ApplicationHandle<MonitorApplication>,
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if options.after_publication > Duration::from_secs(10) {
        return Err(Error::Command("monitor publication delay exceeds ten seconds").into());
    }
    let client = MonitorClient::new(handle.clone());
    runner::<Schedules>(
        node,
        &handle,
        client.target(SCHEDULES)?,
        client.target(PROBES)?,
        StartProbe::ID,
        None,
    )
    .await?;
    runner::<Probes>(
        node,
        &handle,
        client.target(PROBES)?,
        client.target(CHECKS)?,
        RecordCheck::ID,
        Some(options),
    )
    .await?;
    runner::<Checks>(
        node,
        &handle,
        client.target(CHECKS)?,
        client.target(ALERTS)?,
        RecordAlert::ID,
        None,
    )
    .await?;
    let supervisor = ActivitySupervisor::new(handle.activities::<Probes>()?, 15_000)?;
    node.spawn_worker(move |cancel| async move {
        let mut delay = POLL_MS;
        while !cancel.is_cancelled() {
            let outcome = supervisor.run_once(0, None).await?;
            delay = poll_delay(delay, matches!(outcome, ActivityRunOutcome::Idle { .. }));
            if matches!(outcome, ActivityRunOutcome::IdentityConflict { .. }) {
                return Err(ServiceError::Runtime(Error::Command(
                    "native probe completion identity conflict",
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
