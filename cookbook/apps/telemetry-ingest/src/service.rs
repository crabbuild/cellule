use crate::{
    DeviceClient, DeviceKey, DeviceSnapshot, Devices, ProgressError, ProjectionOutcome,
    ProjectionState, Summaries, SummaryClient, TelemetryIngest, wire as codec,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire,
    },
    primitives::effects::{EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
};
use prost::Message as _;
use serde::Serialize;
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

/// Assembly and native delivery errors retain their original source and evidence.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node activation or owned worker assembly failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Registry, peer authorization, or wire contract failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native source claim/delivery/settlement failure.
    #[error(transparent)]
    Delivery(#[from] EffectSupervisorError),
    /// Coherent source progress query failure.
    #[error(transparent)]
    Progress(#[from] ProgressError),
}
/// Observation after real signed summary publication, before source acknowledgment.
#[derive(Clone, Debug, Serialize)]
pub struct DeliveryProgress {
    /// Complete delivered summary and source revision.
    pub summary: DeviceSnapshot,
    /// Stable native Effect identity.
    pub effect_id: String,
    /// Commit sequence in the source device Cell.
    pub source_commit_sequence: u64,
    /// Commit sequence in the independent summary Cell.
    pub summary_commit_sequence: u64,
    /// Receiver decision, including harmless stale delivery.
    pub outcome: ProjectionOutcome,
}
/// Exercised local interruption controls and bounded nonblocking observations.
#[derive(Clone, Default)]
pub struct DeliveryOptions {
    /// Pins controls to one existing scoped intent; otherwise selects the first transport delivery.
    pub controlled_effect: Option<[u8; 32]>,
    /// Delays the selected delivery by at most ten seconds, allowing real reordering.
    pub before_delivery: Duration,
    /// Delays that delivery's first reply after native publication, by at most ten seconds.
    pub after_publication: Duration,
    /// Drops that reply after durable publication; recovery resolves the same native inbox identity.
    pub drop_reply_once: bool,
    /// Optional bounded diagnostics; dropping observations never changes durability.
    pub progress: Option<mpsc::Sender<DeliveryProgress>>,
}
struct Authorizer {
    source: CellTarget,
    destination: CellTarget,
    key: DeviceKey,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.target() != &self.destination || !request.permits("cookbook.telemetry.device") {
            return Err(Error::PeerAuthorization(
                "telemetry summary capability differs",
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
                    .is_some_and(|v| v.source_cell == self.source.cell_id().as_bytes()) =>
            {
                Ok(())
            }
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("telemetry delivery has no Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "telemetry receiver accepts only summary commands",
                    ));
                };
                if identity.source_cell != self.source.cell_id().as_bytes()
                    || command.command_id != 1
                    || command.codec_version != 1
                {
                    return Err(Error::PeerAuthorization(
                        "telemetry source or receiver command differs",
                    ));
                }
                let summary: DeviceSnapshot = codec::decode(&command.input, 4096)?;
                summary.validate()?;
                if summary.device != self.key {
                    return Err(Error::PeerAuthorization(
                        "telemetry summary targets a different aggregate",
                    ));
                }
                Ok(())
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported telemetry peer operation",
            )),
        }
    }
}
struct Transport {
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
}
impl PeerRoundTrip for Transport {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peer = self.peer.clone();
        let options = self.options.clone();
        let used = self.used.clone();
        let cancel = self.cancel.clone();
        Box::pin(async move {
            let request = peer.verify_request(&bytes)?;
            let invocation = match request.operation() {
                Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                    let Some(wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(Error::Peer("unsupported telemetry delivery command"));
                    };
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(Error::Peer("telemetry observation has no identity"))?;
                    let effect_id: [u8; 32] =
                        identity.effect_id.as_slice().try_into().map_err(|_| {
                            Error::Peer("telemetry observation identity length differs")
                        })?;
                    Some((
                        codec::decode::<DeviceSnapshot>(&command.input, 4096)?,
                        effect_id,
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            let selected = invocation.as_ref().is_some_and(|(_, effect_id, _)| {
                options
                    .controlled_effect
                    .is_none_or(|expected| expected == *effect_id)
            });
            let first = selected && !used.swap(true, Ordering::SeqCst);
            if first {
                tokio::select! {()=tokio::time::sleep(options.before_delivery)=>{},()=cancel.cancelled()=>{}}
            }
            let reply = peer.send(target, bytes, limit).await?;
            if let Some((summary, effect_id, source_commit_sequence)) = invocation {
                let response = wire::PeerReply::decode(reply.as_slice())?;
                if let Some(wire::peer_reply::Outcome::Mutation(mutation)) = response.outcome
                    && let Some(wire::mutation_reply::Outcome::Result(result)) = mutation.outcome
                    && let Some(wire::mutation_result::Result::CommandOutput(bytes)) = result.result
                {
                    let receipt = mutation
                        .receipt
                        .ok_or(Error::Peer("telemetry summary reply has no receipt"))?;
                    let outcome: ProjectionOutcome = codec::decode(&bytes, 16)?;
                    if let Some(sender) = &options.progress {
                        let _ = sender.try_send(DeliveryProgress {
                            summary,
                            effect_id: blake3::Hash::from_bytes(effect_id).to_hex().to_string(),
                            source_commit_sequence,
                            summary_commit_sequence: receipt.commit_sequence,
                            outcome,
                        });
                    }
                    if first {
                        tokio::select! {()=tokio::time::sleep(options.after_publication)=>{},()=cancel.cancelled()=>{}}
                        if options.drop_reply_once {
                            return Err(Error::PeerTransportUnknown {
                                context: "telemetry reply lost after summary publication",
                                source: Box::new(std::io::Error::new(
                                    std::io::ErrorKind::ConnectionReset,
                                    "injected lost summary reply",
                                )),
                            });
                        }
                    }
                }
            }
            Ok(reply)
        })
    }
}
/// Starts two owned native delivery workers per explicit device, permitting genuine out-of-order delivery.
///
/// The local profile admits 1..2 source devices alongside one Queue and audit Cell. A summary page is never used as a complete source roster. Source and
/// summary owners may be different nodes, but tenant, application, and compiled
/// descriptors must match. On partial startup failure the embedding drains owners.
pub async fn spawn_delivery(
    source_node: &LocalNode,
    summary_node: &LocalNode,
    source_handle: ApplicationHandle<TelemetryIngest>,
    summary_handle: ApplicationHandle<TelemetryIngest>,
    keys: &[DeviceKey],
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if keys.is_empty()
        || keys.len() > 2
        || keys
            .iter()
            .enumerate()
            .any(|(index, key)| keys[..index].contains(key))
        || options.before_delivery > Duration::from_secs(10)
        || options.after_publication > Duration::from_secs(10)
    {
        return Err(Error::Command(
            "telemetry delivery requires 1..2 distinct devices and delays at most ten seconds",
        )
        .into());
    }
    let expected = crate::AuditClient::new(source_handle.clone())?;
    let actual = crate::AuditClient::new(summary_handle.clone())?;
    if expected.target() != actual.target()
        || source_handle.compiled().descriptor_digest()
            != summary_handle.compiled().descriptor_digest()
    {
        return Err(Error::Identity("telemetry delivery scopes or descriptors differ").into());
    }
    let mut sources = Vec::with_capacity(keys.len());
    let mut controlled_matches = 0;
    for key in keys {
        let summary = SummaryClient::for_device(summary_handle.clone(), key)?;
        let inbox = summary_node.open_cell(summary.target(), &Summaries).await?;
        let client = DeviceClient::new(source_handle.clone(), key.clone())?;
        source_node.open_cell(client.target(), &Devices).await?;
        if let Some(effect_id) = options.controlled_effect {
            controlled_matches += usize::from(
                client
                    .effect_status(effect_id, None)
                    .await
                    .map_err(ProgressError::from)?
                    .output
                    .is_some(),
            );
        }
        if client
            .progress(None)
            .await?
            .output
            .is_some_and(|v| v.state == ProjectionState::Failed)
        {
            return Err(Error::Command(
                "telemetry device has failed projection evidence; investigate before serving",
            )
            .into());
        }
        let peer = LocalPeer::new(
            source_handle.compiled().registry(),
            summary.target().clone(),
            inbox.clone(),
            Arc::new(Authorizer {
                source: client.target().clone(),
                destination: summary.target().clone(),
                key: key.clone(),
            }),
        );
        sources.push((client, peer));
    }
    if options.controlled_effect.is_some() && controlled_matches != 1 {
        return Err(Error::Identity(
            "controlled telemetry intent must belong to one declared device",
        )
        .into());
    }
    let used = Arc::new(AtomicBool::new(false));
    for (client, peer) in sources {
        for _ in 0..2 {
            let source = source_handle.effects::<Devices>(client.target().clone())?;
            let client = client.clone();
            let peer = peer.clone();
            let options = options.clone();
            let used = used.clone();
            source_node.spawn_worker(move |cancel| async move {
                let principal = PeerPrincipal {
                    issuer: "telemetry-ingest".into(),
                    subject: "tenant-summary-projector".into(),
                    actions: vec![
                        "cell.read".into(),
                        "cell.write".into(),
                        "cookbook.telemetry.device".into(),
                    ],
                };
                let transport = Transport {
                    peer: peer.clone(),
                    options,
                    used,
                    cancel: cancel.clone(),
                };
                // Both supported delays can consume twenty seconds. Leave ten
                // more for native publication, resolution, and acknowledgment.
                let supervisor = EffectSupervisor::new(
                    source,
                    EffectPeerClient::new(peer.signer(), principal, Arc::new(transport)),
                    30000,
                )?;
                while !cancel.is_cancelled() {
                    match supervisor.run_once().await? {
                        EffectRunOutcome::Failed { .. } => {
                            return Err(ServiceError::Runtime(Error::Command(
                                "telemetry summary delivery exhausted its attempts",
                            )));
                        }
                        EffectRunOutcome::Delivered { destination, .. } => {
                            if matches!(
                                codec::decode::<ProjectionOutcome>(destination.result(), 16)?,
                                ProjectionOutcome::Conflict | ProjectionOutcome::Capacity
                            ) {
                                return Err(ServiceError::Runtime(Error::Command(
                                    "telemetry summary rejected summary; native answer retained",
                                )));
                            }
                        }
                        EffectRunOutcome::Idle { .. } => {
                            let progress = match client.progress(None).await {
                                Ok(value) => value.output,
                                Err(ProgressError::Changed) => None,
                                Err(source) => return Err(source.into()),
                            };
                            if progress.is_some_and(|v| v.state == ProjectionState::Failed) {
                                return Err(ServiceError::Runtime(Error::Command(
                                    "telemetry latest summary intent failed or expired",
                                )));
                            }
                        }
                        _ => {}
                    }
                    tokio::select! {
                        () = cancel.cancelled() => {},
                        () = tokio::time::sleep(Duration::from_millis(50)) => {},
                    }
                }
                Ok::<_, ServiceError>(())
            })?;
        }
    }
    Ok(())
}
